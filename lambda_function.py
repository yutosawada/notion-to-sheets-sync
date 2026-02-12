import os
import json
import time
import traceback
from datetime import datetime, timezone, timedelta

import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build


NOTION_API_BASE = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"

# 取りこぼし対策（同一秒に編集が重なる/ネットワーク遅延）
# 「前回同期時刻 - N秒」以降を取り直す（Upsertなので問題なし）
SYNC_LOOKBACK_SECONDS = 5

# config シートに最終同期時刻を保存するセル
CONFIG_LAST_SYNC_CELL = "config!B2"


SENSITIVE_KEYS = {"authorization", "auth", "token", "password", "secret", "api_key", "notion_api_token", "google_service_account_json"}

def _redact(value):
    try:
        if isinstance(value, dict):
            return {k: ("***" if str(k).lower() in SENSITIVE_KEYS else _redact(v)) for k, v in value.items()}
        if isinstance(value, list):
            return [_redact(v) for v in value]
        if isinstance(value, str) and any(k in value.lower() for k in SENSITIVE_KEYS):
            return "***"
        return value
    except Exception:
        return "***"

def log(level: str, event: str, **fields):
    safe_fields = {k: _redact(v) for k, v in fields.items()}
    record = {"level": level, "event": event, "ts_epoch": time.time(), **safe_fields}
    print(json.dumps(record, ensure_ascii=False))


def _env(name: str, required: bool = True, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if required and not v:
        raise ValueError(f"Missing env var: {name}")
    return v


def extract_text_property(properties: dict, prop_name: str) -> str:
    p = properties.get(prop_name)
    if not p:
        return ""

    p_type = p.get("type")

    if p_type == "title":
        parts = p.get("title", [])
        return "".join([x.get("plain_text", "") for x in parts]).strip()

    if p_type == "rich_text":
        parts = p.get("rich_text", [])
        return "".join([x.get("plain_text", "") for x in parts]).strip()

    if p_type == "select":
        sel = p.get("select")
        return (sel.get("name") if sel else "") or ""

    if p_type == "multi_select":
        arr = p.get("multi_select", [])
        return ", ".join([x.get("name", "") for x in arr if x.get("name")])

    if p_type == "status":
        st = p.get("status")
        return (st.get("name") if st else "") or ""

    if p_type == "date":
        d = p.get("date")
        if not d:
            return ""
        start = d.get("start") or ""
        end = d.get("end") or ""
        return f"{start} -> {end}" if end else start

    return ""


def normalize_date_value(value: str) -> str:
    if not value:
        return ""
    start = value.split("->", 1)[0].strip()
    if not start:
        return ""
    if start.endswith("Z"):
        start = start[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(start)
        return f"{dt.month}/{dt.day}/{dt.year}"
    except ValueError:
        try:
            d = datetime.strptime(start[:10], "%Y-%m-%d").date()
            return f"{d.month}/{d.day}/{d.year}"
        except ValueError:
            return start


def col_to_a1(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def a1_quote_sheet_name(sheet_name: str) -> str:
    # スペースや記号がある場合に備えてクオート
    if any(c in sheet_name for c in [" ", "!", "'", '"']):
        return "'" + sheet_name.replace("'", "''") + "'"
    return sheet_name


def build_sheets_client(service_account_info: dict):
    creds = service_account.Credentials.from_service_account_info(
        service_account_info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def read_last_sync_time(sheets, spreadsheet_id: str) -> str:
    """
    config!B2 を読む。空なら "" を返す。
    """
    try:
        resp = (
            sheets.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=CONFIG_LAST_SYNC_CELL)
            .execute()
        )
        values = resp.get("values", [])
        if not values or not values[0] or not values[0][0]:
            return ""
        return str(values[0][0]).strip()
    except Exception as e:
        # configシートがない等でも初回扱いにしたいので握りつぶす
        log("WARN", "read_last_sync_failed_fallback_to_full", error=str(e))
        return ""


def write_last_sync_time(sheets, spreadsheet_id: str, iso_ts: str) -> None:
    """
    config!B2 に最終同期時刻を書き込む
    """
    sheets.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=CONFIG_LAST_SYNC_CELL,
        valueInputOption="RAW",
        body={"values": [[iso_ts]]},
    ).execute()


def parse_iso_datetime(s: str) -> datetime:
    """
    Notionの last_edited_time は Z 付き ISO (例: 2025-12-26T21:20:03.675Z)
    Pythonで扱えるように変換
    """
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s).astimezone(timezone.utc)


def to_notion_iso(dt: datetime) -> str:
    """
    Notion filter 用に ISO 文字列（Z付き）にする
    """
    dt = dt.astimezone(timezone.utc)
    # ミリ秒はあってもなくてもOKだが、ここは秒精度で十分
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def fetch_pages_from_notion_db(
    notion_token: str,
    database_id: str,
    last_sync_iso: str | None,
) -> list[dict]:
    """
    last_sync_iso があれば last_edited_time で差分取得。なければ全件。
    """
    url = f"{NOTION_API_BASE}/databases/{database_id}/query"
    headers = {
        "Authorization": f"Bearer {notion_token}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }

    payload: dict = {"page_size": 100}

    if last_sync_iso:
        # 取りこぼし防止で少し巻き戻す
        try:
            last_dt = parse_iso_datetime(last_sync_iso)
            last_dt = last_dt - timedelta(seconds=SYNC_LOOKBACK_SECONDS)
            after_iso = to_notion_iso(last_dt)
        except Exception:
            after_iso = last_sync_iso  # パース失敗ならそのまま使う

        # Notionの timestamp filter
        payload["filter"] = {
            "timestamp": "last_edited_time",
            "last_edited_time": {"after": after_iso},
        }
        log("INFO", "notion_query_mode_delta", after=payload["filter"]["last_edited_time"]["after"])
    else:
        log("INFO", "notion_query_mode_full")

    all_results: list[dict] = []
    page_num = 0
    total_api_ms = 0

    log("INFO", "notion_query_start", database_id=database_id, page_size=100)

    while True:
        page_num += 1
        t0 = time.time()
        resp = requests.post(url, headers=headers, json=payload, timeout=30)
        api_ms = int((time.time() - t0) * 1000)
        total_api_ms += api_ms

        log(
            "INFO",
            "notion_query_page_done",
            page_num=page_num,
            status_code=resp.status_code,
            api_ms=api_ms,
            start_cursor=payload.get("start_cursor"),
        )

        if resp.status_code >= 400:
            # レスポンス本文を記録しない最小限の構造化ログ
            log("ERROR", "notion_query_error", status_code=resp.status_code, reason=getattr(resp, "reason", None), headers={k: resp.headers.get(k) for k in ("x-request-id", "retry-after") if k in resp.headers})
            raise RuntimeError(f"Notion API error {resp.status_code}")

        data = resp.json()
        results = data.get("results", [])
        all_results.extend(results)

        log(
            "INFO",
            "notion_query_page_parsed",
            page_num=page_num,
            results_in_page=len(results),
            total_results_so_far=len(all_results),
            has_more=bool(data.get("has_more")),
        )

        if data.get("has_more"):
            payload["start_cursor"] = data.get("next_cursor")
            time.sleep(0.2)
        else:
            break

    log(
        "INFO",
        "notion_query_complete",
        pages_fetched=page_num,
        total_results=len(all_results),
        total_api_ms=total_api_ms,
    )

    return all_results


def ensure_header_if_empty(sheets, spreadsheet_id: str, start_range: str, header: list[str]):
    """
    A1の値が空ならヘッダを書く（初回セットアップ用）
    """
    sheet_name = start_range.split("!")[0]
    sheet_name_q = a1_quote_sheet_name(sheet_name)
    header_range = f"{sheet_name_q}!A1:{col_to_a1(len(header))}1"

    resp = sheets.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=header_range,
    ).execute()
    values = resp.get("values", [])
    if values and any(cell.strip() for cell in values[0] if isinstance(cell, str)):
        return  # 既にヘッダがある

    log("INFO", "sheet_header_write", range=header_range)
    sheets.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=header_range,
        valueInputOption="RAW",
        body={"values": [header]},
    ).execute()


def read_existing_id_to_row_index(sheets, spreadsheet_id: str, start_range: str) -> dict[str, int]:
    """
    データシートのA列(notioin_page_id)を読み、page_id -> 行番号(1-indexed) の辞書を作る。
    ヘッダは1行目なので、データは2行目以降を想定。
    """
    sheet_name = start_range.split("!")[0]
    sheet_name_q = a1_quote_sheet_name(sheet_name)

    # A列を全部読む（必要なら A2:A などでもOK）
    id_range = f"{sheet_name_q}!A2:A"
    resp = sheets.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=id_range,
        majorDimension="COLUMNS",
    ).execute()

    cols = resp.get("values", [])
    if not cols:
        return {}

    ids = cols[0]
    mapping: dict[str, int] = {}
    # ids[0] は2行目
    for i, v in enumerate(ids):
        if not v:
            continue
        mapping[str(v).strip()] = 2 + i  # 行番号
    return mapping


def batch_upsert_rows(
    sheets,
    spreadsheet_id: str,
    start_range: str,
    header: list[str],
    rows_data: list[list[str]],
):
    """
    rows_data は「ヘッダ抜き」のデータ行配列。
    page_id が既存なら更新、無ければ末尾に追加。
    """
    sheet_name = start_range.split("!")[0]
    sheet_name_q = a1_quote_sheet_name(sheet_name)
    num_cols = len(header)
    last_col = col_to_a1(num_cols)

    id_to_row = read_existing_id_to_row_index(sheets, spreadsheet_id, start_range)

    # 末尾の次の行を計算（既存がなければ2行目から）
    next_append_row = (max(id_to_row.values()) + 1) if id_to_row else 2

    updates = []
    appended = 0
    updated = 0

    for row in rows_data:
        page_id = row[0]
        if page_id in id_to_row:
            r = id_to_row[page_id]
            updated += 1
        else:
            r = next_append_row
            next_append_row += 1
            id_to_row[page_id] = r
            appended += 1

        rng = f"{sheet_name_q}!A{r}:{last_col}{r}"
        updates.append({"range": rng, "values": [row]})

    if not updates:
        return {"updated": 0, "appended": 0}

    log("INFO", "sheets_batch_update_start", updates=len(updates), updated=updated, appended=appended)

    sheets.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"valueInputOption": "USER_ENTERED", "data": updates},
    ).execute()

    log("INFO", "sheets_batch_update_done", updated=updated, appended=appended)
    return {"updated": updated, "appended": appended}


def lambda_handler(event, context):
    request_id = getattr(context, "aws_request_id", None)
    start_ts = time.time()

    # 機微情報を避けるため、イベントの高レベル情報のみを記録
    log("INFO", "job_start", request_id=request_id, event_type=type(event).__name__, event_keys=list(event.keys()) if isinstance(event, dict) else None)

    try:
        # 必須設定
        notion_token = _env("NOTION_API_TOKEN")
        database_id = _env("NOTION_DATABASE_ID")

        spreadsheet_id = _env("GOOGLE_SHEETS_ID")
        start_range = _env("GOOGLE_SHEETS_RANGE", default="Raw!A1")

        sa_json_str = _env("GOOGLE_SERVICE_ACCOUNT_JSON")
        service_account_info = json.loads(sa_json_str)

        # Notionプロパティ名（デフォルトはNotionの表示名）
        company_prop = _env("NOTION_PROP_COMPANY", required=False, default="企業名")
        active_prop = _env("NOTION_PROP_ACTIVE_FLAG", required=False, default="Active Flag")
        add_date_prop = _env("NOTION_PROP_ADD_DATE", required=False, default="Add Date")
        state_prop = _env("NOTION_PROP_STATE", required=False, default="State")

        process_prop = _env("NOTION_PROP_PROCESS_OF_VCM", required=False, default="Process of VCM")
        category_prop = _env("NOTION_PROP_CATEGORY", required=False, default="Category")
        hq_prop = _env("NOTION_PROP_HQ", required=False, default="HQ")
        opp_date_prop = _env("NOTION_PROP_OPPORTUNITY_DATE", required=False, default="Opportunity Date")

        contacted_date_prop = _env("NOTION_PROP_CONTACTED_DATE", required=False, default="Contacted Date")
        negotiation_date_prop = _env(
            "NOTION_PROP_IN_NEGOTIATION_DATE", required=False, default="In Negotiation Date"
        )
        collaboration_date_prop = _env(
            "NOTION_PROP_IN_COLLABORATION_DATE", required=False, default="In Collaboration Date"
        )
        closed_date_prop = _env("NOTION_PROP_CLOSED_DATE", required=False, default="Closed Date")
        discover_date_prop = _env("NOTION_PROP_DISCOVER_DATE", required=False, default="Discover Date")
        assess_date_prop = _env("NOTION_PROP_ASSESS_DATE", required=False, default="Assess Date")
        purchase_date_prop = _env("NOTION_PROP_PURCHASE_DATE", required=False, default="Purchase Date")
        pilot_date_prop = _env("NOTION_PROP_PILOT_DATE", required=False, default="Pilot Date")
        adopt_date_prop = _env("NOTION_PROP_ADOPT_DATE", required=False, default="Adopt Date")

        log(
            "INFO",
            "config_loaded",
            request_id=request_id,
            database_id=database_id,
            spreadsheet_id=spreadsheet_id,
            start_range=start_range,
        )

        sheets = build_sheets_client(service_account_info)

        # 0) 最終同期時刻を取得
        last_sync_iso = read_last_sync_time(sheets, spreadsheet_id)
        log("INFO", "last_sync_loaded", last_sync=last_sync_iso or "(empty)")

        # 1) Notionから差分取得（初回は全件）
        pages = fetch_pages_from_notion_db(notion_token, database_id, last_sync_iso or None)

        # 2) 行データへ変換（ヘッダ + data）
        header = [
            "notion_page_id",
            "created_time",
            "last_edited_time",
            "企業名",
            "Active Flag",
            "Add Date",
            "State",
            "Process of VCM",
            "Category",
            "HQ",
            "Opportunity Date",
            "Contacted Date",
            "In Negotiation Date",
            "In Collaboration Date",
            "Closed Date",
            "Discover Date",
            "Assess Date",
            "Purchase Date",
            "Pilot Date",
            "Adopt Date",
        ]

        ensure_header_if_empty(sheets, spreadsheet_id, start_range, header)

        rows_data: list[list[str]] = []
        max_last_edited: datetime | None = None

        # フィールドマッピング（キー名 -> Notion ��ロパティ名）
        field_mappings = {
            "company": company_prop,
            "active": active_prop,
            "add_date": add_date_prop,
            "state": state_prop,
            "process": process_prop,
            "category": category_prop,
            "hq": hq_prop,
            "opp_date": opp_date_prop,
            "contacted": contacted_date_prop,
            "negotiation": negotiation_date_prop,
            "collaboration": collaboration_date_prop,
            "closed": closed_date_prop,
            "discover": discover_date_prop,
            "assess": assess_date_prop,
            "purchase": purchase_date_prop,
            "pilot": pilot_date_prop,
            "adopt": adopt_date_prop,
        }
        empty_counts = {k: 0 for k in field_mappings.keys()}

        t0 = time.time()
        for page in pages:
            page_id = page.get("id", "")
            created_time = page.get("created_time", "")
            last_edited_time = page.get("last_edited_time", "")
            props = page.get("properties", {}) or {}

            # last_edited の最大を取る（次回同期の基準）
            if last_edited_time:
                try:
                    dt = parse_iso_datetime(last_edited_time)
                    if (max_last_edited is None) or (dt > max_last_edited):
                        max_last_edited = dt
                except Exception:
                    pass

            company = extract_text_property(props, company_prop)
            active_flag = extract_text_property(props, active_prop)
            add_date = extract_text_property(props, add_date_prop)
            state = extract_text_property(props, state_prop)

            process_of_vcm = extract_text_property(props, process_prop)
            category = extract_text_property(props, category_prop)
            hq = extract_text_property(props, hq_prop)
            opportunity_date = normalize_date_value(extract_text_property(props, opp_date_prop))

            contacted_date = normalize_date_value(extract_text_property(props, contacted_date_prop))
            in_negotiation_date = normalize_date_value(extract_text_property(props, negotiation_date_prop))
            in_collaboration_date = normalize_date_value(extract_text_property(props, collaboration_date_prop))
            closed_date = normalize_date_value(extract_text_property(props, closed_date_prop))
            discover_date = normalize_date_value(extract_text_property(props, discover_date_prop))
            assess_date = normalize_date_value(extract_text_property(props, assess_date_prop))
            purchase_date = normalize_date_value(extract_text_property(props, purchase_date_prop))
            pilot_date = normalize_date_value(extract_text_property(props, pilot_date_prop))
            adopt_date = normalize_date_value(extract_text_property(props, adopt_date_prop))

            # フィールド値をマッピング
            field_values = {
                "company": company,
                "active": active_flag,
                "add_date": add_date,
                "state": state,
                "process": process_of_vcm,
                "category": category,
                "hq": hq,
                "opp_date": opportunity_date,
                "contacted": contacted_date,
                "negotiation": in_negotiation_date,
                "collaboration": in_collaboration_date,
                "closed": closed_date,
                "discover": discover_date,
                "assess": assess_date,
                "purchase": purchase_date,
                "pilot": pilot_date,
                "adopt": adopt_date,
            }
            # 空のフィールドをカウント
            for key, value in field_values.items():
                if not value:
                    empty_counts[key] += 1

            rows_data.append(
                [
                    page_id,
                    created_time,
                    last_edited_time,
                    company,
                    active_flag,
                    add_date,
                    state,
                    process_of_vcm,
                    category,
                    hq,
                    opportunity_date,
                    contacted_date,
                    in_negotiation_date,
                    in_collaboration_date,
                    closed_date,
                    discover_date,
                    assess_date,
                    purchase_date,
                    pilot_date,
                    adopt_date,
                ]
            )

        log(
            "INFO",
            "transform_done",
            ms=int((time.time() - t0) * 1000),
            notion_pages=len(pages),
            empty_counts=empty_counts,
        )

        # 3) Sheetへ upsert（差分反映）
        result = batch_upsert_rows(
            sheets=sheets,
            spreadsheet_id=spreadsheet_id,
            start_range=start_range,
            header=header,
            rows_data=rows_data,
        )

        # 4) 最終同期時刻更新
        # 更新が0件でも「同期は走った」ので時刻は更新してOK（運用方針次第）
        # 取りこぼしを嫌うなら max_last_edited を採用するのが安全
        if max_last_edited:
            new_last_sync = to_notion_iso(max_last_edited)
        else:
            # 変更が無かった場合は「今」を入れる
            new_last_sync = to_notion_iso(datetime.now(timezone.utc))

        write_last_sync_time(sheets, spreadsheet_id, new_last_sync)
        log("INFO", "last_sync_saved", last_sync=new_last_sync)

        total_ms = int((time.time() - start_ts) * 1000)
        log(
            "INFO",
            "job_success",
            request_id=request_id,
            delta_pages=len(pages),
            updated=result["updated"],
            appended=result["appended"],
            total_ms=total_ms,
        )

        return {
            "status": "ok",
            "request_id": request_id,
            "delta_pages": len(pages),
            "updated": result["updated"],
            "appended": result["appended"],
            "last_sync": new_last_sync,
            "total_ms": total_ms,
        }

    except Exception as e:
        total_ms = int((time.time() - start_ts) * 1000)
        log(
            "ERROR",
            "job_failed",
            request_id=request_id,
            error=str(e),
            traceback=traceback.format_exc()[:12000],
            total_ms=total_ms,
        )
        raise
