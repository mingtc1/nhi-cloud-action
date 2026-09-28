"""Incrementally synchronize processed NHI drug data to Cloudflare D1.

The previous importer dropped and rebuilt the whole table. This version keeps the
table and indexes, skips identical datasets, and writes only inserted, changed,
or removed drug codes. It also applies conservative read/write budget gates.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests


ACCOUNT_ID = os.environ.get("CLOUDFLARE_ACCOUNT_ID")
API_TOKEN = os.environ.get("CLOUDFLARE_API_TOKEN")
ANALYTICS_TOKEN = os.environ.get("CLOUDFLARE_ANALYTICS_TOKEN") or API_TOKEN
DATABASE_ID = os.environ.get("D1_DATABASE_ID")
USAGE_DIAGNOSTIC: str | None = None

DAILY_READ_LIMIT = int(os.environ.get("D1_DAILY_READ_LIMIT", "5000000"))
DAILY_WRITE_LIMIT = int(os.environ.get("D1_DAILY_WRITE_LIMIT", "100000"))
READ_PAUSE_THRESHOLD = int(os.environ.get("D1_READ_PAUSE_THRESHOLD", "3500000"))
WRITE_PAUSE_THRESHOLD = int(os.environ.get("D1_WRITE_PAUSE_THRESHOLD", "70000"))
UNKNOWN_USAGE_WRITE_CAP = int(os.environ.get("D1_UNKNOWN_USAGE_WRITE_CAP", "10000"))
MAX_CHANGED_ROWS = int(os.environ.get("D1_MAX_CHANGED_ROWS", "2000"))
MAX_DELETE_RATIO = float(os.environ.get("D1_MAX_DELETE_RATIO", "0.05"))
PAGE_SIZE = int(os.environ.get("D1_COMPARE_PAGE_SIZE", "1000"))

COLUMNS = [
    "異動", "藥品代號", "藥品英文名稱", "藥品中文名稱", "成分",
    "規格量", "規格單位", "單複方", "支付價", "有效起日",
    "有效迄日", "藥商", "製造廠名稱", "劑型", "藥品分類",
    "分類分組名稱", "ATC代碼", "給付規定章節", "藥品代碼超連結",
    "給付規定章節連結", "許可證字號",
]
KEY_COLUMN = "藥品代號"

SYNC_STATE_SCHEMA = """
CREATE TABLE IF NOT EXISTS sync_state (
  source TEXT PRIMARY KEY,
  data_hash TEXT,
  last_checked_at TEXT,
  last_changed_at TEXT,
  last_status TEXT,
  total_records INTEGER,
  pending_hash TEXT,
  pending_reason TEXT,
  next_retry_at TEXT,
  updated_at TEXT DEFAULT (datetime('now'))
)
""".strip()


@dataclass(frozen=True)
class Diff:
    inserted: list[dict[str, str]]
    updated: list[dict[str, str]]
    deleted: list[str]

    @property
    def changed_count(self) -> int:
        return len(self.inserted) + len(self.updated) + len(self.deleted)


class D1Error(RuntimeError):
    pass


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_row(row: dict[str, Any]) -> dict[str, str]:
    return {column: "" if row.get(column) is None else str(row.get(column, "")) for column in COLUMNS}


def load_csv(csv_path: str) -> dict[str, dict[str, str]]:
    rows: dict[str, dict[str, str]] = {}
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        missing = [column for column in COLUMNS if column not in (reader.fieldnames or [])]
        if missing:
            raise ValueError(f"CSV 缺少必要欄位: {', '.join(missing)}")
        for line_no, raw_row in enumerate(reader, start=2):
            row = normalize_row(raw_row)
            code = row[KEY_COLUMN].strip()
            if not code:
                raise ValueError(f"CSV 第 {line_no} 列缺少藥品代號")
            if code in rows:
                raise ValueError(f"CSV 藥品代號重複: {code}")
            row[KEY_COLUMN] = code
            rows[code] = row
    if not rows:
        raise ValueError("CSV 沒有任何可同步資料")
    return rows


def dataset_hash(rows: dict[str, dict[str, str]]) -> str:
    digest = hashlib.sha256()
    for code in sorted(rows):
        payload = [rows[code][column] for column in COLUMNS]
        digest.update(json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def compare_rows(
    incoming: dict[str, dict[str, str]],
    existing: dict[str, dict[str, str]],
) -> Diff:
    incoming_codes = set(incoming)
    existing_codes = set(existing)
    inserted = [incoming[code] for code in sorted(incoming_codes - existing_codes)]
    deleted = sorted(existing_codes - incoming_codes)
    updated = [
        incoming[code]
        for code in sorted(incoming_codes & existing_codes)
        if incoming[code] != existing[code]
    ]
    return Diff(inserted=inserted, updated=updated, deleted=deleted)


def estimated_rows_written(diff: Diff) -> int:
    # Each row can touch the table plus three existing indexes. The extra unit
    # per row and fixed margin keep the preflight estimate conservative.
    return (diff.changed_count * 5) + 10


def sql_quote(value: Any) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def d1_query(sql: str, params: list[Any] | None = None) -> dict[str, Any]:
    endpoint = (
        f"https://api.cloudflare.com/client/v4/accounts/{ACCOUNT_ID}"
        f"/d1/database/{DATABASE_ID}/query"
    )
    response = requests.post(
        endpoint,
        headers={"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"},
        json={"sql": sql, "params": params or []},
        timeout=(30, 120),
    )
    response.raise_for_status()
    payload = response.json()
    if not payload.get("success"):
        raise D1Error(f"D1 query failed: {payload.get('errors') or 'unknown error'}")
    result = (payload.get("result") or [{}])[0]
    if not result.get("success", False):
        raise D1Error(f"D1 statement failed: {result.get('error') or 'unknown error'}")
    return result


def ensure_sync_state() -> None:
    d1_query(SYNC_STATE_SCHEMA)


def get_sync_state() -> dict[str, Any] | None:
    result = d1_query("SELECT * FROM sync_state WHERE source = ? LIMIT 1", ["nhi"])
    rows = result.get("results") or []
    return rows[0] if rows else None


def get_sync_state_if_exists() -> dict[str, Any] | None:
    result = d1_query(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'sync_state' LIMIT 1"
    )
    if not (result.get("results") or []):
        return None
    return get_sync_state()


def fetch_existing_rows() -> tuple[dict[str, dict[str, str]], int]:
    quoted_columns = ", ".join(f'"{column}"' for column in COLUMNS)
    rows: dict[str, dict[str, str]] = {}
    rows_read = 0
    last_code = ""
    while True:
        result = d1_query(
            f'SELECT {quoted_columns} FROM nhi_drugs '
            f'WHERE "{KEY_COLUMN}" > ? ORDER BY "{KEY_COLUMN}" LIMIT ?',
            [last_code, PAGE_SIZE],
        )
        page = result.get("results") or []
        rows_read += int((result.get("meta") or {}).get("rows_read") or 0)
        for raw_row in page:
            row = normalize_row(raw_row)
            code = row[KEY_COLUMN]
            if not code or code in rows:
                raise D1Error(f"D1 藥品代號缺漏或重複: {code or '（空白）'}")
            rows[code] = row
        if len(page) < PAGE_SIZE:
            break
        last_code = str(page[-1][KEY_COLUMN])
    return rows, rows_read


def get_account_usage() -> dict[str, int] | None:
    global USAGE_DIAGNOSTIC
    USAGE_DIAGNOSTIC = None
    query = """
    query D1DailyUsage($accountTag: string!, $date: Date!) {
      viewer {
        accounts(filter: { accountTag: $accountTag }) {
          d1AnalyticsAdaptiveGroups(limit: 10000, filter: { date_geq: $date, date_leq: $date }) {
            sum { rowsRead rowsWritten }
          }
        }
      }
    }
    """
    today = datetime.now(timezone.utc).date().isoformat()
    try:
        response = requests.post(
            "https://api.cloudflare.com/client/v4/graphql",
            headers={"Authorization": f"Bearer {ANALYTICS_TOKEN}", "Content-Type": "application/json"},
            json={"query": query, "variables": {"accountTag": ACCOUNT_ID, "date": today}},
            timeout=(30, 60),
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("errors"):
            first_error = payload["errors"][0]
            USAGE_DIAGNOSTIC = str(first_error.get("message") or "GraphQL Analytics API error")
            return None
        accounts = (((payload.get("data") or {}).get("viewer") or {}).get("accounts") or [])
        groups = accounts[0].get("d1AnalyticsAdaptiveGroups", []) if accounts else []
        return {
            "rows_read": sum(int((group.get("sum") or {}).get("rowsRead") or 0) for group in groups),
            "rows_written": sum(int((group.get("sum") or {}).get("rowsWritten") or 0) for group in groups),
        }
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "unknown"
        USAGE_DIAGNOSTIC = f"GraphQL Analytics API HTTP {status}"
        return None
    except (requests.RequestException, ValueError, TypeError) as exc:
        USAGE_DIAGNOSTIC = f"GraphQL Analytics API unavailable: {type(exc).__name__}"
        return None


def budget_decision(
    usage: dict[str, int] | None,
    predicted_read: int,
    predicted_write: int,
) -> tuple[bool, str]:
    if usage is None:
        if predicted_write > UNKNOWN_USAGE_WRITE_CAP:
            return False, (
                "無法取得帳戶用量，且預估寫入 "
                f"{predicted_write:,} 超過未知用量保守上限 {UNKNOWN_USAGE_WRITE_CAP:,}"
            )
        return True, "帳戶用量暫時無法取得；本次異動小於保守上限"

    projected_read = usage["rows_read"] + predicted_read
    projected_write = usage["rows_written"] + predicted_write
    if projected_read > READ_PAUSE_THRESHOLD:
        return False, f"預估讀取將達 {projected_read:,}，超過操作門檻 {READ_PAUSE_THRESHOLD:,}"
    if projected_write > WRITE_PAUSE_THRESHOLD:
        return False, f"預估寫入將達 {projected_write:,}，超過操作門檻 {WRITE_PAUSE_THRESHOLD:,}"
    if projected_read >= DAILY_READ_LIMIT or projected_write >= DAILY_WRITE_LIMIT:
        return False, "預估用量將碰到 Cloudflare D1 每日硬上限"
    return True, "讀寫預算足夠"


def data_quality_decision(diff: Diff, existing_count: int, incoming_count: int) -> tuple[bool, str]:
    if existing_count and incoming_count < int(existing_count * 0.8):
        return False, (
            f"新資料僅 {incoming_count:,} 筆，低於現有 {existing_count:,} 筆的 80%"
        )
    if existing_count and len(diff.deleted) / existing_count > MAX_DELETE_RATIO:
        return False, (
            f"預計刪除 {len(diff.deleted):,} 筆，超過現有資料的 {MAX_DELETE_RATIO:.0%}"
        )
    if diff.changed_count > MAX_CHANGED_ROWS:
        return False, (
            f"總異動 {diff.changed_count:,} 筆，超過自動更新門檻 {MAX_CHANGED_ROWS:,}"
        )
    return True, "資料異動量在允許範圍內"


def generate_incremental_sql(
    diff: Diff,
    data_hash: str,
    total_records: int,
    sql_path: str,
) -> None:
    # Wrangler wraps SQL-file imports in a transaction. Cloudflare explicitly
    # requires BEGIN/COMMIT to be removed from imported SQL files.
    statements: list[str] = []
    quoted_columns = ", ".join(f'"{column}"' for column in COLUMNS)

    for row in diff.inserted:
        values = ", ".join(sql_quote(row[column]) for column in COLUMNS)
        statements.append(f"INSERT INTO nhi_drugs ({quoted_columns}) VALUES ({values});")

    update_columns = [column for column in COLUMNS if column != KEY_COLUMN]
    for row in diff.updated:
        assignments = ", ".join(
            f'"{column}" = {sql_quote(row[column])}' for column in update_columns
        )
        statements.append(
            f'UPDATE nhi_drugs SET {assignments}, updated_at = datetime(\'now\') '
            f'WHERE "{KEY_COLUMN}" = {sql_quote(row[KEY_COLUMN])};'
        )

    for code in diff.deleted:
        statements.append(f'DELETE FROM nhi_drugs WHERE "{KEY_COLUMN}" = {sql_quote(code)};')

    statements.extend([
        "CREATE TABLE IF NOT EXISTS sync_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, sync_time TEXT DEFAULT (datetime('now')), status TEXT, total_records INTEGER);",
        f"INSERT INTO sync_logs (status, total_records) VALUES ('Success', {total_records});",
        "INSERT INTO sync_state (source, data_hash, last_checked_at, last_changed_at, last_status, total_records, pending_hash, pending_reason, next_retry_at, updated_at) "
        f"VALUES ('nhi', {sql_quote(data_hash)}, datetime('now'), datetime('now'), 'success', {total_records}, NULL, NULL, NULL, datetime('now')) "
        "ON CONFLICT(source) DO UPDATE SET data_hash=excluded.data_hash, last_checked_at=excluded.last_checked_at, "
        "last_changed_at=excluded.last_changed_at, last_status=excluded.last_status, total_records=excluded.total_records, "
        "pending_hash=NULL, pending_reason=NULL, next_retry_at=NULL, updated_at=excluded.updated_at;",
    ])
    Path(sql_path).write_text("\n".join(statements) + "\n", encoding="utf-8")


def execute_wrangler(sql_path: str) -> None:
    toml_content = f'''name = "nhi-cloud-action"
compatibility_date = "2026-09-27"

[[d1_databases]]
binding = "DB"
database_name = "nhi-drugs"
database_id = "{DATABASE_ID}"
'''
    Path("wrangler.toml").write_text(toml_content, encoding="utf-8")
    result = subprocess.run(
        ["npx", "wrangler@latest", "d1", "execute", "nhi-drugs", "--remote", f"--file={sql_path}"],
        env=os.environ.copy(),
    )
    if result.returncode != 0:
        raise D1Error(f"Wrangler failed with exit code {result.returncode}")


def record_no_changes(data_hash: str, total_records: int) -> None:
    d1_query(
        "INSERT INTO sync_state (source, data_hash, last_checked_at, last_changed_at, last_status, total_records, updated_at) "
        "VALUES (?, ?, datetime('now'), COALESCE((SELECT sync_time FROM sync_logs WHERE status = 'Success' ORDER BY id DESC LIMIT 1), datetime('now')), 'no_changes', ?, datetime('now')) "
        "ON CONFLICT(source) DO UPDATE SET data_hash=excluded.data_hash, last_checked_at=excluded.last_checked_at, "
        "last_status='no_changes', total_records=excluded.total_records, pending_hash=NULL, pending_reason=NULL, "
        "next_retry_at=NULL, updated_at=excluded.updated_at",
        ["nhi", data_hash, total_records],
    )


def write_report(path: str, report: dict[str, Any]) -> None:
    Path(path).write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a", encoding="utf-8") as output:
            output.write(f"status={report['status']}\n")
            output.write(f"changed={report.get('changed', 0)}\n")
            output.write(f"reason={str(report.get('reason', '')).replace(chr(10), ' ')}\n")


def validate_live_database() -> dict[str, Any]:
    """Validate the live D1 guard rails without downloading or changing data."""
    table_result = d1_query(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('nhi_drugs', 'sync_logs') ORDER BY name"
    )
    tables = [row.get("name") for row in (table_result.get("results") or [])]
    missing_tables = sorted({"nhi_drugs", "sync_logs"} - set(tables))
    if missing_tables:
        raise D1Error(f"D1 缺少必要資料表: {', '.join(missing_tables)}")

    index_result = d1_query(
        "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'nhi_drugs' ORDER BY name"
    )
    indexes = [row.get("name") for row in (index_result.get("results") or [])]
    required_indexes = {"idx_drug_code", "idx_license", "idx_atc", "idx_group_name"}
    missing_indexes = sorted(required_indexes - set(indexes))

    nhi_log_result = d1_query(
        "SELECT sync_time, status, total_records FROM sync_logs WHERE status = 'Success' ORDER BY id DESC LIMIT 1"
    )
    latest_nhi_log = (nhi_log_result.get("results") or [None])[0]
    tfda_log_result = d1_query(
        "SELECT sync_time, status, total_records FROM sync_logs WHERE status = 'TFDA Sync Success' ORDER BY id DESC LIMIT 1"
    )
    latest_tfda_log = (tfda_log_result.get("results") or [None])[0]
    state = get_sync_state_if_exists()
    usage = get_account_usage()

    status = "preflight_ok" if not missing_indexes else "preflight_warning"
    reason = (
        "D1 連線、必要資料表及索引正常"
        if not missing_indexes
        else f"D1 缺少索引: {', '.join(missing_indexes)}"
    )
    return {
        "status": status,
        "checked_at": now_utc(),
        "reason": reason,
        "tables": tables,
        "indexes": indexes,
        "latest_nhi_sync_log": latest_nhi_log,
        "latest_tfda_sync_log": latest_tfda_log,
        "sync_state": state,
        "usage": usage,
        "usage_diagnostic": USAGE_DIAGNOSTIC,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Incrementally synchronize NHI data to Cloudflare D1")
    parser.add_argument("--csv", default="cleaned_nhi_data_no_zero.csv")
    parser.add_argument("--report", default="upload_report.json")
    parser.add_argument("--dry-run", action="store_true", help="Compare and budget-check without writing D1")
    parser.add_argument(
        "--validate-live",
        action="store_true",
        help="Validate live D1 connectivity, schema, indexes, and usage without downloading or writing",
    )
    args = parser.parse_args()

    if not all([ACCOUNT_ID, API_TOKEN, DATABASE_ID]):
        raise ValueError("Missing CLOUDFLARE_ACCOUNT_ID, CLOUDFLARE_API_TOKEN, or D1_DATABASE_ID")
    if args.validate_live:
        report = validate_live_database()
        write_report(args.report, report)
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0 if report["status"] == "preflight_ok" else 1
    if not Path(args.csv).exists():
        raise FileNotFoundError(args.csv)

    incoming = load_csv(args.csv)
    incoming_hash = dataset_hash(incoming)
    if args.dry_run:
        state = get_sync_state_if_exists()
    else:
        ensure_sync_state()
        state = get_sync_state()

    if state and state.get("data_hash") == incoming_hash:
        report = {
            "status": "no_changes",
            "checked_at": now_utc(),
            "total_records": len(incoming),
            "changed": 0,
            "reason": "清理後資料指紋與上次成功版本相同",
        }
        if not args.dry_run:
            record_no_changes(incoming_hash, len(incoming))
        write_report(args.report, report)
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0

    usage_before = get_account_usage()
    allowed, reason = budget_decision(usage_before, len(incoming), 0)
    if not allowed:
        report = {
            "status": "deferred_quota",
            "checked_at": now_utc(),
            "total_records": len(incoming),
            "changed": 0,
            "usage": usage_before,
            "reason": reason,
        }
        write_report(args.report, report)
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0

    existing, compare_rows_read = fetch_existing_rows()
    diff = compare_rows(incoming, existing)
    predicted_write = estimated_rows_written(diff)
    data_allowed, data_reason = data_quality_decision(diff, len(existing), len(incoming))
    budget_allowed, budget_reason = budget_decision(usage_before, compare_rows_read, predicted_write)
    report = {
        "status": "dry_run" if args.dry_run else "ready",
        "checked_at": now_utc(),
        "total_records": len(incoming),
        "existing_records": len(existing),
        "inserted": len(diff.inserted),
        "updated": len(diff.updated),
        "deleted": len(diff.deleted),
        "changed": diff.changed_count,
        "compare_rows_read": compare_rows_read,
        "estimated_rows_written": predicted_write,
        "usage": usage_before,
        "reason": data_reason if not data_allowed else budget_reason,
    }

    if diff.changed_count == 0:
        report["status"] = "no_changes"
        report["reason"] = "逐列比對結果無異動；已建立目前資料指紋"
        if not args.dry_run:
            record_no_changes(incoming_hash, len(incoming))
    elif not data_allowed:
        report["status"] = "deferred_data_anomaly"
    elif not budget_allowed:
        report["status"] = "deferred_quota"
    elif not args.dry_run:
        sql_path = "incremental_import.sql"
        generate_incremental_sql(diff, incoming_hash, len(incoming), sql_path)
        execute_wrangler(sql_path)
        report["status"] = "success"
        report["reason"] = "增量更新完成"

    write_report(args.report, report)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
