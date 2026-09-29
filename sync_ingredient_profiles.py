"""Synchronize compact TFDA ingredient profiles to Cloudflare D1.

The source dataset contains more than 100,000 ingredient rows.  This job keeps
all parsing on the GitHub runner and writes one compact row per NHI license,
only when that profile changed.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import re
import sys
import unicodedata
import zipfile
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import requests

import upload_d1


DATASET_URL = "https://data.fda.gov.tw/data/opendata/export/43/csv"
SOURCE_NAME = "tfda_ingredients"
PAGE_SIZE = 1000
MAX_CHANGED_ROWS = int(os.environ.get("D1_INGREDIENT_MAX_CHANGED_ROWS", "2000"))
MIN_COVERAGE_RATIO = float(os.environ.get("D1_INGREDIENT_MIN_COVERAGE", "0.70"))

TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS drug_ingredient_profiles (
  license_no TEXT PRIMARY KEY,
  profile_hash TEXT NOT NULL,
  components_json TEXT NOT NULL,
  component_count INTEGER NOT NULL,
  profile_status TEXT NOT NULL,
  updated_at TEXT DEFAULT (datetime('now'))
)
""".strip()


@dataclass(frozen=True)
class ProfileDiff:
    inserted: list[dict[str, Any]]
    updated: list[dict[str, Any]]
    deleted: list[str]

    @property
    def changed_count(self) -> int:
        return len(self.inserted) + len(self.updated) + len(self.deleted)


def clean_license_no(raw: Any) -> str:
    return re.sub(r"[\s\u3000]+", "", str(raw or ""))


def normalize_text(raw: Any) -> str:
    value = unicodedata.normalize("NFKC", str(raw or "")).upper().strip()
    return re.sub(r"\s+", " ", value)


def normalize_unit(raw: Any) -> str:
    unit = normalize_text(raw).replace("Μ", "U")
    aliases = {
        "UG": "MCG",
        "ΜG": "MCG",
        "MICROGRAM": "MCG",
        "MICROGRAMS": "MCG",
        "GRAM": "G",
        "GRAMS": "G",
        "GM": "G",
    }
    return aliases.get(unit, unit)


def normalize_amount(raw: Any) -> str:
    value = str(raw or "").strip()
    if not value:
        return ""
    try:
        number = Decimal(value)
    except InvalidOperation:
        return ""
    normalized = format(number.normalize(), "f")
    return "0" if normalized in {"-0", ""} else normalized


def component_from_row(row: dict[str, str]) -> tuple[dict[str, str], bool]:
    code = normalize_text(row.get("成分代碼"))
    name = normalize_text(row.get("成分名稱"))
    amount = normalize_amount(row.get("含量"))
    unit = normalize_unit(row.get("含量單位"))
    component = {
        "code": code,
        "name": name,
        "amount": amount,
        "unit": unit,
        "label": normalize_text(row.get("處方標示")),
        "description": normalize_text(row.get("含量描述")),
    }
    comparable = bool((code or name) and amount and unit)
    return component, comparable


def comparison_signature(component: dict[str, str]) -> str:
    identity = component["code"] or component["name"]
    amount = Decimal(component["amount"])
    unit = component["unit"]
    factors = {"G": Decimal("1000000"), "MG": Decimal("1000"), "MCG": Decimal("1")}
    if unit in factors:
        amount *= factors[unit]
        unit = "MCG"
    canonical_amount = format(amount.normalize(), "f")
    return "|".join((identity, canonical_amount, unit))


def build_profiles(
    nhi_csv_path: str,
    dataset_zip_path: str,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    licenses: set[str] = set()
    with open(nhi_csv_path, "r", encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        if "許可證字號" not in (reader.fieldnames or []):
            raise ValueError("NHI CSV 缺少許可證字號欄位")
        for row in reader:
            license_no = clean_license_no(row.get("許可證字號"))
            if license_no:
                licenses.add(license_no)

    grouped: dict[str, list[dict[str, str]]] = {}
    source_digest = hashlib.sha256()
    with zipfile.ZipFile(dataset_zip_path) as archive:
        csv_names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if len(csv_names) != 1:
            raise ValueError(f"TFDA ZIP 應包含一個 CSV，實際為 {len(csv_names)} 個")
        raw = archive.read(csv_names[0])
        source_digest.update(raw)
        text = io.TextIOWrapper(io.BytesIO(raw), encoding="utf-8-sig", newline="")
        reader = csv.DictReader(text)
        required = {"許可證字號", "處方標示", "成分名稱", "成分代碼", "含量描述", "含量", "含量單位"}
        missing = sorted(required - set(reader.fieldnames or []))
        if missing:
            raise ValueError(f"TFDA 成分資料缺少欄位: {', '.join(missing)}")
        for row in reader:
            license_no = clean_license_no(row.get("許可證字號"))
            if license_no in licenses:
                grouped.setdefault(license_no, []).append(row)

    source_hash = source_digest.hexdigest()
    profiles: dict[str, dict[str, Any]] = {}
    ready_count = 0
    partial_count = 0
    for license_no, rows in grouped.items():
        components: list[dict[str, str]] = []
        all_comparable = True
        seen: set[str] = set()
        for row in rows:
            component, comparable = component_from_row(row)
            signature = comparison_signature(component) if comparable else json.dumps(
                component, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            )
            if signature in seen:
                continue
            seen.add(signature)
            components.append(component)
            all_comparable = all_comparable and comparable
        components.sort(key=lambda item: (
            item["code"] or item["name"], item["amount"], item["unit"], item["label"], item["description"]
        ))
        signatures = sorted(comparison_signature(item) for item in components)
        profile_hash = hashlib.sha256("\n".join(signatures).encode("utf-8")).hexdigest()
        status = "ready" if components and all_comparable else "partial"
        ready_count += status == "ready"
        partial_count += status == "partial"
        profiles[license_no] = {
            "license_no": license_no,
            "profile_hash": profile_hash,
            "components_json": json.dumps(components, ensure_ascii=False, separators=(",", ":")),
            "component_count": len(components),
            "profile_status": status,
        }

    report = {
        "nhi_license_count": len(licenses),
        "matched_license_count": len(profiles),
        "unmatched_license_count": len(licenses - set(profiles)),
        "coverage_ratio": (len(profiles) / len(licenses)) if licenses else 0,
        "ready_profile_count": ready_count,
        "partial_profile_count": partial_count,
        "source_hash": source_hash,
    }
    return profiles, report


def download_dataset(path: str) -> None:
    partial = f"{path}.part"
    with requests.get(DATASET_URL, stream=True, timeout=(30, 300)) as response:
        response.raise_for_status()
        with open(partial, "wb") as target:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    target.write(chunk)
    os.replace(partial, path)


def table_exists() -> bool:
    result = upload_d1.d1_query(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'drug_ingredient_profiles' LIMIT 1"
    )
    return bool(result.get("results") or [])


def get_state() -> dict[str, Any] | None:
    result = upload_d1.d1_query(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'sync_state' LIMIT 1"
    )
    if not (result.get("results") or []):
        return None
    result = upload_d1.d1_query("SELECT * FROM sync_state WHERE source = ? LIMIT 1", [SOURCE_NAME])
    rows = result.get("results") or []
    return rows[0] if rows else None


def fetch_existing_profiles() -> tuple[dict[str, dict[str, Any]], int]:
    profiles: dict[str, dict[str, Any]] = {}
    rows_read = 0
    last_license = ""
    while True:
        result = upload_d1.d1_query(
            "SELECT license_no, profile_hash, components_json, component_count, profile_status "
            "FROM drug_ingredient_profiles WHERE license_no > ? ORDER BY license_no LIMIT ?",
            [last_license, PAGE_SIZE],
        )
        page = result.get("results") or []
        rows_read += int((result.get("meta") or {}).get("rows_read") or 0)
        for row in page:
            profiles[row["license_no"]] = row
        if len(page) < PAGE_SIZE:
            break
        last_license = page[-1]["license_no"]
    return profiles, rows_read


def compare_profiles(
    incoming: dict[str, dict[str, Any]],
    existing: dict[str, dict[str, Any]],
) -> ProfileDiff:
    incoming_keys = set(incoming)
    existing_keys = set(existing)
    inserted = [incoming[key] for key in sorted(incoming_keys - existing_keys)]
    deleted = sorted(existing_keys - incoming_keys)
    updated = [
        incoming[key]
        for key in sorted(incoming_keys & existing_keys)
        if any(str(incoming[key][field]) != str(existing[key].get(field, "")) for field in (
            "profile_hash", "component_count", "profile_status"
        ))
    ]
    return ProfileDiff(inserted=inserted, updated=updated, deleted=deleted)


def estimated_rows_written(diff: ProfileDiff) -> int:
    # One table row and its primary-key index, plus a conservative margin.
    return diff.changed_count * 2 + 10


def generate_sql(diff: ProfileDiff, profile_hash: str, total_records: int, path: str) -> None:
    statements: list[str] = []
    columns = ("license_no", "profile_hash", "components_json", "component_count", "profile_status")
    quoted = ", ".join(columns)
    for row in diff.inserted:
        values = ", ".join(upload_d1.sql_quote(row[column]) for column in columns)
        statements.append(f"INSERT INTO drug_ingredient_profiles ({quoted}) VALUES ({values});")
    for row in diff.updated:
        statements.append(
            "UPDATE drug_ingredient_profiles SET "
            f"profile_hash={upload_d1.sql_quote(row['profile_hash'])}, "
            f"components_json={upload_d1.sql_quote(row['components_json'])}, "
            f"component_count={int(row['component_count'])}, "
            f"profile_status={upload_d1.sql_quote(row['profile_status'])}, "
            "updated_at=datetime('now') "
            f"WHERE license_no={upload_d1.sql_quote(row['license_no'])};"
        )
    for license_no in diff.deleted:
        statements.append(
            f"DELETE FROM drug_ingredient_profiles WHERE license_no={upload_d1.sql_quote(license_no)};"
        )
    statements.extend([
        "CREATE TABLE IF NOT EXISTS sync_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, sync_time TEXT DEFAULT (datetime('now')), status TEXT, total_records INTEGER);",
        f"INSERT INTO sync_logs (status, total_records) VALUES ('TFDA Ingredient Sync Success', {total_records});",
        "INSERT INTO sync_state (source, data_hash, last_checked_at, last_changed_at, last_status, total_records, pending_hash, pending_reason, next_retry_at, updated_at) "
        f"VALUES ({upload_d1.sql_quote(SOURCE_NAME)}, {upload_d1.sql_quote(profile_hash)}, datetime('now'), datetime('now'), 'success', {total_records}, NULL, NULL, NULL, datetime('now')) "
        "ON CONFLICT(source) DO UPDATE SET data_hash=excluded.data_hash, last_checked_at=excluded.last_checked_at, "
        "last_changed_at=excluded.last_changed_at, last_status=excluded.last_status, total_records=excluded.total_records, "
        "pending_hash=NULL, pending_reason=NULL, next_retry_at=NULL, updated_at=excluded.updated_at;",
    ])
    Path(path).write_text("\n".join(statements) + "\n", encoding="utf-8")


def profiles_hash(profiles: dict[str, dict[str, Any]]) -> str:
    digest = hashlib.sha256()
    for license_no in sorted(profiles):
        row = profiles[license_no]
        digest.update(f"{license_no}|{row['profile_hash']}|{row['profile_status']}\n".encode("utf-8"))
    return digest.hexdigest()


def write_report(path: str, report: dict[str, Any]) -> None:
    Path(path).write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a", encoding="utf-8") as output:
            output.write(f"status={report['status']}\n")
            output.write(f"changed={report.get('changed', 0)}\n")
            output.write(f"reason={str(report.get('reason', '')).replace(chr(10), ' ')}\n")


def load_prior_reservation(path: str | None) -> tuple[int, int]:
    if not path or not Path(path).exists():
        return 0, 0
    report = json.loads(Path(path).read_text(encoding="utf-8"))
    if report.get("status") != "success":
        return 0, 0
    return int(report.get("compare_rows_read") or 0), int(report.get("estimated_rows_written") or 0)


def record_no_changes(data_hash: str, total_records: int) -> None:
    upload_d1.d1_query(
        "INSERT INTO sync_state (source, data_hash, last_checked_at, last_changed_at, last_status, total_records, updated_at) "
        "VALUES (?, ?, datetime('now'), datetime('now'), 'no_changes', ?, datetime('now')) "
        "ON CONFLICT(source) DO UPDATE SET data_hash=excluded.data_hash, last_checked_at=excluded.last_checked_at, "
        "last_status='no_changes', total_records=excluded.total_records, pending_hash=NULL, pending_reason=NULL, "
        "next_retry_at=NULL, updated_at=excluded.updated_at",
        [SOURCE_NAME, data_hash, total_records],
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Synchronize compact TFDA ingredient profiles")
    parser.add_argument("--nhi-csv", default="cleaned_nhi_data_no_zero.csv")
    parser.add_argument("--report", default="ingredient_profile_report.json")
    parser.add_argument("--prior-report", default="upload_report.json")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not all([upload_d1.ACCOUNT_ID, upload_d1.API_TOKEN, upload_d1.DATABASE_ID]):
        raise ValueError("Missing CLOUDFLARE_ACCOUNT_ID, CLOUDFLARE_API_TOKEN, or D1_DATABASE_ID")

    zip_path = "tfda_ingredient_profiles.zip"
    try:
        download_dataset(zip_path)
        incoming, build_report = build_profiles(args.nhi_csv, zip_path)
    finally:
        if Path(zip_path).exists():
            Path(zip_path).unlink()

    if not incoming:
        raise ValueError("沒有任何 TFDA 成分資料可同步")
    if build_report["coverage_ratio"] < MIN_COVERAGE_RATIO:
        report = {
            "status": "deferred_data_anomaly",
            "checked_at": upload_d1.now_utc(),
            **build_report,
            "changed": 0,
            "reason": f"許可證匹配率低於 {MIN_COVERAGE_RATIO:.0%}，停止寫入",
        }
        write_report(args.report, report)
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0

    incoming_hash = profiles_hash(incoming)
    upload_d1.ensure_sync_state() if not args.dry_run else None
    state = get_state()
    exists = table_exists()
    if exists and state and state.get("data_hash") == incoming_hash:
        report = {
            "status": "no_changes",
            "checked_at": upload_d1.now_utc(),
            **build_report,
            "changed": 0,
            "reason": "健保藥品對應的 TFDA 成分 profile 與上次相同",
        }
        if not args.dry_run:
            upload_d1.d1_query(
                "UPDATE sync_state SET last_checked_at=datetime('now'), last_status='no_changes', "
                "pending_hash=NULL, pending_reason=NULL, next_retry_at=NULL, updated_at=datetime('now') WHERE source=?",
                [SOURCE_NAME],
            )
        write_report(args.report, report)
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0

    existing, compare_rows_read = fetch_existing_profiles() if exists else ({}, 0)
    diff = compare_profiles(incoming, existing)
    predicted_write = estimated_rows_written(diff)
    prior_read, prior_write = load_prior_reservation(args.prior_report)
    usage = upload_d1.get_account_usage()
    effective_usage = None if usage is None else {
        "rows_read": usage["rows_read"] + prior_read,
        "rows_written": usage["rows_written"] + prior_write,
    }
    budget_allowed, budget_reason = upload_d1.budget_decision(
        effective_usage,
        compare_rows_read,
        predicted_write,
    )
    excessive_change = bool(existing) and diff.changed_count > MAX_CHANGED_ROWS
    excessive_delete = bool(existing) and len(diff.deleted) / len(existing) > upload_d1.MAX_DELETE_RATIO
    report = {
        "status": "dry_run" if args.dry_run else "ready",
        "checked_at": upload_d1.now_utc(),
        **build_report,
        "existing_records": len(existing),
        "inserted": len(diff.inserted),
        "updated": len(diff.updated),
        "deleted": len(diff.deleted),
        "changed": diff.changed_count,
        "compare_rows_read": compare_rows_read,
        "estimated_rows_written": predicted_write,
        "usage": usage,
        "reserved_prior_read": prior_read,
        "reserved_prior_write": prior_write,
        "reason": budget_reason,
    }

    if excessive_delete:
        report["status"] = "deferred_data_anomaly"
        report["reason"] = "TFDA profile 預計刪除比例超過安全門檻"
    elif excessive_change:
        report["status"] = "deferred_data_anomaly"
        report["reason"] = f"TFDA profile 異動 {diff.changed_count:,} 筆，超過自動更新門檻 {MAX_CHANGED_ROWS:,}"
    elif not budget_allowed:
        report["status"] = "deferred_quota"
    elif diff.changed_count == 0:
        report["status"] = "no_changes"
        report["reason"] = "逐列比對結果無異動"
        if not args.dry_run:
            record_no_changes(incoming_hash, len(incoming))
    elif not args.dry_run:
        if not exists:
            upload_d1.d1_query(TABLE_SCHEMA)
        sql_path = "incremental_ingredient_profiles.sql"
        generate_sql(diff, incoming_hash, len(incoming), sql_path)
        upload_d1.execute_wrangler(sql_path)
        report["status"] = "success"
        report["reason"] = "TFDA 成分 profile 增量更新完成"

    write_report(args.report, report)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
