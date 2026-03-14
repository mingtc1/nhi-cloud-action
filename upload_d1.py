"""
Upload processed NHI drug data to Cloudflare D1 via REST HTTP API.

Usage:
    python upload_d1.py [--csv <path>]

Environment Variables Required:
    CLOUDFLARE_ACCOUNT_ID  - Cloudflare Account ID
    CLOUDFLARE_API_TOKEN   - API Token with D1 edit permission
    D1_DATABASE_ID         - D1 Database UUID
"""

import os
import sys
import csv
import json
import requests
import argparse

# ── Configuration ──────────────────────────────────────────────
ACCOUNT_ID = os.environ.get("CLOUDFLARE_ACCOUNT_ID")
API_TOKEN = os.environ.get("CLOUDFLARE_API_TOKEN")
DATABASE_ID = os.environ.get("D1_DATABASE_ID")

BATCH_SIZE = 500  # D1 free tier allows up to 1,000 statements per batch

# CSV columns → DB columns (order matters for INSERT)
COLUMNS = [
    "異動", "藥品代號", "藥品英文名稱", "藥品中文名稱", "成分",
    "規格量", "規格單位", "單複方", "支付價", "有效起日",
    "有效迄日", "藥商", "製造廠名稱", "劑型", "藥品分類",
    "分類分組名稱", "ATC代碼", "給付規定章節", "藥品代碼超連結",
    "給付規定章節連結", "許可證字號",
]


def d1_query(sql_statements):
    """
    Execute one or more SQL statements against D1 via the REST API.
    Accepts a single dict or a list of dicts: { "sql": "...", "params": [...] }
    """
    url = (
        f"https://api.cloudflare.com/client/v4/accounts/"
        f"{ACCOUNT_ID}/d1/database/{DATABASE_ID}/query"
    )
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json",
    }

    payload = sql_statements if isinstance(sql_statements, list) else [sql_statements]
    
    # Remove empty params to avoid strict validation errors
    for item in payload:
        if "params" in item and not item["params"]:
            del item["params"]

    print(f"  [API] Sending {len(payload)} statements to D1...")
    resp = requests.post(url, headers=headers, json=payload, timeout=120)
    
    if resp.status_code != 200:
        print(f"\n[D1 API ERROR] HTTP {resp.status_code}")
        print(f"[D1 API ERROR BODY] {resp.text}\n")
        resp.raise_for_status()

    data = resp.json()
    if not data.get("success"):
        errors = data.get("errors", [])
        print(f"\n[D1 API ERROR JSON] {errors}\n")
        raise RuntimeError(f"D1 API error: {errors}")
        
    return data


def clear_table():
    """Delete all existing rows so we can do a full refresh."""
    print("  Clearing existing data …")
    d1_query({"sql": "DELETE FROM nhi_drugs", "params": []})
    print("  ✓ Table cleared.")


def upload_csv(csv_path):
    """Read CSV and batch-insert rows into D1."""
    placeholders = ", ".join(["?"] * len(COLUMNS))
    col_names = ", ".join([f'"{c}"' for c in COLUMNS])
    insert_sql = f"INSERT INTO nhi_drugs ({col_names}) VALUES ({placeholders})"

    rows_total = 0
    batch = []

    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            params = [row.get(c, "") for c in COLUMNS]
            batch.append({"sql": insert_sql, "params": params})

            if len(batch) >= BATCH_SIZE:
                rows_total += len(batch)
                print(f"  Inserting batch … ({rows_total} rows so far)")
                d1_query(batch)
                batch = []

    # Flush remaining rows
    if batch:
        rows_total += len(batch)
        d1_query(batch)

    print(f"  ✓ Total {rows_total} rows inserted into D1.")
    return rows_total


def verify():
    """Quick sanity check — print row count."""
    result = d1_query({"sql": "SELECT COUNT(*) as cnt FROM nhi_drugs", "params": []})
    count = result["result"][0]["results"][0]["cnt"]
    print(f"  ✓ Verification: {count} rows in nhi_drugs table.")
    return count


def main():
    parser = argparse.ArgumentParser(description="Upload NHI data to Cloudflare D1")
    parser.add_argument(
        "--csv",
        default="cleaned_nhi_data_no_zero.csv",
        help="Path to the processed CSV file",
    )
    args = parser.parse_args()

    # Pre-flight checks
    if not all([ACCOUNT_ID, API_TOKEN, DATABASE_ID]):
        print("Error: Missing required environment variables.")
        print("  CLOUDFLARE_ACCOUNT_ID, CLOUDFLARE_API_TOKEN, D1_DATABASE_ID")
        sys.exit(1)

    if not os.path.exists(args.csv):
        print(f"Error: CSV file not found: {args.csv}")
        sys.exit(1)

    print(f"═══ Uploading {args.csv} to Cloudflare D1 ═══")

    # Step 1: Clear old data
    clear_table()

    # Step 2: Batch insert new data
    upload_csv(args.csv)

    # Step 3: Verify
    verify()

    print("═══ D1 Upload Complete ═══")


if __name__ == "__main__":
    main()
