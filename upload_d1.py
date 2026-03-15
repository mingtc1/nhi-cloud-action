"""
Upload processed NHI drug data to Cloudflare D1 via Wrangler CLI.

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
import subprocess
import argparse

# ── Configuration ──────────────────────────────────────────────
ACCOUNT_ID = os.environ.get("CLOUDFLARE_ACCOUNT_ID")
API_TOKEN = os.environ.get("CLOUDFLARE_API_TOKEN")
DATABASE_ID = os.environ.get("D1_DATABASE_ID")

# CSV columns → DB columns (order matters for INSERT)
COLUMNS = [
    "異動", "藥品代號", "藥品英文名稱", "藥品中文名稱", "成分",
    "規格量", "規格單位", "單複方", "支付價", "有效起日",
    "有效迄日", "藥商", "製造廠名稱", "劑型", "藥品分類",
    "分類分組名稱", "ATC代碼", "給付規定章節", "藥品代碼超連結",
    "給付規定章節連結", "許可證字號",
]


def generate_sql(csv_path, sql_path):
    """Read CSV and generate a standalone .sql file with all INSERTs."""
    print(f"  [1/2] Generating {sql_path} from {csv_path}...")
    
    statements = [
        "DROP TABLE IF EXISTS nhi_drugs;",
        "CREATE TABLE nhi_drugs (id INTEGER PRIMARY KEY AUTOINCREMENT, 異動 TEXT, 藥品代號 TEXT, 藥品英文名稱 TEXT, 藥品中文名稱 TEXT, 成分 TEXT, 規格量 TEXT, 規格單位 TEXT, 單複方 TEXT, 支付價 TEXT, 有效起日 TEXT, 有效迄日 TEXT, 藥商 TEXT, 製造廠名稱 TEXT, 劑型 TEXT, 藥品分類 TEXT, 分類分組名稱 TEXT, ATC代碼 TEXT, 給付規定章節 TEXT, 藥品代碼超連結 TEXT, 給付規定章節連結 TEXT, 許可證字號 TEXT, updated_at TEXT DEFAULT (datetime('now')));",
        "CREATE INDEX IF NOT EXISTS idx_drug_code ON nhi_drugs(藥品代號);",
        "CREATE INDEX IF NOT EXISTS idx_license ON nhi_drugs(許可證字號);",
        "CREATE INDEX IF NOT EXISTS idx_atc ON nhi_drugs(ATC代碼);"
    ]

    col_names = ", ".join([f'"{c}"' for c in COLUMNS])
    count = 0

    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            vals = []
            for c in COLUMNS:
                # Clean up quotes to valid SQL strings
                val = str(row.get(c, ""))
                val_escaped = val.replace("'", "''")
                vals.append(f"'{val_escaped}'")
            
            stmt = f"INSERT INTO nhi_drugs ({col_names}) VALUES ({', '.join(vals)});"
            statements.append(stmt)
            count += 1

    with open(sql_path, "w", encoding="utf-8") as f:
        f.write("\n".join(statements))
        
    print(f"  ✓ Valid SQL file generated with {count} rows.")


def execute_wrangler(sql_path):
    """Run `npx wrangler d1 execute <uuid> --remote --file=<path>` to push changes."""
    print(f"  [2/2] Rebuilding D1 database using Wrangler CLI...")
    
    env = os.environ.copy()
    # Wrangler CLI reads these natively:
    # CLOUDFLARE_API_TOKEN, CLOUDFLARE_ACCOUNT_ID
    
    cmd = [
        "npx", "wrangler@latest", "d1", "execute", 
        DATABASE_ID, 
        "--remote", 
        f"--file={sql_path}"
    ]
    
    print(f"     Running: {' '.join(cmd)}")
    
    # We pipe stdout and stderr dynamically so we don't buffer excessively
    # and output is streamed directly to GitHub Actions console
    process = subprocess.Popen(
        cmd, 
        env=env,
        stdout=subprocess.PIPE, 
        stderr=subprocess.STDOUT,
        text=True
    )
    
    for line in iter(process.stdout.readline, ''):
        print(f"     > {line}", end="")
        
    process.stdout.close()
    return_code = process.wait()
    
    if return_code != 0:
        print(f"\n[WRANGLER ERROR] failed with exit code {return_code}")
        sys.exit(1)
        
    print("\n  ✓ Wrangler execution successful!")


def main():
    parser = argparse.ArgumentParser(description="Upload NHI data to Cloudflare D1 via Wrangler")
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

    sql_file = "import.sql"
    generate_sql(args.csv, sql_file)
    execute_wrangler(sql_file)

    print("═══ D1 Upload Complete ═══")


if __name__ == "__main__":
    main()
