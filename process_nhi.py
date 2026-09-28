import re
import os
import subprocess
import pandas as pd
import argparse
from datetime import datetime

# License mappings
lic_mapping = {
    "01": "衛署藥製", "02": "衛署藥輸", "03": "衛署成製", "09": "衛署菌疫製",
    "10": "衛署菌疫輸", "19": "衛署成輸", "20": "衛署罕藥輸", "21": "衛署罕藥製",
    "22": "衛署罕菌疫輸", "23": "衛署罕菌疫製", "41": "衛署藥陸輸", "51": "衛部藥製",
    "52": "衛部藥輸", "53": "衛部成製", "59": "衛部菌疫製", "60": "衛部菌疫輸",
    "69": "衛部成輸", "70": "衛部罕藥輸", "71": "衛部罕藥製", "72": "衛部罕菌疫輸",
    "73": "衛部罕菌疫製", "91": "衛部藥陸輸", "12": "內衛藥製", "13": "內衛藥輸",
    "14": "內衛成製", "15": "內衛菌疫製", "16": "內衛菌疫輸"
}

def extract_license_number(url):
    if pd.isna(url) or not isinstance(url, str):
        return ""
    urls = url.split(",")
    for u in urls:
        if "licId=" in u:
            match = re.search(r"licId=([A-Za-z0-9]+)", u)
            if match:
                licId = match.group(1).strip()
                if len(licId) == 8:
                    prefix = licId[:2]
                    number = licId[2:]
                    if prefix in lic_mapping:
                        return f"{lic_mapping[prefix]}字第{number}號"
                    else:
                        return f"未知證別({prefix})字第{number}號"
    return ""

def process_nhi_data(output_path, exclude_zero=False):
    official_url = os.environ.get(
        "NHI_OFFICIAL_URL",
        "https://info.nhi.gov.tw/api/iode0000s01/Dataset?rId=A21030000I-E41001-001"
    )
    proxy_url = os.environ.get(
        "NHI_PROXY_URL",
        "https://nhi-proxy.mingster.workers.dev/download"
    )
    proxy_token = os.environ.get("NHI_PROXY_TOKEN", "")
    source_mode = os.environ.get("NHI_SOURCE_MODE", "direct_then_proxy")
    download_path = "A21030000I-E41001-001.csv"
    partial_path = f"{download_path}.part"

    import sys
    import requests

    CHUNK_SIZE = 1024 * 1024  # 1 MB chunks — stable for large files
    if source_mode == "direct":
        sources = [("NHI official source", official_url, False)]
    elif source_mode == "proxy":
        sources = [("Cloudflare proxy", proxy_url, True)]
    else:
        sources = [
            ("NHI official source", official_url, False),
            ("Cloudflare proxy fallback", proxy_url, True),
        ]

    print("1. Downloading NHI Data...")
    downloaded_ok = False
    for source_name, source_url, is_proxy in sources:
        headers = {"User-Agent": "nhi-cloud-action/1.0"}
        if is_proxy and proxy_token:
            headers["Authorization"] = f"Bearer {proxy_token}"
        if is_proxy and os.environ.get("NHI_PROXY_DIAGNOSTIC") == "1":
            headers["X-Diagnostic-Dry-Run"] = "1"

        print(f"   Source: {source_name}")
        print(f"   URL: {source_url}")
        try:
            if not is_proxy:
                curl_result = subprocess.run(
                    [
                        "curl",
                        "--fail-with-body",
                        "--location",
                        "--ipv4",
                        "--http1.1",
                        "--connect-timeout", "30",
                        "--max-time", "3600",
                        "--user-agent",
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                        "--header", "Accept: text/csv,*/*",
                        "--output", partial_path,
                        source_url,
                    ],
                    check=False,
                )
                if curl_result.returncode != 0:
                    print(
                        f"   {source_name} curl failed with exit code "
                        f"{curl_result.returncode}."
                    )
                    if os.path.exists(partial_path):
                        os.remove(partial_path)
                    continue
                os.replace(partial_path, download_path)
                downloaded = os.path.getsize(download_path)
                print(
                    f"   Downloaded successfully: {downloaded // 1024 // 1024} MB "
                    f"→ {download_path}"
                )
                downloaded_ok = True
                break

            with requests.get(
                source_url,
                headers=headers,
                stream=True,           # stream=True: don't buffer entire response
                timeout=(30, 3600),    # (connect timeout, read timeout)
            ) as resp:
                if resp.status_code == 429:
                    print(f"   {source_name} rate limited the request (HTTP 429).")
                    continue
                if resp.status_code != 200:
                    first_chunk = next(resp.iter_content(chunk_size=1024), b"")
                    detail = first_chunk.decode("utf-8", errors="replace").strip()
                    if detail:
                        print(f"   Error detail: {detail[:1000]}")
                    print(f"   {source_name} returned HTTP {resp.status_code}.")
                    continue
                total = int(resp.headers.get("Content-Length", 0))
                downloaded = 0
                with open(partial_path, "wb") as out_file:
                    for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                        if chunk:
                            out_file.write(chunk)
                            downloaded += len(chunk)
                            if total:
                                pct = downloaded / total * 100
                                print(f"\r   Progress: {downloaded // 1024 // 1024} MB / {total // 1024 // 1024} MB ({pct:.1f}%)", end="", flush=True)
                os.replace(partial_path, download_path)
                print(f"\n   Downloaded successfully: {downloaded // 1024 // 1024} MB → {download_path}")
                downloaded_ok = True
                break
        except Exception as e:
            print(f"   {source_name} failed: {e}")
            if os.path.exists(partial_path):
                os.remove(partial_path)

    if not downloaded_ok:
        print("Download Error: All configured sources failed. Exiting.")
        sys.exit(1)

    print("2. Loading dataset...")
    try:
        df = pd.read_csv(download_path, encoding='utf-8', dtype=str)
    except Exception as e:
        df = pd.read_csv(download_path, encoding='big5', dtype=str)
        
    print("3. Filtering historical data...")
    today = datetime.now()
    roc_year = today.year - 1911
    current_date_int = int(f"{roc_year:03d}{today.strftime('%m%d')}")
    
    df['有效起日_num'] = pd.to_numeric(df['有效起日'], errors='coerce').fillna(9999999)
    df['有效迄日_num'] = pd.to_numeric(df['有效迄日'], errors='coerce').fillna(0)
    
    cleaned_df = df[(df['有效起日_num'] <= current_date_int) & (df['有效迄日_num'] > current_date_int)].copy()
    cleaned_df.drop(columns=['有效起日_num', '有效迄日_num'], inplace=True)
    
    print(f"Rows after date filtering: {len(cleaned_df)}")

    if exclude_zero:
        print("4. Filtering out zero-price items...")
        cleaned_df['支付價_num'] = pd.to_numeric(cleaned_df['支付價'], errors='coerce').fillna(0)
        cleaned_df = cleaned_df[cleaned_df['支付價_num'] > 0].copy()
        cleaned_df.drop(columns=['支付價_num'], inplace=True)
        print(f"Rows after zero-price filtering: {len(cleaned_df)}")
        
    print("5. Extracting License Numbers...")
    cleaned_df['許可證字號'] = cleaned_df['藥品代碼超連結'].apply(extract_license_number)
    
    print(f"6. Saving to {output_path}...")
    cleaned_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    if os.path.exists(download_path):
        os.remove(download_path)
    
    print("Processing Complete!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process NHI Drug Data")
    parser.add_argument("--output", default="processed_nhi_data.csv", help="Output file path")
    parser.add_argument("--exclude-zero", action="store_true", help="Exclude items with a 0 payment price")
    args = parser.parse_args()
    
    process_nhi_data(args.output, args.exclude_zero)
