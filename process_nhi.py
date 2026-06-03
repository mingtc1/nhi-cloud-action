import urllib.request
import re
import os
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
    # Cloudflare Worker proxy URL (bypasses NHI geo-restriction on GitHub IPs)
    proxy_url = os.environ.get(
        "NHI_PROXY_URL",
        "https://nhi-proxy.mingster.workers.dev/download"
    )
    proxy_token = os.environ.get("NHI_PROXY_TOKEN", "")
    download_path = "A21030000I-E41001-001.csv"

    print("1. Downloading NHI Data via Cloudflare Proxy...")
    print(f"   Proxy: {proxy_url}")

    headers = {"User-Agent": "nhi-cloud-action/1.0"}
    if proxy_token:
        headers["Authorization"] = f"Bearer {proxy_token}"

    import time
    import urllib.error

    MAX_RETRIES = 3
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(proxy_url, headers=headers)
            # 60-minute timeout (94MB CSV can be slow over the wire)
            with urllib.request.urlopen(req, timeout=3600) as response:
                with open(download_path, 'wb') as out_file:
                    out_file.write(response.read())
            print(f"   Downloaded successfully to {download_path}")
            break
        except urllib.error.HTTPError as e:
            if e.code == 429:
                # Rate limited — no point retrying immediately
                print(f"   Rate limited by proxy (HTTP 429). Exiting without retry.")
                import sys; sys.exit(1)
            print(f"   Attempt {attempt}/{MAX_RETRIES} failed: HTTP {e.code} {e.reason}")
            if attempt == MAX_RETRIES:
                print("Download Error: All retry attempts failed. Exiting.")
                import sys; sys.exit(1)
            time.sleep(10 * attempt)  # back-off: 10s, 20s
        except Exception as e:
            print(f"   Attempt {attempt}/{MAX_RETRIES} failed: {e}")
            if attempt == MAX_RETRIES:
                print("Download Error: All retry attempts failed. Exiting.")
                import sys; sys.exit(1)
            time.sleep(10 * attempt)

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
