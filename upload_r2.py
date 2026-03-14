import os
import boto3
from botocore.config import Config

# Configuration read from GitHub Actions Secrets
# You must set these in your GitHub Repo: Settings -> Secrets and variables -> Actions
R2_ACCOUNT_ID = os.environ.get('R2_ACCOUNT_ID')
R2_ACCESS_KEY_ID = os.environ.get('R2_ACCESS_KEY_ID')
R2_SECRET_ACCESS_KEY = os.environ.get('R2_SECRET_ACCESS_KEY')
R2_BUCKET_NAME = os.environ.get('R2_BUCKET_NAME')

def upload_to_r2(file_path, object_name=None):
    if not all([R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME]):
        print("Error: Missing one or more R2 environment variables.")
        print("Please ensure R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, and R2_BUCKET_NAME are set.")
        return False

    if object_name is None:
        object_name = os.path.basename(file_path)

    r2_endpoint_url = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"

    print(f"Connecting to R2 Bucket: {R2_BUCKET_NAME}")
    
    # Initialize the S3 client directly pointed at Cloudflare R2
    s3 = boto3.client(
        's3',
        endpoint_url=r2_endpoint_url,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        config=Config(signature_version='s3v4')
    )

    try:
        print(f"Uploading {file_path} to R2 as {object_name}...")
        s3.upload_file(file_path, R2_BUCKET_NAME, object_name)
        print(f"Successfully uploaded {object_name} to {R2_BUCKET_NAME}!")
        return True
    except Exception as e:
        print(f"Upload failed: {e}")
        return False

if __name__ == "__main__":
    target_file = "cleaned_nhi_data_no_zero.csv"
    
    if os.path.exists(target_file):
        upload_to_r2(target_file, "latest_nhi_drug_list.csv")
    else:
        print(f"Target file {target_file} not found. Did the process_nhi step complete successfully?")
        exit(1)
