import boto3
import os
import glob

# ---- Configuration ----
BUCKET_NAME = "judgements-integration-sample-bucket"   # Replace with your bucket
PREFIX = "processing"              # Files starting with this

# Create S3 client (uses AWS creds from env vars or ~/.aws/credentials)
s3 = boto3.client(
    's3'
)
def upload_processing_files():
    # Find files starting with "processing"
    files = glob.glob(f"{PREFIX}*")

    if not files:
        print("No files found.")
        return

    for file_path in files:
        file_name = os.path.basename(file_path)  # key in S3
        year = file_name.replace(".jsonl", '').split('_')[-1]
        key = key = f'logs/year={year}/court=36_29/{file_name}'
        print(key)
        try:
            s3.upload_file(file_path, BUCKET_NAME, key)
            print(f"✅ Uploaded {file_name} to s3://{BUCKET_NAME}/{file_name}")
        except Exception as e:
            print(f"❌ Failed to upload {file_name}: {e}")

if __name__ == "__main__":
    upload_processing_files()
