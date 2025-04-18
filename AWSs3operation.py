from dotenv import load_dotenv
import os
import boto3
import pandas as pd
from sqlalchemy import create_engine


# Load environment variables from .env file
load_dotenv()
# Access credentials securely
aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
s3 = boto3.client('s3',aws_access_key_id=aws_access_key,aws_secret_access_key=aws_secret_key,
                    region_name='ap-south-1')

# first testing to list buckets
response = s3.list_buckets()
print([bucket['Name'] for bucket in response['Buckets']])
# Get the first (and only) bucket name
if response['Buckets']:
    mybucket_name = response['Buckets'][0]['Name']
    print(f"Bucket name: {mybucket_name}")
else:
    print("No buckets found.")

bucket_name = mybucket_name
#local_input_folder = './unzipped_folder'
#output_file_path = './output/unique_transformed_data.csv'

def upload_singlefile_to_s3(local_path, s3_path):
    try:
        s3.upload_file(local_path, bucket_name, s3_path)
        print(f"Uploaded {local_path} to s3://{bucket_name}/{s3_path}")
    except Exception as e:
        print(f"Error uploading {local_path}: {e}")

# Upload all files in a local folder to a specific S3 folder
def upload_multiplefilefromfolder_to_s3(local_folder_path, s3_folder_prefix):
    if not os.path.isdir(local_folder_path):
        print(f"Folder not found: {local_folder_path}")
        return
    for root, dirs, files in os.walk(local_folder_path):
        for filename in files:
            local_file_path = os.path.join(root, filename)
            relative_path = os.path.relpath(local_file_path, local_folder_path)
            s3_path = os.path.join(s3_folder_prefix, relative_path).replace("\\", "/")
            upload_singlefile_to_s3(local_file_path, s3_path)




