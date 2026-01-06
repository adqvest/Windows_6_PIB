import boto3
from botocore.client import Config
import re

ACCESS_KEY_ID = 'AKIAYCVSRU2U3XP2DB75'
ACCESS_SECRET_KEY = '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'

bucket_name = 'adqvests3bucket'
folder_name = 'NSE_INVESTOR_INFORMATION_CORPUS_2/'

# Set up client
s3_client = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=ACCESS_SECRET_KEY,
    config=Config(signature_version='s3v4', region_name='ap-south-1')
)

pattern = re.compile(r'^(.*_)(\d+)(\.pdf)$')
continuation_token = None
all_keys = []

# Step 1: Collect all file keys (with pagination)
while True:
    if continuation_token:
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=folder_name,
            ContinuationToken=continuation_token
        )
    else:
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=folder_name
        )

    contents = response.get('Contents', [])
    all_keys.extend([obj['Key'] for obj in contents if not obj['Key'].endswith('/')])

    if response.get('IsTruncated'):
        continuation_token = response['NextContinuationToken']
    else:
        break

# Print total count
print(f"Total files found: {len(all_keys)}")

# # Step 2: Rename each file by inserting 'O' before numeric File_ID
# for old_key in all_keys:
#     filename = old_key.split('/')[-1]
#     match = pattern.match(filename)
#     break

#     if match:
#         new_filename = f"{match.group(1)}O{match.group(2)}{match.group(3)}"
#         new_key = folder_name + new_filename

#         print(f"Renaming: {old_key} → {new_key}")

#         # Copy to new key
#         s3_client.copy_object(
#             Bucket=bucket_name,
#             CopySource={'Bucket': bucket_name, 'Key': old_key},
#             Key=new_key
#         )

#         # Delete original key
#         s3_client.delete_object(Bucket=bucket_name, Key=old_key)
