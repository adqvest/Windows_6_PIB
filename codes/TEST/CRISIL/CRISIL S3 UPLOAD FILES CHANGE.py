import boto3
from botocore.client import Config
import re

ACCESS_KEY_ID = 'AKIAYCVSRU2U3XP2DB75'
ACCESS_SECRET_KEY = '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'

bucket_name = 'adqvests3bucket'
folder_name = 'CRISIL_RATINGS_CORPUS/'

# Set up client
s3_client = boto3.client('s3', aws_access_key_id=ACCESS_KEY_ID, aws_secret_access_key=ACCESS_SECRET_KEY, config=Config(signature_version='s3v4',region_name='ap-south-1'))

pattern = re.compile(r'^(.*_)(\d+)(\.(?:pdf|html))$')
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

# Step 2: Rename files within the same double-slash folder
pattern = re.compile(r'^(.*_)(\d+)(\.(?:pdf|html))$')
rename_plan = []
conflicts = []
idk = []
already_has_b = []

for old_key in all_keys:
    filename = old_key.split('/')[-1]
    
    if 'B' in filename:
       already_has_b.append(old_key)
       continue
    
    match = pattern.match(filename)
    
    if match:
        new_filename = f"{match.group(1)}B{match.group(2)}{match.group(3)}"
        new_key = f"CRISIL_RATINGS_CORPUS/{new_filename}"  
        
        # Check if new key already exists
        if new_key in all_keys:
            conflicts.append((old_key, new_key))
        elif new_key not in all_keys:
            rename_plan.append((old_key, new_key))
        else:
            idk.append((old_key, new_key))
            

print(f"Files to rename: {len(rename_plan)}")
print(f"Conflicts (already exist): {len(conflicts)}")

if conflicts:
    print("\nSample conflicts:")
    for old_key, new_key in conflicts[:5]:
        print(f"  {old_key} → {new_key} (ALREADY EXISTS)")

# Execute renames
print(f"\nReady to rename {len(rename_plan)} files")
# renamed_count = 0
# failed_renames = []

# for i, (old_key, new_key) in enumerate(rename_plan):
#     try:
#         print(f"Renaming: {old_key} → {new_key}")
        
#         # Copy to new key (within same folder structure)
#         s3_client.copy_object(Bucket=bucket_name,CopySource={'Bucket': bucket_name, 'Key': old_key},
#             Key=new_key)
        
#         # Delete original key
#         s3_client.delete_object(Bucket=bucket_name, Key=old_key)
        
#         renamed_count += 1
        
#         # Progress update
#         if (i + 1) % 100 == 0 or (i + 1) == len(rename_plan):
#             print(f"Progress: {i + 1}/{len(rename_plan)} files renamed")
            
#     except Exception as e:
#         print(f"Failed to rename {old_key}: {str(e)}")
#         failed_renames.append((old_key, str(e)))

# print("\nRename Summary:")
# print(f"Successfully renamed: {renamed_count} files")
# print(f"Failed renames: {len(failed_renames)} files")