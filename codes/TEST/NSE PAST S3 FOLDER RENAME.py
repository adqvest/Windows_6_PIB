import boto3
from botocore.client import Config
import re

ACCESS_KEY_ID = 'AKIAYCVSRU2U3XP2DB75'
ACCESS_SECRET_KEY = '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'

bucket_name = 'adqvests3bucket'
folder_name = 'NSE_INVESTOR_INFORMATION_CORPUS_3/'

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

double_slash_keys = []
single_slash_keys = []

for key in all_keys:
    if key.startswith('NSE_INVESTOR_INFORMATION_CORPUS_3//'):
        double_slash_keys.append(key)
    elif key.startswith('NSE_INVESTOR_INFORMATION_CORPUS_3/') and not key.startswith('NSE_INVESTOR_INFORMATION_CORPUS_3//'):
            single_slash_keys.append(key)

# Step 2: Rename files within the same double-slash folder
pattern = re.compile(r'^(.*_)(\d+)(\.pdf)$')
rename_plan = []
conflicts = []
already_has_o_or_p = []

# # Filter to only double-slash folder files
# single_slash_keys = [key for key in all_keys if key.startswith('NSE_INVESTOR_INFORMATION_CORPUS_2/')]
# print(f"Files in double-slash folder: {len(single_slash_keys)}")
count = 0
for old_key in single_slash_keys:
    filename = old_key.split('/')[-1]
    
    if 'P' in filename:
       already_has_o_or_p.append(old_key)
       continue
    
    match = pattern.match(filename)
    
    if match:
        new_filename = f"{match.group(1)}P{match.group(2)}{match.group(3)}"
        new_key = f"NSE_INVESTOR_INFORMATION_CORPUS_3/{new_filename}"  # Keep double slash
        
        # Check if new key already exists
        if new_key in all_keys:
            conflicts.append((old_key, new_key))
        else:
            rename_plan.append((old_key, new_key))
    count+=1   
    print(count) 
    print('_'*30)
print(f"Files to rename: {len(rename_plan)}")
print(f"Conflicts (already exist): {len(conflicts)}")

if conflicts:
    print("\nSample conflicts:")
    for old_key, new_key in conflicts[:5]:
        print(f"  {old_key} → {new_key} (ALREADY EXISTS)")

# Execute renames
print(f"\nReady to rename {len(rename_plan)} files within double-slash folder")
# print("Type 'RENAME' to proceed:")
# confirmation = input()

# if confirmation == 'RENAME':
# renamed_count = 0
# failed_renames = []

# for i, (old_key, new_key) in enumerate(rename_plan):
#     try:
#         print(f"Renaming: {old_key} → {new_key}")
        
#         # Copy to new key (within same folder structure)
#         s3_client.copy_object(
#             Bucket=bucket_name,
#             CopySource={'Bucket': bucket_name, 'Key': old_key},
#             Key=new_key
#         )
        
#         # Delete original key
#         s3_client.delete_object(Bucket=bucket_name, Key=old_key)
        
#         renamed_count += 1
        
#         # Progress update
#         if (i + 1) % 100 == 0 or (i + 1) == len(rename_plan):
#             print(f"Progress: {i + 1}/{len(rename_plan)} files renamed")
            
#     except Exception as e:
#         print(f"Failed to rename {old_key}: {str(e)}")
#         failed_renames.append((old_key, str(e)))

# print(f"\nRename Summary:")
# print(f"Successfully renamed: {renamed_count} files")
# print(f"Failed renames: {len(failed_renames)} files")
    
# else:
#     print("Rename cancelled.")  