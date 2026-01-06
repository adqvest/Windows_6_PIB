# # -*- coding: utf-8 -*-
# """
# Created on Wed Nov 23 08:57:39 2022

# @author: Abdulmuizz
# """
# import boto3
# from botocore.config import Config
# import pandas as pd
# import requests
# import os

# os.chdir("C:/Users/Administrator/AdQvestDir/codes/Amazon_Processing_Codes")

# final_df = pd.read_excel("C:/Users/Administrator/AdQvestDir/codes/Amazon_Processing_Codes/Analyst_Meet.xlsx")

# ACCESS_KEY_ID = 'AKIAYCVSRU2U3XP2DB75'
# ACCESS_SECRET_KEY = '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'
# BUCKET_NAME = 'adqvests3bucket'

# for idx, row in final_df[382:].iterrows():
    
#     print(idx)
    
#     r =  requests.get(row['File_Link'],headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.70"})
    
#     path = "C:/Users/Administrator/AdQvestDir/codes/Amazon_Processing_Codes/" + row['File_Name']
#     limit = 0
#     while True:
#         try:
#             with open(path,'wb') as f:
#                 f.write(r.content)
#                 f.close()
#             break
#         except:
#             limit += 1
#             if limit > 10:
#                 break

#     if limit > 10: 
#         continue
    
#     with open(path, 'rb') as data:
    
#         s3 = boto3.resource(
#             's3',
#             aws_access_key_id=ACCESS_KEY_ID,
#             aws_secret_access_key=ACCESS_SECRET_KEY,
#             config=Config(signature_version='s3v4',region_name = 'ap-south-1')
        
#         )
#         #Uploading the file to S3 bucket
#         s3.Bucket(BUCKET_NAME).put_object(Key='BSE_Analyst_Meet/'+row['File_Name'], Body=data)

    
#     os.remove(path)

import pandas as pd
import requests
url = "https://amc.ppfas.com/schemes/assets-under-management/2023/PPFAS_MF_Disclosure_of_monthly_Avg_AUM_April_2023.XLS"#?08052023"
r = requests.get(url)
print(r)
df = pd.read_excel(r.content)
print(df)
