
import os
import re
import boto3
from pytz import timezone
import datetime as datetime
import pandas as pd
import requests
import time

import warnings
warnings.filterwarnings('ignore')

from botocore.config import Config
from botocore.exceptions import NoCredentialsError

import sys
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log

engine = adqvest_db.db_conn()

from clickhouse_driver import Client
host = 'ec2-52-88-156-240.us-west-2.compute.amazonaws.com'
user_name = 'default'
password_string = 'Clickhouse@2024'
db_name = 'AdqvestDB'
client = Client(host, user=user_name, password=password_string, database=db_name)

os.environ["VOYAGE_API_KEY"] = "pa-97JpiKNlfEEkhs8dBSZOloXM7aw3BI3NJ0ra-zaCSIk"

def upload_to_s3(url,filename):
   
    os.chdir(r'C:\Users\Administrator\AdQvestDir\Junk_Care')
    print(url)
    r = requests.get(url)
    if b"not found" in r.content.lower():
            print("String NOT FOUND exists in response")
            return 'File Not Available'
    with open(filename, 'wb') as f:
        f.write(r.content)
        f.close()
    if r.status_code == 200:
        with open('C:/Users/Administrator/AdQvestDir/Junk_Care/'+filename, 'wb') as file:
            file.write(r.content)
            print("PDF downloaded successfully.")
            
        file_size = os.path.getsize('C:/Users/Administrator/AdQvestDir/Junk_Care/'+filename)
        size_threshold = 1 * 1024  # 1 KB in bytes

        if file_size > size_threshold:
            print(f"File size ({file_size} bytes) is greater than 1 KB; proceeding with S3 upload.")

            ACCESS_KEY_ID = 'AKIAYCVSRU2U3XP2DB75'
            ACCESS_SECRET_KEY = '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'
            BUCKET_NAME = 'adqvests3bucket'
            
            key = 'CARE_RATINGS_CORPUS/'
            s3 = boto3.resource('s3', aws_access_key_id=ACCESS_KEY_ID, aws_secret_access_key=ACCESS_SECRET_KEY,
                config=Config(signature_version='s3v4', region_name='ap-south-1'))
            
            data_s3 =  open(filename, 'rb')
            try:
                s3.Bucket(BUCKET_NAME).put_object(Key=key+filename, Body=data_s3)
                print("RAN TILL HERE")
            except NoCredentialsError:
                print("Credentials not available")
            print('Done for CARE_RATINGS_CORPUS')
            
            key_2='CARE_RATINGS_CORPUS_TO_BE_CHUNKED/'
            s3 = boto3.resource('s3', aws_access_key_id=ACCESS_KEY_ID, aws_secret_access_key=ACCESS_SECRET_KEY,
                config=Config(signature_version='s3v4', region_name='ap-south-1')
                )
            data_s3 =  open(filename, 'rb')
            try:
                s3.Bucket(BUCKET_NAME).put_object(Key=key_2+filename, Body=data_s3)
            except NoCredentialsError:
                print("Credentials not available")
            print('Done for CARE_RATINGS_CORPUS_TO_BE_CHUNKED')
            return 'Done'
        else:
            return 'SIZE ISSUE'
    else:
        return 'SITE ISSUE'

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    # os.chdir('C:/Users/Administrator/AdQvestDir')
    engine = adqvest_db.db_conn()
    # ****   Date Time *****
    india_time = timezone('Asia/Kolkata')

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = ''
    if (py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if (run_by == 'Adqvest_Bot'):
            log.job_start_log_by_bot(table_name, py_file_name, job_start_time)
        else:
            log.job_start_log(table_name, py_file_name, job_start_time, scheduler)
          
        links_df = pd.read_sql("select * from CARE_RATINGS_DAILY_DATA_CORPUS where Chunking_Status = 'ISSUE';",engine)
        links_df = links_df.reset_index(drop=True)
        # links_df = links_df[:600]
        print(len(links_df))
        
        for x in range(len(links_df)):
            file_name = str(links_df.loc[x]["Generated_File_Name"]) 
            print(file_name)
            filepath = os.path.join(r'C:\Users\Administrator\AdQvestDir\Junk_Care', file_name)
            print(filepath)
            file_url = str(links_df.loc[x]["File_Link"])
            try:
                status_val=upload_to_s3(file_url, file_name)
            except:
                status_val='TIMEOUT ISSUE'
                
            try:
                if status_val=='Done':
                    query = f'Update CARE_RATINGS_DAILY_DATA_CORPUS set S3_Upload_Status="Done" where Generated_File_Name="{links_df.loc[x]["Generated_File_Name"]}" and File_ID={int(links_df.loc[x]["File_ID"])}'
                    connection.execute(query)
                    time.sleep(2)

                    query = f'Update CARE_RATINGS_DAILY_DATA_CORPUS set Chunking_Status=Null where Generated_File_Name="{links_df.loc[x]["Generated_File_Name"]}" and File_ID={int(links_df.loc[x]["File_ID"])}'
                    connection.execute(query)
                    time.sleep(2)
                    print('Done updates')
                else:
                    query = f'Update CARE_RATINGS_DAILY_DATA_CORPUS set S3_Upload_Status="{status_val}" , Chunking_Status = Null  where Generated_File_Name="{links_df.loc[x]["Generated_File_Name"]}" and File_ID={int(links_df.loc[x]["File_ID"])}'
                    connection.execute(query)
                    time.sleep(2)
                    print(status_val)
            except:
                engine = adqvest_db.db_conn()
                connection = engine.connect()

                if status_val=='Done':
                    query = f'Update CARE_RATINGS_DAILY_DATA_CORPUS set S3_Upload_Status="Done" where Generated_File_Name="{links_df.loc[x]["Generated_File_Name"]}" and File_ID={int(links_df.loc[x]["File_ID"])}'
                    connection.execute(query)
                    time.sleep(2)

                    query = f'Update CARE_RATINGS_DAILY_DATA_CORPUS set Chunking_Status=Null where Generated_File_Name="{links_df.loc[x]["Generated_File_Name"]}" and File_ID={int(links_df.loc[x]["File_ID"])}'
                    connection.execute(query)
                    time.sleep(2)
                    print('Done updates')
                else:
                    query = f'Update CARE_RATINGS_DAILY_DATA_CORPUS set S3_Upload_Status="{status_val}", Chunking_Status = Null where Generated_File_Name="{links_df.loc[x]["Generated_File_Name"]}" and File_ID={int(links_df.loc[x]["File_ID"])}'
                    connection.execute(query)
                    time.sleep(2)
                    print(status_val)

            # os.remove(file_name)
            if os.path.exists(filepath):
                os.remove(filepath)
            else:
                print(f"The file {file_name} does not exist.")     
                
        log.job_end_log(table_name, job_start_time, no_of_ping)    
    except Exception as e:
         error_type = str(re.search("'(.+?)'", str(type(e))).group(1))
         error_msg = str(e) + " line " + str(sys.exc_info()[2].tb_lineno)
         print(error_type)
         print(error_msg)

         log.job_error_log(table_name, job_start_time, error_type, error_msg, no_of_ping)

if __name__ == '__main__':
     run_program(run_by='manual')