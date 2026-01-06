import pandas as pd
import boto3
from botocore.config import Config
import re
from sqlalchemy import text

import sys
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
engine = adqvest_db.db_conn()
import JobLogNew as log

import datetime    
from pytz import timezone
india_time = timezone('Asia/Kolkata')
today = datetime.datetime.now(india_time)
  
def run_program(run_by='Adqvest_Bot', py_file_name=None):    
    #****   Date Time *****
    
    # job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = ''
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        # AWS Configuration
        ACCESS_KEY_ID = 'AKIAYCVSRU2U3XP2DB75'
        ACCESS_SECRET_KEY = '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'
        BUCKET_NAME = 'adqvests3bucket'
        S3_PREFIX = "CARE_Ratings/"  
        S3_PREFIX_NEW1 = "CARE_RATINGS_CORPUS/" 
        S3_PREFIX_NEW2 = "CARE_RATINGS_CORPUS_TO_BE_CHUNKED/"
        s3 = boto3.resource("s3", aws_access_key_id=ACCESS_KEY_ID, aws_secret_access_key=ACCESS_SECRET_KEY, config=Config(signature_version="s3v4", region_name="ap-south-1"))

        bucket = s3.Bucket(BUCKET_NAME)

        query = "SELECT * FROM CARE_RATINGS_DAILY_DATA_CORPUS_NEW WHERE S3_Upload_Comments IS NULL OR S3_Upload_Comments != 'Rename Done';"
        raw_df = pd.read_sql(query, con=engine)
        raw_df = raw_df[40001:60000]
        print(len(raw_df))

        for index, row in raw_df.iterrows():
            old_file_name = row["File_Name"]
            new_file_name = row["Generated_File_Name"]

            old_s3_key = S3_PREFIX + old_file_name  # Old file path in S3
            new_s3_key = S3_PREFIX + new_file_name # New file path in S3
            new_s3_key1 = S3_PREFIX_NEW1 + new_file_name  # Moving file path in S3 Corpus
            new_s3_key2 = S3_PREFIX_NEW2 + new_file_name  # Moving file path in S3 Corpus To Be Chunked

            try:
                # Step 1: Copy old file with new name
                copy_source = {"Bucket": BUCKET_NAME, "Key": old_s3_key}
                bucket.copy(copy_source, new_s3_key)
                print(f"Renamed: {old_s3_key} → {new_s3_key}")

                # Step 2: Move new file to 2 other locations
                copy_source1 = {"Bucket": BUCKET_NAME, "Key": new_s3_key}
                bucket.copy(copy_source1, new_s3_key1)
                print(f"Moved: {new_s3_key1}")

                # copy_source1 = {"Bucket": BUCKET_NAME, "Key": new_s3_key}
                bucket.copy(copy_source1, new_s3_key2)
                print(f"Moved: {new_s3_key2}")

                # bucket.copy(Bucket=BUCKET_NAME, CopySource={"Bucket": BUCKET_NAME, "Key": new_s3_key}, Key=new_s3_key1)
                
                
                # bucket.copy(Bucket=BUCKET_NAME, CopySource={"Bucket": BUCKET_NAME, "Key": new_s3_key}, Key=new_s3_key2)
                # print(f"Moved: {new_s3_key2}")

                # Step 3: Delete old file
                bucket.Object(old_s3_key).delete()
                print(f"Deleted: {old_s3_key}")

                connection = engine.connect()
                query = text("UPDATE CARE_RATINGS_DAILY_DATA_CORPUS_NEW SET S3_Upload_Comments= 'Rename Done' WHERE File_Name = :old")
                connection.execute(query, {"old": old_file_name}) 
                print('Rename is executed') 

            except:
                print(f"Updating DB: {old_file_name} → Rename Failed") 
                connection = engine.connect()
                query = text("UPDATE CARE_RATINGS_DAILY_DATA_CORPUS_NEW SET S3_Upload_Comments = 'Rename Failed' WHERE File_Name = :old")
                connection.execute(query, {"old": old_file_name}) 
                print(query)     
       
        log.job_end_log(table_name,job_start_time,no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')        