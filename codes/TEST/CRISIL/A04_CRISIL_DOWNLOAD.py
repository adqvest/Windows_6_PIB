import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import boto3
from botocore.config import Config
import re
from sqlalchemy import text
import requests
import sys
import time
import os
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

        query = "SELECT * FROM CRISIL_DAILY_DATA_CORPUS Where S3_Upload_Status is Null;"
        raw_df = pd.read_sql(query, con=engine)
        raw_df = raw_df[:800].reset_index(drop =True)
        print(len(raw_df))

        for index, row in raw_df.iterrows():
            old_file_name = row["Rating_File_Name"]
            new_file_name = row["Generated_File_Name"]
            link = row['Rating_File_Link']
            file_id = row['File_ID']
            print(file_id)
            try:
                
                r =  requests.get(link,verify = False,headers={"User-Agent": "XY"}, timeout = 60)
                if b'file not found' in r.content.lower():
                    connection = engine.connect()                
                    with engine.begin() as connection:  # Automatically handles commit/rollback
                        query = text("""
                            UPDATE CRISIL_DAILY_DATA_CORPUS 
                            SET S3_Upload_Comments = 'File Not Available' 
                            WHERE Rating_File_Link = :link 
                            AND Rating_File_Name = :old 
                            AND File_ID = :id 
                            AND Generated_File_Name = :new
                            LIMIT 1;
                        """)
                        
                        result = connection.execute(query, {"link": link, "old": old_file_name, "id": file_id,"new": new_file_name,})
                        print("Rows updated as File Not Available:", result.rowcount)  # To check if the update worked
                        print('_' * 80)
                    continue   
                # robot.add_link(url)
                time.sleep(1)
                save_path = r'C:\Users\Administrator\AdQvestDir\Junk_CRISIL'
                sanitized_file_name = new_file_name.replace('/', '_').replace('\\', '_').replace(':', '_').replace('*', '_').replace('?', '_').replace('"', '_').replace('<', '_').replace('>', '_').replace('|', '_')
                
                completeName = os.path.join(save_path, sanitized_file_name)
                print(completeName)
                # completeName = os.path.join(save_path, file_name)
                with open(completeName,'wb') as f:
                    os.chdir(r'C:\Users\Administrator\AdQvestDir\Junk_CRISIL')
                    f.write(r.content)
                    f.close()
                path = "C:/Users/Administrator/AdQvestDir/Junk_CRISIL/" + sanitized_file_name
                try:
                    # data =  open(path, 'rb')
                    ACCESS_KEY_ID = 'AKIAYCVSRU2U3XP2DB75'
                    ACCESS_SECRET_KEY = '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'
                    BUCKET_NAME = 'adqvests3bucket'

                    s3 = boto3.resource('s3', aws_access_key_id=ACCESS_KEY_ID, aws_secret_access_key=ACCESS_SECRET_KEY, config=Config(signature_version='s3v4',region_name = 'ap-south-1'))
                    
                    # #Uploading the file to S3 bucket
                    # with open(path, 'rb') as data:
                    #     s3.Bucket(BUCKET_NAME).put_object(Key='CRISIL_Ratings/'+sanitized_file_name, Body=data)
                    # print('Done for CRISIL_Ratings')

                    with open(path, 'rb') as data:
                        s3.Bucket(BUCKET_NAME).put_object(Key='CRISIL_RATINGS_CORPUS/'+sanitized_file_name, Body=data)
                    print('Done for CRISIL_RATINGS_CORPUS')
                    
                    # For the third upload - open fresh again
                    with open(path, 'rb') as data:
                        s3.Bucket(BUCKET_NAME).put_object(Key='CRISIL_RATINGS_CORPUS_TO_BE_CHUNKED/'+sanitized_file_name, Body=data)
                    print('Done for CRISIL_RATINGS_CORPUS_TO_BE_CHUNKED')       
                    
                    connection = engine.connect()                
                    with engine.begin() as connection:  # Automatically handles commit/rollback
                        query = text("""
                            UPDATE CRISIL_DAILY_DATA_CORPUS 
                            SET S3_Upload_Status= 'Done' 
                            WHERE Rating_File_Link = :link 
                            AND Rating_File_Name = :old 
                            AND File_ID = :id 
                            AND Generated_File_Name = :new
                            LIMIT 1;
                        """)
                        
                        result = connection.execute(query, {"link": link, "old": old_file_name, "id": file_id,"new": new_file_name,})
                        print("Rows updated as Done:", result.rowcount)  # To check if the update worked
                        print('S3 Updated')
                        print('_' * 80)

                except:
                    print(f"Updating DB: {new_file_name} → Download Failed") 
                    with engine.begin() as connection:  # Automatically handles commit/rollback
                        query = text("""
                            UPDATE CRISIL_DAILY_DATA_CORPUS 
                            SET S3_Upload_Comments = 'Failed' 
                            WHERE Rating_File_Link = :link 
                            AND Rating_File_Name = :old 
                            AND File_ID = :id 
                            AND Generated_File_Name = :new
                            LIMIT 1;
                        """)
                        
                        result = connection.execute(query, {"link": link, "old": old_file_name, "id": file_id,"new": new_file_name,})
                        print("Rows updated as Failed:", result.rowcount)
                        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
                        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
                        print(error_msg) 
                        print('_' * 80)
                os.remove(path)
            except:
                error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
                error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
                print(error_msg) 
                   
        log.job_end_log(table_name,job_start_time,no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')        