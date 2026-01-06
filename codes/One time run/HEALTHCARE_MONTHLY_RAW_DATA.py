import requests
import sys
import warnings
import os
import time
from bs4 import BeautifulSoup
import numpy as np
import re
from pytz import timezone
from pandas.io import sql
from calendar import monthrange
import datetime as datetime
import pandas as pd
import sqlalchemy
import win32com.client as win32
import boto3
from botocore.config import Config
warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log


def run_program(run_by='Adqvest_Bot', py_file_name=None):
    engine = adqvest_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    job_start_time = datetime.datetime.now(india_time)
    table_name = 'HEALTHCARE_MONTHLY_RAW_DATA'
    if (py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
        links = pd.read_sql(
            'select File_Name_Ref,Relevant_Date,Link from AdqvestDB.HEALTHCARE_MONTHLY_RAW_DATA_FILES_LINKS where Relevant_Date>"2017-03-31" and Status is NULL and Download_Status is not NULL order by Relevant_Date',
            engine)
        links['File_Name_Ref'] = links['File_Name_Ref'].apply(lambda x: x + ".xls")
        if (links.empty):
            print('no new data')
            pass
        else:
            count=0
            os.chdir('C:\\Users\\Administrator\\AdQvestDir\\Junk folder\\')
            for a, values in links.iterrows():
                file = values['File_Name_Ref']
                date = values['Relevant_Date']
                link = values['Link']
                state = values['File_Name_Ref'].split('_')[2]
                district = values['File_Name_Ref'].split('_')[3].strip('.xls')
                access_credentials = pd.read_sql("select * from AWS_S3_ACCESS_CREDENTIALS", engine)
                ACCESS_KEY_ID = access_credentials["Acess_Key_ID"][0]
                ACCESS_SECRET_KEY = access_credentials["Access_Secret_Key"][0]
                file_name = file
                s3 = boto3.resource(
                    's3',
                    aws_access_key_id=ACCESS_KEY_ID,
                    aws_secret_access_key=ACCESS_SECRET_KEY,
                    config=Config(signature_version='s3v4', region_name='ap-south-1')
                )
                s3.Bucket('adqvests3bucket').download_file('HEALTHCARE_DATA/' + file_name, file_name)
                fname = 'C:\\Users\\Administrator\\AdQvestDir\\Junk folder\\' + file
                df = pd.read_html(file, header=[1])[0]
                df1 = df.iloc[1:, :3]
                df1.columns = ['Department', 'Report_Index', 'Description']
                df1['Department'] = df1['Department'].fillna(method='ffill')
                df1['Department'] = df1['Department'].apply(lambda x: re.search(r'\[.+?\]', x).group(0).strip('[]'))
                df1.reset_index(drop=True, inplace=True)
                desc_new = []
                desc_table = pd.read_sql('select * from AdqvestDB.HEALTHCARE_MONTHLY_RAW_DATA_DESC', engine)
                for i in range(df1.shape[0]):
                    if desc_table['Description'].isin([df1['Description'].iloc[i]]).any() == False:
                        desc_new.append(i)
                if len(desc_new) > 0:
                    df_desc['Description'].iloc[desc_new].to_sql("AdqvestDB.HEALTHCARE_MONTHLY_RAW_DATA_DESC",index=False, if_exists='append', con=engine)
                df_index = df1.merge(desc_table)
                df_index.drop('Description', axis=1, inplace=True)
                df_index.rename(columns={'Index_Nos': 'Description'}, inplace=True)
                s = pd.Series(df.columns)
                sub_district = s.replace(to_replace=r'\d+', value=np.nan, regex=True)
                sub1 = sub_district.fillna(method='ffill')[9:]
                dist_list = sub1.unique().tolist()
                df2 = df.iloc[:, 9:]
                df2.columns = sub1
                hmis = pd.DataFrame()
                final_df = pd.DataFrame()
                for i in dist_list:
                    dummy = df2.filter(like=i, axis=1)
                    dummy = dummy.iloc[1:]
                    if dummy.shape[1] > 5:
                        dummy = dummy.iloc[:, :5]
                    dummy.columns = ['Total', 'Public', 'Private', 'Urban', 'Rural']
                    dummy['Sub_District'] = i
                    hmis = hmis.append(dummy, ignore_index=True)
                    c = hmis[hmis['Sub_District'] == i]
                    c.reset_index(drop=True, inplace=True)
                    final_dummy = pd.concat([c, df_index], axis=1)
                    final_df = final_df.append(final_dummy)
                    final_df.reset_index(drop=True, inplace=True)
                final_df['State'] = state
                final_df['District'] = district
                final_df['Relevant_Date'] = date
                os.rename(fname, fname.strip('.xls') + ".txt")
                f = open(fname.strip('.xls') + ".txt", "r")
                with open(fname.strip('.xls') + ".txt") as f:
                    published_date = re.search(r'Status As On\:\s\d+\s\w+\s\d+', f.read()).group(0)
                    published_date = published_date.split(':')[1].split(',')[0].strip(' ')
                pub_date = datetime.datetime.strptime(published_date, "%d %b %Y").date()
                os.remove(fname.strip('.xls') + ".txt")
                final_df['Published_Date'] = pub_date
                final_df = final_df[
                    ['State', 'District', 'Sub_District', 'Department', 'Report_Index', 'Description', 'Total',
                     'Public', 'Private', 'Urban', 'Rural', 'Relevant_Date', 'Published_Date']]
                final_df.to_sql("HEALTHCARE_MONTHLY_RAW_DATA", index=False, if_exists='append', con=engine)
                print(final_df.head(2))
                connection = engine.connect()
                connection.execute("UPDATE AdqvestDB.HEALTHCARE_MONTHLY_RAW_DATA_FILES_LINKS set Status='Data Scrapped', Scrapped_Time=NOW() where Link=%s",link)
            count=count+final_df.shape[0]
            print("No of rows inserted :",count)
        log.job_end_log(table_name, job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'", str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])
        print(error_msg)
        a=input()
        print(a)
        connection = engine.connect()
        connection.execute("UPDATE AdqvestDB.HEALTHCARE_MONTHLY_RAW_DATA_FILES_LINKS set Status='No' where Link=%s",link)
        log.job_error_log(table_name, job_start_time, error_type, error_msg, no_of_ping)


if __name__ == '__main__':
    run_program(run_by='manual')
