import boto3
from botocore.config import Config
import os
from datetime import datetime, timezone
import pandas as pd
import datetime
from datetime import datetime
from datetime import timedelta






import sqlalchemy
import pandas as pd
from pandas.io import sql
import datetime as datetime
from pytz import timezone
import sys
import time
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import glob
import re
import psycopg2
from boto3.session import Session
import warnings
warnings.filterwarnings('ignore')
from dateutil.relativedelta import relativedelta
#%%
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import ClickHouse_db
import adqvest_db
import JobLogNew as log
import adqvest_s3

def run_program(run_by = 'Adqvest_Bot', py_file_name = None):
    # os.chdir('/home/ubuntu/AdQvestDir')
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    client = ClickHouse_db.db_conn()

    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday =  today - days

    job_start_time = datetime.datetime.now(india_time)
    table_name = "REPORT_SHARING"
    scheduler = ''
    no_of_ping = 0

    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]

    try :
        if(run_by == 'Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
            
        
#%%
        
        ACCESS_KEY_ID = adqvest_s3.s3_cred()[0]
        ACCESS_SECRET_KEY = adqvest_s3.s3_cred()[1]
        
        BUCKET_NAME = 'alt-data-report-4-thurro'

        s3 = boto3.resource(
            's3',
            aws_access_key_id=ACCESS_KEY_ID,
            aws_secret_access_key=ACCESS_SECRET_KEY,
            config=boto3.session.Config(signature_version='s3v4',region_name = 'ap-south-1')
        )
        session = Session(aws_access_key_id=ACCESS_KEY_ID, aws_secret_access_key=ACCESS_SECRET_KEY)
        s3 = session.resource('s3')
        my_bucket = s3.Bucket(BUCKET_NAME)
        file_name=[]
        file_name_clean=[]
        Relevant_Date=[]
        obj_link=[]
        sub_sec=[]
        runtime=[]
        
        # for obj in my_bucket.objects.filter(Prefix ='NIIF_PPT'):
        for obj in my_bucket.objects.all():
           
            file=obj.key.split("/")[1]
            print(file)
            if file=='ajim_premji_PPT.pptx':
                
                    file=obj.key.split("/")[1].split(".")[0]
                    file_name.append(file)
                    file2=obj.key.split("/")[1].split(".")[0].replace("_"," ")
                    file_name_clean.append(file2)
                    date_str=str(obj.last_modified)
                    date_rel=date_str.split("+")[0]
                    date=date_str.split(" ")[0]
                    run_time=datetime.datetime.strptime(date_rel, "%Y-%m-%d %H:%M:%S")
                    runtime.append(run_time)
                    Relevant_Date.append(date)
                    temp_link=obj.key.split("/")[1]
                    sub_sec.append('LIGHT HOUSE PPT')
                    link1="https://alt-data-report-4-thurro.s3.ap-south-1.amazonaws.com/ajim_premji_PPT/ajim_premji_PPT.pptx#toolbar=0"  
                    obj_link.append(link1)
                    try:
                        temp_link=temp_link.replace(" ","+")
                    except:
                        pass

           

                    
            
        
        df1=pd.DataFrame()
        df1['Report_Raw']=file_name
        df1['Report']=file_name_clean
        df1['Object_Link']=obj_link
        df1['Relevant_Date']=Relevant_Date
        df1['Runtime']=runtime
        df1['Runtime']=pd.to_datetime(df1['Runtime'])
        df1['Relevant_Date']=pd.to_datetime(df1['Relevant_Date'])
        print(df1)
        ######## converting GMT to IST #############
        # df1['Runtime']=df1['Runtime']+timedelta(seconds=0, minutes=30, hours=5)
    
        # wd = '/home/ubuntu/'
        # os.chdir(wd)
        # df2=pd.read_excel("thurro_pdf_reports_mapping.xlsx")
        # mapping = dict(df2[['Report_Raw', 'Sector']].values)
        # mapping2 = dict(df2[['Report_Raw', 'Sub_Sector']].values)
        # mapping3 = dict(df2[['Report_Raw', 'Extra_Tag']].values)

        df1['Sector'] ='LIGHT HOUSE PPT'
        df1['Sub_Sector'] =sub_sec
        df1['Extra_Tag'] =''

        df1 = df1.dropna(axis=0, subset=['Sector'])
        df1=df1[['Report_Raw','Report','Sector','Sub_Sector','Extra_Tag','Object_Link','Relevant_Date','Runtime']]
        df1['Report']=df1['Report'].apply(lambda x: x.upper())
        df1=df1.drop_duplicates(keep="first")
        
    ############ Establishing connection with PostGreSQL #########################
        postgres_test_con = psycopg2.connect(
        host="ec2-3-108-253-129.ap-south-1.compute.amazonaws.com",
        database="adqvest",
        user="postgres",
        password="@Thur&TPa@##123",
        port=5432)
        cursor_test = postgres_test_con.cursor()

        postgres_prod_con = psycopg2.connect(
        host="3.109.104.45",
        database="adqvest_thurro",
        user="postgres",
        password="@Thur&PRod@##123",
        port=5432)
        cursor_prod = postgres_prod_con.cursor()

        ############ deleting previous day's data ##########################
        sql1  = "delete from alt_data_daily_report_link where report in ('AJIM PREMJI PPT')"
        cursor_test.execute(sql1)
        #r=cursor_test.fetchall()
        postgres_test_con.commit()

        print("deleting previous production data ")
        sql_2  = "delete from alt_data_daily_report_link where report in ('AJIM PREMJI PPT')"
        cursor_prod.execute(sql_2)
        #r=cursor_test.fetchall()
        postgres_prod_con.commit()
        ###############################################
        ############ deleting previous day's data ##########################
        sql1  = "delete from alt_data_daily_report_link where report='AJIM PREMJI PPT'"
        cursor_test.execute(sql1)
        #r=cursor_test.fetchall()
        postgres_test_con.commit()

        print("deleting previous production data ")
        sql_2  = "delete from alt_data_daily_report_link where report='AJIM PREMJI PPT'"
        cursor_prod.execute(sql_2)
        #r=cursor_test.fetchall()
        postgres_prod_con.commit()
        ###############################################



        ############# Inserting lastest data ########################
        df2=df1
        df2['user_id_list']="{64,384}"
        df2['file_Type']='pptx'
        df2=df2[['Report_Raw','Report','Sector','Sub_Sector','Extra_Tag','Object_Link','Relevant_Date','Runtime','user_id_list','file_Type']]
        df_list = df2.values.tolist()
        sql2  ="""INSERT INTO alt_data_daily_report_link (report_raw,report, sector, sub_sector,extra_tag,report_link, relevant_date, runtime, user_id_list, file_Type) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        cursor_test.executemany(sql2,df_list)
        postgres_test_con.commit()
        cursor_test.rowcount
        
        # date_temp=df1['Relevant_Date'][0]
        print(df1['Relevant_Date'])
        print("Latest data inserted in Test")
        # print("Latest date:",date_temp)
        cursor_test.execute("ROLLBACK")
        postgres_test_con.commit()
        # cursor_test.commit()
        
        ############## inserting into production table #################
        print("Prodcution data insert")
        df3=df1
        df3['user_id_list']="{64,377}"
        df3['file_Type']='pptx'
        df3=df3[['Report_Raw','Report','Sector','Sub_Sector','Extra_Tag','Object_Link','Relevant_Date','Runtime','user_id_list','file_Type']]
        df_list = df3.values.tolist()
        sql3  ="""INSERT INTO alt_data_daily_report_link (report_raw,report, sector, sub_sector,extra_tag,report_link, relevant_date, runtime, user_id_list, file_Type) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        
        cursor_prod.executemany(sql3,df_list)
        postgres_prod_con.commit()
        cursor_prod.rowcount
        print("Latest data inserted in Production")
        # print("Latest date:",date_temp)

        log.job_end_log(table_name,job_start_time,no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        # error_msg = str(sys.exc_info()[1])
        exc_type, exc_obj, exc_tb = sys.exc_info()
        error_msg = str(sys.exc_info()[1]) + " line " + str(exc_tb.tb_lineno)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)


if(__name__=='__main__'):
    run_program(run_by='manual')
