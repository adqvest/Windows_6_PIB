import sqlalchemy
import pandas as pd
from pandas.io import sql
import calendar
import os
import requests
from bs4 import BeautifulSoup
from dateutil import parser
from time import sleep
import re
import datetime as datetime
from pytz import timezone
from dateutil.relativedelta import relativedelta
import numpy as np
import sys
import time
import time
import zipfile
import io
import warnings
warnings.filterwarnings('ignore')

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import dbfunctions
import adqvest_db
import JobLogNew as log
import adqvest_s3
import ClickHouse_db

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    client = ClickHouse_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = ''
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)


        # years=['2013','2014','2015','2016','2017','2018','2019','2020','2021','2022','2023']
        # years=['2017']
        # years=['2018','2017']
        table_name='US_FDA_WEEKLY_MONTHLY_DATA'

        max_rel_date = client.execute(f"select max(Relevant_Date) from AdqvestDB.{table_name}")
        max_rel_date = str([a_tuple[0] for a_tuple in max_rel_date][0])

        max_rel_date=pd.to_datetime(max_rel_date).date()

        latest_date=max_rel_date+relativedelta(months=1)
        month=latest_date.month
        year=str(latest_date.year)

        if month <9:
            mon='0'+str(month)

       
        url=f"https://datadashboard.fda.gov/content/sdata/Shipment-Details-{year}-{mon}.zip"
        os.chdir('C:/Users/Administrator/Pharma//')
        for file in os.listdir():
            if (file.endswith('.zip') or file.endswith('.csv')):
                os.remove(file)

        r=requests.get(url,verify=False)

        if r.status_code==404:
            print("Data not yet come")
            pass
        else:

            z = zipfile.ZipFile(io.BytesIO(r.content))
            z.extractall()
            files=z.filelist

            file_list=[]
            final_df=pd.DataFrame()
            for file in files:
                file=str(file)
                file_new=str(file.split("compress")[0].split("=")[1].replace("'",""))
                # print(file_new)
                file_list.append(file_new.strip())
                df_file=pd.DataFrame(file_list,columns=['File_Name'])

                df_file['Temp_Name']=df_file['File_Name'].str.split("-")
                df_file['Temp_Name']=df_file['File_Name'].apply(lambda x:x.split("-")[1].split(".")[0].strip())
                df_file['Temp_Name']=f"{year} W"+df_file['Temp_Name']

                df_file['Relevant_Date']=df_file['Temp_Name'].apply(lambda x:datetime.datetime.strptime(x + '-1', "%Y W%W-%u")-days)

                del df_file['Temp_Name']

                df_file['Relevant_Date']=df_file['Relevant_Date'].apply(lambda x:x.date())

            for i, row in df_file.iterrows():
            

                os.chdir('C:/Users/Administrator/Pharma//')
                file=row['File_Name']
                print(file)
                rel_date=row['Relevant_Date']
                print(rel_date)
                
                file_new='Pharma_'+str(rel_date).replace('-',"_")+'.csv'
                os.rename(file,file_new)
                try:
                    data=pd.read_csv(file_new)

                except:
                    try:
                        data=pd.read_csv(file_new,encoding='latin-1')
                    except:
                        data=pd.read_csv(file_new,header=0,engine='python',error_bad_lines=False)    
                    
                key = 'PHARMA/'
                # os.chdir('C:/Users/Administrator/Pharma//')
                wd=os.getcwd()
                filepath = wd +'\\'+ file_new
                dbfunctions.to_s3bucket(filepath, key)

                
                df1=data.copy()
                cols=list(data.columns)
                columns=[col.replace("/","_").replace(" ","_") for col in cols]
                df1.columns=columns
                date_columns=['Arrival_Date','Submission_Date','Submission_Date','Final_Disposition_Activity_Date']
                for col in date_columns:
                    try:
                        df1[col]=df1[col].apply(lambda x:datetime.datetime.strptime(x,"%Y/%m/%d %H:%M:%S").date())
                    except:
                        try:
                            df1[col]=df1[col].apply(lambda x:datetime.datetime.strptime(x,"%m/%d/%Y %H:%M").date())
                        except:
                            pass

                df1['Relevant_Date']=rel_date
                df1['Runtime']=datetime.datetime.now()

                df1[['Relevant_Date','Final_Disposition_Activity_Date','Submission_Date','Arrival_Date']]=df1[['Relevant_Date','Final_Disposition_Activity_Date','Submission_Date','Arrival_Date']].apply(lambda x:pd.to_datetime(x, format ='%Y-%m-%d' ).dt.date)

                for i in ['Relevant_Date','Final_Disposition_Activity_Date','Submission_Date','Arrival_Date']:
                    
                    df1[i] = df1[i].astype(object).where(df1[i].notnull(), None)
                    
                for i in df1.columns:
                    if i not in ['Relevant_Date','Runtime','Final_Disposition_Activity_Date','Submission_Date','Arrival_Date']:
                        df1[i] = np.where(df1[i].isna(), None, df1[i])
                        
                for i in df1.columns:
                    if i not in ['Relevant_Date','Runtime','Final_Disposition_Activity_Date','Submission_Date','Arrival_Date']:
                #         df1[i] = np.where(df1[i].isna(), None, df1[i])
                        df1[i]=df1[i].astype(str)  
                    
                for i in df1.columns:
                    if i not in ['Relevant_Date','Runtime','Final_Disposition_Activity_Date','Submission_Date','Arrival_Date']:
                        df1[i] = np.where(df1[i]=='None', None, df1[i]) 


                final_df=pd.concat([final_df,df1])
                final_df=final_df[['Final_Disposition_Activity_Date','Entry_DOC_Line', 'Arrival_Date', 'Submission_Date',
                   'Port_of_Entry_Distrct_Abrvtn', 'Country_Of_Origin', 'Product_Code',
                   'Product_Code_Description', 'Manufacturer_FEI_Number',
                   'Manufacturer_Legal_Name', 'Manufacturer_Line1_Address',
                   'Manufacturer_Line2_Address', 'Manufacturer_City_Name',
                   'Manufacturer_ISO_Country_Code', 'Filer_FEI_Number', 'Filer_Legal_Name',
                   'Filer_Line1_Address', 'Filer_Line2_Address', 'Filer_City_Name',
                   'Filer_State_Code', 'Filer_County_Code', 'Filer_Zip_Code',
                   'Filer_ISO_Country_Code', 'Final_Disposition_Activity_Description','Relevant_Date', 'Runtime']]


            ''' splitting the dataframe before pushing as it is large '''
            print("Uploading into Clickhouse")
            for i in range(0,len(final_df),100000):
                j=i+100000
                df=final_df[i:j]
                client.execute("INSERT INTO US_FDA_WEEKLY_MONTHLY_DATA VALUES",df.values.tolist()) 
                time.sleep(10)

            # for i in range(0,len(final_df),100000):
            #     j=i+100000
            #     df=final_df[i:j]
                

            #     engine = adqvest_db.db_conn()

            #     df.to_sql(name = "US_FDA_WEEKLY_MONTHLY_DATA",if_exists = 'append',con = engine,index = False)
            #     time.sleep(5)

            #     engine = adqvest_db.db_conn()
            #     df.to_sql(name = "US_FDA_WEEKLY_MONTHLY_DATA_Temp_Rahul",if_exists = 'append',con = engine,index = False)
            #     time.sleep(5)

            #     engine = adqvest_db.db_conn()    
            #     query = 'select * from AdqvestDB.US_FDA_WEEKLY_MONTHLY_DATA_Temp_Rahul'
            #     df = pd.read_sql(query,engine)
            #     time.sleep(10)
                
            #     client.execute("INSERT INTO US_FDA_WEEKLY_MONTHLY_DATA VALUES",df.values.tolist()) 
            #     time.sleep(10)   
            #     query =  "Delete From US_FDA_WEEKLY_MONTHLY_DATA_Temp_Rahul"
            #     connection.execute(query)
            #     connection.execute('commit')
            #     time.sleep(20)
        log.job_end_log(table_name, job_start_time, no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'", str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name, job_start_time, error_type, error_msg, no_of_ping)
if (__name__ == '__main__'):
    run_program(run_by='manual')
