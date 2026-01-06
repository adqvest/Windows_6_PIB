from bs4 import BeautifulSoup
import PyPDF2
import pandas as pd
import numpy as np
import requests
import datetime as datetime
from pytz import timezone
import warnings
warnings.filterwarnings('ignore')
from pandas.core.common import flatten
import os
import re
import csv
import time
import io
import os
#os.chdir(r'D:\Adqvest\ncdex')
import sqlalchemy
import sys
import boto3
from botocore.config import Config
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import JobLogNew as log
import adqvest_db
import adqvest_s3
#import textract

def findbestlongform(short,long):
    sindex=len(short)-1
    lindex=len(long)-1
    long=long.lower()
    short=short.lower()
    match=[]
    for i in range(0,sindex+1):
        curchar=short[sindex]
        while (lindex>=0 and long[lindex]!=curchar) or (sindex==0 and lindex>0 and long[lindex-1].isspace()==False):
            lindex-=1
        if lindex<0:
            return ''
        lindex-=1
        sindex-=1
    return long[lindex:]


def get_match(pdf_file):
    try:
        pdfFileObject = open(pdf_file, 'rb')

        pdfReader = PyPDF2.PdfFileReader(pdfFileObject)

        print(" No. Of Pages :", pdfReader.numPages)
        a=str()
        for i in range(pdfReader.numPages):
            pageObject = pdfReader.getPage(i)
            a=a+pageObject.extractText()

        pdfFileObject.close()

    except:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36'}
        r = requests.get(pdf_file,headers=headers)

        with open('1.html', 'wb') as f:
            f.write(r.content)
        a = textract.process('1.html')
        a = a.decode("utf-8")
        print('Ok')
        os.remove('1.html')

    a=a.replace('\n','').replace(r'\s\s+',' ')
    short_forms=re.findall(r'\((.*?)\)', a)

    final_short_forms=[x for x in short_forms if x.isupper()]
    final_short_forms=list(set(final_short_forms))
    final_short_forms=[x for x in final_short_forms if len(x)>2]

    long_forms=[a[a.find('('+x+')')-100:a.find('('+x+')')-1] for x in  final_short_forms]
    df=pd.DataFrame()
    df['Short_Form']=final_short_forms
    df['Long_Form']=long_forms
    df['Short_Form']=df['Short_Form'].str.strip()
    df['Long_Form']=df['Long_Form'].str.strip()
    values=[]
    for i,j in df.iterrows():
        values.append(findbestlongform(j['Short_Form'],j['Long_Form']))

    df['Match']=values

    return df


def run_program(run_by='Adqvest_Bot', py_file_name=None):

    engine = adqvest_db.db_conn()
    connection = engine.connect()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'ABBREVIATION_LOGGER_Temp_Abdul'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        super_final_df=pd.DataFrame()

        #access_credentials = pd.read_sql("select * from AWS_S3_ACCESS_CREDENTIALS", engine)
        #ACCESS_KEY_ID = access_credentials["Acess_Key_ID"][0]
        #ACCESS_SECRET_KEY = access_credentials["Access_Secret_Key"][0]
        ACCESS_KEY_ID = adqvest_s3.s3_cred()[0]
        ACCESS_SECRET_KEY = adqvest_s3.s3_cred()[1]
        s3 = boto3.resource(
            's3',
            aws_access_key_id=ACCESS_KEY_ID,
            aws_secret_access_key=ACCESS_SECRET_KEY,
            config=Config(signature_version='s3v4', region_name='ap-south-1')
        )
        os.chdir("C:/Users/Administrator/AdQvestDir/ABBREVIATION")


# =============================================================================
#         print('IN')
#         last_date = pd.read_sql("SELECT max(Relevant_Date) as Max FROM ABBREVIATION_LOGGER where Source='ICRA'",engine)['Max'][0]
#         links = pd.read_sql("SELECT * FROM ICRA_DAILY_FILES_LINKS where Download_Status='Yes' and Relevant_Date>'"+last_date+"'",engine)
#         print('OUT')
# =============================================================================

        links = pd.read_sql("SELECT * FROM ICRA_DAILY_FILES_LINKS where Download_Status='Yes' and Abbreviation_Status is null limit 0",engine)
        icra_counter = 0
        print(links.head())
        final_df=pd.DataFrame()
        if(links.empty==False):

            for a,values in links.iterrows():

                try:
                    file_name=values['File_Name']
                except:
                    continue

                try:
                    os.remove(file_name)

                except:
                    pass

                try:
                    s3.Bucket('adqvests3bucket').download_file('ICRA/' + file_name, file_name)
                except:
                    continue

                try:
                    final_df=pd.concat([final_df,get_match(file_name)])
                    query = "update ICRA_DAILY_FILES_LINKS set Abbreviation_Status='Yes' where Links = '"+values['Links']+"'"
                    print(query)
                    connection.execute(query)
                    connection.execute('commit')
                except:
                    query = "update ICRA_DAILY_FILES_LINKS set Abbreviation_Status='No' where Links = '"+values['Links']+"'"
                    print(query)
                    connection.execute(query)
                    connection.execute('commit')
                    pass

                try:
                    os.remove(file_name)
                except:
                    pass

                icra_counter += 1
                print('*******************************************************************************')
                print(f'ICRA: {icra_counter}')

            final_df['Relevant_Date']=today.strftime('%Y-%m-%d')
            final_df['Runtime']=today
            final_df['Source']='ICRA'
            super_final_df=pd.concat([final_df,super_final_df])



# =============================================================================
#         last_date = pd.read_sql("SELECT max(Relevant_Date) as Max FROM ABBREVIATION_LOGGER where Source='Care Ratings'",engine)['Max'][0]
#         links = pd.read_sql("SELECT * FROM CARE_RATINGS_DAILY_FILES_LINKS where Download_Status='Yes' and Relevant_Date>'"+last_date+"'",engine)
# =============================================================================

        links = pd.read_sql("SELECT * FROM CARE_RATINGS_DAILY_FILES_LINKS where Download_Status = 'Yes' and Abbreviation_Status is null",engine)
        care_counter = 0
        print(links.head())
        final_df=pd.DataFrame()
        if(links.empty==False):
            for a,values in links.iterrows():

                try:
                    file_name=values['File_Name']
                except:
                    continue

                try:
                    os.remove(file_name)

                except:
                    pass

                try:
                    s3.Bucket('adqvests3bucket').download_file('CARE_Ratings/' + file_name, file_name)
                except:
                    continue

                try:
                    super_final_df = get_match(file_name)
                    query = "update CARE_RATINGS_DAILY_FILES_LINKS set Abbreviation_Status='Yes' where Relevant_Date='" + values['Relevant_Date'].strftime("%Y-%m-%d") + "' and Company_Name = '"+values["Company_Name"]+"'"
                    print(query)
                    connection.execute(query)
                    connection.execute('commit')
                except:
                    query = "update CARE_RATINGS_DAILY_FILES_LINKS set Abbreviation_Status='No' where Relevant_Date='" + values['Relevant_Date'].strftime("%Y-%m-%d") + "' and Company_Name = '"+values["Company_Name"]+"'"
                    print(query)
                    connection.execute(query)
                    connection.execute('commit')
                    pass

                try:
                    os.remove(file_name)
                except:
                    pass

                if super_final_df.shape[0] != 0:
                    super_final_df['Relevant_Date']=today.strftime('%Y-%m-%d')
                    super_final_df['Runtime']=today
                    super_final_df['Source']='Care Ratings'
                    super_final_df=super_final_df[~(super_final_df['Match']=='')]
                    super_final_df.reset_index(drop=True,inplace=True)
                    super_final_df['Match']=super_final_df['Match'].apply(lambda x:str(x))
                    super_final_df=super_final_df.replace('nan',np.nan)
                    super_final_df.dropna(inplace=True)
                    super_final_df=super_final_df[~super_final_df['Short_Form'].str.contains('\d')]
                    super_final_df['Match']=super_final_df['Match'].str.strip().str.title()
                    super_final_df.reset_index(drop=True,inplace=True)
                    super_final_df = super_final_df.dropna()
                    super_final_df.to_sql(name='ABBREVIATION_LOGGER_Temp_Abdul',con=engine,if_exists='append',index=False)
                else:
                    pass

                care_counter += 1
                print('*******************************************************************************')
                print(f'Care Ratings: {care_counter}')






# =============================================================================
#         last_date = pd.read_sql("SELECT max(Relevant_Date) as Max FROM ABBREVIATION_LOGGER where Source='CRISIL'",engine)['Max'][0]
#         links = pd.read_sql("SELECT * FROM CRISIL_FILES_LINKS_DAILY_DATA where Download_Status = 'Yes' and Relevant_Date>'"+last_date+"'",engine)
# =============================================================================
        links = pd.read_sql("SELECT * FROM CRISIL_FILES_LINKS_DAILY_DATA where Download_Status = 'Yes' and Abbreviation_Status is null limit 0", engine)
        crisil_counter = 0
        print(links.head())
        final_df=pd.DataFrame()
        if(links.empty==False):
            for a,values in links.iterrows():


                try:
                    file_name=values['Rating_File_Name']
                except:
                    continue

                try:
                    os.remove(file_name)

                except:
                    pass

                try:
                    s3.Bucket('adqvests3bucket').download_file('CRISIL_Ratings/' + file_name, file_name)
                except:
                    continue

                try:
                    final_df=pd.concat([final_df,get_match(file_name)])
                    query = "update CRISIL_FILES_LINKS_DAILY_DATA set Abbreviation_Status='Yes' where Relevant_Date='" + values['Relevant_Date'].strftime('%Y-%m-%d') + "' and Pr_Id=" + str(values['Pr_Id'])
                    connection.execute(query)
                    connection.execute('commit')
                except:
                    query = "update CRISIL_FILES_LINKS_DAILY_DATA set Abbreviation_Status='No' where Relevant_Date='" + values['Relevant_Date'].strftime('%Y-%m-%d') + "' and Pr_Id=" + str(values['Pr_Id'])
                    connection.execute(query)
                    connection.execute('commit')
                    pass

                try:
                    os.remove(file_name)
                except:
                    pass

                crisil_counter += 1
                print('*******************************************************************************')
                print(f'CRISIL: {crisil_counter}')

            final_df['Relevant_Date']=today.strftime('%Y-%m-%d')
            final_df['Runtime']=today
            final_df['Source']='CRISIL'
            super_final_df=pd.concat([final_df,super_final_df])



# =============================================================================
#         links = pd.read_sql("SELECT * FROM BSE_REPORTS where Download_Status = 'Yes' and Abbreviation_Status is null", engine)
#         annual_counter = 0
#         print(links.head())
#         final_df=pd.DataFrame()
#         if(links.empty==False):
#             for a,values in links.iterrows():
#
#
#                 try:
#                     file_name=values['File_Name']
#                 except:
#                     continue
#
#                 try:
#                     os.remove(file_name)
#
#                 except:
#                     pass
#
#                 try:
#                     s3.Bucket('adqvests3bucket').download_file('Annual_Reports/' + file_name, file_name)
#                 except:
#                     continue
#
#                 try:
#                     final_df=pd.concat([final_df,get_match(file_name)])
#
#                 except:
#                     pass
#
#                 try:
#                     os.remove(file_name)
#                 except:
#                     pass
#
#                 annual_counter += 1
#                 print('*******************************************************************************')
#                 print(f'Annual Reports: {annual_counter}')
#
#             final_df['Relevant_Date']=today.strftime('%Y-%m-%d')
#             final_df['Runtime']=today
#             final_df['Source']='Annual Reports'
#             super_final_df=pd.concat([final_df,super_final_df])
# =============================================================================

        #last_date = pd.read_sql("SELECT max(Relevant_Date) as Max FROM ABBREVIATION_LOGGER where Source='ICRA'",engine)['Max'][0]
        # links = pd.read_sql("SELECT * FROM BSE_ANNUAL_REPORTS where Abbreviation_Status is null",engine)
        #
        #
        # print(links.head())
        # final_df=pd.DataFrame()
        # if(links.empty==False):
        #
        #     for a,values in links.iterrows():
        #
        #         try:
        #             os.remove(file_name)
        #
        #         except:
        #             pass
        #
        #         #try:
        #         file_name=values['File_Name']
        #         s3.Bucket('adqvests3bucket').download_file('Annual_Reports/' + file_name, file_name)
        #         final_df=pd.concat([final_df,get_match(file_name)])
        #
        #         #except:
        #             #continue
        #
        #
        #
        #         try:
        #             os.remove(file_name)
        #         except:
        #             pass
        #
        #     final_df['Relevant_Date']=today.strftime('%Y-%m-%d')
        #     final_df['Runtime']=today
        #     final_df['Source']='Annual Reports'
        #     super_final_df=pd.concat([final_df,super_final_df])
        #     connection.execute("update BSE_ANNUAL_REPORTS set Abbreviation_Status = 'Yes' where File_Name = '"+file_name+"'")


# =============================================================================
#         total_df=pd.read_sql("SELECT * FROM ABBREVIATION_LOGGER",engine)
# =============================================================================

# =============================================================================
#         super_final_df=super_final_df.drop_duplicates(['Short_Form','Match'])
# =============================================================================
        # super_final_df=super_final_df[~(super_final_df['Match']=='')]
        # super_final_df.reset_index(drop=True,inplace=True)
        # super_final_df['Match']=super_final_df['Match'].apply(lambda x:str(x))
        # super_final_df=super_final_df.replace('nan',np.nan)
        # super_final_df.dropna(inplace=True)
        # super_final_df=super_final_df[~super_final_df['Short_Form'].str.contains('\d')]
        # super_final_df['Match']=super_final_df['Match'].str.strip().str.title()
        # super_final_df.reset_index(drop=True,inplace=True)
        # super_final_df = super_final_df.dropna()
        #super_final_df.reset_index(drop=True,inplace=True)
# =============================================================================
#         total_df.reset_index(drop=True,inplace=True)
# =============================================================================

# =============================================================================
#         super_final_df=super_final_df[~super_final_df['Short_Form'].isin(total_df['Short_Form'])].dropna()
# =============================================================================

        # super_final_df.to_sql(name='ABBREVIATION_LOGGER_Temp_Abdul',con=engine,if_exists='append',index=False)

        log.job_end_log(table_name,job_start_time, no_of_ping)

    except:

        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
