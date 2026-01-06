import requests
import pandas as pd
import os
from pytz import timezone
import datetime
import sys
import io
import re
import warnings
warnings.filterwarnings('ignore')

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import httpx

from zenrows import ZenRowsClient
zen_req = ZenRowsClient("eabc162057980f957e89b6e71cb469e438b41393")

 
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')

#---- DATABASE AND COMPANY FUNCTIONS
import adqvest_db
import JobLogNew as log
import dbfunctions
from adqvest_robotstxt import Robots
robot = Robots(__file__)
#%%
# Working_table='WORKING_TABLE'
# key1 = 'BSE_ANNUAL_REPORTS_CORPUS_TO_BE_CHUNKED_NEW/'
# key2 = 'BSE_ANNUAL_REPORTS_CORPUS_NEW/'

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Referer": "https://www.google.com/",
}

def download_file_content(pdf_url,filename,table_name,engine):
    try:

        # Send the GET request to download the PDF file
        # response = requests.get(pdf_url, headers=headers)
        session = requests.Session()
        retry = Retry(total=5, backoff_factor=5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        session.verify = True
        print(pdf_url)
        pdf_url = re.sub(r"[\x00-\x1F]+", "-", pdf_url).rstrip(".")
        pdf_url = re.sub(r'\.pdf.*$', '.pdf', pdf_url, flags=re.IGNORECASE).strip()
        pdf_url = pdf_url.replace('?', '-')
        print(pdf_url)
        response=requests.get(pdf_url,headers=HEADERS,timeout=60,verify=False)
        print(response.status_code)

        # Check if the request was successful
        if response.status_code == 200:
            print('Success')
            # Store the PDF file content in a BytesIO object
            pdf_file = io.BytesIO(response.content)
            return pdf_file.getvalue()  # Return the content as bytes

        elif response.status_code == 404:
            print(f"Failed to download file from {pdf_url}. Status code: {response.status_code}")
            return 'File Not Available' 

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        error_msg = str(sys.exc_info()[1]) + " line " + str(exc_tb.tb_lineno)
        print(error_msg)
        raise
               
def uplod_pdfs(pdf_content,filepath,keys):
    
        with open(filepath, 'wb') as file:
            file.write(pdf_content)
            
        dbfunctions.to_s3bucket(filepath, keys[0])
        dbfunctions.to_s3bucket(filepath, keys[1])
        print("Pdf Uplodedin both buckets........")
#%%
def run_program(run_by = 'Adqvest_Bot', py_file_name = None):
    # os.chdir('/home/ubuntu/AdQvestDir')
    os.chdir('C:/Users/Administrator/AdQvestDir')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    india_time = timezone('Asia/Kolkata')

    job_start_time = datetime.datetime.now(india_time)
    table_name = 'GENERIC_S3_UPLOAD'
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
        lookup_dict={'NSE_DOC_LINKS_FROM_CORPUS_DAILY_DATA':["NSE_DOC_LINKS_FROM_CORPUS_DAILY_DATA_TO_BE_CHUNKED/","NSE_DOC_LINKS_FROM_CORPUS/"]}
        for Working_table,keys in lookup_dict.items():
            
            query = f"SELECT * FROM {Working_table} where S3_Upload_Status is Null AND S3_Upload_Comments is Null Order By Company_Name;"
            raw_df = pd.read_sql(query, con=engine)
            raw_df = raw_df.reset_index(drop=True)
            print(len(raw_df))
    
            for index, row in raw_df.iterrows():
                link = row['File_Link'] 
                filename = row['File_Name']
                filepath = os.path.join(r'C:\\Users\\Administrator\\AdQvestDir\\Junk_One_Time', filename)
                try:
                    print(filename)
                    pdf_content = download_file_content(link,filename,Working_table,engine)
                    # print(pdf_content)

                except:
                    print('Moving to the next file')
                    error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    error_msg = str(sys.exc_info()[1]) + " line " + str(exc_tb.tb_lineno)
                    connection=engine.connect()
                    connection.execute(f"update {Working_table} set S3_Upload_Comments='{str(error_type)}' where File_Name ='{filename}'")
                    connection.execute("commit")
                    print('-' *80)
                    continue

                #if pdf_content:
                #   uplod_pdfs(pdf_content,filepath,keys)
                   
                #   connection=engine.connect()
                #   connection.execute(f"UPDATE {Working_table} SET S3_Upload_Status='Done' WHERE File_Name ='{filename}';")
                #   connection.execute("commit")
                #   print(f'PDF downloaded for the file {filename}')
                #   print('-' *80)

                if (pdf_content =='File Not Available') or (pdf_content==None):
                    print('PDF_CONTENT returned as None')
                    connection=engine.connect()
                    connection.execute(f"update {Working_table} set S3_Upload_Comments='File Not Available' where File_Name ='{filename}'")
                    connection.execute("commit")
                    print('-' *80)
                elif pdf_content:
                    uplod_pdfs(pdf_content,filepath,keys)
                    connection=engine.connect()
                    connection.execute(f"UPDATE {Working_table} SET S3_Upload_Status='Done' WHERE File_Name ='{filename}';")
                    connection.execute("commit")
                    print(f'PDF downloaded for the file {filename}')
                    print('-' *80)
                    
                if os.path.exists(filepath):
                    os.remove(filepath)
                else:
                    print(f"The file {filename} does not exist.")
        
        log.job_end_log(table_name,job_start_time,no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        error_msg = str(sys.exc_info()[1]) + " line " + str(exc_tb.tb_lineno)
        print(error_msg)

        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')