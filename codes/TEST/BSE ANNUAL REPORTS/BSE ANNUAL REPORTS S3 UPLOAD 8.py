
import requests
import pandas as pd
import os
from pytz import timezone
import datetime
import sys
import io
import re
from PyPDF2 import PdfReader
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')

#---- DATABASE AND COMPANY FUNCTIONS
import adqvest_db
import JobLogNew as log
import dbfunctions
from adqvest_robotstxt import Robots
robot = Robots(__file__)

def download_file_content(pdf_url):
    headers = {
        'user-agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36'
        )
    }

    # Send the GET request to download the PDF file
    response = requests.get(pdf_url, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        # Store the PDF file content in a BytesIO object
        pdf_file = io.BytesIO(response.content)
        return pdf_file.getvalue()  # Return the content as bytes
    else:
        print(f"Failed to download file from {pdf_url}. Status code: {response.status_code}")
        return None

def run_program(run_by = 'Adqvest_Bot', py_file_name = None):
    # os.chdir('/home/ubuntu/AdQvestDir')
    os.chdir('C:/Users/Administrator/AdQvestDir')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    india_time = timezone('Asia/Kolkata')

    job_start_time = datetime.datetime.now(india_time)
    table_name = "BSE_ANNOUNCEMENT_ANNUAL_REPORT_COMPANY_WISE_YEARLY_DATA"
    scheduler = ''
    no_of_ping = 0

    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]

    try :
        if(run_by == 'Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        query = "SELECT * FROM BSE_ANNOUNCEMENT_ANNUAL_REPORT_COMPANY_WISE_YEARLY_DATA where S3_Upload_Status is Null;"
        raw_df = pd.read_sql(query, con=engine)
        raw_df = raw_df[100001:]
        print(len(raw_df))

        for index, row in raw_df.iterrows():
            link = row['Encoded_PDF_Link']
            filename = row['File_Name']
            
            filepath = os.path.join(r'C:\\Users\\Administrator\\AdQvestDir\\Junk_One_Time', filename)
            key1 = 'BSE_ANNUAL_REPORTS_CORPUS_TO_BE_CHUNKED_NEW/'
            print(key1)
            key2 = 'BSE_ANNUAL_REPORTS_CORPUS_NEW/'
            print(key2)
            
            pdf_content = download_file_content(link)
            if pdf_content:
                with open(filepath, 'wb') as file:
                    file.write(pdf_content)
                    
                dbfunctions.to_s3bucket(filepath, key1)
                dbfunctions.to_s3bucket(filepath, key2)
                
                connection=engine.connect()
                connection.execute("update BSE_ANNOUNCEMENT_ANNUAL_REPORT_COMPANY_WISE_YEARLY_DATA set S3_Upload_Status='Done' where File_Name = '" +str(filename)+"'")
                connection.execute("commit")
                print(f'PDF downloaded for the file {filename}')
                print('_' *80)
            else:
                print('PDF_CONTENT returned as None')
                connection=engine.connect()
                connection.execute("update BSE_ANNOUNCEMENT_ANNUAL_REPORT_COMPANY_WISE_YEARLY_DATA set S3_Upload_Status='Failed' where File_Name = '" +str(filename)+"'")
                connection.execute("commit")
                print('_' *80)
                
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