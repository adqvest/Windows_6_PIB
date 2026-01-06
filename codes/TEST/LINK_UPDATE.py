import os
import io
import re
# import ast
# import PyPDF2
import boto3
# import json
import voyageai
# import psycopg2
# import anthropic
# # import adqvest_db
# import pandas as pd
# import numpy as np
from pytz import timezone
import datetime as datetime
import pypdfium2 as pdfium

# from collections import Counter

# from pdfminer.converter import TextConverter
# from pdfminer.layout import LAParams

# from psycopg2.extras import execute_values
# from pgvector.psycopg2 import register_vector

# import pytesseract
# from pdf2image import convert_from_path

import warnings
warnings.filterwarnings('ignore')

# import datetime as dt
# import pytz
# import time

# import adqvest_s3
# import boto3
# import ntpath
from botocore.config import Config
# import requests
# import boto3
# from botocore.exceptions import NoCredentialsError

import pdfquery
# import json
from lxml import etree
# from io import StringIO

import sys
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
# import ClickHouse_db

engine = adqvest_db.db_conn()
# client = ClickHouse_db.db_conn()


def extract_page_numbers(content):
    # Return all occurrences of page numbers
    page_numbers = re.findall(r'page number:\s*(\d+)', content.lower())
    return page_numbers

def split_content_by_pages(content):
    # Split content into sections by page number
    sections = re.split(r'(page number:\s*\d+)', content.lower())
    # Remove empty sections and combine headers with their content
    page_contents = []
    for i in range(1, len(sections), 2):
        if i+1 < len(sections):
            page_content = sections[i] + sections[i+1]
            page_numbers = re.findall(r'page number:\s*(\d+)', page_content)
            if page_numbers:
                page_numbers = page_numbers[0]
                page_contents.append((page_numbers, page_content))
    return page_contents

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    # os.chdir('C:/Users/Administrator/AdQvestDir')
    engine = adqvest_db.db_conn()
    # client = ClickHouse_db.db_conn()
    from clickhouse_driver import Client


    host = 'ec2-52-11-204-251.us-west-2.compute.amazonaws.com'
    user_name = 'default'
    password_string = 'Clickhouse@2024'
    db_name = 'AdqvestDB'

    client = Client(host, user=user_name, password=password_string, database=db_name)

    # ****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today = datetime.datetime.now(india_time)
    days = datetime.timedelta(1)
    yesterday = today - days

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

        import pandas as pd

        engine = adqvest_db.db_conn()
        connection = engine.connect()

        # query = "select * from AdqvestDB.INVESTOR_REPORTS_DAILY_DATA ORDER BY Company_Name ASC;"
        query = "select * from AdqvestDB.GEN_INSURANCE_SCRAPY_CORPUS_CLEAN_Temp_Final_3 ORDER BY Company ASC;"
        data = pd.read_sql(query, con=engine)

        doc = client.execute(f'''SELECT distinct(document_id) FROM thurro_pdf_documents_vector_db_gen_insurance_corpus_final_3;''')
        print(len(doc))

        companies = doc

        company = [x[0] for x in companies]
        data['File_ID'] = data['File_ID'].astype(str)
        company = [str(x) for x in company]

        data_1 = data[data['File_ID'].isin(company)]
        data_1.reset_index(drop = True, inplace = True)

        count = 0
        for x in range(len(data_1)):
            data_1["Company"][x] = data_1["Company"][x].replace("'","\\'")
            url_value = data_1["Url"][x].replace("'", "''")
            document_id_value = data_1["File_ID"][x]
            print(document_id_value)
            
            doc_contents = client.execute(f'''SELECT count(*) FROM thurro_pdf_documents_vector_db_gen_insurance_corpus_final_3 WHERE document_id = '{document_id_value}';''')

            doc_count = doc_contents[0][0]
            for page_num in range(1, doc_count + 1):
                new_url = f"{url_value}#page={page_num}"
                
                # Get the content for the specific row we want to update
                row_content = client.execute(f'''
                    SELECT document_content_small_case
                    FROM thurro_pdf_documents_vector_db_gen_insurance_corpus_final_3
                    WHERE document_id = '{document_id_value}'
                    ORDER BY document_content_small_case
                    LIMIT 1 OFFSET {page_num - 1}
                ''')
                
                if row_content:
                    content_to_match = row_content[0][0]
                    client.execute(f'''
                        ALTER TABLE thurro_pdf_documents_vector_db_gen_insurance_corpus_final_3
                        UPDATE document_link = '{new_url}'
                        WHERE document_id = '{document_id_value}'
                        AND document_content_small_case = '{content_to_match.replace("'", "''")}'
                    ''')
                           
            client.execute(f'''ALTER TABLE thurro_pdf_documents_vector_db_life_insurance_final_v3 UPDATE vector_db_status = 'LINK UPDATED' WHERE document_id = '{document_id_value}';''')
            print("COMPLETED LOOP:",count)
            print('____________________________________________________________________________________________________________________________________')
            count += 1
                            
            if count % 900 == 0:
                 print("OPTIMIZING DATA")
                 client.execute(f'''OPTIMIZE TABLE thurro_pdf_documents_vector_db_life_insurance_final_v3 FINAL;''')    

        log.job_end_log(table_name, job_start_time, no_of_ping)    

    except Exception as e:
         error_type = str(re.search("'(.+?)'", str(type(e))).group(1))
         error_msg = str(e) + " line " + str(sys.exc_info()[2].tb_lineno)
         print(error_type)
         print(error_msg)

         log.job_error_log(table_name, job_start_time, error_type, error_msg, no_of_ping)

if __name__ == '__main__':
     run_program(run_by='manual') 