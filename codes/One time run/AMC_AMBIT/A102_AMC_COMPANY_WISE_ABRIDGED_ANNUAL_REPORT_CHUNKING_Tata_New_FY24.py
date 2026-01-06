import os
import io
import re
import boto3
import json
import voyageai
import pandas as pd
import numpy as np
from pytz import timezone
import datetime as datetime
import pypdfium2 as pdfium
from io import BytesIO
import pdfplumber

import warnings
warnings.filterwarnings('ignore')


from botocore.config import Config
import pdfquery
from lxml import etree

from datetime import timedelta
from dateutil.relativedelta import relativedelta
from clickhouse_driver import Client
#%%
import sys
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
engine = adqvest_db.db_conn()
    
#%%
os.environ["VOYAGE_API_KEY"] = "pa-97JpiKNlfEEkhs8dBSZOloXM7aw3BI3NJ0ra-zaCSIk"
# os.environ["VOYAGE_API_KEY"] = "pa-zTzrc5RTasN81O4SZXWF9iehCfFOpK0GwH_e7iK8rhI"

host = 'ec2-52-11-204-251.us-west-2.compute.amazonaws.com'
user_name = 'default'
password_string = 'Clickhouse@2024'
db_name = 'AdqvestDB'
#%%


def pdf_to_text(pdf_file):
    """Convert a PDF file directly to a list of text per page."""
    pdf = pdfquery.PDFQuery(io.BytesIO(pdf_file))
    pdf.load()  # Load all pages

    pages = pdf.extract([['pages', 'LTPage']])
    page_elements = pages['pages']
    
    def extract_text_from_element(element):
        """Recursively extract text from an XML element and its children."""
        text_list = []
        if element.text and element.text.strip():
            text_list.append(element.text.strip())
        
        for child in element:
            text_list.extend(extract_text_from_element(child))
        
        return text_list
    
    text_per_page = []
    
    for page in page_elements:
        xml_data = etree.tostring(page, pretty_print=True).decode('utf-8')
        
        root = etree.fromstring(xml_data)
        
        text_list = extract_text_from_element(root)
        
        extracted_text = '\n'.join(text_list)
        
        text_per_page.append(extracted_text)
    
    return text_per_page

def read_large_file_names_from_s3_folder(bucket_name, folder_name, size_threshold_mb=1):
    ACCESS_KEY_ID = 'AKIAYCVSRU2U3XP2DB75'
    ACCESS_SECRET_KEY = '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'
    
    # Convert the size threshold to bytes (1 MB = 1,048,576 bytes)
#     size_threshold_bytes = size_threshold_mb * 1024 * 1024
    
    s3_client = boto3.client(
        's3',
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=ACCESS_SECRET_KEY,
        config=Config(signature_version='s3v4', region_name='ap-south-1')
    )
    
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=folder_name)
    
    large_file_names = []
    page_count = 0
    for page in page_iterator:
        if 'Contents' in page:
            for obj in page['Contents']:
#                 if obj['Size'] > size_threshold_bytes:
                large_file_names.append(obj['Key'])
        # if page_count > 10:
        #     break
        page_count += 1
    return large_file_names

def read_all_file_names_from_s3_folder(bucket_name, folder_name):
    count = 0
    ACCESS_KEY_ID = 'AKIAYCVSRU2U3XP2DB75'
    ACCESS_SECRET_KEY = '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'
    s3_client = boto3.client(
        's3',
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=ACCESS_SECRET_KEY,
        config=Config(signature_version='s3v4', region_name='ap-south-1')
    )
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=folder_name)
    
    file_names = []
    for page in page_iterator:
        if 'Contents' in page:
            for obj in page['Contents']:
                file_names.append(obj['Key'])
                
    return file_names

def read_all_file_content(obj,bucket_name='adqvests3bucket'):
    print("i am here")
    ACCESS_KEY_ID = 'AKIAYCVSRU2U3XP2DB75'
    ACCESS_SECRET_KEY = '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'
    
    s3 = boto3.client(
        's3',
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=ACCESS_SECRET_KEY,
        config=Config(signature_version='s3v4', region_name='ap-south-1')
    )

    files_content = {}

#     file_key = obj['Key']
    file_key = obj
    if not file_key.endswith('/'):
        file_name = file_key.split('/')[-1]
        file_response = s3.get_object(Bucket=bucket_name, Key=file_key)
        file_content = file_response['Body'].read()
        # files_content[file_name] = file_content
    
    return file_content,file_name

def delete_file_from_s3(file_name,bucket_name = 'adqvests3bucket'):
    
    ACCESS_KEY_ID = 'AKIAYCVSRU2U3XP2DB75'
    ACCESS_SECRET_KEY = '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'
    
    s3 = boto3.client(
        's3',
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=ACCESS_SECRET_KEY,
        config=Config(signature_version='s3v4', region_name='ap-south-1')
    )
    
    try:
        s3.delete_object(Bucket=bucket_name, Key=file_name)
#         print(f"Deleted {file_name} from S3 bucket: {bucket_name}")
        print(f"Deleted from S3 bucket")
    except Exception as e:
        print(f"Error deleting {file_name} from S3 bucket: {e}")

def extract_text_from_pdf(files_content,file_name):
    
    # Splitting file name to extract details
    parts = file_name.split('_')
    # Extract the company name (everything before the first '0')
    company_name = ' '.join(parts[:parts.index('0')])

    # Find the index containing 'FY'
    fy_index = next(i for i, part in enumerate(parts) if 'FY' in part)

    # Initialize quarter variable
    quarter = None

    # Extract relevant year based on pattern
    # if 'Q' in parts[fy_index - 1]:
    #     # Pattern with quarter (Q1/Q2/Q3/Q4)
    #     quarter = parts[fy_index - 1]
    #     relevant_year = f"{quarter} {parts[fy_index]}"
    # else:
    #     # Pattern without quarter (just FY)
    #     relevant_year = parts[fy_index]
    relevant_year = parts[fy_index]
    # Extract file ID and remove .pdf
    file_id = parts[-1].replace('.pdf', '')

    # Extract document type (everything between the '0' and the FY/quarter part)
    doc_type_end = fy_index - 2 if quarter else fy_index - 1
    report_type = ' '.join(parts[parts.index('0') + 1:doc_type_end + 1])
    
    # pdf_content = files_content[file_name]
    # Load the PDF document
    # pdf = pdfium.PdfDocument(io.BytesIO(pdf_content))
    
    all_page_text=[]
    table_list=[]
    
    pdf_path = BytesIO(files_content) 
    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages):
            # print(f"Processing Page {i+1}")
            # if i<8:
            page_text = page.extract_text()
            text= ''
            text += 'PAGE NUMBER: ' + str(i+1) + '\n\n'
            text += 'COMPANY NAME: ' + str(company_name.strip()) + '\n\n'
            text += 'REPORT TYPE: ' + str(report_type.strip()) + '\n\n'
            text += 'RELEVANT YEAR: ' + str(relevant_year.strip()) + '\n\n'
            text += page_text
            text += '\n\n'
            text = re.sub('\n\n\n','\n\n', text)
            tables = page.extract_tables()
            if len(tables)>0:
                table2=[i for i in tables]
                # df = pd.DataFrame(table2[0],columns=table2[0][0])
                table_list.append(tables)
            else:
                table_list.append([])
            all_page_text.append(text)
            if tables:
                # print('Tables Found')
                print(f"Processing Page {i+1}")
            
    
    return company_name,report_type,relevant_year,file_id,all_page_text,table_list

def create_embeddings(chunked_text):
    
    vo = voyageai.Client(api_key="pa-97JpiKNlfEEkhs8dBSZOloXM7aw3BI3NJ0ra-zaCSIk")
    # Embed the documents
    doc_embds = vo.embed(
        chunked_text, model="voyage-2", input_type="document"
    ).embeddings
    
    return doc_embds

def extract_chunks_from_text(text):
    
    chunks = text.split("PAGE NUMBER: ")
    chunks = chunks[1:]
    return chunks

def analyse_chunk(chunk):
    
    is_true = False 
    
    pattern_1 = r"\bQ[1-4] & Year (201[0-9]|202[0-4]) \b"
    pattern_2 = r"\bQ[1-4] (201[0-9]|202[0-4]) \b"
    pattern_3 = r"\bQ[1-4](201[0-9]|202[0-4]) \b"
    pattern_4 = r"\bQ[1-4] (1[0-9]|2[0-4]) \b"
    pattern_5 = r"\bQ[1-4](1[0-9]|2[0-4]) \b"

    if re.findall(pattern_1, chunk) != []:
        is_true = True

    elif re.findall(pattern_2, chunk) != []:
        is_true = True

    elif re.findall(pattern_3, chunk) != []:
        is_true = True

    elif re.findall(pattern_4, chunk) != []:
        is_true = True

    elif re.findall(pattern_5, chunk) != []:
        is_true = True
        
    return is_true


def replace_multiple_patterns(text, replacements):
    # Create a list of unique placeholders
    placeholders = {pattern: f"__PLACEHOLDER_{i}__" for i, pattern in enumerate(replacements)}

    # Replace each pattern with its unique placeholder
    for pattern, placeholder in placeholders.items():
        text = re.sub(pattern, placeholder, text)

    # Replace each placeholder with its final replacement value
    for pattern, placeholder in placeholders.items():
        text = re.sub(placeholder, replacements[pattern], text)

    return text

def modify_chunk(chunk):

    # Automatically generate the replacement patterns
    replacements = {}
    for year in range(2010, 2050):
        replacements[f"Q1 {year}"] = f"Q4 FY{year}"
        replacements[f"Q2 {year}"] = f"Q1 FY{year + 1}"
        replacements[f"Q3 {year}"] = f"Q2 FY{year + 1}"
        replacements[f"Q4 {year}"] = f"Q3 FY{year + 1}"

    for year in range(10, 50):
        replacements[f"Q1 {year}"] = f"Q4 FY{year}"
        replacements[f"Q2 {year}"] = f"Q1 FY{year + 1}"
        replacements[f"Q3 {year}"] = f"Q2 FY{year + 1}"
        replacements[f"Q4 {year}"] = f"Q3 FY{year + 1}"

    for year in range(2010, 2050):
        replacements[f"H1 {year}"] = f"H1 CY{year}"
        replacements[f"H2 {year}"] = f"H2 CY{year + 1}"
        replacements[f"H3 {year}"] = f"H3 CY{year + 1}"
        replacements[f"H4 {year}"] = f"H4 CY{year + 1}"

    for year in range(10, 50):
        replacements[f"H1 {year}"] = f"H1 CY{year}"
        replacements[f"H2 {year}"] = f"H2 CY{year + 1}"
        replacements[f"H3 {year}"] = f"H3 CY{year + 1}"
        replacements[f"H4 {year}"] = f"H4 CY{year + 1}"
        
    for year in range(2010, 2050):
        replacements[f"Q1{year}"] = f"Q4 FY{year}"
        replacements[f"Q2{year}"] = f"Q1 FY{year + 1}"
        replacements[f"Q3{year}"] = f"Q2 FY{year + 1}"
        replacements[f"Q4{year}"] = f"Q3 FY{year + 1}"

    for year in range(10, 50):
        replacements[f"Q1{year}"] = f"Q4 FY{year}"
        replacements[f"Q2{year}"] = f"Q1 FY{year + 1}"
        replacements[f"Q3{year}"] = f"Q2 FY{year + 1}"
        replacements[f"Q4{year}"] = f"Q3 FY{year + 1}"

    for year in range(2010, 2050):
        replacements[f"H1{year}"] = f"H1 CY{year}"
        replacements[f"H2{year}"] = f"H2 CY{year + 1}"
        replacements[f"H3{year}"] = f"H3 CY{year + 1}"
        replacements[f"H4{year}"] = f"H4 CY{year + 1}"

    for year in range(10, 50):
        replacements[f"H1{year}"] = f"H1 CY{year}"
        replacements[f"H2{year}"] = f"H2 CY{year + 1}"
        replacements[f"H3{year}"] = f"H3 CY{year + 1}"
        replacements[f"H4{year}"] = f"H4 CY{year + 1}"

    # Apply replacements to the third element of each tuple in filtered_data
    chunk = replace_multiple_patterns(chunk, replacements)
    return chunk

def parse_quarter_year(qy_str):
    # Check if the string contains a quarter
    qy_str = qy_str.lower()
    if 'q' in qy_str:
        q, fy = qy_str.split(' ')
        quarter = int(q[1])
        year = int(fy.replace('fy', '20'))
        
        if quarter in [1, 2, 3]:
            # For Q1, Q2, Q3: map to April, July, October
            month = 3 + (quarter * 3)
            year = year - 1  # Since Q1 FY25 starts in April 2024, which is FY25 (2025)
        elif quarter == 4:
            # For Q4: map to January of the fiscal year
            month = 3
            year = year
        else:
            raise ValueError(f"Invalid quarter: {quarter}")
        day = 1  # Set day as 1
        return datetime.datetime(year, month, day).date() + relativedelta(months=1) - timedelta(days=1)
    else:
        # If only fiscal year is provided, set to January 1st
        fy = qy_str
        year = int(fy.replace('fy', '20'))
        return datetime.datetime(year, 3, 1).date() + relativedelta(months=1) - timedelta(days=1)
    

#%% 


        

        

def extract_page_data(pdf_path, page_number):
    try:
        # Open the PDF file
        # response = requests.get(pdf_path)
        # pdf_path = BytesIO(file_content)  # Convert to a file-like object
        with pdfplumber.open(pdf_path) as pdf:
            # Check if page number is valid
            if page_number <= len(pdf.pages):
                # Get the specific page
                page = pdf.pages[page_number - 1]  # Subtract 1 as page indexing starts from 0
                # Extract text from the page
                text = page.extract_text()
                # Extract tables if any
                tables = page.extract_tables()
                
                print(tables)
                df = pd.DataFrame()
                df['Text'] = text
                df['Table'] = tables
                return text, tables
            else:
                return f"Error: Page number {page_number} exceeds total pages {len(pdf.pages)}"
    except Exception as e:
        print(e)
        return f"Error: {str(e)}"

# Example usage
# os.chdir(r'C:\Users\sakhu\Downloads')
# os.chdir(r"C:\Users\Santonu\Desktop\ADQvest\Error files\today error\AMC_AMBIT")
# pdf_path = "https://vatseelabs-s3.kotakmf.com/FormsDownloads/Annual-Report/Abridged-Annual-Report-FY-2023-24/AbridgedAnnualReport202324.pdf"
# pdf_path='Edelweiss Abridged AR24 (Debt)_30072024_112738_PM.pdf'
# page_number = 55  # Page number you want to extract

# # Extract data
# data,data1 = extract_page_data(pdf_path, page_number)

# table2=[i for i in data1]
# df = pd.DataFrame(table2[0],columns=table2[0][0])

# Print or process the extracted data
#print("Text:", data['text'])
#print("\nTables:", data['tables'])
#%%
def run_program(run_by='Adqvest_Bot', py_file_name=None):
    # os.chdir('C:/Users/Administrator/AdQvestDir')
    engine = adqvest_db.db_conn()
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
        
        bucket_name = 'adqvests3bucket'
        folder_name = 'AMC_COMPANY_WISE_ABRIDGED_ANNUAL_REPORT'

        execution_list=pd.read_sql("select Company_Name,count(*) as No_links from AMC_COMPANY_WISE_ABRIDGED_ANNUAL_REPORT_LINKS where  S3_Update_Status is not null group by Company_Name order by No_links asc;", con=engine)
        execution_list=execution_list['Company_Name'].to_list()
        
        response_org = read_all_file_names_from_s3_folder(bucket_name, folder_name)
        response_org = [ x for x in response_org if 'AMC_COMPANY_WISE_ABRIDGED_ANNUAL_REPORT/' in x]
        
        # req_quarter=['FY22','FY23','FY24']
        req_quarter=['FY24']
        execution_list = ["TATA Mutual Fund"]
        # req_file_id=['5443','5444','5445','5434','5435','5436','5362','5363','5364']
        req_file_id=['5362','5363']
        response_org=[i for i in response_org if i.split('_')[-2] in req_quarter]
        response_org=[i for i in response_org if i.split('_')[-1].replace('.pdf','') in req_file_id]
        print(response_org)


        
        
        
        for company in execution_list:
            response=[i for i in response_org if i.split('/')[-1].split('0')[0].replace('_',' ').strip() in company]
            doc_ids = client.execute(f'''SELECT distinct document_id FROM thurro_pdf_documents_vector_db_amc_wise_abridged_annual_reports_new2;''')
            doc_ids = [str(x).replace('(','').replace(',)','').replace("'", "").strip() for x in doc_ids]
            print(len(doc_ids))
            
            
            for obj in response:
                    print(obj)
                    files_content,file_name = read_all_file_content(obj)
                    file_name_without_extension = file_name.replace('.pdf', '')
                    
                    print("STARTED FILE :",file_name)
            
                # try:
                    company_name,report_type,relevant_year,file_id,text,tables = extract_text_from_pdf(files_content,file_name)
                    link_dept = pd.read_sql(f"select Distinct File_Link from AMC_COMPANY_WISE_ABRIDGED_ANNUAL_REPORT_LINKS where File_Id = {int(file_id)};",con=engine)
                    link=link_dept['File_Link'][0]
    
                    if str(file_id) not in doc_ids :
                        
                            print(relevant_year)
                            if relevant_year is not None:
                                if 'Q' in relevant_year: 
                                    if int(relevant_year[5:]) >= 22 and int(relevant_year[5:]) < 30:
                                        company_name = company_name.replace("'", "\\'")  # Escaping single quotes
                                        company_small = company_name.lower() 
                                        document_date = parse_quarter_year(relevant_year)
                                    else:
                                        print("FILE NOT RELEVANT ") 
                                        connection=engine.connect()
                                        connection.execute(f"update AMC_COMPANY_WISE_ABRIDGED_ANNUAL_REPORT_LINKS set Chunking_Status='Not Relevant' where  File_Id = {int(file_id)}")
                                        connection.execute("commit")
                                        continue      
                                elif int(relevant_year[2:]) >= 22 and int(relevant_year[2:]) < 30:
                                        company_name = company_name.replace("'", "\\'")  # Escaping single quotes
                                        company_small = company_name.lower()
                                        document_date = parse_quarter_year(relevant_year)
                                else:
                                    print("FILE NOT RELEVANT ") 
                                    connection=engine.connect()
                                    connection.execute(f"update AMC_COMPANY_WISE_ABRIDGED_ANNUAL_REPORT_LINKS set Chunking_Status='Not Relevant' where File_Id = {int(file_id)}")
                                    connection.execute("commit")
                                    continue        
                            else:
                                print("RELEVANT YEAR NOT FOUND") 
                                connection=engine.connect()
                                connection.execute(f"update AMC_COMPANY_WISE_ABRIDGED_ANNUAL_REPORT_LINKS set Chunking_Status='Issues' where  File_Id = {int(file_id)}")
                                connection.execute("commit")
                                continue
                            
                            if len(text)>0:
                                for i, chunk in enumerate(text):
                                    chunk = chunk.replace("\\", "\\\\")  # Escaping backslashes
                                    chunk = chunk.replace("\n", "\\n")  # Escaping newlines
                                    chunk = chunk.replace("\r", "\\r")  # Escaping carriage returns
                                    chunk = chunk.replace("'","\\'") # Escaping single quotes
                                    # chunk = 'PAGE NUMBER: ' + chunk
    
                                    chunk_small = chunk.lower()
                                    table=tables[i]
                                    # table_lower = table.applymap(lambda x: x.lower() if isinstance(x, str) else x)

                
                                    is_true = analyse_chunk(chunk)
                                    if is_true:
                                        new_chunk = modify_chunk(chunk)
                                    else:
                                        new_chunk = chunk
                
                                    try:
                                        table_content_str=''
                                        table_embed = str(create_embeddings('No table')).replace('[[','[').replace(']]',']')
                                        table_embed_small = str(create_embeddings('No table'.lower())).replace('[[','[').replace(']]',']')

                                        # print(table_content_str)
                                        chunk_embed = str(create_embeddings(chunk)).replace('[[','[').replace(']]',']')
                                        chunk_embed_small = str(create_embeddings(chunk_small)).replace('[[','[').replace(']]',']')
                                        client.execute(f"INSERT INTO thurro_pdf_documents_vector_db_amc_wise_abridged_annual_reports_new2 (document_id,document_company,document_company_small_case,document_type,document_year,document_date,document_link,document_content,document_content_small_case,document_content_modify,table_content,embedding,embedding_small_case,table_embedding,table_embedding_small) VALUES ('{file_id}','{company_name}','{company_small}','{report_type}','{relevant_year}','{document_date}','{link}','''{chunk}''','''{chunk_small}''','''{new_chunk}''','''{table_content_str}''',{chunk_embed},{chunk_embed_small},{table_embed},{table_embed_small});") 
                                        # client.execute(f"INSERT INTO thurro_pdf_documents_vector_db_amc_wise_abridged_annual_reports_new2 (document_id,document_company,document_company_small_case,document_type,document_year,document_date,document_link,document_content,document_content_small_case,document_content_modify,table_content,embedding,embedding_small_case) VALUES ('{file_id}','{company_name}','{company_small}','{report_type}','{relevant_year}','{document_date}','{link}','''{chunk}''','''{chunk_small}''','''{new_chunk}''',{chunk_embed},{chunk_embed_small});") 
                                    except:
                                        print("****** PAGE WAS SKIPPED ******",i)  
                            else:
                                print("FILE IS TOO SMALL")
                                continue
                            
                            connection=engine.connect()           
                            update_query = f"update AMC_COMPANY_WISE_ABRIDGED_ANNUAL_REPORT_LINKS set Chunking_Status = 'Done' where File_Id = {int(file_id)};"
                            print(update_query)
                            connection.execute(update_query)

                            print('FILE COMPLETED')
                            # delete_file_from_s3(obj)

                    else:
                        print('FILE ALREADY CHUNKED')
                        # continue
                        # delete_file_from_s3(obj)
            
                    
            
                # except:
                #     print("****************** SOMETHING WRONG WITH FILE ******************")
                #     connection=engine.connect()
                #     connection.execute("update AMC_COMPANY_WISE_ABRIDGED_ANNUAL_REPORT_LINKS set Chunking_Status='Issues' where Distinct_File_Name = '" +str(file_name_without_extension)+"'")
        
                
        log.job_end_log(table_name, job_start_time, no_of_ping)    
    except Exception as e:
         error_type = str(re.search("'(.+?)'", str(type(e))).group(1))
         error_msg = str(e) + " line " + str(sys.exc_info()[2].tb_lineno)
         print(error_type)
         print(error_msg)

         log.job_error_log(table_name, job_start_time, error_type, error_msg, no_of_ping)

if __name__ == '__main__':
     run_program(run_by='manual')