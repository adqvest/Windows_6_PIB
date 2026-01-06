

import os
import io
import re
import boto3
import voyageai
from pytz import timezone
import datetime as datetime
import pypdfium2 as pdfium
import pandas as pd
from sqlalchemy import text

import warnings
warnings.filterwarnings('ignore')

from botocore.config import Config
import pdfquery
from lxml import etree

import sys
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log

engine = adqvest_db.db_conn()

from clickhouse_driver import Client
host = 'ec2-52-88-156-240.us-west-2.compute.amazonaws.com'
user_name = 'default'
password_string = 'Clickhouse@2024'
db_name = 'AdqvestDB'
client = Client(host, user=user_name, password=password_string, database=db_name)

from datetime import timedelta
from dateutil.relativedelta import relativedelta

os.environ["VOYAGE_API_KEY"] = "pa-97JpiKNlfEEkhs8dBSZOloXM7aw3BI3NJ0ra-zaCSIk"

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

def read_all_file_names_from_s3_folder(bucket_name, folder_name):
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

        files_content[file_name] = file_content
    
    return files_content,file_name

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
        print("Deleted from S3 bucket")
    except Exception as e:
        print(f"Error deleting {file_name} from S3 bucket: {e}")

def extract_text_from_pdf(files_content,file_name):
    
    # Splitting file name to extract details
    parts = file_name.split('_')
    # Extract the company name (everything before the first '0')
    company_name = ' '.join(parts[:parts.index('0')])

    # Find the index containing 'FY'
    try:
        fy_index = next(i for i, part in enumerate(parts) if 'FY' in part)
    except:
        fy_index = None
    
    # Initialize quarter variable
    quarter = None

    if fy_index:    
        # Extract relevant year based on pattern
        if 'Q' in parts[fy_index - 1]:
            # Pattern with quarter (Q1/Q2/Q3/Q4)
            quarter = parts[fy_index - 1]
            relevant_year = f"{quarter} {parts[fy_index]}"
        elif 'F' in parts[fy_index]:
            # Pattern without quarter (just FY)
            relevant_year = parts[fy_index]
    else:
        relevant_year = None
        
    file_id = parts[-1].replace('.pdf', '')

    report_type = None
    # Extract document type (everything between the '0' and the FY/quarter part)
    if fy_index:
        doc_type_end = fy_index - 2 if quarter else fy_index - 1
        report_type = ' '.join(parts[parts.index('0') + 1:doc_type_end + 1])
    if report_type == '':
        report_type = None
    
    pdf_content = files_content[file_name]

    # Load the PDF document
    pdf = pdfium.PdfDocument(io.BytesIO(pdf_content))
    text_content = ''
    for page_number in range(len(pdf)):
        page_text = pdf[page_number].get_textpage().get_text_range()
        text_content += 'PAGE NUMBER: ' + str(page_number+1) + '\n\n'
        text_content += 'COMPANY NAME: ' + str(company_name.strip()) + '\n\n'
        # text_content += 'REPORT TYPE: ' + str(report_type.strip()) + '\n\n'
        text_content += 'REPORT TYPE: ' + str(report_type.strip() if report_type else None) + '\n\n'
        # text_content += 'RELEVANT YEAR: ' + str(relevant_year.strip()) + '\n\n'
        text_content += 'RELEVANT YEAR: ' + str(relevant_year.strip() if relevant_year else None) + '\n\n'
        # text_content += 'RELEVANT YEAR: ' + str(relevant_year.strip()) + '\n\n'
        text_content += page_text
        text_content += '\n\n'
    text_content = re.sub('\n\n\n','\n\n', text_content)
    return company_name,report_type,relevant_year,file_id,text_content

def create_embeddings(chunked_text):
    
    vo = voyageai.Client(api_key="pa-97JpiKNlfEEkhs8dBSZOloXM7aw3BI3NJ0ra-zaCSIk")
    # Embed the documents
    doc_embds = vo.embed(
        chunked_text, model="voyage-2", input_type="document"
    ).embeddings
    
    return doc_embds

def extract_chunks_from_text(text_content):
    
    chunks = text_content.split("PAGE NUMBER: ")
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


def replace_multiple_patterns(text_content, replacements):
    # Create a list of unique placeholders
    placeholders = {pattern: f"__PLACEHOLDER_{i}__" for i, pattern in enumerate(replacements)}

    # Replace each pattern with its unique placeholder
    for pattern, placeholder in placeholders.items():
        text_content = re.sub(pattern, placeholder, text_content)

    # Replace each placeholder with its final replacement value
    for pattern, placeholder in placeholders.items():
        text_content = re.sub(placeholder, replacements[pattern], text_content)

    return text_content

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

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    # os.chdir('C:/Users/Administrator/AdQvestDir')
    engine = adqvest_db.db_conn()
    # ****   Date Time *****
    india_time = timezone('Asia/Kolkata')

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
        folder_name = 'ICRA_CORPUS_TO_BE_CHUNKED/'
        
        response = read_all_file_names_from_s3_folder(bucket_name, folder_name)
        response = [ x for x in response if 'ICRA_CORPUS_TO_BE_CHUNKED/' in x]
        response = response [400:]
        print(len(response))
        
        doc_ids = client.execute('''SELECT distinct document_id FROM ICRA_DAILY_DATA_CORPUS_CHUNKED_1;''')
        doc_ids = [str(x).replace('(','').replace(',)','') for x in doc_ids]
        print(len(doc_ids))
        
        for obj in response:
            print(obj)
            files_content,file_name = read_all_file_content(obj)
            print("STARTED FILE :",file_name)
        
            try:
                company_name,report_type,relevant_year,file_id,text_content = extract_text_from_pdf(files_content,file_name)
                if str(file_id) not in doc_ids :
                    
                    print(relevant_year)
                    
                    if relevant_year is not None:
                        if 'Q' in relevant_year: 
                            if int(relevant_year[5:]) >= 18 and int(relevant_year[5:]) < 30:
                                company_name = company_name.replace("'", "\\'")  # Escaping single quotes
                                document_date = parse_quarter_year(relevant_year)
                            else:
                                print("FILE NOT RELEVANT ") 
                                connection=engine.connect()
                                connection.execute("update ICRA_DAILY_DATA_CORPUS_NEW set Chunking_Status='Not Relevant' where File_Name = '" +str(file_name).replace('.pdf', '')+"'")
                                connection.execute("commit")
                                continue      
                        elif int(relevant_year[2:]) >= 18 and int(relevant_year[2:]) < 30:
                                company_name = company_name.replace("'", "\\'")  # Escaping single quotes
                                document_date = parse_quarter_year(relevant_year)
                        else:
                            print("FILE NOT RELEVANT ") 
                            connection=engine.connect()
                            connection.execute("update ICRA_DAILY_DATA_CORPUS_NEW set Chunking_Status='Not Relevant' where File_Name = '" +str(file_name).replace('.pdf', '')+"'")
                            connection.execute("commit")
                            continue        
                    else:
                        print("RELEVANT YEAR NOT FOUND") 
                        connection=engine.connect()
                        connection.execute("update ICRA_DAILY_DATA_CORPUS_NEW set Chunking_Status='Issues' where File_Name = '" +str(file_name).replace('.pdf', '')+"'")
                        connection.execute("commit")
                        continue
        
                    extracted_chunks = extract_chunks_from_text(text_content)
        
                    if (len(extracted_chunks)>0):
        
                        for chunk,page_number in zip(extracted_chunks,range(len(extracted_chunks))):
                            chunk = chunk.replace("\\", "\\\\")  # Escaping backslashes
                            chunk = chunk.replace("\n", "\\n")  # Escaping newlines
                            chunk = chunk.replace("\r", "\\r")  # Escaping carriage returns
                            chunk = chunk.replace("'","\\'") # Escaping single quotes
                            chunk = 'PAGE NUMBER: ' + chunk
                            page = page_number+1
        
                            is_true = analyse_chunk(chunk)
                            if is_true:
                                new_chunk = modify_chunk(chunk)
                            else:
                                new_chunk = chunk    
        
                            chunk_small = chunk.lower()                    
                            company_name_small = company_name.lower()  
                            print('Till Here')                                
                            try:
                                chunk_embed = str(create_embeddings(chunk)).replace('[[','[').replace(']]',']')
                                chunk_embed_small = str(create_embeddings(chunk_small)).replace('[[','[').replace(']]',']')   
                                client.execute(f"INSERT INTO ICRA_DAILY_DATA_CORPUS_CHUNKED_1 (document_id,document_company,document_company_small_case,document_type,document_year,document_date,page_number,document_content,document_content_small_case,document_content_modify,embedding,embedding_small_case) VALUES ('{file_id}','{company_name}','{company_name_small}','{report_type}','{relevant_year}','{document_date}',{page},'''{chunk}''','''{chunk_small}''','''{new_chunk}''',{chunk_embed},{chunk_embed_small});") 
                                
                                connection=engine.connect() 
                                connection.execute(text("UPDATE ICRA_DAILY_DATA_CORPUS_NEW SET Chunking_Status='Done' WHERE Generated_File_Name = :file_name"),{"file_name": file_name})
                                # connection.commit()
                                
                            except:
                                print("****** PAGE WAS SKIPPED ******",page_number)     
        
                        delete_file_from_s3(obj)
                        print('FILE COMPLETED')
        
                    else:
                        print("FILE IS TOO SMALL")
        
                else:
                    print('FILE ALREADY CHUNKED')
                    delete_file_from_s3(obj)
        
            except:
                print("****************** SOMETHING WRONG WITH FILE ******************")
        
        connection = engine.connect()
        query = "select * from AdqvestDB.ICRA_DAILY_DATA_CORPUS_NEW ORDER BY File_ID ASC;"
        data = pd.read_sql(query, con=engine)

        doc = client.execute('''SELECT distinct document_id as id , page_number as pn FROM ICRA_DAILY_DATA_CORPUS_CHUNKED_1 where document_link = '' ;''')
        len(doc)
        
        companies = {str(x[0]): x[1] for x in doc}  
        company_ids = set(companies.keys())

        data = data[data['File_ID'].astype(str).isin(company_ids)]
        data.reset_index(drop=True, inplace=True)
        
        count = 0
        
        from collections import defaultdict
        doc_map = defaultdict(list)
        for doc_id, page_number in doc:
            doc_map[str(doc_id)].append(int(page_number))  
            
        for x in range(len(data)):
            document_id = str(int(float(data["File_ID"][x]))) 
            document_link = data["File_Link"][x]
            page_numbers = doc_map.get(document_id, [])
        
            for page in page_numbers:
                updated_link = document_link + f'#page={page}'
                print(f"Updating: {document_id} → {updated_link}")    

                client.execute(f'''ALTER TABLE ICRA_DAILY_DATA_CORPUS_CHUNKED_1 UPDATE document_link = '{updated_link}' WHERE document_id = '{document_id}' and page_number = '{page}' ;''')
                client.execute(f'''ALTER TABLE ICRA_DAILY_DATA_CORPUS_CHUNKED_1 UPDATE vector_db_status = 'LINK UPDATED' WHERE document_id = '{document_id}';''')
                print("COMPLETED LOOP:",count)
                count += 1
            
        if count % 900 == 0:
            print("OPTIMIZING DATA")
            client.execute('''OPTIMIZE TABLE ICRA_DAILY_DATA_CORPUS_CHUNKED_1 FINAL;''')        
                
        log.job_end_log(table_name, job_start_time, no_of_ping)    
    except Exception as e:
         error_type = str(re.search("'(.+?)'", str(type(e))).group(1))
         error_msg = str(e) + " line " + str(sys.exc_info()[2].tb_lineno)
         print(error_type)
         print(error_msg)

         log.job_error_log(table_name, job_start_time, error_type, error_msg, no_of_ping)

if __name__ == '__main__':
     run_program(run_by='manual')