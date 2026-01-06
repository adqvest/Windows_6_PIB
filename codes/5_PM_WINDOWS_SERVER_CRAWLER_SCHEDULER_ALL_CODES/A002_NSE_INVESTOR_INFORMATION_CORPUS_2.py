import warnings
warnings.filterwarnings('ignore')

import os
import io
import re
import boto3
import voyageai
import pandas as pd
from sqlalchemy import text
import fitz  
import time

import pypdfium2 as pdfium

from botocore.config import Config

import pdfquery
from lxml import etree

import time
from pytz import timezone
import datetime as datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta

import sys
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log

engine = adqvest_db.db_conn()
connection = engine.connect()

from clickhouse_driver import Client
host = 'ec2-13-204-23-152.ap-south-1.compute.amazonaws.com'
user_name = 'default'
password_string = 'Clickhouse@2025'
db_name = 'AdqvestDB'
client = Client(host, user=user_name, password=password_string, database=db_name)

# ****   Date Time *****
india_time = timezone('Asia/Kolkata')
today = datetime.datetime.now(india_time)
days = datetime.timedelta(1)
yesterday = today - days
timestamp = datetime.datetime.now(india_time).strftime('%Y-%m-%d %H:%M:%S')

os.environ["VOYAGE_API_KEY"] = "pa-97JpiKNlfEEkhs8dBSZOloXM7aw3BI3NJ0ra-zaCSIk"
# os.environ["VOYAGE_API_KEY"] = "pa-zTzrc5RTasN81O4SZXWF9iehCfFOpK0GwH_e7iK8rhI"

def pdf_to_text(pdf_file):
    """Convert a PDF file directly to a list of text per page."""
    # Load the PDF
    pdf = pdfquery.PDFQuery(io.BytesIO(pdf_file))
    pdf.load()  # Load all pages
    
    # Get a list of pages
    pages = pdf.extract([['pages', 'LTPage']])
    page_elements = pages['pages']
    
    def extract_text_from_element(element):
        """Recursively extract text from an XML element and its children."""
        text_list = []
        if element.text and element.text.strip():
            text_list.append(element.text.strip())
        
        # Recursively process all child elements
        for child in element:
            text_list.extend(extract_text_from_element(child))
        
        return text_list
    
    # Initialize a list to store text for each page
    text_per_page = []
    
    for page in page_elements:
        # Convert each page to XML (in-memory)
        xml_data = etree.tostring(page, pretty_print=True).decode('utf-8')
        
        # Parse XML from the in-memory string
        root = etree.fromstring(xml_data)
        
        # Extract text from the XML tree
        text_list = extract_text_from_element(root)
        
        # Join all text entries into a single string with line breaks
        extracted_text = '\n'.join(text_list)
        
        # Append to the list of texts per page
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
    # Skip if the key is the folder itself
    if not file_key.endswith('/'):

        file_name = file_key.split('/')[-1]

        # Reading each file's content
        file_response = s3.get_object(Bucket=bucket_name, Key=file_key)
        file_content = file_response['Body'].read()

        files_content[file_name] = file_content
#         print(f"File '{file_name}' read from S3 Bucket!")
    
    return files_content,file_name

# Function to delete file from S3
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

def extract_text_from_pdf(files_content, file_name, threshold=10, image_coverage_threshold=0.8):
    
    # Splitting file name to extract details
    parts = file_name.split('_')

    # Extract the company name (everything before the first '0')
    company_name = ' '.join(parts[:parts.index('0')])

    # Find the index of the year and file ID
    relevant_year_index = next(i for i, part in enumerate(parts) if 'FY' in part)

    # Extract relevant year and file ID
    relevant_year = parts[relevant_year_index - 1] + ' ' + parts[relevant_year_index]  # Example: 'Q1 FY23'
    file_id = parts[-1]

    # Extract document type (everything between the '0' and the relevant year)
    report_type = ' '.join(parts[parts.index('0') + 1:relevant_year_index - 1])

    file_id = file_id.replace('.pdf','')

    pdf_content = files_content[file_name]

    # Load the PDF document
    pdf = pdfium.PdfDocument(io.BytesIO(pdf_content))

    text = ''
    total_pages = len(pdf)
    page_texts = []

    for page_number in range(total_pages):
        page_text = pdf[page_number].get_textpage().get_text_range()
        page_texts.append(page_text)

        text += 'PAGE NUMBER: ' + str(page_number+1) + '\n\n'
        text += 'COMPANY NAME: ' + str(company_name.strip()) + '\n\n'
        text += 'REPORT TYPE: ' + str(report_type.strip()) + '\n\n'
        text += 'RELEVANT YEAR: ' + str(relevant_year.strip()) + '\n\n'
        text += page_text
        text += '\n\n' 
    text = re.sub('\n\n\n','\n\n', text)

    scanned_pages = []
    pdf_fitz = fitz.open(stream=pdf_content, filetype="pdf")

    for page_num, page in enumerate(pdf_fitz):
        text_len = len(page_texts[page_num].strip())

        low_text = text_len < threshold

        large_image = False
        for img in page.get_images(full=True):
            xref = img[0]
            pix = fitz.Pixmap(pdf_fitz, xref)
            if pix.width >= page.rect.width * 0.9 and pix.height >= page.rect.height * 0.9:
                large_image = True
                break

        if low_text and large_image:
            scanned_pages.append(page_num + 1)

    is_whole_doc_scanned = (len(scanned_pages) == total_pages)
    pdf_fitz.close()

    return company_name,report_type,relevant_year,file_id,text,scanned_pages,is_whole_doc_scanned 

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

# Function to replace multiple patterns using a placeholder method
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
    
def run_program(run_by='Adqvest_Bot', py_file_name=None):
    # os.chdir('C:/Users/Administrator/AdQvestDir')
    engine = adqvest_db.db_conn()
    # ****   Date Time *****
    india_time = timezone('Asia/Kolkata')

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS'
    if (py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if (run_by == 'Adqvest_Bot'):
            log.job_start_log_by_bot(table_name, py_file_name, job_start_time)
        else:
            log.job_start_log(table_name, py_file_name, job_start_time, scheduler)
        
        connection = engine.connect()
        bucket_name = 'adqvests3bucket'
        folder_name = 'NSE_INVESTOR_INFORMATION_CORPUS_TO_BE_CHUNKED_2'

        # response = read_all_file_names_from_s3_folder(bucket_name, folder_name)
        response = read_large_file_names_from_s3_folder(bucket_name, folder_name)
        response = [ x for x in response if 'NSE_INVESTOR_INFORMATION_CORPUS_TO_BE_CHUNKED_2/' in x]
        print(len(response))

        response = response[1:]
        # response = response[:]
        # response = [x for x in response if re.search(r'Hdfc_Bank_Limited_0_Investor_Presentation_Q2_FY26_AZ402', x)]

        doc_ids = client.execute('''SELECT distinct Document_Id FROM NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS_CHUNKED;''')
        doc_ids = [str(x).replace('(','').replace(',)','') for x in doc_ids]
        print(len(doc_ids))

        for obj in response:
            print(obj)
            files_content,file_name = read_all_file_content(obj)
            print("STARTED FILE :",file_name)

            try:
                try:
                    company_name, report_type, relevant_year, file_id, text_content,scanned_pages,is_whole_doc_scanned = extract_text_from_pdf(files_content, file_name)
                except Exception as e:
                    print(e)
                    if 'Failed to load document (PDFium: Data format error)' in str(e):
                        query = f"UPDATE NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS SET Chunking_Comments = 'File Not Available' WHERE File_Name = '{file_name}'"
                        print(query)
                        connection.execute(query)
                    elif 'Failed to load document (PDFium: Incorrect password error)' in str(e):
                        query = f"UPDATE NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS SET Chunking_Comments = 'Password Issue' WHERE File_Name = '{file_name}'"
                        print(query)
                        connection.execute(query)  
                    elif 'Failed to load page' in str(e):
                        query = f"UPDATE NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS SET Chunking_Comments = 'Corrupt Document' WHERE File_Name = '{file_name}'"
                        print(query)
                        connection.execute(query)       
                    else:
                        query = f"UPDATE NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS SET Chunking_Comments = '{str(e)}' WHERE File_Name = '{file_name}'"
                        print(query)
                        connection.execute(query)
                    # connection.commit()
                    time.sleep(2)
                    continue
                
                if is_whole_doc_scanned:
                    query = f"UPDATE NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS SET Chunking_Comments = 'Scanned Document' WHERE File_Name = '{file_name}'"
                    print(query)
                    connection.execute(query)
                    continue

                if str(file_id) not in doc_ids :
                    print(relevant_year)

                    if int(relevant_year[5:])>=18 and int(relevant_year[5:])<30:
                        company_name = company_name.replace("'","\\'") # Escaping single quotes
                        document_date = parse_quarter_year(relevant_year)

                        extracted_chunks = extract_chunks_from_text(text_content)

                        if (len(extracted_chunks)>0):
                            # text_data = pdf_to_text(files_content[file_name])

                            for chunk,page_number in zip(extracted_chunks,range(len(extracted_chunks))):
                                chunk = chunk.replace("\\", "\\\\")  # Escaping backslashes
                                chunk = chunk.replace("\n", "\\n")  # Escaping newlines
                                chunk = chunk.replace("\r", "\\r")  # Escaping carriage returns
                                chunk = chunk.replace("'","\\'") # Escaping single quotes
                                chunk = 'PAGE NUMBER: ' + chunk
                                page = page_number + 1

                                is_true = analyse_chunk(chunk)
                                if is_true:
                                    new_chunk = modify_chunk(chunk)
                                else:
                                    new_chunk = chunk

                                try:
                                    chunk_embed = str(create_embeddings(chunk)).replace('[[','[').replace(']]',']')
                                    # client.execute(f"INSERT INTO thurro_pdf_documents_vector_db_final_2 (document_id,document_company,document_type,document_year,document_date,page_number,document_content,document_content_modify,embedding) VALUES ('{file_id}','{company_name}','{report_type}','{relevant_year}','{document_date}','{page}','''{chunk}''','''{new_chunk}''',{chunk_embed});") 
                                    client.execute(f"INSERT INTO NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS_CHUNKED (Document_Id,Document_Company,Document_Type,Document_Year,Document_Date,Page_Number,Document_Content,Embedding,Runtime_Chunking) VALUES ('{file_id}','{company_name.title()}','{report_type}','{relevant_year}','{document_date}',{page},'''{chunk}''',{chunk_embed},'{timestamp}');")
                                except:
                                    print("****** PAGE WAS SKIPPED ******",page_number)     

                            delete_file_from_s3(obj)
                            print('FILE COMPLETED')
                            
                            connection = engine.connect()
                            query='Update NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS set Chunking_Status="Done" where Company_Name="'+company_name+'" and File_ID="'+str(file_id)+'" and Relevant_Quarter="'+relevant_year+'"'
                            connection.execute(text(query))
                            # connection.commit()
                            time.sleep(2)
                            print('Done updates for files that are chunked')

                        else:
                            print("FILE IS TOO SMALL")
                    else:
                        print("FILE NOT RELEVANT")
                        query='Update NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS set Chunking_Status="Not Relevant" where Company_Name="'+company_name+'" and File_ID="'+str(file_id)+'" and Relevant_Quarter="'+relevant_year+'"'
                        connection = engine.connect()
                        connection.execute(text(query))
                        # connection.commit()
                        time.sleep(2)
                        print('Done updates for files that are not relevant')

                else:
                    print('FILE ALREADY CHUNKED')
                    delete_file_from_s3(obj)
                    connection = engine.connect()
                    query='Update NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS set Chunking_Status="Done" where Company_Name="'+company_name+'" and File_ID="'+str(file_id)+'" and Relevant_Quarter="'+relevant_year+'"'
                    connection.execute(text(query))
                    # connection.commit()
                    time.sleep(2)
                    print('Done updates for files that are already chunked')

            except:
                print("****************** SOMETHING WRONG WITH FILE ******************")
                # query=f"Update NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS set Chunking_Comments='ISSUE' where File_Name = '{file_name}'"
                # print(query)
                # try:
                #     connection.execute(query)
                #     print('Done updates as ISSUE')
                # except:
                # # connection.commit()
                #     time.sleep(2)
                #     continue
        
        print("_________________________________________ LINK UPDATE __________________________________________")        

        connection = engine.connect()
        query = "select * from AdqvestDB.NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS ORDER BY File_ID ASC;"
        data = pd.read_sql(query, con=engine)

        doc = client.execute('''SELECT distinct Document_Id as id , Page_Number as pn FROM NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS_CHUNKED where Document_Link = '' ;''')
        print(len(doc))

        if len(doc)>1:
            companies = doc
            company = [x[0] for x in companies]
            data = data[data['File_ID'].isin(company)]
            data.reset_index(drop = True, inplace = True)
            print(len(data))

            # Prepare your data
            update_data = []
            for x in range(len(data)):
                doc_id = data["File_ID"][x]
                link = data["File_Link"][x].replace("'", "\\'")  # Escape single quotes
                update_data.append((doc_id, link))

            # Execute updates in batches
            batch_size = 1000
            total_batches = (len(update_data) + batch_size - 1) // batch_size

            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min((batch_num + 1) * batch_size, len(update_data))
                batch = update_data[start_idx:end_idx]
                
                case_parts = [f"WHEN Document_Id = '{doc_id}' THEN '{link}'" for doc_id, link in batch]
                case_statement_link = "CASE " + " ".join(case_parts) + " ELSE Document_Link END"
                doc_ids = ", ".join([f"'{doc_id}'" for doc_id, _ in batch])
                
                # Update in one batch query
                query = f"""
                    ALTER TABLE NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS_CHUNKED
                    UPDATE 
                        Document_Link = {case_statement_link},
                        Vector_Db_Status = 'LINK UPDATED'
                    WHERE Document_Id IN ({doc_ids})
                    
                """
                
                # Execute the query
                client.execute(query)
                print(f"Completed batch {batch_num + 1}/{total_batches}") 

            query = """
                    ALTER TABLE NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS_CHUNKED
                    UPDATE 
                        Document_Link = concat(Document_Link, '#page=', toString(Page_Number))
                    WHERE Document_Link NOT ILIKE '%#page=%'
                    AND Document_Link ILIKE '%.pdf%'
                    AND Document_Link NOT LIKE '%.html%' 
                    AND Document_Link NOT LIKE '%.htm%';
                    """
                
            # Execute the query
            client.execute(query)
            print("Completed PAGE number updation") 

            query = """
                    ALTER TABLE NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS_CHUNKED
                    UPDATE 
                        Chunk_Id = concat(Document_Id, '_', toString(Page_Number))
                    WHERE Chunk_Id ='';
                    """
                
            # Execute the query
            client.execute(query)
            print("Completed Chunk_Id updation")

        connection = engine.connect()
        query = "select * from AdqvestDB.NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS ORDER BY File_ID ASC;"
        data = pd.read_sql(query, con=engine)

        doc = client.execute('''SELECT distinct Document_Id as id FROM NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS_CHUNKED where Runtime_Scraped is Null ;''')
        print(len(doc))

        if len(doc)>0:
            companies = doc
            company = [x[0] for x in companies]
            data = data[data['File_ID'].isin(company)]
            data.reset_index(drop = True, inplace = True)
            print(len(data))

            # Prepare your data
            update_data = []
            for x in range(len(data)):
                doc_id = data["File_ID"][x]
                runtime = data["Runtime"][x]
                update_data.append((doc_id, runtime))

            # Execute updates in batches
            batch_size = 1000
            total_batches = (len(update_data) + batch_size - 1) // batch_size

            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min((batch_num + 1) * batch_size, len(update_data))
                batch = update_data[start_idx:end_idx]

                case_parts = [f"WHEN Document_Id = '{doc_id}' THEN toDateTime('{runtime}') " for doc_id, runtime in batch]
                case_statement_link = "CASE " + " ".join(case_parts) + " ELSE Runtime_Scraped END"
                doc_ids = ", ".join([f"'{doc_id}'" for doc_id, _ in batch])
                                
                # Update in one batch query
                query = f"""
                    ALTER TABLE NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS_CHUNKED
                    UPDATE 
                        Runtime_Scraped = {case_statement_link}
                    WHERE Document_Id IN ({doc_ids})
                    
                """
                
                # Execute the query
                client.execute(query)
                print(f"Completed batch {batch_num + 1}/{total_batches}") 

        connection = engine.connect()
        query = "select * from AdqvestDB.NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS ORDER BY File_ID ASC;"
        data = pd.read_sql(query, con=engine)

        doc = client.execute('''SELECT distinct Document_Id as id FROM NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS_CHUNKED where Published_Date is Null ;''')
        print(len(doc))

        if len(doc)>0:
            companies = doc
            company = [x[0] for x in companies]
            data = data[data['File_ID'].isin(company)]
            data.reset_index(drop = True, inplace = True)
            print(len(data))

            # Prepare your data
            update_data = []
            for x in range(len(data)):
                doc_id = data["File_ID"][x]
                publish_date = str(data["Broadcast_Date"][x])
                update_data.append((doc_id, publish_date))

            # Execute updates in batches
            batch_size = 1000
            total_batches = (len(update_data) + batch_size - 1) // batch_size

            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min((batch_num + 1) * batch_size, len(update_data))
                batch = update_data[start_idx:end_idx]

                case_parts = [f"WHEN Document_Id = '{doc_id}' THEN toDateTime('{publish_date}') " for doc_id, publish_date in batch]
                case_statement_link = "CASE " + " ".join(case_parts) + " ELSE Published_Date END"
                doc_ids = ", ".join([f"'{doc_id}'" for doc_id, _ in batch])
                
                # Update in one batch query
                query = f"""
                    ALTER TABLE NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS_CHUNKED
                    UPDATE 
                        Published_Date = {case_statement_link}
                    WHERE Document_Id IN ({doc_ids})
                    
                """
                
                # Execute the query
                client.execute(query)
                print(f"Completed batch {batch_num + 1}/{total_batches}") 
            
        print("_________________________________________ SYMBOL UPDATE __________________________________________")     
        
        connection = engine.connect()
        query = "select * from AdqvestDB.NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS ORDER BY File_ID ASC;"
        data = pd.read_sql(query, con=engine)
        
        dcomp = client.execute('''SELECT distinct Document_Company as dcomp FROM NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS_CHUNKED where Symbol = '';''')
        print(len(dcomp))
        
        if len(dcomp) > 0:
            companies = dcomp
            company = [x[0] for x in companies]
               
            data = data[data['Company_Name'].isin(company)]
            data.reset_index(drop = True, inplace = True)
            print(len(data))
            
            update_data = []
            for x in range(len(data)): 
                company_name = data["Company_Name"].iloc[x] 
                symbol = data["Symbol"].iloc[x]
                update_data.append((company_name, symbol))
        
            # Execute updates in batches
            batch_size = 100
            total_batches = (len(update_data) + batch_size - 1) // batch_size
            
            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min((batch_num + 1) * batch_size, len(update_data))
                batch = update_data[start_idx:end_idx]
                
                case_parts = []
                for comp, sym in batch:
                    safe_sym = str(sym).replace("'", "''")
                    safe_comp = str(comp).replace("'", "''").replace("&", "and")
                
                    case_parts.append(f"WHEN replaceAll(Document_Company, '&', 'and') = '{safe_comp}' THEN '{safe_sym}'")
                
                comps = ', '.join(["'" + str(comp).replace("'", "''").replace('&', 'and') + "'" for comp, _ in batch])
                
                query = f"""
                    ALTER TABLE NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS_CHUNKED
                    UPDATE Symbol = CASE { " ".join(case_parts) } ELSE Symbol END
                    WHERE replaceAll(Document_Company, '&', 'and') IN ({comps}) AND Symbol = ''
                """
        
                client.execute(query)
                print(f"Completed batch {batch_num + 1}/{total_batches}")     
        
        print("_________________________________________ SECTOR, INDUSTRY ETC UPDATE __________________________________________")

        log.job_end_log(table_name, job_start_time, no_of_ping)    
    except Exception as e:
         error_type = str(re.search("'(.+?)'", str(type(e))).group(1))
         error_msg = str(e) + " line " + str(sys.exc_info()[2].tb_lineno)
         print(error_type)
         print(error_msg)

         log.job_error_log(table_name, job_start_time, error_type, error_msg, no_of_ping)

if __name__ == '__main__':
     run_program(run_by='manual')    