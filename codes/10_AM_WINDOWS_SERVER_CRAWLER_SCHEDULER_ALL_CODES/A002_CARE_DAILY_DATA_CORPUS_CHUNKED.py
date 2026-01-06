# -*- coding: utf-8 -*-
"""
Created on Tue Apr  1 13:14:03 2025

@author: GOKUL
"""

import warnings
warnings.filterwarnings('ignore')
import pandas as pd
import os
from sqlalchemy import text
sqltext = text
from bs4 import BeautifulSoup
from pytz import timezone
import io
import re
import boto3
import voyageai
import datetime as datetime
import pypdfium2 as pdfium
import warnings
warnings.filterwarnings('ignore')
from botocore.config import Config
import pdfquery
from lxml import etree
from datetime import timedelta
from dateutil.relativedelta import relativedelta

import sys
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
engine = adqvest_db.db_conn()
import JobLogNew as log

from clickhouse_driver import Client
host = '172.31.67.17'
user_name = 'default'
password_string = 'Clickhouse@2025'
db_name = 'AdqvestDB'
client = Client(host, user=user_name, password=password_string, database=db_name)

os.environ["VOYAGE_API_KEY"] = "pa-97JpiKNlfEEkhs8dBSZOloXM7aw3BI3NJ0ra-zaCSIk"

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

def read_all_file_content(key,file_name,bucket_name='adqvests3bucket'):
    print("fetching file from S3")
    s3 = boto3.client('s3',
                      config=Config(signature_version='s3v4', region_name='ap-south-1'))

    files_content = {}
    file_key = key + file_name

    file_response = s3.get_object(Bucket=bucket_name, Key=file_key)
    file_content = file_response['Body'].read()

    files_content[file_name] = file_content
    
    return files_content,file_name

# Function to delete file from S3
def delete_file_from_s3(file_name,bucket_name = 'adqvests3bucket'):
    
    s3 = boto3.client('s3', 
                      config=Config(signature_version='s3v4', region_name='ap-south-1'))
    
    try:
        s3.delete_object(Bucket=bucket_name, Key=file_name)
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
    
def extract_text_from_html(files_content, file_name, file_name_actual):
    # Splitting file name to extract details - keeping the same logic as PDF version
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

    file_id = parts[-1].replace('.html', '')
    # Extract document type (everything between the '0' and the relevant year)
    report_type = None
    # Extract document type (everything between the '0' and the FY/quarter part)
    if fy_index:
        doc_type_end = fy_index - 2 if quarter else fy_index - 1
        report_type = ' '.join(parts[parts.index('0') + 1:doc_type_end + 1])
    if report_type == '':
        report_type = None
        
    file_id = file_id.replace('.pdf', '')
    file_id = file_id.replace('.html', '')

    # Get HTML content
    html_content = files_content[file_name_actual]
    if isinstance(html_content, bytes):
        html_content = html_content.decode('utf-8', errors='replace')  # or errors='ignore'


    # Parse HTML
    soup = BeautifulSoup(html_content, 'html.parser')

    # Remove script and style elements
    for script in soup(["script", "style"]):
        script.decompose()

    # Get text
    text_content = soup.get_text(separator='\n')
    
    # Clean up text
    lines = [line.strip() for line in text.split('\n')]
    text_content = '\n'.join(line for line in lines if line)

    # Format the text similar to PDF output
    formatted_text = 'PAGE NUMBER: 1\n\n'  # HTML doesn't have pages, so we use 1
    formatted_text += f'COMPANY NAME: {company_name.strip()}\n\n'
    formatted_text += f'REPORT TYPE: {report_type.strip()}\n\n'
    formatted_text += f'RELEVANT YEAR: {relevant_year.strip()}\n\n'
    formatted_text += text_content
    formatted_text += '\n\n'

    # Clean up multiple newlines
    formatted_text = re.sub('\n\n\n', '\n\n', formatted_text)

    return company_name, report_type, relevant_year, file_id, formatted_text

def create_embeddings(chunked_text):
    
    vo = voyageai.Client(api_key="pa-97JpiKNlfEEkhs8dBSZOloXM7aw3BI3NJ0ra-zaCSIk")
    # Embed the documents
    doc_embds = vo.embed(chunked_text, model="voyage-2", input_type="document").embeddings
    
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
        text_text = re.sub(pattern, placeholder, text)

    # Replace each placeholder with its final replacement value
    for pattern, placeholder in placeholders.items():
        text_text = re.sub(placeholder, replacements[pattern], text)

    return text_text

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
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    timestamp = datetime.datetime.now(india_time).strftime('%Y-%m-%d %H:%M:%S')
    
    # job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'CARE_RATINGS_DAILY_DATA_CORPUS_CHUNKING'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
              
        connection=engine.connect()
        
        folder_name = 'CARE_RATINGS_CORPUS_TO_BE_CHUNKED/'

        # response = read_all_file_names_from_s3_folder(bucket_name, folder_name)
        # response = [ x for x in response if 'CARE_RATINGS_CORPUS_TO_BE_CHUNKED/' in x]
        # response = response [1:]
        
        query = "select * from CARE_RATINGS_DAILY_DATA_CORPUS where Chunking_Status is Null and S3_Upload_Status = 'Done' "
        care_ratings = pd.read_sql(query,con=engine)
        print(len(care_ratings))
        
        doc_links = client.execute('''SELECT distinct Document_Link FROM CARE_DAILY_DATA_CORPUS_CHUNKED;''')
        doc_links = [link for (link,) in doc_links]
        print(len(doc_links))
        
        doc_ids = client.execute('''SELECT distinct Document_Id FROM CARE_DAILY_DATA_CORPUS_CHUNKED;''')
        doc_ids = [str(row[0]).strip().strip("'") for row in doc_ids if row[0] is not None]
        print(len(doc_ids))
        
        for i in range(len(care_ratings)):
            company_name = care_ratings['Company_Name'][i]
            link = care_ratings['File_Link'][i]
            new_file_name = care_ratings['Generated_File_Name'][i]
            table_file_name = new_file_name.replace("'", "")
            print(table_file_name)
            year = care_ratings['Relevant_Date'][i].year
            month = care_ratings['Relevant_Date'][i].month

            if(month >= 4):
                year = year+1
            if(month < 4):
                year = year
        
            if(link not in doc_links):
                print('We can go ahead')
                try:
                    files_content,file_name = read_all_file_content(folder_name,new_file_name)
                except:
                    update_query = f"update CARE_RATINGS_DAILY_DATA_CORPUS set Chunking_Comments = 'ISSUE' where Generated_File_Name = '{table_file_name}' and Relevant_Date = '{care_ratings['Relevant_Date'][i]}'"
                    connection.execute(sqltext(update_query))
                    # connection.commit()
                    continue
                
                print("STARTED FILE :",file_name)
                # file_name_updated = company_name.replace(' ','_')+'_0_'+'CREDIT_RATING_'+'FY'+str(year)+'_'+str(file_id)+'.pdf'
        
                try:
                    # company_name,report_type,relevant_year,file_id,text_content = extract_text_from_pdf(files_content,file_name)
                    try:
                        company_name,report_type,relevant_year,file_id,text_cont = extract_text_from_pdf(files_content,file_name)
                    except:
                        print('Failed here')
                        connection=engine.connect()
                        connection.execute(f"update CARE_RATINGS_DAILY_DATA_CORPUS set Chunking_Comments='ISSUE' where Generated_File_Name = '{table_file_name}' and Relevant_Date = '{care_ratings['Relevant_Date'][i]}'")
                        connection.execute("commit")
                        # delete_file_from_s3(obj)
                        continue  
                    if str(file_id) not in doc_ids :
                        print(relevant_year)
                        if 'Q' in relevant_year: 
                            if int(relevant_year[5:]) >= 18 and int(relevant_year[5:]) < 30:
                                company_name = company_name.replace("'", "\\'")  # Escaping single quotes
                                document_date = parse_quarter_year(relevant_year)
                            else:
                                print("FILE NOT RELEVANT ") 
                                connection=engine.connect()
                                connection.execute("update CARE_RATINGS_DAILY_DATA_CORPUS_NEW set Chunking_Status='Not Relevant' where Generated_File_Name = '" +str(table_file_name)+"'")
                                connection.execute("commit")
                                continue      
                        elif int(relevant_year[2:]) >= 18 and int(relevant_year[2:]) < 30:
                                company_name = company_name.replace("'", "\\'")  # Escaping single quotes
                                document_date = parse_quarter_year(relevant_year)
                        else:
                            print("FILE NOT RELEVANT ") 
                            connection=engine.connect()
                            connection.execute("update CARE_RATINGS_DAILY_DATA_CORPUS_NEW set Chunking_Status='Not Relevant' where Generated_File_Name = '" +str(table_file_name)+"'")
                            connection.execute("commit")
                            delete_file_from_s3(file_name)
                            continue

                        extracted_chunks = extract_chunks_from_text(text_cont)
    
                        if (len(extracted_chunks)>0):
                            # text_data = pdf_to_text(files_content[file_name])
    
                            for chunk,page_number in zip(extracted_chunks,range(len(extracted_chunks))):
                                chunk = chunk.replace("\\", "\\\\")  # Escaping backslashes
                                chunk = chunk.replace("\n", "\\n")  # Escaping newlines
                                chunk = chunk.replace("\r", "\\r")  # Escaping carriage returns
                                chunk = chunk.replace("'","\\'") # Escaping single quotes
                                chunk = 'PAGE NUMBER: ' + chunk
                                page = page_number+1
    
                                # is_true = analyse_chunk(chunk)
                                # if is_true:
                                #     new_chunk = modify_chunk(chunk)
                                # else:
                                new_chunk = chunk
                                company_small = company_name.lower()
                                chunk_small = chunk.lower()
    
                                try:
                                    chunk_embed = str(create_embeddings(chunk)).replace('[[','[').replace(']]',']')
                                    chunk_embed_small = str(create_embeddings(chunk_small)).replace('[[','[').replace(']]',']')   
                                    # client.execute(f"INSERT INTO CARE_DAILY_DATA_CORPUS_CHUNKED (Document_Id,Chunk_Id,Document_Company,Document_Type,Document_Year,Document_Date,Page_Number,Document_Content,Embedding,Runtime_Chunking) VALUES ('{file_id}','{company_name.str.title()}','{report_type}','{relevant_year}','{document_date}',{page},'''{chunk}''',{chunk_embed},'{datetime.datetime.now(india_time)}');") 
                                    client.execute(f"INSERT INTO CARE_DAILY_DATA_CORPUS_CHUNKED (Document_Id,Document_Company,Document_Type,Document_Year,Document_Date,Page_Number,Document_Content,Embedding,Runtime_Chunking) VALUES ('{file_id}','{company_name.title()}','{report_type}','{relevant_year}','{document_date}',{page},'''{chunk}''',{chunk_embed},'{timestamp}');")
                                    update_query = f"update CARE_RATINGS_DAILY_DATA_CORPUS set Chunking_Status = 'Done' where Generated_File_Name = '{table_file_name}' and Relevant_Date = '{care_ratings['Relevant_Date'][i]}'"
                                    connection.execute(sqltext(update_query))
                                    connection.execute("commit")
                                    # connection.close()
                                
                                except:
                                    print("****** PAGE WAS SKIPPED ******",page_number)     
    
                            print('FILE COMPLETED')
                            delete_file_from_s3(file_name)
    
                        else:
                            print("FILE IS TOO SMALL")
        
                    else:
                        print('FILE ALREADY CHUNKED')
                        delete_file_from_s3(file_name)
                        update_query = f"update CARE_RATINGS_DAILY_DATA_CORPUS set Chunking_Status = 'Done' where Generated_File_Name = '{table_file_name}' and Relevant_Date = '{care_ratings['Relevant_Date'][i]}'"
                        connection.execute(sqltext(update_query))
                        connection.execute("commit")
                        continue
        
                except:
                    print("****************** SOMETHING WRONG WITH FILE ******************")                     
                    updated_link = link.replace('%','%%')
                    update_query = f"update CARE_RATINGS_DAILY_DATA_CORPUS set Chunking_Comments = 'ISSUE' where Generated_File_Name = '{table_file_name}' and Relevant_Date = '{care_ratings['Relevant_Date'][i]}'"
                    connection.execute(sqltext(update_query))
                    # connection.execute("commit")
            else:
                print('LINK EXISTS CHUNKED')
                delete_file_from_s3(new_file_name)
                update_query = f"update CARE_RATINGS_DAILY_DATA_CORPUS set Chunking_Status = 'Done' where Generated_File_Name = '{table_file_name}' and Relevant_Date = '{care_ratings['Relevant_Date'][i]}'"
                connection.execute(sqltext(update_query))
                connection.execute("commit")
                continue

        connection = engine.connect()
        query = "select * from AdqvestDB.CARE_RATINGS_DAILY_DATA_CORPUS ORDER BY File_ID ASC;"
        data = pd.read_sql(query, con=engine)

        doc = client.execute('''SELECT distinct Document_Id as id , Page_Number as pn FROM CARE_DAILY_DATA_CORPUS_CHUNKED where Document_Link = '' ;''')
        print(len(doc))

        if len(doc) > 0:
            companies = doc
            company = [x[0] for x in companies]
            data = data[data['File_ID'].isin(company)]
            data.reset_index(drop = True, inplace = True)

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
                    ALTER TABLE CARE_DAILY_DATA_CORPUS_CHUNKED
                    UPDATE 
                        Document_Link = {case_statement_link},
                        Vector_Db_Status = 'LINK UPDATED'
                    WHERE Document_Id IN ({doc_ids})
                    
                """
                
                # Execute the query
                client.execute(query)
                print(f"Completed batch {batch_num + 1}/{total_batches}") 

            query = """
                    ALTER TABLE CARE_DAILY_DATA_CORPUS_CHUNKED
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
                    ALTER TABLE CARE_DAILY_DATA_CORPUS_CHUNKED
                    UPDATE 
                        Chunk_Id = concat(Document_Id, '_', toString(Page_Number))
                    WHERE Chunk_Id ='';
                    """
                
            # Execute the query
            client.execute(query)
            print("Completed Chunk_Id updation")

        connection = engine.connect()
        query = "select * from AdqvestDB.CARE_RATINGS_DAILY_DATA_CORPUS ORDER BY File_ID ASC;"
        data = pd.read_sql(query, con=engine)

        doc = client.execute('''SELECT distinct Document_Id as id FROM CARE_DAILY_DATA_CORPUS_CHUNKED where Runtime_Scraped is Null ;''')
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
                    ALTER TABLE CARE_DAILY_DATA_CORPUS_CHUNKED
                    UPDATE 
                        Runtime_Scraped = {case_statement_link}
                    WHERE Document_Id IN ({doc_ids})
                    
                """
                
                # Execute the query
                client.execute(query)
                print(f"Completed batch {batch_num + 1}/{total_batches}") 

        connection = engine.connect()
        query = "select * from AdqvestDB.CARE_RATINGS_DAILY_DATA_CORPUS ORDER BY File_ID ASC;"
        data = pd.read_sql(query, con=engine)

        doc = client.execute('''SELECT distinct Document_Id as id FROM CARE_DAILY_DATA_CORPUS_CHUNKED where Published_Date is Null;''')
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
                publish_date = str(data["Relevant_Date"][x]) + ' 12:00:00'
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
                    ALTER TABLE CARE_DAILY_DATA_CORPUS_CHUNKED
                    UPDATE 
                        Published_Date = {case_statement_link}
                    WHERE Document_Id IN ({doc_ids})
                    
                """
                
                # Execute the query
                client.execute(query)
                print(f"Completed batch {batch_num + 1}/{total_batches}")  
        
        log.job_end_log(table_name,job_start_time,no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')   