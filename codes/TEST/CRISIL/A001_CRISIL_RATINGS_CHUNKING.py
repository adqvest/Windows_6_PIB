import warnings
warnings.filterwarnings('ignore')
import pandas as pd
import os
from sqlalchemy import text
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
from bs4 import BeautifulSoup

import sys
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
engine = adqvest_db.db_conn()
import JobLogNew as log

from clickhouse_driver import Client
host = 'ec2-52-88-156-240.us-west-2.compute.amazonaws.com'
user_name = 'default'
password_string = 'Clickhouse@2024'
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
    ACCESS_KEY_ID = 'AKIAYCVSRU2U3XP2DB75'
    ACCESS_SECRET_KEY = '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'
    
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY_ID, aws_secret_access_key=ACCESS_SECRET_KEY, 
                      config=Config(signature_version='s3v4', region_name='ap-south-1'))

    files_content = {}
    file_key = key + file_name

    file_response = s3.get_object(Bucket=bucket_name, Key=file_key)
    file_content = file_response['Body'].read()

    files_content[file_name] = file_content
    
    return files_content,file_name

# Function to delete file from S3
def delete_file_from_s3(file_name,bucket_name = 'adqvests3bucket'):
    
    ACCESS_KEY_ID = 'AKIAYCVSRU2U3XP2DB75'
    ACCESS_SECRET_KEY = '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'
    
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY_ID, aws_secret_access_key=ACCESS_SECRET_KEY, 
                      config=Config(signature_version='s3v4', region_name='ap-south-1'))
    
    try:
        s3.delete_object(Bucket=bucket_name, Key=file_name)
        print("Deleted from S3 bucket")
    except Exception as e:
        print(f"Error deleting {file_name} from S3 bucket: {e}")

def extract_text_from_pdf(files_content,file_name,file_name_actual):

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
    text = soup.get_text(separator='\n')
    
    # Clean up text
    lines = [line.strip() for line in text.split('\n')]
    text = '\n'.join(line for line in lines if line)

    # Format the text similar to PDF output
    formatted_text = 'PAGE NUMBER: 1\n\n'  # HTML doesn't have pages, so we use 1
    formatted_text += f'COMPANY NAME: {company_name.strip()}\n\n'
    formatted_text += f'REPORT TYPE: {report_type.strip()}\n\n'
    formatted_text += f'RELEVANT YEAR: {relevant_year.strip()}\n\n'
    formatted_text += text
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
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    
    # job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'CRISIL_DAILY_DATA_CORPUS_CHUNKING'
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
        
        folder_name = 'CRISIL_RATINGS_CORPUS_TO_BE_CHUNKED/'

        query = "select * from CRISIL_DAILY_DATA_CORPUS_NEW where Chunking_Status IS NULL"
        crisil_ratings = pd.read_sql(query,con=engine)
        crisil_ratings = crisil_ratings.reset_index(drop=True)
        print(len(crisil_ratings))

        doc_links = client.execute('''SELECT distinct document_link FROM CRISIL_DAILY_DATA_CORPUS_CHUNKED_1;''')
        doc_links = [link for (link,) in doc_links]
        print(len(doc_links))

        doc_ids = client.execute('''SELECT distinct document_id FROM CRISIL_DAILY_DATA_CORPUS_CHUNKED_1;''')
        doc_ids = [str(x).replace('(','').replace(',)','') for x in doc_ids]
        print(len(doc_ids))

        for i in range(len(crisil_ratings)):
            company_name = crisil_ratings['Company_Name'][i]
            link = crisil_ratings['Rating_File_Link'][i]
            file_name = crisil_ratings['Generated_File_Name'][i]
            year = crisil_ratings['Relevant_Date'][i].year
            month = crisil_ratings['Relevant_Date'][i].month
            old_file_name = crisil_ratings['Rating_File_Name'][i]
            rel_Date = crisil_ratings['Relevant_Date'][i]
            table_file_id = crisil_ratings['File_ID'][i]
            if(month >= 4):
                year = year+1
            if(month < 4):
                year = year

            if(link not in doc_links):
                                
                try:
                    files_content,file_name = read_all_file_content(folder_name,file_name)
                except:
                    updated_link = link.replace('%','%%')
                    connection = engine.connect()                
                    with engine.begin() as connection:  # Automatically handles commit/rollback
                        query = text("""
                            UPDATE CRISIL_DAILY_DATA_CORPUS_NEW 
                            SET Chunking_Status = 'Check File' 
                            WHERE Rating_File_Link = :link 
                            AND Rating_File_Name = :old 
                            AND File_ID = :id 
                            AND Generated_File_Name = :new
                            AND Relevant_Date = :rdate
                            LIMIT 1;
                        """)
                        
                        result = connection.execute(query, {"link": updated_link, "old": old_file_name, "id": table_file_id,"new": file_name,"rdate":rel_Date})
                        print("Rows updated as Chunking Status S3:", result.rowcount)
                        print('_' * 80)
                        continue
                
                print("STARTED FILE :",file_name)
                if('.html' in file_name):
                    file_name_updated = company_name.replace(' ','_')+'_0_'+'CREDIT_RATING_'+'FY'+str(year)+'_'+str(table_file_id)+'.html'
                if('.pdf' in file_name):
                    file_name_updated = company_name.replace(' ','_')+'_0_'+'CREDIT_RATING_'+'FY'+str(year)+'_'+str(table_file_id)+'.pdf'

                try:
                    if('.pdf' in file_name):
                        company_name,report_type,relevant_year,file_id,text_content = extract_text_from_pdf(files_content,file_name_updated,file_name)
                    if('.html' in file_name):
                        company_name,report_type,relevant_year,file_id,text_content = extract_text_from_html(files_content,file_name_updated,file_name)
                    if str(file_id) not in doc_ids :
                        print(relevant_year)
                        if int(relevant_year[3:])>=18 and int(relevant_year[3:])<30:
                            company_name = company_name.replace("'","\\'") # Escaping single quotes
                            relevant_year = 'FY'+str(relevant_year[4:])
                            document_date = parse_quarter_year(relevant_year)

                            extracted_chunks = extract_chunks_from_text(text_content)

                            if (len(extracted_chunks)>0):
                                for chunk,page_number in zip(extracted_chunks,range(len(extracted_chunks))):
                                    chunk = chunk.replace("\\", "\\\\")  # Escaping backslashes
                                    chunk = chunk.replace("\n", "\\n")  # Escaping newlines
                                    chunk = chunk.replace("\r", "\\r")  # Escaping carriage returns
                                    chunk = chunk.replace("'","\\'") # Escaping single quotes
                                    chunk = 'PAGE NUMBER: ' + chunk
                                    page = page_number+1

                                    new_chunk = chunk
                                    company_small = company_name.lower()
                                    chunk_small = chunk.lower()

                                    try:
                                        chunk_embed = str(create_embeddings(chunk)).replace('[[','[').replace(']]',']')
                                        chunk_embed_small = str(create_embeddings(chunk_small)).replace('[[','[').replace(']]',']')
                                        client.execute(f"INSERT INTO CRISIL_DAILY_DATA_CORPUS_CHUNKED_1 (document_id,document_company,document_company_small_case,document_type,document_year,document_date,document_link,page_number,document_content,document_content_small_case,document_content_modify,embedding,embedding_small_case) VALUES ('{file_id}','{company_name}','{company_small}','{report_type}','{relevant_year}','{document_date}','{link}',{page},'{chunk}','{chunk_small}','{new_chunk}',{chunk_embed},{chunk_embed_small});") 
                                        
                                        print('Document chucked and inserted into clickhouse')
                                        
                                        updated_link = link.replace('%','%%')
                                        connection = engine.connect()                
                                        with engine.begin() as connection:  # Automatically handles commit/rollback
                                            query = text("""
                                                UPDATE CRISIL_DAILY_DATA_CORPUS_NEW 
                                                SET Chunking_Status = 'Done' 
                                                WHERE Rating_File_Link = :link 
                                                AND Rating_File_Name = :old 
                                                AND File_ID = :id 
                                                AND Generated_File_Name = :new
                                                AND Relevant_Date = :rdate
                                                LIMIT 1;
                                            """)
                                            
                                            result = connection.execute(query, {"link": updated_link, "old": old_file_name, "id": file_id,"new": file_name,"rdate":rel_Date})
                                            print("Rows updated as Chunking Status Done:", result.rowcount)  # To check if the update worked
                                            print('sql table updated')
                                            print('_' * 80)
                                        # connection.execute("commit")
                                    except:
                                        print("****** PAGE WAS SKIPPED ******",page_number)     

                                print('FILE COMPLETED')
                                delete_file_from_s3(file_name)

                            else:
                                print("FILE IS TOO SMALL")
                        else:
                            print("FILE NOT RELEVANT")
                            updated_link = link.replace('%','%%')
                            connection = engine.connect()                
                            with engine.begin() as connection:  # Automatically handles commit/rollback
                                query = text("""
                                    UPDATE CRISIL_DAILY_DATA_CORPUS_NEW 
                                    SET Chunking_Status = 'Not Relevant' 
                                    WHERE Rating_File_Link = :link 
                                    AND Rating_File_Name = :old 
                                    AND File_ID = :id 
                                    AND Generated_File_Name = :new
                                    AND Relevant_Date = :rdate
                                    LIMIT 1;
                                """)
                                
                                result = connection.execute(query, {"link": updated_link, "old": old_file_name, "id": file_id,"new": file_name,"rdate":rel_Date})
                                print("Rows updated as Chunking Status Done:", result.rowcount)
                                print('_' * 80)
                    else:

                        print('FILE ALREADY CHUNKED')

                except:
                    print("****************** SOMETHING WRONG WITH FILE ******************")                 
                    updated_link = link.replace('%','%%')
                    connection = engine.connect()                
                    with engine.begin() as connection:  # Automatically handles commit/rollback
                        query = text("""
                            UPDATE CRISIL_DAILY_DATA_CORPUS_NEW 
                            SET Chunking_Status = 'Check File' 
                            WHERE Rating_File_Link = :link 
                            AND Rating_File_Name = :old 
                            AND File_ID = :id 
                            AND Generated_File_Name = :new
                            AND Relevant_Date = :rdate
                            LIMIT 1;
                        """)
                        
                        result = connection.execute(query, {"link": updated_link, "old": old_file_name, "id": table_file_id,"new": file_name,"rdate":rel_Date})
                        print("Rows updated as Chunking Status File Check:", result.rowcount)
                        print('_' * 80)
                    
            else:
                print('LINK EXISTS CHUNKED')
                delete_file_from_s3(file_name)
                updated_link = link.replace('%','%%')
                connection = engine.connect()                
                with engine.begin() as connection:  # Automatically handles commit/rollback
                    query = text("""
                        UPDATE CRISIL_DAILY_DATA_CORPUS_NEW 
                        SET Chunking_Status = 'Done' 
                        WHERE Rating_File_Link = :link 
                        AND Rating_File_Name = :old 
                        AND File_ID = :id 
                        AND Generated_File_Name = :new
                        AND Relevant_Date = :rdate
                        LIMIT 1;
                    """)
                    
                    result = connection.execute(query, {"link": updated_link, "old": old_file_name, "id": table_file_id,"new": file_name,"rdate":rel_Date})
                    print("Rows updated as Chunking Status Done:", result.rowcount)
                    print('_' * 80)
        log.job_end_log(table_name,job_start_time,no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')