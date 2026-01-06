import warnings
warnings.filterwarnings('ignore')

import os
import io
import re
import traceback
import boto3
import voyageai
import pandas as pd
from sqlalchemy import text as qtext
import fitz  
import time
from bs4 import BeautifulSoup

import pypdfium2 as pdfium

from botocore.config import Config

import pdfquery
from lxml import etree

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

# ****   Date Time *****
india_time = timezone('Asia/Kolkata')
today = datetime.datetime.now(india_time)
days = datetime.timedelta(1)
yesterday = today - days
timestamp = datetime.datetime.now(india_time).strftime('%Y-%m-%d %H:%M:%S')

os.environ["VOYAGE_API_KEY"] = "pa-97JpiKNlfEEkhs8dBSZOloXM7aw3BI3NJ0ra-zaCSIk"

DEFAULT_S3_BUCKET = 'adqvests3bucket'

# Threading
# from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Lock for thread-unsafe pypdfium2
PDF_LOCK = threading.Lock()

# Small helper for safe SQL escaping when needed
def sql_escape(s):
    if s is None:
        return ''
    return str(s).replace("\\", "\\\\").replace("'", "\\'")

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
    with PDF_LOCK:
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

class CareRatingsProcessor:
    """Custom processor for CARE_RATINGS with database-driven file selection"""
    
    def __init__(self):
        self.table_name = "CARE_RATINGS_DAILY_DATA_CORPUS"
        self.s3_folder = "CARE_RATINGS_CORPUS_TO_BE_CHUNKED/"
        self.chunk_table_name = "CARE_DAILY_DATA_CORPUS_CHUNKED"
        
        
    # def extract_text_from_pdf(files_content,file_name):
    def extract_text_from_pdf(self, files_content, file_name, threshold=10, image_coverage_threshold=0.8):    

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
        
        with PDF_LOCK:
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
            
            scanned_pages = []
            is_whole_doc_scanned = False
       
        return company_name, report_type, relevant_year, file_id, text_content, scanned_pages, is_whole_doc_scanned
    
    def get_files_to_process(self, limit=None):
        """CARE_RATINGS database-driven file selection - your exact flow"""
        
        from clickhouse_driver import Client
        host = 'ec2-13-204-23-152.ap-south-1.compute.amazonaws.com'
        user_name = 'default'
        password_string = 'Clickhouse@2025'
        db_name = 'AdqvestDB'
        client = Client(host, user=user_name, password=password_string, database=db_name)
        
        engine = adqvest_db.db_conn()
        
        # Your exact CARE_RATINGS logic
        query = "SELECT * FROM CARE_RATINGS_DAILY_DATA_CORPUS WHERE Chunking_Status IS NULL AND S3_Upload_Status = 'Done'"
        care_ratings = pd.read_sql(query, con=engine)
        print(f"Found {len(care_ratings)} CARE_RATINGS files")
        
        doc_links = client.execute('''SELECT DISTINCT Document_Link FROM CARE_DAILY_DATA_CORPUS_CHUNKED;''')
        doc_links = [link for (link,) in doc_links]
        print(f"Already processed links: {len(doc_links)}")
        
        doc_ids = client.execute('''SELECT DISTINCT Document_Id FROM CARE_DAILY_DATA_CORPUS_CHUNKED;''')
        doc_ids = [str(row[0]).strip().strip("'") for row in doc_ids if row[0] is not None]
        print(f"Already processed IDs: {len(doc_ids)}")
        
        # Your exact filtering logic
        files_to_process = []
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
                new_file_name = care_ratings['Generated_File_Name'][i]
        
                # Normalize apostrophes and quotes
                new_file_name = new_file_name.replace("'", "'").replace("'", "'")  # Convert curly to straight
                new_file_name = new_file_name.replace(""", '"').replace(""", '"')  # Convert curly quotes
                
                if(link not in doc_links):
                    print('We can go ahead')
                    s3_file_path = f"{self.s3_folder}{new_file_name}"
                    files_to_process.append(s3_file_path)
        
        # Apply limit if specified
        if limit:
            files_to_process = files_to_process[:limit]
            
        print(f"CARE_RATINGS files to process this run: {len(files_to_process)}")
        return files_to_process

class CrisilProcessor:
    """Custom processor for CRISIL with database-driven file selection"""
    
    def __init__(self):
        self.table_name = "CRISIL_DAILY_DATA_CORPUS"
        self.s3_folder = "CRISIL_RATINGS_CORPUS_TO_BE_CHUNKED/"
        self.chunk_table_name = "CRISIL_DAILY_DATA_CORPUS_CHUNKED"
    
    def extract_text_from_pdf(self, files_content, file_name, threshold=10, image_coverage_threshold=0.8):
        """CRISIL specific PDF extraction logic - same as your original"""
        
        # Use your exact extraction logic
        parts = file_name.split('_')
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

        with PDF_LOCK:
            pdf = pdfium.PdfDocument(io.BytesIO(pdf_content))
            text_content = ''
            for page_number in range(len(pdf)):
                page_text = pdf[page_number].get_textpage().get_text_range()
                text_content += 'PAGE NUMBER: ' + str(page_number+1) + '\n\n'
                text_content += 'COMPANY NAME: ' + str(company_name.strip()) + '\n\n'
                text_content += 'REPORT TYPE: ' + str(report_type.upper().strip() if report_type else None) + '\n\n'
                text_content += 'RELEVANT YEAR: ' + str(relevant_year.strip() if relevant_year else None) + '\n\n'
                text_content += page_text
                text_content += '\n\n'
            text_content = re.sub('\n\n\n','\n\n', text_content)
            
            # Need to return scanned_pages and is_whole_doc_scanned for compatibility
            scanned_pages = []
            is_whole_doc_scanned = False
        
        return company_name, report_type.upper() if report_type else None, relevant_year, file_id, text_content, scanned_pages, is_whole_doc_scanned
    
    def extract_text_from_html(self, files_content, file_name, threshold=10, image_coverage_threshold=0.8):
        """CRISIL specific HTML extraction logic - same as your original"""
        
        # Use your exact HTML extraction logic
        parts = file_name.split('_')
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
        html_content = files_content[file_name]
        if isinstance(html_content, bytes):
            html_content = html_content.decode('utf-8', errors='replace')

        # Parse HTML
        soup = BeautifulSoup(html_content, 'html.parser')

        # Remove script and style elements
        for script in soup(["script", "style"]):
            script.decompose()

        # Get text
        text_content = soup.get_text(separator='\n')
        
        # Clean up text
        lines = [line.strip() for line in text_content.split('\n')]
        text_content = '\n'.join(line for line in lines if line)

        # Format the text similar to PDF output
        formatted_text = 'PAGE NUMBER: 1\n\n'  # HTML doesn't have pages, so we use 1
        formatted_text += f'COMPANY NAME: {company_name.strip()}\n\n'
        formatted_text += f'REPORT TYPE: {report_type.upper().strip() if report_type else None}\n\n'
        formatted_text += f'RELEVANT YEAR: {relevant_year.strip() if relevant_year else None}\n\n'
        formatted_text += text_content
        formatted_text += '\n\n'

        # Clean up multiple newlines
        formatted_text = re.sub('\n\n\n', '\n\n', formatted_text)

        # Need to return scanned_pages and is_whole_doc_scanned for compatibility
        scanned_pages = []
        is_whole_doc_scanned = False

        return company_name, report_type.upper() if report_type else None, relevant_year, file_id, formatted_text, scanned_pages, is_whole_doc_scanned
    
    def extract_text_from_pdf_or_html(self, files_content, file_name, threshold=10, image_coverage_threshold=0.8):
        """Wrapper method that handles both PDF and HTML files"""
        if '.pdf' in file_name:
            return self.extract_text_from_pdf(files_content, file_name, threshold, image_coverage_threshold)
        elif '.html' in file_name:
            return self.extract_text_from_html(files_content, file_name, threshold, image_coverage_threshold)
        else:
            raise ValueError(f"Unsupported file type for {file_name}")
    
    def get_files_to_process(self, limit=None):
        """CRISIL database-driven file selection - your exact flow"""
        
        from clickhouse_driver import Client
        host = 'ec2-13-204-23-152.ap-south-1.compute.amazonaws.com'
        user_name = 'default'
        password_string = 'Clickhouse@2025'
        db_name = 'AdqvestDB'
        client = Client(host, user=user_name, password=password_string, database=db_name)
        
        engine = adqvest_db.db_conn()
        
        # Your exact CRISIL logic
        query = "SELECT * FROM CRISIL_DAILY_DATA_CORPUS WHERE Chunking_Status IS NULL AND S3_Upload_Status = 'Done' AND Chunking_Comments is Null"
        crisil_ratings = pd.read_sql(query, con=engine)
        print(f"Found {len(crisil_ratings)} CRISIL files")
        
        doc_links = client.execute('''SELECT DISTINCT Document_Link FROM CRISIL_DAILY_DATA_CORPUS_CHUNKED;''')
        doc_links = [link for (link,) in doc_links]
        print(f"Already processed links: {len(doc_links)}")
        
        doc_ids = client.execute('''SELECT DISTINCT Document_Id FROM CRISIL_DAILY_DATA_CORPUS_CHUNKED;''')
        doc_ids = [str(row[0]).strip().strip("'") for row in doc_ids if row[0] is not None]
        print(f"Already processed IDs: {len(doc_ids)}")
        
        # Your exact filtering logic
        files_to_process = []
        for i in range(len(crisil_ratings)):
            company_name = crisil_ratings['Company_Name'][i]
            link = crisil_ratings['Rating_File_Link'][i]  # Different column name for CRISIL
            table_file_id = crisil_ratings['File_ID'][i]
            old_file_name = crisil_ratings['Rating_File_Name'][i]
            new_file_name = crisil_ratings['Generated_File_Name'][i]
            new_file_name = new_file_name.replace("'", "''")  # Different escaping for CRISIL
            rel_Date = crisil_ratings['Relevant_Date'][i]
            
            year = crisil_ratings['Relevant_Date'][i].year
            month = crisil_ratings['Relevant_Date'][i].month
            print('FILE ID:', table_file_id)
        
            if(month >= 4):
                year = year+1
            if(month < 4):
                year = year
                
            if(link not in doc_links):
                # Create the S3 path for processing
                new_file_name_1 = crisil_ratings['Generated_File_Name'][i]
        
                # Normalize apostrophes and quotes
                new_file_name_1 = new_file_name_1.replace("'", "'").replace("'", "'")  # Convert curly to straight
                new_file_name_1 = new_file_name_1.replace(""", '"').replace(""", '"')  # Convert curly quotes
                
                if(link not in doc_links):
                    print('We can go ahead')
                    s3_file_path = f"{self.s3_folder}{new_file_name_1}"
                    files_to_process.append(s3_file_path)
                
        # Apply limit if specified
        if limit:
            files_to_process = files_to_process[:limit]
            
        print(f"CRISIL files to process this run: {len(files_to_process)}")
        return files_to_process
    
class PibProcessor:
    """Custom processor for PIB_REPORTS with tokenization and unique chunking logic"""
    
    def __init__(self):
        self.table_name = "PIB_REPORTS_DAILY_DATA_CORPUS"
        self.s3_folder = "PIB_CORPUS_TO_BE_CHUNKED/"
        self.chunk_table_name = "PIB_REPORTS_DAILY_DATA_CORPUS_CHUNKED"
        
    def calculate_tokens(self, text):
        """PIB-specific token calculation"""
        import tiktoken
        encoding = tiktoken.get_encoding("cl100k_base")
        return len(encoding.encode(text))
    
    def run_tokenization_step(self, engine):
        """PIB-specific tokenization step that runs before chunking"""
        print("Running PIB tokenization step...")
        
        df_links = pd.read_sql("SELECT * FROM AdqvestDB.PIB_REPORTS_DAILY_DATA_CORPUS WHERE S3_Upload_Status='Done' AND Token_Length IS NULL", engine)
        
        for i, row in df_links.iterrows():
            try:
                filename = row['File_Name']
                print(f"Tokenizing File {i}: {filename}")
                
                # Use PIB_CORPUS folder for tokenization (different from chunking folder)
                response = 'PIB_CORPUS/' + filename
                
                files_content, file_name = read_all_file_content(response)
                pdf_content = files_content[file_name]
                
                import pypdfium2 as pdfium
                import io
                with PDF_LOCK:
                    pdf = pdfium.PdfDocument(io.BytesIO(pdf_content))

                    text = ''
                    for page_number in range(len(pdf)):
                        page_text = pdf[page_number].get_textpage().get_text_range()
                        text += page_text
                        text += '\n\n'

                    text = re.sub('\n\n\n', '\n\n', text)
                    token_len = self.calculate_tokens(text)
                    print("Token Length:", token_len)
                
                connection = engine.connect()
                connection.execute(f"UPDATE PIB_REPORTS_DAILY_DATA_CORPUS SET Token_Length = {token_len} WHERE File_Name = '{str(row['File_Name'])}'")
                connection.execute("commit")
                
            except Exception as e:
                print(f"Tokenization failed for {filename}: {str(e)}")
                connection = engine.connect()
                connection.execute(f"UPDATE PIB_REPORTS_DAILY_DATA_CORPUS SET Token_Length = 'Failed' WHERE File_Name = '{str(row['File_Name'])}'")
                connection.execute("commit")
        
        print("PIB tokenization completed")
    
    def extract_text_from_pdf(self, files_content, file_name, row_data, threshold=10, image_coverage_threshold=0.8):
        """PIB-specific PDF extraction with unique metadata structure"""
        
        # PIB uses different field mapping
        company = row_data['Ministry']  # Ministry instead of Company_Name
        doc_id = row_data['File_ID']
        link = row_data['File_Link']
        rel_date = row_data['Relevant_Date']
        rel_year = row_data['Relevant_Quarter'].split('_')[1]  # Extract from quarter field
        report_type = 'Press Release'  # Fixed type for PIB
        
        pdf_content = files_content[file_name]
        
        import pypdfium2 as pdfium
        import io

        with PDF_LOCK:
            pdf = pdfium.PdfDocument(io.BytesIO(pdf_content))
            
            # PIB has unique text structure - no PAGE NUMBER, different metadata
            text_content = ''
            text_content += 'MINISTRY NAME: ' + str(company.strip()) + '\n\n'
            text_content += 'REPORT TYPE: ' + str(report_type.strip()) + '\n\n'
            text_content += 'RELEVANT YEAR: ' + str(rel_year.strip()) + '\n\n'
            
            for page_number in range(len(pdf)):
                page_text = pdf[page_number].get_textpage().get_text_range()
                page_text = re.sub('\n\n\n', '\n\n', page_text)
                text_content += page_text
                text_content += '\n\n'
            
            # PIB doesn't check for scanned documents
            scanned_pages = []
            is_whole_doc_scanned = False
        
        # Return PIB-specific data
        return company, report_type, rel_year, doc_id, text_content, scanned_pages, is_whole_doc_scanned, link, rel_date
    
    def process_single_file(self, row_data, ch_client, engine, timestamp):
        """PIB-specific file processing with unique chunking logic"""
        
        try:
            filename = row_data['File_Name']
            doc_id = row_data['File_ID']
            
            # Check if already processed
            doc_ids = ch_client.execute('''SELECT DISTINCT Document_Id FROM PIB_REPORTS_DAILY_DATA_CORPUS_CHUNKED;''')
            doc_ids = [str(x).replace('(', '').replace(',)', '') for x in doc_ids]
            
            if str(doc_id) not in doc_ids:
                print(f"Processing file: {filename}")
                
                response = f"{self.s3_folder}{filename}"
                files_content, file_name = read_all_file_content(response)
                
                company, report_type, rel_year, doc_id, text_content, scanned_pages, is_whole_doc_scanned, link, rel_date = self.extract_text_from_pdf(
                    files_content, file_name, row_data
                )
                
                # PIB has unique chunking - page by page with embeddings
                pdf_content = files_content[file_name]
                import pypdfium2 as pdfium
                import io
                pdf = pdfium.PdfDocument(io.BytesIO(pdf_content))
                
                for page_number in range(len(pdf)):
                    page_text = pdf[page_number].get_textpage().get_text_range()
                    page_text = re.sub('\n\n\n', '\n\n', page_text)
                    page_text = page_text.replace("\\", "\\\\").replace("'", "\\'")
                    
                    if not page_text.strip():
                        print(f"Page {page_number+1} is empty - skipping")
                        continue
                    
                    # PIB creates embeddings during chunking (different from standard flow)
                    chunk_embed = str(create_embeddings(page_text)).replace('[[', '[').replace(']]', ']')
                    
                    rel_date_dt_str = datetime.datetime.combine(rel_date, datetime.datetime.min.time()).replace(hour=12)
                    
                    # PIB has unique ClickHouse insert structure
                    ch_client.execute(f"""
                        INSERT INTO PIB_REPORTS_DAILY_DATA_CORPUS_CHUNKED 
                        (Document_Id, Document_Company, Document_Type, Document_Year, Document_Date, Published_Date, Document_Link, Page_Number, Document_Content, Embedding, Vector_Db_Status, Runtime_Chunking) 
                        VALUES 
                        ('{doc_id}', '{company.title()}', '{report_type}', '{rel_year}', '{rel_date}', '{rel_date_dt_str}', '{link}', {page_number+1}, '{page_text}', {chunk_embed}, 'LINK UPDATED', '{timestamp}');
                    """)
                
                # Update source table
                connection = engine.connect()
                query = "UPDATE PIB_REPORTS_DAILY_DATA_CORPUS SET Chunking_Status='Done' WHERE File_Name = :filename AND File_Link = :link;"
                connection.execute(qtext(query), {"filename": row_data["File_Name"], "link": row_data["File_Link"]})
                
                print(f'Chunking done for file: {filename}')
                
                # PIB deletes files after processing
                delete_file_from_s3(response)
                print('FILE COMPLETED')
                
                return True
            else:
                print(f"File {filename} already processed")
                return True
                
        except Exception as e:
            print(f"Processing failed for {filename}: {str(e)}")
            
            connection = engine.connect()
            
            if 'Failed to load document (PDFium: Data format error)' in str(e):
                error_msg = 'File Not Available'
            elif 'Failed to load document (PDFium: Incorrect password error)' in str(e):
                error_msg = 'Password Issue'
            elif 'Failed to load page' in str(e):
                error_msg = 'Corrupt Document'
            else:
                error_msg = str(e)
            
            query = "UPDATE PIB_REPORTS_DAILY_DATA_CORPUS SET Chunking_Comments = :error WHERE File_Name = :filename"
            connection.execute(qtext(query), {"filename": row_data["File_Name"], "error": error_msg})
            
            return False
    
    def get_files_to_process(self, limit=None):
        """PIB database-driven file selection with tokenization step"""
        
        # First run tokenization
        engine = adqvest_db.db_conn()
        self.run_tokenization_step(engine)
        
        # Then get files for chunking
        df_links = pd.read_sql(
            "SELECT * FROM AdqvestDB.PIB_REPORTS_DAILY_DATA_CORPUS WHERE S3_Upload_Status='Done' AND Chunking_Status IS NULL ORDER BY Relevant_date DESC", 
            engine
        )
        
        print(f"Found {len(df_links)} PIB files for processing")
        
        if limit:
            df_links = df_links.head(limit)
        
        # PIB returns dataframe rows instead of S3 paths
        return df_links.to_dict('records')    

def create_embeddings(chunked_text):
    
    vo = voyageai.Client(api_key="pa-97JpiKNlfEEkhs8dBSZOloXM7aw3BI3NJ0ra-zaCSIk")
    
    # Embed the documents
    doc_embds = vo.embed(chunked_text, model="voyage-2", input_type="document").embeddings
    return doc_embds

def extract_chunks_from_text(text):
    
    chunks = text.split("PAGE NUMBER: ")
    chunks = chunks[1:]
    return chunks

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

# -----------------------------------------------
# PARALLEL WORKER (SAFE FOR TEST MODE)
# -----------------------------------------------

# ------------------ PART 5: PARALLEL WORKER + THREADPOOL WRAPPER ------------------

# configure these before running
MAX_WORKERS = 5

def process_single_pdf(obj, custom_processor=None,table_config=None):
    logs = []   # store ordered logs for this thread

    def log_print(msg):
        logs.append(msg)

    try:
        
        connection = engine.connect()
        file_path = obj

        files_content, file_name = read_all_file_content(file_path)
        log_print(f"STARTED FILE : {file_name}")

        parts = file_name.split('_')
        file_id = parts[-1].replace('.pdf', '').replace('.html', '')
        
        if custom_processor:
            table_to_update = custom_processor.table_name
        else:
            table_to_update = table_config["table"]
        
        try:
            # Use custom processor if provided, otherwise use default function
            if custom_processor:
                if isinstance(custom_processor, CrisilProcessor):
                    # CRISIL: Handle both PDF and HTML
                    company_name, report_type, relevant_year, file_id, text_content, scanned_pages, is_whole_doc_scanned = custom_processor.extract_text_from_pdf_or_html(files_content, file_name)
                    log_print(f"Used CRISIL extraction for {'.pdf' if '.pdf' in file_name else '.html'} file")
                    
                elif isinstance(custom_processor, CareRatingsProcessor):
                    # CARE_RATINGS: PDF only
                    company_name, report_type, relevant_year, file_id, text_content, scanned_pages, is_whole_doc_scanned = custom_processor.extract_text_from_pdf(files_content, file_name)
                    log_print("Used CARE_RATINGS extraction logic")
                    
                elif isinstance(custom_processor, PibProcessor):
                    # PIB processes row data, not file paths
                    row_data = obj  # obj is a dictionary of row data
                    file_name = row_data['File_Name']
                    log_print(f"STARTED PIB FILE: {file_name}")
                    
                    from clickhouse_driver import Client
                    host = 'ec2-13-204-23-152.ap-south-1.compute.amazonaws.com'
                    user_name = 'default'
                    password_string = 'Clickhouse@2025'
                    db_name = 'AdqvestDB'
                    ch_client = Client(host, user=user_name, password=password_string, database=db_name)
                    
                    # Use PIB's custom processing
                    success = custom_processor.process_single_file(row_data, ch_client, engine, timestamp)
                    
                    if success:
                        log_print("FILE COMPLETED")
                    else:
                        log_print("FILE FAILED")
                        
                    return file_name, logs

                else:
                    # Fallback for unknown custom processors
                    company_name, report_type, relevant_year, file_id, text_content, scanned_pages, is_whole_doc_scanned = custom_processor.extract_text_from_pdf(files_content, file_name)
                    log_print(f"Used {custom_processor.__class__.__name__} extraction logic")    
                    
            else:
                # Default extraction for standard tables
                company_name, report_type, relevant_year, file_id, text_content, scanned_pages, is_whole_doc_scanned = extract_text_from_pdf(files_content, file_name)
                log_print("Used default extraction logic")
                
        except Exception as e:
            err_txt = str(e)
            if 'Failed to load document (PDFium: Data format error)' in err_txt:
                query = f"UPDATE {table_to_update} SET Chunking_Comments = 'File Not Available' WHERE File_ID= '{file_id}'"
                connection.execute(qtext(query))
            elif 'Failed to load document (PDFium: Incorrect password error)' in err_txt:
                query = f"UPDATE {table_to_update} SET Chunking_Comments = 'Password Issue' WHERE File_ID= '{file_id}'"
                connection.execute(qtext(query))
            elif 'Failed to load page' in err_txt:
                query = f"UPDATE {table_to_update} SET Chunking_Comments = 'Corrupt Document' WHERE File_ID= '{file_id}'"
                connection.execute(qtext(query))
            else:
                query = f"UPDATE {table_to_update} SET Chunking_Comments = '{sql_escape(err_txt)}' WHERE File_ID= '{file_id}'"
                connection.execute(qtext(query))
            time.sleep(2)
            log_print(query)
            return file_name, logs      
        
        if is_whole_doc_scanned:
            query = f"UPDATE {table_to_update} SET Chunking_Comments = 'Scanned Document' WHERE File_ID= '{file_id}'"
            log_print(query)
            connection.execute(qtext(query))
            return file_name, logs

        # --- IMPORTANT: use local_client ---
        from clickhouse_driver import Client
        host = 'ec2-13-204-23-152.ap-south-1.compute.amazonaws.com'
        user_name = 'default'
        password_string = 'Clickhouse@2025'
        db_name = 'AdqvestDB'
        ch_client = Client(host, user=user_name, password=password_string, database=db_name)
        
        if custom_processor:
            chunk_table = custom_processor.chunk_table_name
        elif table_config:
            chunk_table = table_config["chunk_table"]
        else:
            raise ValueError("No chunk table specified")
        doc_ids = ch_client.execute(f'SELECT DISTINCT Document_Id FROM {chunk_table};')
        
        doc_ids = [str(x).replace('(','').replace(',)','') for x in doc_ids]
        log_print(f"GOT DOC IDS FOR: {chunk_table}")

        if str(file_id) not in doc_ids:

            log_print(f"RELEVANT YEAR: {relevant_year}")

            if relevant_year is not None:
                if 'Q' in relevant_year: 
                    if int(relevant_year[5:]) >= 18 and int(relevant_year[5:]) < 30:
                        company_name = company_name.replace("'", "\\'")  # Escaping single quotes
                        document_date = parse_quarter_year(relevant_year)
                        log_print(f"RELEVANT YEAR (Quarterly): {relevant_year}")
                    else:
                        log_print("FILE NOT RELEVANT (Quarterly year out of range)")
                        query = f"UPDATE {table_to_update} SET Chunking_Status='Not Relevant' WHERE File_ID= '{file_id}'"
                        log_print(query)
                        connection.execute(qtext(query))
                        return file_name, logs
                        
                elif int(relevant_year[2:]) >= 18 and int(relevant_year[2:]) < 30:
                    company_name = company_name.replace("'", "\\'")  # Escaping single quotes
                    document_date = parse_quarter_year(relevant_year)
                    log_print(f"RELEVANT YEAR (Annual): {relevant_year}")
                else:
                    log_print("FILE NOT RELEVANT (Annual year out of range)")
                    query = f"UPDATE {table_to_update} SET Chunking_Status='Not Relevant' WHERE File_ID= '{file_id}'"
                    log_print(query)
                    connection.execute(qtext(query))
                    return file_name, logs
                    
            else:
                log_print("RELEVANT YEAR NOT FOUND")
                query = f"UPDATE {table_to_update} SET Chunking_Comments='ISSUE' WHERE File_ID= '{file_id}'"
                log_print(query)
                connection.execute(qtext(query))
                return file_name, logs    

            extracted_chunks = extract_chunks_from_text(text_content)

            if len(extracted_chunks) > 0:

                for chunk, page_number in zip(extracted_chunks, range(len(extracted_chunks))):
                    chunk = chunk.replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r").replace("'", "\\'")
                    chunk = 'PAGE NUMBER: ' + chunk
                    page = page_number + 1

                    try:
                        chunk_embed = str(create_embeddings(chunk)).replace('[[','[').replace(']]',']')
                        log_print(f"Inserting INTO CH table: Page {page_number+1}")
                        ch_client.execute(f"INSERT INTO {chunk_table} (Document_Id,Document_Company,Document_Type,Document_Year,Document_Date,Page_Number,Document_Content,Embedding,Runtime_Chunking) VALUES ('{file_id}','{company_name.title()}','{report_type}','{relevant_year}','{document_date}',{page},'''{chunk}''',{chunk_embed},'{timestamp}');")

                    except Exception as ie:
                        log_print(f"****** PAGE SKIPPED {page_number} ****** {ie}")

                log_print('FILE COMPLETED')
                # connection.execute(q)
                query = f"UPDATE {table_to_update} SET Chunking_Status='Done' WHERE File_ID= '{file_id}'"
                log_print(query)
                connection.execute(qtext(query))
                log_print(f'Done updates for {file_name}')
                delete_file_from_s3(obj)

                return file_name, logs

            else:
                log_print("FILE IS TOO SMALL")
                query = f"UPDATE {table_to_update} SET Chunking_Status='Done' WHERE File_ID= '{file_id}'"
                log_print(query)
                # connection.execute(qtext(query))
                return file_name, logs

        else:
            log_print("FILE ALREADY CHUNKED")
            # connection.execute(q)
            query = f"UPDATE {table_to_update} SET Chunking_Status='Done' WHERE File_ID= '{file_id}'"
            log_print(query)
            connection.execute(qtext(query))
            return file_name, logs

    except Exception as e:
        log_print(f"FAILED because: {str(e)}")
        # query = f"UPDATE {table_to_update} SET Chunking_Status='Done' WHERE File_Name = '{file_name}'"
        # log_print(query)
        # connection.execute(qtext(query))
        return file_name, logs
    
def process_single_table(table_config):
    """Process a single table with optional custom processor"""
    table_name = table_config["table"]
    custom_processor = table_config.get("custom_processor", None)
    
    try:
        print(f"Starting chunking for table: {table_name}")
        if custom_processor:
            print(f"Starting: {table_name} ({custom_processor.__class__.__name__})")
        else:
            print(f"Starting: {table_name} (Default)")
            
        result = run_chunking_for_table(
            table_config["table"], 
            table_config["s3_folder"], 
            table_config["chunk_table"],
            table_config,
            custom_processor,  # Pass the custom processor
            skip_first=1, 
            limit=5, 
            max_workers=MAX_WORKERS
        )
        print(f"Completed: {table_name}")
        return table_name, "SUCCESS", result
    except Exception as e:
        print(f"Failed: {table_name} - {str(e)}")
        return table_name, "ERROR", str(e)

def run_parallel_for_response(response_list, custom_processor=None, table_config=None, max_workers=MAX_WORKERS):
    if not response_list:
        if custom_processor:
            table_name = custom_processor.table_name
        else:
            table_name = table_config["table"]  
            
        print(f"\n{'='*80}")
        print(f"SKIPPING: {table_name} (No files to process)")
        print('='*80)
        return []
        
    if custom_processor:
        table_name = custom_processor.table_name
    else:
        table_name = table_config["table"]        
    
    print(f"\n{'='*80}")
    print(f"PROCESSING TABLE: {table_name}")
    print(f"Files to process: {len(response_list)}")
    print(f"Using {max_workers} parallel workers")
    print('='*80)
    
    results = []

    # with ThreadPoolExecutor(max_workers=max_workers) as exe:
    #     future_to_key = {exe.submit(process_single_pdf, k, custom_processor,table_config): k for k in response_list}

    #     for future in as_completed(future_to_key):
    #         k = future_to_key[future]
    #         try:
    #             file_name, logs = future.result()
    #             results.append((file_name, logs))
    #         except Exception as e:
    #             results.append((k, [f"ERROR: {str(e)}"]))

    for idx, k in enumerate(response_list):
        try:
            print(f"  [{idx+1}/{len(response_list)}] Processing...")
            file_name, logs = process_single_pdf(k, custom_processor, table_config)
            results.append((file_name, logs))
        except Exception as e:
            results.append((k, [f"ERROR: {str(e)}"]))

    # Sort and print results...
    try:
        results.sort(key=lambda x: x[0])
    except TypeError:
        # PIB uses dictionaries, sort by filename instead
        if isinstance(custom_processor, PibProcessor):
            results.sort(key=lambda x: x[0] if isinstance(x[0], str) else str(x[0]))
    print(f"\n{'='*80}")
    print(f"RESULTS FOR TABLE: {table_name}")
    print('='*80)
    
    for file_name, logs in results:
        # Clean up the file name for display
        if isinstance(custom_processor, PibProcessor):
            clean_file_name = file_name  # PIB file_name is already clean
        else:
            clean_file_name = file_name.split('/')[-1]  # Remove S3 path
        
        print(f"\n FILE: {clean_file_name}")
        print("-" * 60)
        
        for line in logs:
            if line.strip():  # Only print non-empty lines
                print(f"   {line}")
        
        print("-" * 60)
    
    print(f"\n TABLE COMPLETED: {table_name}")
    print('='*80)
    
    return results

# Update the run_chunking_for_table to handle CRISIL as well
def run_chunking_for_table(table_name, s3_folder, chunk_table_name,table_config, custom_processor=None, skip_first=None, limit=None, max_workers=MAX_WORKERS):
    """
    Modified to handle CARE_RATINGS, CRISIL, and standard S3 listing
    """
    
    # Special handling for custom processors with database-driven file selection
    if custom_processor and hasattr(custom_processor, 'get_files_to_process'):
        processor_name = custom_processor.__class__.__name__
        print(f"Using {processor_name} database-driven file selection")
        response = custom_processor.get_files_to_process(limit=limit)
        
        if skip_first:
            response = response[skip_first:]        
            
    else:
        # Standard S3 listing for other tables
        bucket_name = DEFAULT_S3_BUCKET
        folder_name = s3_folder.rstrip('/') + '/'
        
        response = read_large_file_names_from_s3_folder(bucket_name, folder_name)
        response = [x for x in response if folder_name in x]
        
        s3_filters = table_config.get("s3_filters")
        if s3_filters:
            for filter_term in s3_filters:
                response = [x for x in response if filter_term in x]
            print(f"Applied S3 filters: {s3_filters}")
        
        print("Found files:", len(response))
        
        if len(response) > 0:
            if skip_first:
                response = response[skip_first:]
            if limit:
                response = response[:limit]
    
    print("Processing files count:", len(response))
    
    # Pass custom_processor to run_parallel_for_response
    results = run_parallel_for_response(response, custom_processor, table_config, max_workers=max_workers)
    return results

def run_post_processing_updates(table_config):
    """
    Optimized: Fetch MySQL data ONCE and reuse for all updates
    """
    from clickhouse_driver import Client
    
    source_table = table_config["table"]
    chunk_table = table_config["chunk_table"]
        
    print(f"_________________________________________ POST-PROCESSING: {source_table} __________________________________________")

    # ========================= FETCH MYSQL DATA ONCE =========================
    print("📥 Fetching source data from MySQL...")
    try:
        connection = engine.connect()
        query = f"SELECT * FROM AdqvestDB.{source_table} ORDER BY File_ID ASC;"
        source_data = pd.read_sql(query, con=engine)
        print(f"Loaded {len(source_data)} records from {source_table}")
    except Exception as e:
        print(f"Failed to fetch source data: {str(e)}")
        return

    # ========================= LINK UPDATE =========================
    print("Starting LINK UPDATE...")
    try:
        host = 'ec2-13-204-23-152.ap-south-1.compute.amazonaws.com'
        user_name = 'default'
        password_string = 'Clickhouse@2025'
        db_name = 'AdqvestDB'
        client = Client(host, user=user_name, password=password_string, database=db_name)
        
        doc = client.execute(f'''SELECT DISTINCT Document_Id as id, Page_Number as pn FROM {chunk_table} WHERE Document_Link = '';''')
        print(f"Found {len(doc)} documents needing link updates")

        if len(doc) > 1:
            companies = doc
            company = [x[0] for x in companies]
            
            # Use the ALREADY LOADED data
            data = source_data[source_data['File_ID'].isin(company)].copy()
            data.reset_index(drop=True, inplace=True)
            print(f"Processing {len(data)} records")
            
            if chunk_table == 'BSE_ANNOUNCEMENT_ANNUAL_REPORT_COMPANY_WISE_YEARLY_DATA_CORPUS_CHUNKED':
                update_data = []
                for x in range(len(data)):
                    doc_id = data["File_ID"][x]
                    link = data["Encoded_PDF_Link"][x]
                    if link:
                        link = link.replace("'", "\\'")
                        update_data.append((doc_id, link))
                    else:
                        link = data["PDF_Link"][x]
                        if link:
                            link = link.replace("'", "\\'")
                            print('Link from PDF Link')
                            update_data.append((doc_id, link))
                        else:
                            print('NO Link')
                        continue    

            elif chunk_table =='CRISIL_DAILY_DATA_CORPUS_CHUNKED':
                update_data = []
                for x in range(len(data)):
                    doc_id = data["File_ID"][x]
                    link = data["Rating_File_Link"][x].replace("'", "\\'")  # Escape single quotes
                    update_data.append((doc_id, link))

            else:
                # Rest of your link update logic...
                update_data = []
                for x in range(len(data)):
                    doc_id = data["File_ID"][x]
                    link = data["File_Link"][x].replace("'", "\\'")
                    update_data.append((doc_id, link))
                
            # Batch processing logic (same as before)
            batch_size = 1000
            total_batches = (len(update_data) + batch_size - 1) // batch_size

            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min((batch_num + 1) * batch_size, len(update_data))
                batch = update_data[start_idx:end_idx]
                
                case_parts = [f"WHEN Document_Id = '{doc_id}' THEN '{link}'" for doc_id, link in batch]
                case_statement_link = "CASE " + " ".join(case_parts) + " ELSE Document_Link END"
                doc_ids = ", ".join([f"'{doc_id}'" for doc_id, _ in batch])
                
                query = f"""
                    ALTER TABLE {chunk_table}
                    UPDATE 
                        Document_Link = {case_statement_link},
                        Vector_Db_Status = 'LINK UPDATED'
                    WHERE Document_Id IN ({doc_ids})
                """
                
                client.execute(query)
                print(f"Completed batch {batch_num + 1}/{total_batches}")

            # Page number and Chunk_Id updates (same as before)
            query = f"""
                ALTER TABLE {chunk_table}
                UPDATE 
                    Document_Link = concat(Document_Link, '#page=', toString(Page_Number))
                WHERE Document_Link NOT ILIKE '%#page=%'
                AND Document_Link ILIKE '%.pdf%'
                AND Document_Link NOT LIKE '%.html%' 
                AND Document_Link NOT LIKE '%.htm%';
            """
            client.execute(query)
            print("Completed PAGE number updation")

            query = f"""
                ALTER TABLE {chunk_table}
                UPDATE 
                    Chunk_Id = concat(Document_Id, '_', toString(Page_Number))
                WHERE Chunk_Id = '';
            """
            client.execute(query)
            print("Completed Chunk_Id updation")
        else:
            print("No link updates needed")
    except Exception as e:
        print(f"Link update failed: {str(e)}")

    # ========================= RUNTIME UPDATE =========================
    print("Starting RUNTIME UPDATE...")
    try:
        host = 'ec2-13-204-23-152.ap-south-1.compute.amazonaws.com'
        user_name = 'default'
        password_string = 'Clickhouse@2025'
        db_name = 'AdqvestDB'
        client = Client(host, user=user_name, password=password_string, database=db_name)
        
        doc = client.execute(f'''SELECT DISTINCT Document_Id as id FROM {chunk_table} WHERE Runtime_Scraped IS NULL;''')
        print(f"Found {len(doc)} documents needing runtime updates")

        if len(doc) > 0:
            companies = doc
            company = [x[0] for x in companies]
            
            # Use the ALREADY LOADED data
            data = source_data[source_data['File_ID'].isin(company)].copy()
            data.reset_index(drop=True, inplace=True)
            print(f"Processing {len(data)} records")

            # Rest of runtime update logic (same pattern)...
            update_data = []
            for x in range(len(data)):
                doc_id = data["File_ID"][x]
                runtime = data["Runtime"][x]
                update_data.append((doc_id, runtime))

            batch_size = 1000
            total_batches = (len(update_data) + batch_size - 1) // batch_size

            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min((batch_num + 1) * batch_size, len(update_data))
                batch = update_data[start_idx:end_idx]

                case_parts = [f"WHEN Document_Id = '{doc_id}' THEN toDateTime('{runtime}')" for doc_id, runtime in batch]
                case_statement_link = "CASE " + " ".join(case_parts) + " ELSE Runtime_Scraped END"
                doc_ids = ", ".join([f"'{doc_id}'" for doc_id, _ in batch])
                
                query = f"""
                    ALTER TABLE {chunk_table}
                    UPDATE 
                        Runtime_Scraped = {case_statement_link}
                    WHERE Document_Id IN ({doc_ids})
                """
                
                client.execute(query)
                print(f"Completed batch {batch_num + 1}/{total_batches}")
        else:
            print("No runtime updates needed")
    except Exception as e:
        print(f"Runtime update failed: {str(e)}")

    # ========================= PUBLISHED DATE UPDATE =========================
    print("Starting PUBLISHED DATE UPDATE...")
    try:
        host = 'ec2-13-204-23-152.ap-south-1.compute.amazonaws.com'
        user_name = 'default'
        password_string = 'Clickhouse@2025'
        db_name = 'AdqvestDB'
        client = Client(host, user=user_name, password=password_string, database=db_name)
        
        doc = client.execute(f'''SELECT DISTINCT Document_Id as id FROM {chunk_table} WHERE Published_Date IS NULL;''')
        print(f"Found {len(doc)} documents needing published date updates")

        if len(doc) > 0:
            companies = doc
            company = [x[0] for x in companies]
            
            # Use the ALREADY LOADED data
            data = source_data[source_data['File_ID'].isin(company)].copy()
            data.reset_index(drop=True, inplace=True)
            print(f"Processing {len(data)} records")

            # Published date update logic...
            update_data = []
            for x in range(len(data)):
                doc_id = data["File_ID"][x]
                if chunk_table =='NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS_CHUNKED':
                    publish_date = str(data["Broadcast_Date"][x])

                elif chunk_table =='CMOTS_NSE_CORPORATE_ANNOUNCEMENTS_COMPANY_WISE_DAILY_DATA_CORPUS':
                    publish_date = str(data["Announcement_Date"][x])    
                    
                elif chunk_table =='CAG_AUDIT_REPORTS_RANDOM_DATA_CORPUS_CHUNKED':
                    publish_date = str(data["Report_Date"][x]) + ' 12:00:00'  
                    
                elif chunk_table =='RBI_ANNUAL_REPORT_YEARLY_DATA_CORPUS_CHUNKED':
                    publish_date = str(data["Published_Date"][x]) + ' 12:00:00'
                    
                elif chunk_table =='NSE_DOC_LINKS_FROM_CORPUS_DAILY_DATA_CORPUS_CHUNKED':
                    publish_date = str(data["Source_Document_Date"][x])
                    
                elif chunk_table =='BSE_ANNOUNCEMENT_ANNUAL_REPORT_COMPANY_WISE_YEARLY_DATA_CORPUS_CHUNKED':
                    publish_date = str(data["Report_Date"][x]) + ' 12:00:00'
                    
                elif chunk_table =='RBI_BULLETIN_REPORTS_MONTHLY_DATA_CORPUS_CHUNKED':
                    publish_date = str(data["Published_Date"][x]) + ' 12:00:00'
                                     
                else:
                    publish_date = str(data["Relevant_Date"][x]) + ' 12:00:00'
                update_data.append((doc_id, publish_date))

            batch_size = 1000
            total_batches = (len(update_data) + batch_size - 1) // batch_size

            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min((batch_num + 1) * batch_size, len(update_data))
                batch = update_data[start_idx:end_idx]

                case_parts = [f"WHEN Document_Id = '{doc_id}' THEN toDateTime('{publish_date}')" for doc_id, publish_date in batch]
                case_statement_link = "CASE " + " ".join(case_parts) + " ELSE Published_Date END"
                doc_ids = ", ".join([f"'{doc_id}'" for doc_id, _ in batch])
                
                query = f"""
                    ALTER TABLE {chunk_table}
                    UPDATE 
                        Published_Date = {case_statement_link}
                    WHERE Document_Id IN ({doc_ids})
                """
                
                client.execute(query)
                print(f"Completed batch {batch_num + 1}/{total_batches}")
        else:
            print("No published date updates needed")
    except Exception as e:
        print(f"Published date update failed: {str(e)}")

    # ========================= SYMBOL UPDATE =========================
    if chunk_table =='NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS_CHUNKED':
        print("Starting SYMBOL UPDATE...")
        try:
            host = 'ec2-13-204-23-152.ap-south-1.compute.amazonaws.com'
            user_name = 'default'
            password_string = 'Clickhouse@2025'
            db_name = 'AdqvestDB'
            client = Client(host, user=user_name, password=password_string, database=db_name)
            
            dcomp = client.execute(f'''SELECT DISTINCT Document_Company as dcomp FROM {chunk_table} WHERE Symbol = '';''')
            print(f"Found {len(dcomp)} companies needing symbol updates")
            
            if len(dcomp) > 0:
                companies = dcomp
                company = [x[0] for x in companies]
                
                # Use the ALREADY LOADED data
                if chunk_table == 'CMOTS_NSE_CORPORATE_ANNOUNCEMENTS_COMPANY_WISE_DAILY_DATA_CORPUS':
                    data = source_data[source_data['Company_Name_Clean'].isin(company)].copy()
                else:
                    data = source_data[source_data['Company_Name'].isin(company)].copy()    

                data.reset_index(drop=True, inplace=True)
                print(f"Processing {len(data)} records")
                
                # Symbol update logic...
                update_data = []
                for x in range(len(data)): 
                    if chunk_table == 'CMOTS_NSE_CORPORATE_ANNOUNCEMENTS_COMPANY_WISE_DAILY_DATA_CORPUS':
                        company_name = data["Company_Name_Clean"].iloc[x]
                    else:
                        company_name = data["Company_Name"].iloc[x]
                    symbol = data["Symbol"].iloc[x]
                    update_data.append((company_name, symbol))
            
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
                        ALTER TABLE {chunk_table}
                        UPDATE Symbol = CASE { " ".join(case_parts) } ELSE Symbol END
                        WHERE replaceAll(Document_Company, '&', 'and') IN ({comps}) AND Symbol = ''
                    """
            
                    client.execute(query)
                    print(f"Completed batch {batch_num + 1}/{total_batches}")
            else:
                print("No symbol updates needed")
        except Exception as e:
            print(f"Symbol update failed: {str(e)}")

    if chunk_table == 'NSE_DOC_LINKS_FROM_CORPUS_DAILY_DATA_CORPUS_CHUNKED':
        
        host = 'ec2-13-204-23-152.ap-south-1.compute.amazonaws.com'
        user_name = 'default'
        password_string = 'Clickhouse@2025'
        db_name = 'AdqvestDB'
        client = Client(host, user=user_name, password=password_string, database=db_name)
        
        dcomp = client.execute(f'''SELECT DISTINCT Document_Company as dcomp FROM {chunk_table} WHERE Symbol = '';''')
        print(f"Found {len(dcomp)} companies needing symbol updates")
        
        if len(dcomp) > 0:
            companies = dcomp
            company = [x[0] for x in companies]
               
            data = source_data[source_data['Company_Name'].isin(company)]
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
                    ALTER TABLE {chunk_table}
                    UPDATE Symbol = CASE { " ".join(case_parts) } ELSE Symbol END
                    WHERE replaceAll(Document_Company, '&', 'and') IN ({comps}) AND Symbol = ''
                """
        
                client.execute(query)
                print(f"Completed batch {batch_num + 1}/{total_batches}")     
        
        print("_________________________________________ SECTOR __________________________________________")        
        dcomp = client.execute(f'''SELECT DISTINCT Document_Company as dcomp FROM {chunk_table} WHERE Sector = '';''')
        print(f"Found {len(dcomp)} companies needing symbol updates")
        
        if len(dcomp) > 0:
            companies = dcomp
            company = [x[0] for x in companies]
               
            data = source_data[source_data['Company_Name'].isin(company)]
            data.reset_index(drop = True, inplace = True)
            print(len(data))
            
            update_data = []
            for x in range(len(data)): 
                company_name = data["Company_Name"].iloc[x] 
                symbol = data["Sector"].iloc[x]
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
                    ALTER TABLE {chunk_table}
                    UPDATE Sector = CASE { " ".join(case_parts) } ELSE Sector END
                    WHERE replaceAll(Document_Company, '&', 'and') IN ({comps}) AND Sector = ''
                """
        
                client.execute(query)
                print(f"Completed batch {batch_num + 1}/{total_batches}")     
                
        #--------------------------------------------------INDUSTRY--------------------------------------------------------------------------------        
        dcomp = client.execute(f'''SELECT DISTINCT Document_Company as dcomp FROM {chunk_table} WHERE Industry = '';''')
        print(f"Found {len(dcomp)} companies needing Industry updates")
        
        if len(dcomp) > 0:
            companies = dcomp
            company = [x[0] for x in companies]
               
            data = source_data[source_data['Company_Name'].isin(company)]
            data.reset_index(drop = True, inplace = True)
            print(len(data))
            
            update_data = []
            for x in range(len(data)): 
                company_name = data["Company_Name"].iloc[x] 
                symbol = data["Industry"].iloc[x]
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
                    ALTER TABLE {chunk_table}
                    UPDATE Industry = CASE { " ".join(case_parts) } ELSE Industry END
                    WHERE replaceAll(Document_Company, '&', 'and') IN ({comps}) AND Industry = ''
                """
        
                client.execute(query)
                print(f"Completed batch {batch_num + 1}/{total_batches}")     

         #--------------------------------------------------INDUSTRY-SUB CATEGORY--------------------------------------------------------------------------------
        
        dcomp = client.execute(f'''SELECT DISTINCT Document_Company as dcomp FROM {chunk_table} WHERE Industry_Sub_Category = '';''')
        print(f"Found {len(dcomp)} companies needing Industry_Sub_Category updates")
        
        if len(dcomp) > 0:
            companies = dcomp
            company = [x[0] for x in companies]
               
            data = source_data[source_data['Company_Name'].isin(company)]
            data.reset_index(drop = True, inplace = True)
            print(len(data))
            
            update_data = []
            for x in range(len(data)): 
                company_name = data["Company_Name"].iloc[x] 
                symbol = data["Industry_Sub_Category"].iloc[x]
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
                    ALTER TABLE {chunk_table}
                    UPDATE Industry_Sub_Category = CASE { " ".join(case_parts) } ELSE Industry_Sub_Category END
                    WHERE replaceAll(Document_Company, '&', 'and') IN ({comps}) AND Industry_Sub_Category = ''
                """
        
                client.execute(query)
                print(f"Completed batch {batch_num + 1}/{total_batches}")     

        #--------------------------------------------------INDUSTRY SUB CATEGORY-2--------------------------------------------------------------------------------

        dcomp = client.execute(f'''SELECT DISTINCT Document_Company as dcomp FROM {chunk_table} WHERE Industry_Sub_Category_2 = '';''')
        print(f"Found {len(dcomp)} companies needing Industry_Sub_Category_2 updates")
        
        if len(dcomp) > 0:
            companies = dcomp
            company = [x[0] for x in companies]
               
            data = source_data[source_data['Company_Name'].isin(company)]
            data.reset_index(drop = True, inplace = True)
            print(len(data))
            
            update_data = []
            for x in range(len(data)): 
                company_name = data["Company_Name"].iloc[x] 
                symbol = data["Industry_Sub_Category_2"].iloc[x]
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
                    ALTER TABLE {chunk_table}
                    UPDATE Industry_Sub_Category_2 = CASE { " ".join(case_parts) } ELSE Industry_Sub_Category_2 END
                    WHERE replaceAll(Document_Company, '&', 'and') IN ({comps}) AND Industry_Sub_Category_2 = ''
                """
        
                client.execute(query)
                print(f"Completed batch {batch_num + 1}/{total_batches}")          
    
    print(f"POST-PROCESSING COMPLETED: {source_table}")

def get_table_configs():
    """Returns table configs with custom processors for specific tables"""
    
    # Initialize custom processors
    care_processor = CareRatingsProcessor()
    crisil_processor = CrisilProcessor()
    pib_processor = PibProcessor()  
    
    return [
        # Default tables (7 tables) - no custom processor
        {
            "table": "NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS",
            "s3_folder": "NSE_INVESTOR_INFORMATION_CORPUS_TO_BE_CHUNKED_2/",
            "chunk_table": "NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS_CHUNKED",
            "custom_processor": None,
            "s3_filters": None
        },
        {
            "table": "CMOTS_NSE_CORPORATE_ANNOUNCEMENTS_COMPANY_WISE_DAILY_DATA_CORPUS",
            "s3_folder": "CMOTS_NSE_CORPUS_TO_BE_CHUNKED/",
            "chunk_table": "NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS_CHUNKED",
            "custom_processor": None,
            "s3_filters": None
        },
        
        {
            "table": "SEBI_FINAL_OFFER_DOCUMENTS_RHP_DRHP_DAILY_DATA_CORPUS",
            "s3_folder": "SEBI_RHP_DRHP_CORPUS_TO_BE_CHUNKED_2/",
            "chunk_table": "SEBI_DRHP_FINAL_OFFER_DOCUMENTS_DAILY_DATA_CORPUS_CHUNKED",
            "custom_processor": None,
            "s3_filters": ["Final_Offer_Document"]
        },
        
        {
            "table": "SEBI_FINAL_OFFER_DOCUMENTS_RHP_DRHP_DAILY_DATA_CORPUS",
            "s3_folder": "SEBI_RHP_DRHP_CORPUS_TO_BE_CHUNKED_2/",
            "chunk_table": "SEBI_DRHP_FINAL_OFFER_DOCUMENTS_DAILY_DATA_CORPUS_CHUNKED",
            "custom_processor": None,
            "s3_filters": ["Red_Herring_Documents"]
        },
        
        {
            "table": "SEBI_FINAL_OFFER_DOCUMENTS_RHP_DRHP_DAILY_DATA_CORPUS",
            "s3_folder": "SEBI_RHP_DRHP_CORPUS_TO_BE_CHUNKED_2/",
            "chunk_table": "SEBI_DRHP_FINAL_OFFER_DOCUMENTS_DAILY_DATA_CORPUS_CHUNKED",
            "custom_processor": None,
            "s3_filters": ["Draft_Offer_Document"]
        },
        
        {
            "table": "SEBI_INVIT_ISSUES_DAILY_DATA_CORPUS",
            "s3_folder": "SEBI_INVIT_CORPUS_TO_BE_CHUNKED/",
            "chunk_table": "SEBI_INVIT_ISSUES_DAILY_DATA_CORPUS_CHUNKED",
            "custom_processor": None,
            "s3_filters": ["Draft"]
        },
        
        {
            "table": "SEBI_INVIT_ISSUES_DAILY_DATA_CORPUS",
            "s3_folder": "SEBI_INVIT_CORPUS_TO_BE_CHUNKED/",
            "chunk_table": "SEBI_INVIT_ISSUES_DAILY_DATA_CORPUS_CHUNKED",
            "custom_processor": None,
            "s3_filters": ["0_Invit_Offer"]
        },
        
        {
            "table": "SEBI_INVIT_ISSUES_DAILY_DATA_CORPUS",
            "s3_folder": "SEBI_INVIT_CORPUS_TO_BE_CHUNKED/",
            "chunk_table": "SEBI_INVIT_ISSUES_DAILY_DATA_CORPUS_CHUNKED",
            "custom_processor": None,
            "s3_filters": ["Final_Offer_Document"]
        },
        
        {
            "table": "SEBI_REIT_ISSUE_DAILY_DATA_CORPUS",
            "s3_folder": "SEBI_REIT_CORPUS_TO_BE_CHUNKED/",
            "chunk_table": "SEBI_REIT_ISSUE_DAILY_DATA_CORPUS_CHUNKED",
            "custom_processor": None,
            "s3_filters": ["Reit_Offer_Document"]
        },
        
        {
            "table": "SEBI_REIT_ISSUE_DAILY_DATA_CORPUS",
            "s3_folder": "SEBI_REIT_CORPUS_TO_BE_CHUNKED/",
            "chunk_table": "SEBI_REIT_ISSUE_DAILY_DATA_CORPUS_CHUNKED",
            "custom_processor": None,
            "s3_filters": ["Reit_Draft_Offer_Document"]
        },
        
        {
            "table": "SEBI_REIT_ISSUE_DAILY_DATA_CORPUS",
            "s3_folder": "SEBI_REIT_CORPUS_TO_BE_CHUNKED/",
            "chunk_table": "SEBI_REIT_ISSUE_DAILY_DATA_CORPUS_CHUNKED",
            "custom_processor": None,
            "s3_filters": ["Reit_Final_Offer_Document"]
        },
        
        # Custom tables with database-driven file selection
        {
            "table": care_processor.table_name,
            "s3_folder": care_processor.s3_folder,
            "chunk_table": care_processor.chunk_table_name,
            "custom_processor": care_processor,
            "s3_filters": None
        },
        {
            "table": crisil_processor.table_name,
            "s3_folder": crisil_processor.s3_folder,
            "chunk_table": crisil_processor.chunk_table_name,
            "custom_processor": crisil_processor,
            "s3_filters": None
        },
        
        {
            "table": pib_processor.table_name,
            "s3_folder": pib_processor.s3_folder,
            "chunk_table": pib_processor.chunk_table_name,
            "custom_processor": pib_processor,
            "s3_filters": None
        },
        
        {
            "table": 'BSE_ANNOUNCEMENT_ANNUAL_REPORT_COMPANY_WISE_YEARLY_DATA_CORPUS',
            "s3_folder": 'BSE_ANNUAL_REPORTS_CORPUS_TO_BE_CHUNKED_NEW',
            "chunk_table": 'BSE_ANNOUNCEMENT_ANNUAL_REPORT_COMPANY_WISE_YEARLY_DATA_CORPUS_CHUNKED',
            "custom_processor": None,
            "s3_filters": None
        },
        
        {
            "table": 'INDIA_BUDGET_SPEECHES_YEARLY_DATA_CORPUS',
            "s3_folder": 'GOI_BUDGET_SPEECHES_CORPUS_TO_BE_CHUNKED',
            "chunk_table": 'INDIA_BUDGET_SPEECHES_YEARLY_DATA_CORPUS_CHUNKED',
            "custom_processor": None,
            "s3_filters": None
        },
        
        {
            "table": 'NSE_MARKET_PULSE_MONTHLY_DATA_CORPUS',
            "s3_folder": 'NSE_MARKET_PULSE_CORPUS_TO_BE_CHUNKED_2',
            "chunk_table": 'NSE_MARKET_PULSE_MONTHLY_DATA_CORPUS_CHUNKED',
            "custom_processor": None,
            "s3_filters": None
        },
        
        {
            "table": 'RBI_PRESS_RELEASES_DAILY_DATA_CORPUS',
            "s3_folder": 'RBI_PRESS_RELEASES_DAILY_CORPUS_TO_BE_CHUNKED',
            "chunk_table": 'RBI_PRESS_RELEASES_DAILY_DATA_CORPUS_CHUNKED',
            "custom_processor": None,
            "s3_filters": None
        },
        
        {
            "table": 'INDIA_UNION_BUDGET_YEARLY_DATA_CORPUS',
            "s3_folder": 'INDIA_BUDGET_YEARLY_TO_BE_CHUNKED',
            "chunk_table": 'INDIA_UNION_BUDGET_YEARLY_DATA_CORPUS_CHUNKED',
            "custom_processor": None,
            "s3_filters": None
        },
        
        {
            "table": 'INDIA_BUDGET_ECONOMIC_SURVEY_YEARLY_DATA_CORPUS',
            "s3_folder": 'GOI_BUDGET_ECONOMIC_SURVEY_TO_BE_CHUNKED',
            "chunk_table": 'INDIA_BUDGET_ECONOMIC_SURVEY_YEARLY_DATA_CORPUS_CHUNKED',
            "custom_processor": None,
            "s3_filters": None
        },
        
        {
            "table": 'BANK_BASEL_III_QUARTERLY_LINKS',
            "s3_folder": 'BANK_BASEL_III_DISCLOSURES_CORPUS_TO_BE_CHUNKED',
            "chunk_table": 'BANK_BASEL_III_DISCLOSURES_QUARTERLY_DATA_CORPUS_CHUNKED',
            "custom_processor": None,
            "s3_filters": None
        },
        
        {
            "table": 'DEA_ECONOMY_REPORT_MONTHLY_DATA_CORPUS',
            "s3_folder": 'DEA_MONTHLY_ECONOMY_CORPUS_TO_BE_CHUNKED',
            "chunk_table": 'DEA_ECONOMY_REPORT_MONTHLY_DATA_CORPUS_CHUNKED',
            "custom_processor": None,
            "s3_filters": None
        },
        
        {
            "table": 'NSE_DOC_LINKS_FROM_CORPUS_DAILY_DATA',
            "s3_folder": 'NSE_DOC_LINKS_FROM_CORPUS_DAILY_DATA_TO_BE_CHUNKED',
            "chunk_table": 'NSE_DOC_LINKS_FROM_CORPUS_DAILY_DATA_CORPUS_CHUNKED',
            "custom_processor": None,
            "s3_filters": None
        },
        
        {
            "table": 'RBI_ANNUAL_REPORT_YEARLY_DATA_CORPUS',
            "s3_folder": 'RBI_ANNUAL_REPORT_CORPUS_TO_BE_CHUNKED',
            "chunk_table": 'RBI_ANNUAL_REPORT_YEARLY_DATA_CORPUS_CHUNKED',
            "custom_processor": None,
            "s3_filters": None
        },
        
        {
            "table": 'RBI_BULLETIN_REPORTS_MONTHLY_DATA_CORPUS',
            "s3_folder": 'RBI_BULLETIN_REPORT_CORPUS_TO_BE_CHUNKED',
            "chunk_table": 'RBI_BULLETIN_REPORTS_MONTHLY_DATA_CORPUS_CHUNKED',
            "custom_processor": None,
            "s3_filters": None
        },
        
        {
            "table": "AMC_WISE_MF_FACTSHEETS_MONTHLY_DATA_CORPUS",
            "s3_folder": "AMC_WISE_MF_FACTSHEETS_CORPUS_TO_BE_CHUNKED",
            "chunk_table": "AMC_WISE_MF_FACTSHEETS_MONTHLY_DATA_CORPUS_CHUNKED",
            "custom_processor": None,
            "s3_filters": None
        },
        
        {
            "table": "SEBI_LEGAL_CIRCULARS_RANDOM_DATA_CORPUS",
            "s3_folder": "SEBI_LEGAL_CIRCULAR_CORPUS_TO_BE_CHUNKED",
            "chunk_table": "SEBI_LEGAL_CIRCULARS_RANDOM_DATA_CORPUS_CHUNKED",
            "custom_processor": None,
            "s3_filters": None
        },
        
        {
            "table": "CMOTS_BSE_CORPORATE_ANNOUNCEMENTS_COMPANY_WISE_DAILY_DATA_CORPUS",
            "s3_folder": "CMOTS_BSE_CORPUS_TO_BE_CHUNKED",
            "chunk_table": "CMOTS_BSE_CORPORATE_ANNOUNCEMENTS_COMPANY_WISE_DAILY_DATA_CORPUS_CHUNKED",
            "custom_processor": None,
            "s3_filters": None
        },
        
        {
            "table": "ICRA_DAILY_DATA_CORPUS",
            "s3_folder": "ICRA_CORPUS_TO_BE_CHUNKED",
            "chunk_table": "ICRA_DAILY_DATA_CORPUS_CHUNKED",
            "custom_processor": None,
            "s3_filters": None
        },
        
        {
            "table": "ANAROCK_MARKET_VIEWPOINTS_MONTHLY_DATA_CORPUS",
            "s3_folder": "ANAROCK_HOSPITALITY_MARKET_VIEWPOINTS_CORPUS_TO_BE_CHUNKED",
            "chunk_table": "ANAROCK_MARKET_VIEWPOINTS_MONTHLY_DATA_CORPUS_CHUNKED",
            "custom_processor": None,
            "s3_filters": None
        },
        
        {
            "table": "CAG_STATE_WISE_ACCOUNT_MONTHLY_DATA_CORPUS",
            "s3_folder": "CAG_STATE_WISE_ACCOUNT_MONTHLY_DATA_CORPUS_TO_BE_CHUNKED/",
            "chunk_table": "CAG_STATE_WISE_ACCOUNT_MONTHLY_DATA_CORPUS_CHUNKED",
            "custom_processor": None,
            "s3_filters": None
        },
        
        {
            "table": "CAG_AUDIT_REPORTS_RANDOM_DATA_CORPUS",
            "s3_folder": "CAG_AUDIT_REPORT_FILES_TO_BE_CHUNKED",
            "chunk_table": "CAG_AUDIT_REPORTS_RANDOM_DATA_CORPUS_CHUNKED",
            "custom_processor": None,
            "s3_filters": None
        },
        
        {
            "table": "TRANSUNION_CIBIL_REPORTS_MONTHLY_DATA_CORPUS",
            "s3_folder": "TRANSUNION_CIBIL_REPORTS_MONTHLY_DATA_CORPUS_TO_BE_CHUNKED",
            "chunk_table": "TRANSUNION_CIBIL_REPORTS_MONTHLY_DATA_CORPUS_CHUNKED",
            "custom_processor": None,
            "s3_filters": None
        }]

        
def run_program(run_by='Adqvest_Bot', py_file_name=None):
    job_start_time = datetime.datetime.now(india_time)
    job_table_name = 'GENERIC_CHUNKING_CODE'
    no_of_ping = 0
    import sys
    import faulthandler
    faulthandler.enable() 
    try:
        if (py_file_name is None):
            py_file_name = sys.argv[0].split('.')[0]
        if (run_by == 'Adqvest_Bot'):
            log.job_start_log_by_bot(job_table_name, py_file_name, job_start_time)
        else:
            log.job_start_log(job_table_name, py_file_name, job_start_time, '')        

        # Get table configs with custom processors
        tables_to_run = get_table_configs()
        
        table_max_workers = 5
        print(f"Starting parallel processing of {len(tables_to_run)} tables with {table_max_workers} workers")
        
        # table_results = []
        # with ThreadPoolExecutor(max_workers=table_max_workers) as exe:
        #     future_to_table = {exe.submit(process_single_table, table_config): table_config["table"] 
        #                        for table_config in tables_to_run}
            
        #     print(f"DEBUG: Submitted all futures, waiting for completion...")

        #     completed_count = 0
        #     for future in as_completed(future_to_table):
        #         table_name = future_to_table[future]
        #         completed_count += 1
        #         print(f"DEBUG: Future {completed_count}/{len(tables_to_run)} completed")
        #         try:
        #             table_name, status, result = future.result()
        #             table_results.append((table_name, status, result))
        #             print(f"Table {table_name} completed with status: {status}")
        #         except Exception as e:
        #             table_results.append((table_name, "ERROR", str(e)))
        #             print(f"Table {table_name} failed: {str(e)}")
        #             import traceback
        #             traceback.print_exc()  # <-- ADD THIS

        table_results = []
        print(f"Processing {len(tables_to_run)} tables sequentially (pypdfium2 not thread-safe)")
        
        # Process tables sequentially to avoid pypdfium2 crashes
        for idx, table_config in enumerate(tables_to_run):
            table_name = table_config["table"]
            print(f"\n[TABLE {idx+1}/{len(tables_to_run)}] Starting: {table_name}")
            try:
                table_name, status, result = process_single_table(table_config)
                table_results.append((table_name, status, result))
                print(f"[TABLE {idx+1}/{len(tables_to_run)}] {table_name} completed with status: {status}")
            except Exception as e:
                print(f"[TABLE {idx+1}/{len(tables_to_run)}] {table_name} failed: {str(e)}")
                import traceback
                traceback.print_exc()
                table_results.append((table_name, "ERROR", str(e)))
        
        print(f"DEBUG: Exited ThreadPoolExecutor with block")
        print("Moving to post processing")
        print("\n" + "="*100)
        print("STARTING POST-PROCESSING UPDATES FOR ALL TABLES")
        print("="*100)
        
        # Run post-processing for ALL tables (standard AND custom processor tables)
        for table_config in tables_to_run:
            try:
                run_post_processing_updates(table_config)
            except Exception as e:
                print(f"Post-processing failed for {table_config['table']}: {str(e)}")
        
        print("\n" + "="*100)
        print("POST-PROCESSING COMPLETED FOR ALL TABLES")
        print("="*100)

        print("About to log job end...")
        try:
            log.job_end_log(job_table_name, job_start_time, no_of_ping)   
            print(f"Logging with: table_name='{job_table_name}', job_start_time='{job_start_time}', status=0")
            print("End log completed successfully!")
        except Exception as log_error:
            print(f"End log failed: {str(log_error)}")
    except Exception as e:
        error_type = str(re.search("'(.+?)'", str(type(e))).group(1)) if re.search("'(.+?)'", str(type(e))) else str(type(e))
        error_msg = str(e) + " line " + str(traceback.format_exc())
        print(error_type)
        print(error_msg)
        # log.job_error_log(table_name, job_start_time, error_type, error_msg, 0)
        log.job_error_log(job_table_name, job_start_time, error_type, error_msg, no_of_ping)

if __name__ == "__main__":
     run_program(run_by='manual')