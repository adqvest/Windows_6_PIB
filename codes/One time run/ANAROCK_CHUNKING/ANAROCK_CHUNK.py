import os
import io
import re
import boto3
import voyageai
# import adqvest_db
import datetime as datetime
import pypdfium2 as pdfium

import warnings
warnings.filterwarnings('ignore')
# import adqvest_s3
from botocore.config import Config

import pdfquery
from lxml import etree


from datetime import timedelta
from dateutil.relativedelta import relativedelta



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
        print(f"Deleted from S3 bucket")
    except Exception as e:
        print(f"Error deleting {file_name} from S3 bucket: {e}")



def extract_text_from_pdf(files_content, file_name):
    # Split file name into parts
    parts = file_name.split('_')

    # Extract company name (everything before the first '0')
    company_name = ' '.join(parts[:parts.index('0')])

    # Extract report type (everything after '0' and before year-related parts)
    year_patterns = ['CY', 'Q', 'H']
    relevant_year_index = next(i for i, part in enumerate(parts) if any(pattern in part for pattern in year_patterns))
    report_type = ' '.join(parts[parts.index('0') + 1:relevant_year_index])

    # Extract relevant year (concatenate adjacent parts related to the year)
    relevant_year_parts = parts[relevant_year_index:]
    relevant_year = ' '.join(relevant_year_parts).replace('.pdf', '')

    # Extract file ID (last part of the file name without '.pdf')
    file_id = parts[-1].replace('.pdf', '')

    # Load the PDF content
    pdf_content = files_content[file_name]
    pdf = pdfium.PdfDocument(io.BytesIO(pdf_content))

    # Initialize text output
    text = ''
    for page_number in range(len(pdf)):
        page_text = pdf[page_number].get_textpage().get_text_range()

        text += f'PAGE NUMBER: {page_number + 1}\n\n'
        text += f'COMPANY NAME: {company_name.strip()}\n\n'
        text += f'REPORT TYPE: {report_type.strip()}\n\n'
        text += f'RELEVANT YEAR: {relevant_year.strip()}\n\n'
        text += page_text
        text += '\n\n'
    
    # Remove excessive line breaks
    text = re.sub(r'\n{3,}', '\n\n', text)

    return company_name.strip(), report_type.strip(), relevant_year.strip(), file_id.strip(), text



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
    # Check if the string contains a quarter or half-year
    qy_str = qy_str.lower()
    if 'q' in qy_str:
        q, cy = qy_str.split(' ')
        quarter = int(q[1])
        year = int(cy.replace('cy', ''))
        
        if quarter in [1, 2, 3]:
            # For Q1, Q2, Q3: map to January, April, July
            month = 1 + (quarter - 1) * 3
        elif quarter == 4:
            # For Q4: map to October
            month = 10
        else:
            raise ValueError(f"Invalid quarter: {quarter}")
        day = 1  # Set day as 1
        return datetime.datetime(year, month, day).date() + relativedelta(months=3) - timedelta(days=1)
    elif 'h' in qy_str:
        h, cy = qy_str.split(' ')
        half = int(h[1])
        year = int(cy.replace('cy', ''))
        
        if half == 1:
            # For H1: map to January
            month = 1
        elif half == 2:
            # For H2: map to July
            month = 7
        else:
            raise ValueError(f"Invalid half-year: {half}")
        day = 1  # Set day as 1
        return datetime.datetime(year, month, day).date() + relativedelta(months=6) - timedelta(days=1)
    else:
        # If only calendar year is provided, set to December 31st
        cy = qy_str
        year = int(cy.replace('cy', ''))
        return datetime.datetime(year, 12, 31).date()



from clickhouse_driver import Client


host = 'ec2-52-11-204-251.us-west-2.compute.amazonaws.com'
user_name = 'default'
password_string = 'Clickhouse@2024'
db_name = 'AdqvestDB'

client = Client(host, user=user_name, password=password_string, database=db_name)


bucket_name = 'adqvests3bucket'
folder_name = 'ANAROCK/'

# response = read_all_file_names_from_s3_folder(bucket_name, folder_name)
response = read_large_file_names_from_s3_folder(bucket_name, folder_name)
response = [ x for x in response if 'ANAROCK/' in x]
print(len(response))

response = response[1:]

doc_ids = client.execute(f'''SELECT distinct document_id FROM thurro_pdf_documents_vector_db_anarock_reports;''')
doc_ids = [str(x).replace('(','').replace(',)','') for x in doc_ids]
print(len(doc_ids))

for obj in response:
    print(obj)
    files_content,file_name = read_all_file_content(obj)
    
    print("STARTED FILE :",file_name)

    try:
        company_name,report_type,relevant_year,file_id,text = extract_text_from_pdf(files_content,file_name)
        max_doc_ids = client.execute('''SELECT max(document_id) as max_id FROM thurro_pdf_documents_vector_db_anarock_reports;''')
        max_doc_ids = [str(x).replace('(','').replace(',)','') for x in max_doc_ids]
        file_id = int(max_doc_ids[0]) + 1
        
        if str(file_id) not in doc_ids :
            relevant_year = relevant_year.replace('.pdf','')
            
            print(relevant_year)

            if int(relevant_year[-2:])>=18 and int(relevant_year[-2:])<30:
                company_name = company_name.replace("'","\\'") # Escaping single quotes
                document_date = parse_quarter_year(relevant_year)

                extracted_chunks = extract_chunks_from_text(text)

                if (len(extracted_chunks)>0):
                    # text_data = pdf_to_text(files_content[file_name])

                    for chunk,page_number in zip(extracted_chunks,range(len(extracted_chunks))):
                        chunk = chunk.replace("\\", "\\\\")  # Escaping backslashes
                        chunk = chunk.replace("\n", "\\n")  # Escaping newlines
                        chunk = chunk.replace("\r", "\\r")  # Escaping carriage returns
                        chunk = chunk.replace("'","\\'") # Escaping single quotes
                        chunk = 'PAGE NUMBER: ' + chunk

                        is_true = analyse_chunk(chunk)
                        if is_true:
                            new_chunk = modify_chunk(chunk)
                        else:
                            new_chunk = chunk

                        try:
                            chunk_embed = str(create_embeddings(chunk)).replace('[[','[').replace(']]',']')
                            client.execute(f"INSERT INTO thurro_pdf_documents_vector_db_anarock_reports (document_id,document_company,document_type,document_year,document_date,document_content,document_content_modify,embedding) VALUES ('{file_id}','{company_name}','{report_type}','{relevant_year}','{document_date}','''{chunk}''','''{new_chunk}''',{chunk_embed});") 
                        except:
                            print("****** PAGE WAS SKIPPED ******",page_number)     

                    # delete_file_from_s3(obj)
                    print('FILE COMPLETED')

                else:
                    print("FILE IS TOO SMALL")
            else:
                print("FILE NOT RELEVANT")

        else:

            print('FILE ALREADY CHUNKED')
            # delete_file_from_s3(obj)

    except:
        print("****************** SOMETHING WRONG WITH FILE ******************")

