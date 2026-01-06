# -*- coding: utf-8 -*-
"""
Created on Mon Dec 23 19:29:33 2024

@author: Rahul
"""

import warnings
warnings.filterwarnings('ignore')
import os
import re
import sys
import requests
import pandas as pd
from pytz import timezone
from datetime import datetime
import urllib.parse
from urllib.parse import unquote
from bs4 import BeautifulSoup

from zenrows import ZenRowsClient
zenrows_code="eabc162057980f957e89b6e71cb469e438b41393"
client = ZenRowsClient("eabc162057980f957e89b6e71cb469e438b41393")

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import dbfunctions
import JobLogNew as log
from adqvest_robotstxt import Robots
robot = Robots(__file__)


def validate_year(year_str):
    if pd.isna(year_str):
        return None

    year_str = str(year_str)

    if year_str.lower().startswith('fy'):
        return year_str

    current_year = datetime.now().year
    min_year = 2000
    max_year = current_year + 1

    def is_valid_year(year):
        try:
            year = int(year)
            return min_year <= year <= max_year
        except ValueError:
            return False

    match_short = re.match(r'(\d{4})-(\d{2})$', year_str)
    if match_short:
        start_year = int(match_short.group(1))
        end_year = int(match_short.group(2))
        end_year = int(str(start_year)[:2] + str(end_year).zfill(2))
        if is_valid_year(start_year) and is_valid_year(end_year):
            return year_str
        return None

    match_full = re.match(r'(\d{4})-(\d{4})$', year_str)
    if match_full:
        start_year = int(match_full.group(1))
        end_year = int(match_full.group(2))
        if is_valid_year(start_year) and is_valid_year(end_year):
            return year_str
        return None

    match_single = re.match(r'^(\d{4})$', year_str)
    if match_single:
        year = int(match_single.group(1))
        if is_valid_year(year):
            return year_str
        return None

    return None

########################################################################################################


def company_mapping(x):

    comp=x

    mapping={'Acko General':'Acko General Insurance Limited',
         'Bajaj Allianz General':'Bajaj Allianz General Insurance Company Limited',
         'Cholamandalam MS':'Cholamandalam MS General Insurance Company Limited',
         'Zuno General':'Zuno General Insurance Limited',
         'Future Generali India':'Future Generali India Insurance Company Limited',
         'Go Digit General':'Go Digit General Insurance Limited',
         'ICICI Lombard':'ICICI Lombard General Insurance Company',
         'IFFCO Tokio General':'IFFCO Tokio General Insurance Company Limited',
         'Zurich Kotak Mahindra General':'Zurich Kotak Mahindra General Company Limited',
         'Liberty General':'Liberty  General Insurance Company Limited',
         'Magma HDI':'Magma HDI General Insurance Company Limited',
         'Raheja QBE':'Raheja QBE General Insurance Company Limited',
         'Reliance General':'Reliance General Company Limited',
         'Royal Sundaram':'Royal Sundaram General Company Limited',
         'Shriram General':'Shriram General Insurance Company Limited',
         'New India Assurance':'The New India Assurance Company Limited',
         'Oriental Insurance':'The Oriental Insurance Company Limited',
         'United India':'United India Insurance Company Limited',
         'Universal Sompo':'Universal Sompo General Insurance Company Limited',
         'Niva Bupa':'Niva Bupa Health Insurance Company Limited',
         'Care Health':'Care Health Insurance Limited',
         'ManipalCigna General':'Manipal Cigna Health Insurance Company Limited',
         'Narayana Health':'Narayana Health Insurance Company Limited',
         'Kshema':'Kshema General Insurance Limited',
         'AIC':'Agriculture Insurance Company of India Limited',
         'National Insurance':'National Insurance Company Limited',
         'HDFC ERGO':'HDFC ERGO General Insurance Company Limited',
         'ECGC':'ECGC Limited',
         'Star Health':'Star Health & Allied Insurance Company Limited',
         'Aditya Birla General':'Aditya Birla insurance Company Limited',
         'SBI General':'SBI General Insurance Company Limited',
         'Tata AIG':'Tata AIG General Insurance Company Limited',
         'Navi General':'Navi General Insurance Limited',
         'Galaxy Health': 'Galaxy Health Insurance Company Limited'

     }

    return mapping.get(comp,x)

########################################################################################################


def extract_report_type(row):

    url = urllib.parse.unquote(str(row['Url']).lower() if pd.notna(row['Url']) else '')
    file_name = str(row['File_Name']).lower() if pd.notna(row['File_Name']) else ''
    text = str(row['Text']).lower() if pd.notna(row['Text']) else ''

    report_types = {
        'Public_Disclosure': [ r'\b(pd|p\.d\.)\b(?!\.)', r'public[-_\s]*disclos?[ou]r?e?s?',
            r'public[-_\s]*d[-_\s]*iscl?[ou]s[ou]re', r'/public-disclosures?/',
            r'pd[-_](?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)',
            r'merged[-_\s]*public[-_\s]*discl?[ou]s?[ou]?r?e?',
            r'public[-_\s]*d[-_\s]*iscl?[ou]s[ou]re', r'irda' ],

        'Annual_Report': [ r'\b(annual\s*[-_]?\s*repo?r?t?s?)\b', r'\b(ann?\s*repo?r?t?)\b',
            r'\b(yearly\s*repo?r?t?)\b'],

        'SEBI_Format': [ r'\b(sebi\s*[-_]?\s*format)\b', r'\b(sebi\s*repo?r?t?)\b' ],

        'Voting_Disclosure': [ r'voting[-_\s]*disclos[ou]r?e?'],

        'Policy_Document': [ r'\bstewardship\b', r'\bstewardship[-_\s]*policy\b',  r'\bpolicy\b'],

        'Agents_Report': [ r'\bagent[s]?\b' ],

        'Investor_Presentation': [ r'\binvestor\s*[-_]?\s*presentation\b' ],

        'Press_Releases': [ r'\bpress\s*[-_]?\s*release\b' ],

        'Financial_Report': [ r'\b(financials?\s*[-_]?\s*repo?r?t?)\b',
                             r'\b(financials?\s*[-_]?\s*irda)\b',
                             r'\b(financial\s*disclos[ou]r?e?)\b' ]
    }

    if re.search(r'l[-_\s]*([0-9]{1,2}|100)', file_name, re.IGNORECASE):
        return 'Public_Disclosure'

    for report_type, patterns in report_types.items():
        for pattern in patterns:
            # Check in Url and File_Name
            if re.search(pattern, url, re.IGNORECASE) or re.search(pattern, file_name, re.IGNORECASE):
                return report_type

    # If no match in Url or File_Name, check in Text
    for report_type, patterns in report_types.items():
        for pattern in patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return report_type

    common_indicators = ['public-disclosure', 'public_disclosure', 'public disclos', 'pd_', 'public_d']
    if any(indicator in url or indicator in file_name for indicator in common_indicators):
        return 'Public_Disclosure'

    return None

def extract_nl_number(url, text):
    if text is None:
        nl_string=url
    else:
        nl_string=text+' '+url
    nl_string=nl_string.strip()

    match = re.search(r'nl\s*[_\-]?\s*\d{2}', nl_string, re.IGNORECASE)

    if match and match.group(0)[:1].lower() == 'n':
        number_matched = int(re.sub(r'[^\d]', '', match.group()))
        if number_matched< 50:
            return match.group(0)


    match = re.search(r'nl\s*[_\-]?\s*\d{1}', nl_string, re.IGNORECASE)
    if match and match.group(0)[:1].lower() == 'n':
        return match.group(0)

    return None

def standardize_doc_type(docu_type):
    if docu_type is None:
        return None

    match1 = re.search(r'nl\s*[_\-]?\s*\d{2}', docu_type.strip(), re.IGNORECASE)
    match2= re.search(r'nl\s*[_\-]?\s*\d{1}', docu_type.strip(), re.IGNORECASE)

    if match1:
        number = re.sub(r'[^\d]', '', match1.group())
        if 0<int(number) < 90:
            return f"NL_{number}"
    if match2:
        number = re.sub(r'[^\d]', '', match2.group())
        if 0<int(number) < 90:
            return f"NL_{number}"
    return None

def create_report_year(year, quarter):

    if year is None and quarter is None:
        return None
    if year is not None and quarter is None:
        return year
    if year is not None and quarter is not None:
        return f"{quarter}_{year}"

    return None

def qtr_mapping(x):

    if not x or x in['8','4','5']:
        return None

    quarter=x.lower()

    Quarter_Mapping={'quarter1':'Q1','quarter2':'Q2','quarter3':'Q3','quarter4':'Q4','q1_quarter':'Q1','q2_quarter':'Q2','q3_quarter':'Q3','qtr 1':'Q1',
                    'qtr 2':'Q2','qtr 3':'Q3','qtr 4':'Q4',
                    '3':'Q4','6':'Q1','9':'Q2','12':'Q3'}
    return Quarter_Mapping.get(quarter, x)

def format_financial_year(row):

    if row['FY_Year'] is None:
        return None


    if row['Quarter_4m_Link'] is None:
        try:
            return f"FY{str(row['Year_4m_Link'])[-2:]}"
        except:
            return None


    quarter = row['Quarter_4m_Link'].lower()  # convert to lowercase for consistency
    year = row['FY_Year']


    pattern = r'FY|^\d{4}\s*[-_]\s*\d{2,4}$|^[A-Za-z]+\_\d{4}$' #pattern to exclude years like 2024-25, 2020_22

    match = re.search(pattern, str(year),re.IGNORECASE)
    if match:
        return f"{quarter}_FY{str(year)[-2:]}"


    if quarter in ['q1', 'q2', 'q3']:
        return f"{quarter}_FY{str(year+1)[-2:]}"  # Concatenate year and quarter number (e.g., "2022Q1")
    elif quarter == 'q4':
        return f"Q4_FY{str(year)[-2:]}"  # For Q4, return year + 1 (e.g., "2023Q4")
    else:
        return None


def extractFYYear(x):
    if x is None:
        return None

    pattern1 = r'\b((?:mar|march|jun|june|sep|dec))_(\d{4})\b'
    match = re.search(pattern1, x,re.IGNORECASE)

    if match:
        year = int(match.group(2))
        year = 2000 + year if year < 100 else year
        if year >2026:
            return None
        return year


    pattern = r'FY|^\d{4}\s*[-_]\s*\d{2,4}$|^[A-Za-z]+\_\d{4}$' #pattern to exclude years like 2024-25, 2020_22

    match = re.search(pattern, x.lower(),re.IGNORECASE)
    if match:
        return x


    year_pattern= r'\d{4}'
    match = re.search(year_pattern, x.lower(),re.IGNORECASE)
    if match:
        return int(match.group())

    pattern = r'\b(\d{2})$'
    match = re.search(pattern, x,re.IGNORECASE)

    if match:
        year = int(match.group(1))  # Convert to integer
        # Handle two-digit year (assuming it falls in the 21st century)
        year = 2000 + year if year < 100 else year
        if year >2026:
            return None
        return year

    pattern2 = r'\b((?:mar|march|jun|june|sep|dec))(\d{2})\b'
    match = re.search(pattern2, x,re.IGNORECASE)

    if match:
        year = int(match.group(2))  # Convert to integer
        # Handle two-digit year (assuming it falls in the 21st century)
        year = 2000 + year if year < 100 else year
        if year >2026:
            return None
        return year

def create_Filename(row):

    report_type = row['Report_Type']
    report_quarter = row['Report_Quarter']
    company = row['Company']
    print("Company: ", company)
    num = row['File_ID']

    doc = row['Doc_Number']

    base_file_name = None

    if report_quarter is not None and report_type is not None and doc is not None:
        base_file_name = f'{company}_0_{report_type}_{doc}_{report_quarter}_{num}'
    elif report_quarter is None and report_type is not None and doc is not None:
        base_file_name = f'{company}_0_{report_type}_{doc}_{num}'
    elif report_type is None and report_quarter is not None and doc is not None:
        base_file_name = f'{company}_0_{doc}_{report_quarter}_{num}'
    elif report_type is None and report_quarter is None and doc is not None:
        base_file_name = f'{company}_0_{doc}_{num}'
    elif report_type is not None and report_quarter is not None and doc is None:
        base_file_name = f'{company}_0_{report_type}_{report_quarter}_{num}'
    elif report_quarter is not None and report_type is None and doc is None:
        base_file_name = f'{company}_0_{report_quarter}_{num}'

    if base_file_name == None:
        base_file_name = f'{company}_0_{num}'

    cleaned_file_name = re.sub(r'[^A-Za-z0-9_]+', '_', base_file_name)
    cleaned_file_name = cleaned_file_name.replace('-', '_')
    cleaned_file_name = re.sub(r'_+', '_', cleaned_file_name)
    print("Cleaned FIle Name: ", cleaned_file_name)

    return cleaned_file_name







def run_program(run_by='Adqvest_Bot', py_file_name=None):
    engine = adqvest_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.now(india_time)

    ## job log details
    job_start_time = datetime.now(india_time)
    table_name = ''
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        quarter_patterns={
         r'.*jun.*': 'Q1', r'.*sep.*': 'Q2',r'.*dec.*': 'Q3',r'.*mar.*': 'Q4',
         }

        key1='GENERAL_INSURANCE_CORPUS_MISSING/'
        key2='GENERAL_INSURANCE_CORPUS_TO_BE_CHUNKED_MISSING/'
        path=r'C:/Users/Administrator/AdQvestDir/GENERAL_INSURANCE_3/'

        df_links=pd.read_sql("select * from GEN_INSURANCE_QUARTERLY_DATA_CORPUS where Relevant_Date='2025-04-25'",engine)
        # df_links=df_links[3000:]


        df_links['Url']=df_links['Url'].apply(lambda x:x.replace('\\','/'))

        for i, row in df_links.iterrows():

            company = row['Company']
            cleaned_file_name=row['Cleaned_File_Name']
            try:
                for file in os.listdir(path):
                    if file.endswith('.pdf'):
                        os.remove(path+file)

                link= row['Url']
                print("Link: ", link)
                report_quarter = row['Report_Quarter']

                company = row['Company']
                print("Company: ", company)
                num = row['File_ID']

                doc = row['Doc_Number']

                headers = { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                           'Accept': 'application/pdf,application/ms-excel,application/vnd.ms-excel,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,text/html', 'Accept-Encoding': 'gzip, deflate, br',
                           'Connection': 'keep-alive' }

                response = requests.get(link,verify=False, allow_redirects=True, timeout=120,headers=headers)
                if response.status_code!=200:
                    response = client.get(link, verify=False,headers=headers, timeout=20)
                if response.status_code == 200:
                    content_type = response.headers.get('Content-Type', '').lower()
                    print("Content-Type: ", content_type)

                    if 'application/pdf' in content_type:
                        file_name_with_extension = f"{cleaned_file_name}.pdf"
                        save_path = os.path.join(path, file_name_with_extension)
                        print(f"Saving as PDF: {file_name_with_extension}")

                        with open(save_path, 'wb') as f:
                            f.write(response.content)
                        print(f"Successfully downloaded: {save_path}")

                        dbfunctions.to_s3bucket(save_path, key1)
                        print(f"File successfully uploaded to {key1}")
                        dbfunctions.to_s3bucket(save_path, key2)
                        print(f"File successfully uploaded to {key2}")

                        connection=engine.connect()
                        query = """ UPDATE GEN_INSURANCE_QUARTERLY_DATA_CORPUS SET S3_Upload = 'Done'
                        WHERE File_ID = %s """
                        connection.execute(query, (num,))


                        query6 = "UPDATE GEN_INSURANCE_QUARTERLY_DATA_CORPUS SET Is_PDF= %s WHERE File_ID = %s"
                        connection.execute(query6, (True, num))

                        connection.execute("commit")
                        print('Updated Status as Done')
                        print('_'*88)

                    else:
                        print(f"Skipping file. The link does not point to a PDF. Content-Type: {content_type}")
                        connection=engine.connect()
                        query = """ UPDATE GEN_INSURANCE_QUARTERLY_DATA_CORPUS SET S3_Upload = 'Not PDF'
                        WHERE File_ID = %s """
                        connection.execute(query, (num,))
                        connection.execute("commit")
                        print('Updated Status as Not Downloaded, Response Status')
                        print('_'*88)

                else:
                    print(f"Failed to download. Status code: {response.status_code}")
                    connection=engine.connect()
                    query = """ UPDATE GEN_INSURANCE_QUARTERLY_DATA_CORPUS SET S3_Upload = 'Not Downloaded because of Requests' WHERE File_ID = %s """
                    connection.execute(query, (num,))
                    connection.execute("commit")
                    print('Updated Status as Not Downloaded, Response Status')
                    print('_'*88)

            #     try:
            #         req = urllib.request.Request(link, method='HEAD')
            #         soup = BeautifulSoup(req.content, 'lxml')
            #
            #         if "Page Not Found" in soup.get_text():
            #             print(f"Page Not Found for {link}, skipping this URL.")
            #             connection=engine.connect()
            #             query = """ UPDATE GEN_INSURANCE_QUARTERLY_DATA_CORPUS SET S3_Upload = 'Not Available'
            #             WHERE Url = %s """
            #             connection.execute(query, (link,))
            #             connection.execute("commit")
            #             print('Updated Status as Not available')
            #             print('_'*88)
            #             continue
            #
            #         with urllib.request.urlopen(req) as response:
            #                content_type = response.getheader('Content-Type').lower()
            #                print(f"Content-Type: {content_type}")
            #
            #         if 'application/pdf' in content_type:
            #             file_extension = '.pdf'
            #             print("File is a PDF.")
            #         elif 'application/ms-excel' in content_type:
            #             file_extension = '.xls'
            #             print("File is an Excel.")
            #         else:
            #             connection=engine.connect()
            #             query = """ UPDATE GEN_INSURANCE_QUARTERLY_DATA_CORPUS SET S3_Upload = 'Not PDF'
            #             WHERE Url = %s """
            #             connection.execute(query, (link,))
            #             connection.execute("commit")
            #             print('Updated Status as Not PDF')
            #             print('_'*88)
            #             continue
            #
            #         file_name_with_extension = f"{cleaned_file_name}{file_extension}"
            #         save_path = os.path.join(path, file_name_with_extension)
            #
            #         print(f"Downloading file from {link}...")
            #         urllib.request.urlretrieve(link, save_path)
            #         print(f"Successfully downloaded: {save_path}")
            #
            #         try:
            #             dbfunctions.to_s3bucket(save_path, key1)
            #             print(f"File successfully uploaded to {key1}")
            #         except Exception as e:
            #             print(f"Error uploading file: {e}")
            #
            #         try:
            #             dbfunctions.to_s3bucket(save_path, key2)
            #             print(f"File successfully uploaded to {key2}")
            #         except Exception as e:
            #             print(f"Error uploading file: {e}")
            #
            #         connection=engine.connect()
            #         query = """ UPDATE GEN_INSURANCE_QUARTERLY_DATA_CORPUS SET S3_Upload = 'Done'
            #         WHERE Url = %s """
            #         connection.execute(query, (link,))
            #
            #         query1 = "UPDATE GEN_INSURANCE_QUARTERLY_DATA_CORPUS SET File_ID = %s WHERE Url = %s"
            #         connection.execute(query1, (num, link))
            #
            #         query7 = "UPDATE GEN_INSURANCE_QUARTERLY_DATA_CORPUS SET Cleaned_File_Name= %s WHERE Url = %s"
            #         connection.execute(query7, (cleaned_file_name, link))
            #         connection.execute("commit")
            #         # print('Updated Status as Not Downloaded, Response Status')
            #         print('_'*88)
            #     except:
            #         connection=engine.connect()
            #         query = """ UPDATE GEN_INSURANCE_QUARTERLY_DATA_CORPUS SET S3_Upload = 'Not PDF'
            #         WHERE Url = %s """
            #         connection.execute(query, (link,))
            #         connection.execute("commit")
            #         print('Updated Status as Not Downloaded, Response Status')
            #         print('_'*88)
            #
            #     query3 = "UPDATE GEN_INSURANCE_QUARTERLY_DATA_CORPUS SET Report_Quarter = %s WHERE Url = %s"
            #     connection.execute(query3, (report_quarter, link))
            #
            #     query4 = "UPDATE GEN_INSURANCE_QUARTERLY_DATA_CORPUS SET Report_Type = %s WHERE Url = %s"
            #     connection.execute(query4, (report_type, link))
            #
            #     query5 = "UPDATE GEN_INSURANCE_QUARTERLY_DATA_CORPUS SET Company = %s WHERE Url = %s"
            #     connection.execute(query5, (company, link))
            #
            except:
                connection=engine.connect()
                query = """ UPDATE GEN_INSURANCE_QUARTERLY_DATA_CORPUS SET S3_Upload = 'Failed'
                WHERE File_ID = %s """
                connection.execute(query, (num,))
                connection.execute("commit")
                print('Updated Status as Failed')
                print('_'*88)


        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)
if(__name__=='__main__'):
    run_program(run_by='manual')
