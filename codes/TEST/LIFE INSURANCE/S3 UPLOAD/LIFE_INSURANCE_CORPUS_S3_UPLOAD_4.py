# -*- coding: utf-8 -*-
"""
Created on Mon Dec 23 19:29:33 2024

@author: GOKUL
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
import urllib.request
import urllib.parse
from urllib.parse import unquote
from bs4 import BeautifulSoup
from urllib.error import HTTPError


from zenrows import ZenRowsClient
zenrows_code="eabc162057980f957e89b6e71cb469e438b41393"
client = ZenRowsClient("eabc162057980f957e89b6e71cb469e438b41393")

# sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Windows_3/Adqvest_Function')
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


def extract_company_name_1(url, allowed_domains):
  
    company_mapping = { "ackoassets.com": "Acko Life Insurance Company Limited", 
                       "ctfassets.net": "Acko Life Insurance Company Limited",
                       "acko.com": "Acko Life Insurance Company Limited", 
                       "ageasfederal.com": "Ageas Federal Life Insurance Company Limited", 
                       "avivaindia.com": "Aviva Life Insurance Company Limited",
                       "bajajallianzlife.com": "Bajaj Allianz Life Insurance Company Limited",
                       "bandhanlife.com": "Bandhan Life Insurance Limited",
                       "bhartiaxa.com": "Bharti AXA Life Insurance Company Limited", 
                       "canarahsbclife.com": "Canara Hsbc Life Insurance Company Limited",
                       "creditaccesslife.in": "Creditaccess Life Insurance Company Limited",
                       "edelweisslife.in": "Edelweiss Tokio Life Insurance Company Limited",
                       "life.futuregenerali.in": "Future Generali India Life Insurance Company Limited",
                       "godigit.com": "Godigit Life Insurance Limited", 
                       "hdfclife.com": "HDFC Life Insurance Company Limited",
                       "iciciprulife.com": "ICICI Prudential Life Insurance Company Limited",
                       "indiafirstlife.com": "Indiafirst Life Insurance Company Limited",
                       "maxlifeinsurance.com": "Max Life Insurance Company Limited", 
                       "pramericalife.in": "Pramerica Life Insurance Limited", 
                       "tataaia.com": "Tata AIA Life Insurance Company Limited",
                       "pnbmetlife.com": "PNB Metlife Life Insurance Company Limited",
                       "adityabirlacapital.com": "Aditya Birla Sun Life Insurance", 
                       "shriramlife.com": "Shriram Life Insurance Company Limited", 
                       "sbilife.co.in": 'SBI Life Insurance Company Limited',
                       "kotaklife.com": "Kotak Mahindra Life Insurance Company Limited",
                       "sudlife.in": "Star Union Dai-Ichi Life Insurance Company Limited",
                       "reliancenipponlife.com": "Reliance Nippon Life Insurance Company Limited",
                       "licindia.in":"Life Insurance Corporation Of India"}
    
    if pd.isna(url) or not isinstance(url, str):
        return None
     
    for domain in allowed_domains:
        if domain in url.lower():
            return company_mapping.get(domain)
            
    return None

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

########################################################################################################

def create_report_year(year, quarter):
    if year is None and quarter is None:
        return None

    year = str(year) if year is not None else ''
    quarter = str(quarter) if quarter is not None else ''
    
    # Match for "FY2024", "FY 2024", "FY24", or similar fiscal year patterns
    fy_match = re.search(r'(?:FY\s*)(\d{4}|\d{2})', year, re.IGNORECASE)
    if fy_match:
        fiscal_year = fy_match.group(1)
        if len(fiscal_year) == 2:
            fiscal_year = f"20{fiscal_year}"
        fiscal_year_end = fiscal_year[-2:]
    else:
        # Match for a calendar year like 2023, 2022, etc.
        year_match = re.search(r'\b(\d{4})\b', year)
        if year_match:
            calendar_year = int(year_match.group(1))
            fiscal_year_end = f"{(calendar_year + 1) % 100:02d}"  # Add 1 to get the fiscal year
        else:
            fiscal_year_end = None

    # Match the quarter pattern (Q1, Quarter-1, etc.)
    quarter_match = re.search(r'(?:Q|QUARTER-?)(\d)', quarter, re.IGNORECASE)
    if quarter_match:
        quarter_code = f'Q{quarter_match.group(1)}'
    else:
        return None  # If no valid quarter is found, return None
    
    # If fiscal_year_end is still None, return None
    if fiscal_year_end is None:
        return None
    
    return f"{quarter_code}_FY{fiscal_year_end}"

########################################################################################################

def extract_l_number(url,doc_number):
    if doc_number is not None :
       return doc_number
    match = re.search(r'[Ll][_.\-]?\d+', url)
    if match and match.group(0)[:1].lower() == 'l':  # Ensure that the match starts with "L" or "l"
        return match.group(0)
    return  None

########################################################################################################

def standardize_doc_type(docu_type):
    if docu_type is None: 
        return None

    match = re.search(r'[Ll][_.\-]?\d+', docu_type)
    if match:
        number = re.sub(r'[^\d]', '', match.group()) 
        return f"L_{number}"  
    return docu_type 

########################################################################################################

def parse_year_to_report_quarter(year, report_quarter_3):
    if report_quarter_3 is not None:
        return report_quarter_3

    # Handle "FY YYYY" format (e.g., "FY2024" or "FY 2024")
    fy_match = re.fullmatch(r'FY\s*(\d{4})', str(year), re.IGNORECASE)
    if fy_match:
        fiscal_year_end = fy_match.group(1)[-2:]  
        return f"FY{fiscal_year_end}"

    # Handle single-year values like "2020"
    single_year_match = re.fullmatch(r'\d{4}', str(year))
    if single_year_match:
        fiscal_year_end = single_year_match.group(0)[-2:] 
        return f"FY{fiscal_year_end}"

    # Handle range-year values like "2019-2020"
    range_year_match = re.fullmatch(r'(\d{4})-(\d{4})', str(year))
    if range_year_match:
        fiscal_year_end = range_year_match.group(2)[-2:] 
        return f"FY{fiscal_year_end}"

    # Handle "YYYY-YY" format (e.g., "2010-11")
    short_range_match = re.fullmatch(r'(\d{4})-(\d{2})', str(year))
    if short_range_match:
        base_year = short_range_match.group(1)  
        fiscal_year_end = int(base_year[:2] + short_range_match.group(2)) 
        return f"FY{fiscal_year_end % 100:02d}"  

    # Handle "FY YYYY-YY" (e.g., "FY 2024-25")
    fy_short_range_match = re.fullmatch(r'FY\s*(\d{4})-(\d{2})', str(year), re.IGNORECASE)
    if fy_short_range_match:
        base_year = fy_short_range_match.group(1) 
        fiscal_year_end = int(base_year[:2] + fy_short_range_match.group(2)) 
        return f"FY{fiscal_year_end % 100:02d}"

    # Handle "FY YYYY-YYYY" (e.g., "FY 2024-2025")
    fy_long_range_match = re.fullmatch(r'FY\s*(\d{4})-(\d{4})', str(year), re.IGNORECASE)
    if fy_long_range_match:
        fiscal_year_end = fy_long_range_match.group(2)[-2:]
        return f"FY{fiscal_year_end}"
    
    fy_short_match = re.fullmatch(r'FY(\d{2})', str(year), re.IGNORECASE)
    if fy_short_match:
        return f"FY{fy_short_match.group(1)}"
    
    return None


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

        key1='LIFE_INSURANCE_CORPUS_9/'
        key2='LIFE_INSURANCE_CORPUS_TO_BE_CHUNKED_9/'
        path=r'C:/Users/Administrator/AdQvestDir/LIFE_INSURANCE_4/'

        df_links=pd.read_sql("select * from AdqvestDB.LIFE_INSURANCE_QUARTERLY_DATA_CORPUS where Is_PDF is not Null and S3_Upload is Null",engine)
        print(len(df_links))
        df_links['Year'] = df_links['Year'].apply(validate_year)

        # df_links=df_links[:2000]
        df_links=df_links[18001:24000]
        print(len(df_links))
        import time
        time.sleep(10)
        
        # try:
        #     df_links['Doc_Number'] = df_links['Url'].apply(extract_l_number)
        # except:
        #     df_links['Doc_Number'] = df_links.apply(lambda row: extract_l_number(row['Url'], row['Doc_Number']), axis=1)
        # df_links['Doc_Number'] = df_links['Doc_Number'].apply(standardize_doc_type)
        
        # allowed_domains = ["ackoassets.com","ctfassets.net",
        #                    "acko.com","ageasfederal.com","avivaindia.com","adityabirlacapital.com",
        #                     "bajajallianzlife.com","bandhanlife.com","bhartiaxa.com",
        #                     "canarahsbclife.com","creditaccesslife.in","edelweisslife.in",
        #                     "life.futuregenerali.in","godigit.com","hdfclife.com",
        #                     "iciciprulife.com","indiafirstlife.com","maxlifeinsurance.com",
        #                     "pramericalife.in","tataaia.com","iciciprulife.com","pnbmetlife.com",
        #                     "shriramlife.com","sbilife.co.in","kotaklife.com","sudlife.in",
        #                     "reliancenipponlife.com","licindia.in"]
         
        # df_links['Company_1'] = df_links['Url'].apply(lambda x: extract_company_name_1(x, allowed_domains))
        
        # df_links['Report_Type_1'] = df_links.apply(extract_report_type, axis=1)
        
        # df_links['Quarter'] = df_links['Quarter'].str.lower().replace({r'.*h.*': 'Q2', r'.*9m.*': 'Q3', r'.*fy.*': 'Q4',r'.*12m.*': 'Q4'}, regex=True)
        # df_links['Quarter'] = df_links['Quarter'].str.upper()
        
        # df_links['Report_Quarter'] = df_links.apply(lambda row: create_report_year(row['Year'], row['Quarter']), axis=1)
        
        # # num_none = df_links['Report_Quarter'].isna().sum()
        
        # df_links["Report_Quarter"] = df_links.apply( lambda row: parse_year_to_report_quarter(row["Year"], row["Report_Quarter"]), axis=1 )

        # df_links['File_ID'] = range(1, len(df_links) + 1)
        
        # df_links['File_ID'] = range(100, 100 + len(df_links))
        # print(len(df_links))

        for i, row in df_links.iterrows():
            link = None
            try:
                # print('before checking for pdf')
                for file in os.listdir(path):
                    if file.endswith('.pdf'):
                        os.remove(path+file)
                # print('after checking for pdf') 

                link= row['Url']
                print("Link: ", link)        
                report_type = row['Report_Type']
                print("Report_Type: ", report_type)
                report_quarter = row['Report_Quarter']
                print("Report_Quarter: ", report_quarter)
               
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
                    print("Last option")
                    base_file_name = f'{company}_0_{report_quarter}_{num}'

                if base_file_name is None:
                    # Fallback: If none of the variables are available
                    base_file_name = f'{company}_0_{num}'
 
                print("Base File Name : ", base_file_name)
                
                cleaned_file_name = re.sub(r'[^A-Za-z0-9_]+', '_', base_file_name) 
                cleaned_file_name = cleaned_file_name.replace('-', '_')
                cleaned_file_name = re.sub(r'_+', '_', cleaned_file_name) 
                print("Cleaned FIle Name: ", cleaned_file_name)
                
                # print('_'*88)

                headers = { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36', 
                           'Accept': 'application/pdf,application/ms-excel,application/vnd.ms-excel,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,text/html', 'Accept-Encoding': 'gzip, deflate, br',
                           'Connection': 'keep-alive' }
                
                response = requests.get(link, allow_redirects=True, timeout=120,headers=headers,verify = False)
                # if response.status_code!=200:
                #     # req = urllib.request.Request(link, method='GET')
                #     response = client.get(link, verify=False,headers=headers, timeout=20)
                # response = client.get(link, verify=False,headers=headers, timeout=20)
                if response.status_code == 200:
                    content_type = response.headers.get('Content-Type', '').lower()
                    print("Content-Type: ", content_type)

                    # with urllib.request.urlopen(req) as response:
                    #     content_type = response.getheader('Content-Type').lower()
                    #     print(f"Content-Type: {content_type}")
                    
                    if 'application/pdf' in content_type:
                     # if 'application/pdf' or 'text/plain' in content_type:    
                        # file_extension = '.pdf'
                        # file_name_with_extension = f"{cleaned_file_name}{file_extension}"
                        file_name_with_extension = f"{cleaned_file_name}.pdf"
                        save_path = os.path.join(path, file_name_with_extension)
                        print(f"Saving as PDF: {file_name_with_extension}")

                        # print(f"Downloading file from {link}...")
                        # urllib.request.urlretrieve(link, save_path)
                        # print(f"Successfully downloaded: {save_path}")
                    
                        with open(save_path, 'wb') as f:
                            f.write(response.content)
                        print(f"Successfully downloaded: {save_path}")

                        try:
                            dbfunctions.to_s3bucket(save_path, key1)
                            print(f"File successfully uploaded to {key1}")
                        except Exception as e:
                            print(f"Error uploading file: {e}")

                        try:
                            dbfunctions.to_s3bucket(save_path, key2)
                            print(f"File successfully uploaded to {key2}")
                        except Exception as e:
                            print(f"Error uploading file: {e}")
                        
                        connection=engine.connect()
                        query = """ UPDATE LIFE_INSURANCE_QUARTERLY_DATA_CORPUS SET S3_Upload = 'Done' 
                        WHERE Url = %s """
                        connection.execute(query, (link,))

                        # query1 = "UPDATE COMBO_SCRAPY_RUN_FINAL_GOKUL SET File_ID = %s WHERE Url = %s"
                        # connection.execute(query1, (num, link))
                        
                        # query2 = "UPDATE COMBO_SCRAPY_RUN_FINAL_GOKUL SET Doc_Number = %s WHERE Url = %s"
                        # connection.execute(query2, (doc, link))
                        
                        # query3 = "UPDATE COMBO_SCRAPY_RUN_FINAL_GOKUL SET Report_Quarter = %s WHERE Url = %s"
                        # connection.execute(query3, (report_quarter, link))
                        
                        # query4 = "UPDATE COMBO_SCRAPY_RUN_FINAL_GOKUL SET Report_Type = %s WHERE Url = %s"
                        # connection.execute(query4, (report_type, link))
                        
                        # query5 = "UPDATE COMBO_SCRAPY_RUN_FINAL_GOKUL SET Company = %s WHERE Url = %s"
                        # connection.execute(query5, (company, link))
                        
                        # query6 = "UPDATE COMBO_SCRAPY_RUN_FINAL_GOKUL SET Is_PDF= %s WHERE Url = %s"
                        # connection.execute(query6, (True, link))
                        
                        query7 = "UPDATE LIFE_INSURANCE_QUARTERLY_DATA_CORPUS SET Cleaned_File_Name= %s WHERE Url = %s"
                        connection.execute(query7, (cleaned_file_name, link))
                        
                        connection.execute("commit")
                        print('Updated Status as Done')
                        print('_'*88)
                    
                # else:
                #     print(f"Skipping file. The link does not point to a PDF. Content-Type: {content_type}")

                    elif 'application/ms-excel' in content_type:
                        file_extension = '.xls'
                        print("File is an XLS.")
                        connection=engine.connect()
                        query = """ UPDATE LIFE_INSURANCE_QUARTERLY_DATA_CORPUS SET S3_Upload = 'EXCEL' 
                        WHERE Url = %s """
                        connection.execute(query, (link,))
                        connection.execute("commit")
                        print('Updated Status as EXCEL')
                        print('_'*88)
                        continue  
                    elif 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' in content_type:
                        file_extension = '.xlsx'
                        print("File is an Excel.")  
                        connection=engine.connect()
                        query = """ UPDATE LIFE_INSURANCE_QUARTERLY_DATA_CORPUS SET S3_Upload = 'EXCEL' 
                        WHERE Url = %s """
                        connection.execute(query, (link,))
                        connection.execute("commit")
                        print('Updated Status as EXCEL')
                        print('_'*88)
                        continue   
                    elif 'application/vnd.ms-excel' in content_type:
                        file_extension = '.xls'
                        print("File is an XLS.")  
                        connection=engine.connect()
                        query = """ UPDATE LIFE_INSURANCE_QUARTERLY_DATA_CORPUS SET S3_Upload = 'EXCEL' 
                        WHERE Url = %s """
                        connection.execute(query, (link,))
                        connection.execute("commit")
                        print('Updated Status as EXCEL')
                        print('_'*88)
                        continue      
                    elif 'text/html' in content_type:
                        print("This is an HTML.")  
                        connection=engine.connect()
                        query = """ UPDATE LIFE_INSURANCE_QUARTERLY_DATA_CORPUS SET S3_Upload = 'HTML' 
                        WHERE Url = %s """
                        connection.execute(query, (link,))
                        connection.execute("commit")
                        print('Updated Status as HTML')
                        print('_'*88)
                        continue         
                        
                else:
                    print(f"Failed to download. Status code: {response.status_code}")
                    connection=engine.connect()
                    query = """ UPDATE LIFE_INSURANCE_QUARTERLY_DATA_CORPUS SET S3_Upload = 'Not Downloaded because of Requests' WHERE Url = %s """
                    connection.execute(query, (link,))
                    connection.execute("commit")
                    print('Updated Status as Not Downloaded, Response Status')
                    print('_'*88)
                    
                # query3 = "UPDATE COMBO_SCRAPY_RUN_FINAL_GOKUL SET Report_Quarter = %s WHERE Url = %s"
                # connection.execute(query3, (report_quarter, link))
                
                # query4 = "UPDATE COMBO_SCRAPY_RUN_FINAL_GOKUL SET Report_Type = %s WHERE Url = %s"
                # connection.execute(query4, (report_type, link))
                
                # query5 = "UPDATE COMBO_SCRAPY_RUN_FINAL_GOKUL SET Company = %s WHERE Url = %s"
                # connection.execute(query5, (company, link))
                
            except:
                connection=engine.connect()
                query = """ UPDATE LIFE_INSURANCE_QUARTERLY_DATA_CORPUS SET S3_Upload = 'Failed' WHERE Url = %s """
                connection.execute(query, (link,))
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