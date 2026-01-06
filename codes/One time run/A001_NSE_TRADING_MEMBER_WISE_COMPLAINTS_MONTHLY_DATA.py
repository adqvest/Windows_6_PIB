import re
import sys
import os
import warnings
import datetime
import pandas as pd
import requests
import time
from bs4 import BeautifulSoup
from pytz import timezone
from playwright.sync_api import sync_playwright
from dateutil import parser
warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import numpy as np
import ClickHouse_db
import adqvest_s3
import boto3
import JobLogNew as log
from adqvest_robotstxt import Robots
robot = Robots(__file__)
import cleancompanies

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir')
    engine = adqvest_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details

    job_start_time = datetime.datetime.now(india_time)
    table_name = 'NSE_TRADING_MEMBER_WISE_COMPLAINTS_MONTHLY_DATA'
    no_of_ping = 0

    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = '10_AM_WINDOWS_SERVER2_SCHEDULER'

    def clickhouse_datatype(df, col):
        for i in (col):
            # df[i]= df[i].where(pd.notnull(df[i]), None)
            df[i] = df[i].replace({np.nan: None})
            for j in range(len(df[i])):
                if df[i][j] != None:
                    # print(df[i][j])
                    df[i][j] = float(df[i][j])
        return df

    def clean(text):
        text = text.replace('LIMITED','').replace('LTD','').replace('PRIVATE','').replace('PVT','')
        text = re.sub(r'  +',' ', text).strip()
        text = np.where(text[len(text)-1] == '.' , text[0:len(text)-1], text)
        text = str(text)
        text = re.sub(r'  +',' ', text).strip()
        text = np.where(text[len(text)-1] == '.' , text[0:len(text)-1], text)
        text = str(text)
        text = re.sub(r"\'+",' ', text)
        text = re.sub(r'  +',' ', text).strip()
        return text

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
        # headers = {
        #             'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        #             'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        #             'cache-control': 'no-cache',
        #             # 'cookie': '_ga=GA1.1.752195759.1702390321; AKA_A2=A; _abck=1072141605D576AE970E5B12EC429709~0~YAAQx/XSF8sjWUyPAQAAPSNVUQuKJz5G4NP3Yakq4XOADqXUiMEfqnIhZ32t/iHuMHB7G3XOtZc4BnVdT1gGzRM4BMK9S18kcbe+kO4u8AA0Bma43odZFEj7c171ihypyEXCCzkei9lD0Gi6oynZDrV+JxXwDuFPkM4Qn/dfEz8+7AbGB/hRCk4YvvAtTPa4mV+nB+1p7F4o1Q3SJ+soVhkqfAtBV2zG3aMs6dhoaNBa7GryCsxFZ1yAuxl0ls8V0pQL3Q0UVsipq2hA1PcYU+0ry/t5+TeFdTYMD5r956QK/Wka2rSohqB5hPCC808kM4L1LZAadAzQutSTYFfjRbQTeYZfGaOiB3RDs7XYGbWhDndyMCJeW3PpYvkHMAgyruKbcpPERTYnHS0JKlOgYMJnma2qqObFj5s=~-1~-1~-1; bm_sz=852BCC4EE54A63D29E3B059295457EE3~YAAQx/XSF84jWUyPAQAAPSNVURfFH21kDtYMCrfkqEiDzTpjvGHmsCl4XN5f5h9FawG4/WY2Fw86XWYI56C4M1amHJGZ98s53FOdZmiqcoInPBpOnV/RyvKSQJE21RY2yo7eXe6RxmAXYLeRjCOl8RjSODxOwxrmchV47nDGP09eNWZjKik3YAM6JyaLXbC2pt45F2ySiKZAgAEAlbgId/XSP7pUOc6fG7iW2xG7/fwXRbtz69TEbR8JIfWrTTzZGTeLif8FDktd8KWjuUwkhEwTyyyK1mgDk1UcuKf9WZxRqj8da0vNwGtcMaGTv5MukFdt0Axo07nS9wuqoXfgKR7LSdWfsp3ARsrtXLYwsPPBgwsBqJCM+2KOAM2j9IQKJev+rNPfwR/3vYn+iX+T~4536632~4343090; ak_bmsc=49DB9FA75D54343CF502CA67560D8841~000000000000000000000000000000~YAAQx/XSFwskWUyPAQAABidVURceeFHqU8X2Q1tH/0IfVnPdl8QDI9AOVH3o2eEc7rg0XfsIoKLh5Qi+9JL7IcQNROMSn0rzRjW03A46ohCXlXp0hOLXXG5+OerScZj+UKqSLuw3/0km8F0x4vhZgoY2nCmbEw3uV6hSC59SchwTxHfu+c4PdmNC6lFwR+iPCudWkwUCZ+i1+mg3fvJ9fKaGj4ECb+NVn+vjxiGxBVKgsU7hf7m1mldXXGDhBaZZxY9JXRVuAG8ieNojai9VL0g3xfVWU5mOSqxAOddqPYrU2gZBORL1L4XsCdKO6uKuEM/IW3rBPQrpZS/Qx5N0H/dVvp5zarPAIuBdcMLZrqKEpB762I+C3NygTvHeS4up0tTyhy2M0jWrHXWa0FFnDWqDiW33rZ+8rwdl3IfriAkn7Xa3s2RwCZDHrjGMiTd3rpfwXzonAY+o/r00Mfw=; _ga_QJZ4447QD3=GS1.1.1715056485.23.1.1715056565.0.0.0; _ga_87M7PJ3R97=GS1.1.1715056485.26.1.1715056565.0.0.0; bm_sv=9420D8EBD71538C0AE985992E2294328~YAAQbpYRYK9O402PAQAAfV9WURdN00oj8BNsRvb/DL39JQclJIFQHb6ITPYdp9LcG7hqs0AEPB5HXgShfcMEnAhGCUfA29TsGUUsPtiAOcGHEE5qf05wjX7YpzZ3AZ3v0qJcyx8VpaZ2RzDdpfyLa49yPh1dlVBZgSYc7qkKjZidGufupENnS9HBgShYF6PPfeacCX5eIqz2wokanONgX2//Q0Q5+L6PQmCbkLRfaZFaKA2XCadFyjtwiO4jGCe0QEqV~1; RT="z=1&dm=nseindia.com&si=6d9ba439-2b08-43a2-a672-ab49b451bed8&ss=lvvwcl5d&sl=2&se=8c&tt=1t2&bcn=%2F%2F684d0d46.akstat.io%2F&ld=ei4&nu=2vyf5f43&cl=1qd4&ul=1qdq&hd=1qoq"',
        #             'pragma': 'no-cache',
        #             'priority': 'u=0, i',
        #             'referer': 'https://www.nseindia.com/',
        #             'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        #             'sec-ch-ua-mobile': '?0',
        #             'sec-ch-ua-platform': '"macOS"',
        #             'sec-fetch-dest': 'document',
        #             'sec-fetch-mode': 'navigate',
        #             'sec-fetch-site': 'same-site',
        #             'sec-fetch-user': '?1',
        #             'upgrade-insecure-requests': '1',
        #             'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        #         }

        client = ClickHouse_db.db_conn()
        ACCESS_KEY_ID = adqvest_s3.s3_cred()[0]
        ACCESS_SECRET_KEY = adqvest_s3.s3_cred()[1]
        BUCKET_NAME = 'adqvests3bucket'

        s3 = boto3.resource(
            's3',
            aws_access_key_id=ACCESS_KEY_ID,
            aws_secret_access_key=ACCESS_SECRET_KEY,
            config=boto3.session.Config(signature_version='s3v4',region_name = 'ap-south-1')
        )

        wd = 'C:/Users/Administrator/Junk/'

        last_month = today - 30*days
        print(last_month)
        year = int(last_month.strftime("%Y"))
        month = int(last_month.strftime("%m"))
        day = last_month.strftime("%B")

        if month >= 4:
            FY_end =datetime.date(year+1,3,31)
            FY_start = datetime.date(year,4,1)
        else:
            FY_end =datetime.date(year,3,31)
            FY_start = datetime.date(year-1,4,1)

        year1 = FY_start.strftime("%Y")
        print('year1:',str(year1)[-2:])
        year2 = FY_end.strftime("%Y")
        print('year2:',str(year2)[2:])

        url = 'https://www.nseindia.com/invest/arbitration-status'
        robot.add_link(url)
        pw = sync_playwright().start()
        browser = pw.firefox.launch(headless = False)
        context = browser.new_context(java_script_enabled = True,bypass_csp=True)
        page = context.new_page()
        page.goto(url) #(, wait_until='networkidle') removed by Nidhi on 14 May 2024
        time.sleep(5)
        soup = BeautifulSoup(page.content())
        years = [i.text for i in soup.findAll('h5', class_='heading')]
        
        years = [i for i in years if year1+'-'+year2[2:] in i]
        print(years)
        if len(years) != 0:
            print(years[0])
            soup = BeautifulSoup(page.content())
            with page.expect_download() as download_info:
                page.locator('//li[contains(text(), "Report 1C")]/a').first.click()
            download = download_info.value
            download.save_as(wd + download.suggested_filename)
            filepath = wd + download.suggested_filename
            print(filepath)
            df = pd.read_excel(filepath, header = None)
            print(df)
            
            if df.iloc[:, 0].str.lower().str.contains('report 1c', na=False).any():
                idx = df[df.iloc[:, 0].str.lower().str.contains('report 1c', na=False)].index[0]
                date_text = df.iloc[idx, 0].lower().split('updated on')[-1].strip()
                rel_date = parser.parse(date_text).date()

            max_db_rel_date = pd.read_sql("select max(Relevant_Date) as Max from NSE_TRADING_MEMBER_WISE_COMPLAINTS_MONTHLY_DATA",engine)
            max_db_rel_date = max_db_rel_date["Max"][0]
            if(rel_date>max_db_rel_date):
                print('-------------NSE_TRADING_MEMBER_WISE_COMPLAINTS_MONTHLY_DATA-------------')

                # # file upload s3
                data = open(filepath, 'rb')
                s3.Bucket(BUCKET_NAME).put_object(Key='NSE_TRADING_MEMBERWISE/'+download.suggested_filename, Body=data)
                data.close()
                # os.remove(filepath)

                st_idx = df[df.iloc[:, 0] == 'SN'].index[0]
                df = df.iloc[st_idx:, :]
                df = df.dropna(thresh=25, axis = 1)
                df.columns = ["SN",
                            # 'Member_Code',  # Commented | Pushkar | 13 June 2023
                            "Name_Of_TM",
                            "Defaulter",
                            "UCC_Of_Active_Clients",
                            "Complaints_Received_Against_TM",
                            "Resolved_Thru_Exchange_Or_IGRC",
                            "Non_Actionable",
                            "Advised_Or_Opted_For_Arbitration",
                            "Pending_For_Redressal_With_Exchange",
                            "Percentage_Of_No_Of_Complaints_Received_Per_Active_Client",  # changed,
                            "Percentage_Of_Number_Of_Complaints_Resolved",                # changed,
                            "No_Of_Arbitration_Filed_By_Clients",
                            "Decided_By_The_Arbitrators",
                            "Decided_By_Arbitrators_In_Favour_Of_Clients",
                            "Pending_For_Redressal_With_Arbitrators"]
                df.drop(columns = ["SN"],inplace = True)

                df = df[df["Name_Of_TM"].notnull()]
                df = df[df["Name_Of_TM"].str.lower().str.contains("total") == False]
                df = df[df["Name_Of_TM"] != "NAME OF THE TM"]
                df['Name_Of_TM_Clean'] = df['Name_Of_TM']
                df['Name_Of_TM_Clean'] = df['Name_Of_TM_Clean'].map(clean)

                df["Relevant_Date"] = rel_date
                df["Runtime"] =  pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))

                df = df.replace('-',np.nan)
                df = df.reset_index(drop=True)
                df = df[df.Relevant_Date> max_db_rel_date]
                del df['Name_Of_TM']
                df.rename(columns={'Name_Of_TM_Clean':'Name_Of_TM'},inplace=True)
                df,unmapped = cleancompanies.comp_clean(df, 'Name_Of_TM', 'broking', 'Name_Of_TM_Clean', 'NSE_COMPLAINTS_AND_ARBITRATIONS_REDRESSAL_MONTHLY_DATA')

                df = df[['Name_Of_TM', 'Name_Of_TM_Clean', 'Defaulter','UCC_Of_Active_Clients',
                        'Complaints_Received_Against_TM', 'Resolved_Thru_Exchange_Or_IGRC',
                        'Non_Actionable', 'Advised_Or_Opted_For_Arbitration',
                        'Pending_For_Redressal_With_Exchange',
                        'Percentage_Of_No_Of_Complaints_Received_Per_Active_Client',
                        'Percentage_Of_Number_Of_Complaints_Resolved',
                        'No_Of_Arbitration_Filed_By_Clients', 'Decided_By_The_Arbitrators',
                        'Decided_By_Arbitrators_In_Favour_Of_Clients',
                        'Pending_For_Redressal_With_Arbitrators', 'Relevant_Date', 'Runtime']]

                df.to_sql(name = "NSE_TRADING_MEMBER_WISE_COMPLAINTS_MONTHLY_DATA",if_exists="append",index = False,con = engine)
                print(f'To SQL: {len(df)}')
            else:
                print("Data up to date")

            # Added | Pushkar | 11 May 23
            click_max_date = client.execute("select max(Relevant_Date) from AdqvestDB.NSE_TRADING_MEMBER_WISE_COMPLAINTS_MONTHLY_DATA")
            print('Last updated: ClickHouse ----------->', click_max_date)
            click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])
            query = 'select * from AdqvestDB.NSE_TRADING_MEMBER_WISE_COMPLAINTS_MONTHLY_DATA WHERE Relevant_Date > "' + click_max_date + '"'
            df = pd.read_sql(query, engine)
            df = clickhouse_datatype(df, ['UCC_Of_Active_Clients',
                        'Complaints_Received_Against_TM', 'Resolved_Thru_Exchange_Or_IGRC',
                        'Non_Actionable', 'Advised_Or_Opted_For_Arbitration',
                        'Pending_For_Redressal_With_Exchange',
                        'Percentage_Of_No_Of_Complaints_Received_Per_Active_Client',
                        'Percentage_Of_Number_Of_Complaints_Resolved',
                        'No_Of_Arbitration_Filed_By_Clients', 'Decided_By_The_Arbitrators',
                        'Decided_By_Arbitrators_In_Favour_Of_Clients',
                        'Pending_For_Redressal_With_Arbitrators'])
            client.execute("INSERT INTO NSE_TRADING_MEMBER_WISE_COMPLAINTS_MONTHLY_DATA VALUES",df.values.tolist())
            print(f'To CH: {len(df)}')

            max_db_rel_date = pd.read_sql("select max(Relevant_Date) as Max from NSE_COMPLAINTS_AND_ARBITRATIONS_REDRESSAL_MONTHLY_DATA",engine)
            max_db_rel_date = max_db_rel_date["Max"][0]
            if(rel_date>max_db_rel_date):
                print('\n-------------NSE_COMPLAINTS_AND_ARBITRATIONS_REDRESSAL_MONTHLY_DATA-------------')
                df = pd.read_excel(filepath)
                robot.add_link(url)
                end_idx = df[df.iloc[:, 0] == 'SN'].index[0] - 1
                df = df.iloc[idx+1:end_idx, :]
                df = df.dropna(how='all', axis = 1)
                st_idx = df[df.iloc[:, 0].str.lower().str.contains('total', na=False)].index[0] - 1
                df = df.iloc[st_idx:, :]
                df = df.dropna(how='all', axis = 1)
                df = df.T
                df.columns = df.iloc[0, :]
                df = df.iloc[1:, :]
                df.reset_index(drop = True,inplace= True)
                df = df.dropna(how = 'all', axis = 1)
                df.columns = ['Total_No_Of_Active_Clients', 'Percentage_Of_Complaints_Against_No_Of_Active_Clients', 'Overall_Market_Redressal_Rate']
                df["Relevant_Date"] = rel_date
                df["Runtime"] =  pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
                df = df[df.Relevant_Date> max_db_rel_date]

                df.to_sql(name = "NSE_COMPLAINTS_AND_ARBITRATIONS_REDRESSAL_MONTHLY_DATA",if_exists="append",index = False,con = engine)
                print(f'To SQL: {len(df)}')
            else:
                print('No New Data')
        else:
            print('Current FY Data not published yet')
            
        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')