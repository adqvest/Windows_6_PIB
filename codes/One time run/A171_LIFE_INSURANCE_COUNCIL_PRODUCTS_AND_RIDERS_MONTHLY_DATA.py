import datetime
import os
import re
import sys
import time
import warnings
import requests
import camelot
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from pytz import timezone
warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
import ClickHouse_db
import calendar

def date_value(x):
    x = x.strip()
    x = datetime.datetime.strptime(x, '%B %Y').date()
    return datetime.date(x.year, x.month, calendar.monthrange(x.year, x.month)[1])

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    engine = adqvest_db.db_conn()
    # ****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today = datetime.datetime.now(india_time)

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'LIFE_INSURANCE_COUNCIL_PRODUCTS_AND_RIDERS_MONTHLY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
        client = ClickHouse_db.db_conn()
        product_df = pd.DataFrame()
        url = 'https://www.lifeinscouncil.org/industry%20information/pro_of_life_ins.aspx'
        headers = {"User-Agent": "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.2.8) Gecko/20100722 Firefox/3.6.8 GTB7.1 (.NET CLR 3.5.30729)"}

        ## 1st Request
        # get Cookie, Payload
        r = requests.get(url)
        soup = BeautifulSoup(r.content, 'lxml')

        cookie = [k + "=" + v for k, v in r.cookies.get_dict().items()]
        headers = { 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Content-Type': 'application/x-www-form-urlencoded',
                    "User-Agent": "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.2.8) Gecko/20100722 Firefox/3.6.8 GTB7.1 (.NET CLR 3.5.30729)",
                    'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
                    'Connection': 'keep-alive',
                    'Cookie': cookie[0],
                    'Host': 'www.lifeinscouncil.org',
                    'Origin': 'https://www.lifeinscouncil.org',
                    'Referer': 'https://www.lifeinscouncil.org/industry%20information/nbp.aspx',
                    'Sec-Fetch-Site': 'same-origin'
                    }

        payload_soup = soup.findAll('input', attrs={'type': 'hidden'})
        payload_dict = {'__EVENTTARGET': 'ctl00$MainContent$drpselectinscompany',
                        '__EVENTARGUMENT': '',
                        '__LASTFOCUS': '',
                        'ctl00$txtusername': '',
                        'ctl00$txtpwd': '',
                        'ctl00$MainContent$drpselectinscompany': 'All Companies',
                        }

        for i in payload_soup:
            payload_dict[i['name']] = i['value']

        ## 2nd Request
        # get Year, update Payload
        r = requests.post(url, data=payload_dict, headers=headers)
        soup = BeautifulSoup(r.content, 'lxml')

        year_list = soup.findAll("select", attrs={"id": "MainContent_drplistyear"})[0].findAll("option")[4:]
        year = soup.findAll("select", attrs={"id": "MainContent_drplistyear"})[0].findAll("option")[-1]['value']

        payload_soup = soup.findAll('input', attrs={'type': 'hidden'})
        payload_dict = {'__EVENTARGUMENT': '',
                        '__LASTFOCUS': '',
                        'ctl00$txtusername': '',
                        'ctl00$txtpwd': '',
                        'ctl00$MainContent$drpselectinscompany': 'All Companies',
                        'ctl00$MainContent$drplistyear' : year,
                        'ctl00$MainContent$drplistmonth' : ''
                        }

        for i in payload_soup:
            payload_dict[i['name']] = i['value']
        payload_dict['__EVENTTARGET'] = 'ctl00$MainContent$drplistyear'

        ## 3rd Request
        # get Month, update Payload
        r = requests.post(url, data=payload_dict, headers=headers)
        soup = BeautifulSoup(r.content, 'lxml')

        month_list = soup.findAll("select", attrs={"id": "MainContent_drplistmonth"})[0].findAll("option")[1:]
        month = soup.findAll("select", attrs={"id": "MainContent_drplistmonth"})[0].findAll("option")[-1]['value']

        payload_soup = soup.findAll('input', attrs={'type': 'hidden'})[1:]
        payload_dict = {'__EVENTTARGET': 'ctl00$MainContent$lbtnpdf',
                        '__EVENTARGUMENT': '',
                        '__LASTFOCUS': '',
                        'ctl00$txtusername': '',
                        'ctl00$txtpwd': '',
                        'ctl00$MainContent$drpselectinscompany': 'All Companies',
                        'ctl00$MainContent$drplistyear': year,
                        'ctl00$MainContent$drplistmonth': month}

        for i in payload_soup:
            payload_dict[i['name']] = i['value']

        ## Last Request
        # get PDF content, save in  File
        r = requests.post(url, data=payload_dict, headers=headers, allow_redirects=True)

        with open('C:\\Users\\Administrator\\Junk\\ProductsandRiders.pdf', 'wb') as f:
            f.write(r.content)
            f.close()

        table_list = camelot.read_pdf('C:\\Users\\Administrator\\Junk\\ProductsandRiders.pdf', pages='all')
        table_df = pd.DataFrame()
        for table in table_list[1:]:
            table_df = pd.concat([table_df, table.df])
        table_df["Month"] = month_list[-1].text
        table_df["Year"] = year_list[-1].text


        product_df = pd.concat([product_df, table_df])
        #os.remove(os.listdir()[0])

        product_df["Month"] = month_list[-1].text
        product_df["Year"] = year_list[-1].text

        product_df = product_df[product_df[0].str.lower() != "name of the products"]
        product_df = product_df[product_df[0].str.lower() != "name of the riders"]
        product_df = product_df[product_df[2] != '']
        product_df[2] = product_df[2].apply(lambda x: x.replace("\n",' ').replace("#",'').replace("^",'').replace("$",''))
        product_df = product_df.rename(columns = {0:"Name_Of_Products_Or_Riders",1:"Line_Of_Business",2:"Category",3:"UIN"})

        product_df['Date'] = product_df['Month'] + ' ' + product_df['Year'].astype(str)
        product_df["Relevant_Date"] = product_df['Date'].apply(date_value)
        product_df["Runtime"] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
        product_df.drop(columns = ["Date"],inplace = True)
        product_df_max_rel_date = product_df["Relevant_Date"].max()
        db_max_rel_date = pd.read_sql("select max(Relevant_Date) as Max from LIFE_INSURANCE_COUNCIL_PRODUCTS_AND_RIDERS_MONTHLY_DATA",engine)
        db_max_rel_date = db_max_rel_date["Max"][0]
        product_df.dropna(axis=1,inplace=True)
        if(product_df_max_rel_date > db_max_rel_date):
            print("New Data")
            product_df.to_sql(name = "LIFE_INSURANCE_COUNCIL_PRODUCTS_AND_RIDERS_MONTHLY_DATA",if_exists="append",con = engine,index = False)
        else:
            print("Data Already Present")
        os.remove('C:\\Users\\Administrator\\Junk\\ProductsandRiders.pdf')
        log.job_end_log(table_name,job_start_time,no_of_ping)

    except:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + " Line  " + str(exc_tb.tb_lineno)
        print(error_type)
        print(error_msg)

        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
