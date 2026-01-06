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

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    engine = adqvest_db.db_conn()
    # ****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today = datetime.datetime.now(india_time)

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'LIFE_INSURANCE_COUNCIL_INDIVIDUAL_AGENTS_MONTHLY_DATA'
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
        url = 'https://www.lifeinscouncil.org/industry%20information/individual_agents_data.aspx'

        headers = {"User-Agent": "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.2.8) Gecko/20100722 Firefox/3.6.8 GTB7.1 (.NET CLR 3.5.30729)"}

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

        year_list = soup.findAll("select", attrs={"id": "MainContent_drplistyear"})[0].findAll("option")[4:]
        year = soup.findAll("select", attrs={"id": "MainContent_drplistyear"})[0].findAll("option")[-1]['value']

        payload_soup = soup.findAll('input', attrs={'type': 'hidden'})
        payload_dict = {'__EVENTTARGET': 'ctl00$MainContent$drplistyear',
                        '__EVENTARGUMENT': '',
                        '__LASTFOCUS': '',
                        'ctl00$txtusername': '',
                        'ctl00$txtpwd': '',
                        'ctl00$MainContent$drplistyear': year,
                        'ctl00$MainContent$drplistmonth': '0'
                        }

        for i in payload_soup:
            payload_dict[i['name']] = i['value']

        r = requests.post(url, data=payload_dict, headers=headers)
        soup = BeautifulSoup(r.content, 'lxml')
        month_list = soup.findAll("select", attrs={"id": "MainContent_drplistmonth"})[0].findAll("option")[1:]
        month = soup.findAll("select", attrs={"id": "MainContent_drplistmonth"})[0].findAll("option")[-1]['value']

        payload_soup = soup.findAll('input', attrs={'type': 'hidden'})[1:]
        payload_dict = {'__EVENTTARGET': 'ctl00$MainContent$btnPdf',
                        '__EVENTARGUMENT': '',
                        '__LASTFOCUS': '',
                        'ctl00$txtusername': '',
                        'ctl00$txtpwd': '',
                        'ctl00$MainContent$drplistyear': year,
                        'ctl00$MainContent$drplistmonth': '',
                        'ctl00$MainContent$btngetdata': 'Get Data'
                        }

        for i in payload_soup:
            payload_dict[i['name']] = i['value']

        payload_dict['ctl00$MainContent$drplistmonth'] = month

        r = requests.post(url, data=payload_dict, headers=headers, allow_redirects=True)
        soup = BeautifulSoup(r.content, 'lxml')

        individual_agents_df = pd.read_html(r.content)[-1]
        individual_agents_df.columns = ["Insurance_Provider","No_Of_Agents_Start_Of_FY","Addition_Upto_Relevant_Date","Deletion_Upto_Relevant_Date","Net_No_Of_Agents_On_Relevant_Date"]
        individual_agents_df["Month"] = month_list[-1].text
        individual_agents_df["Year"] = year_list[-1].text

        individual_agents_df = individual_agents_df[individual_agents_df["Insurance_Provider"].str.lower() != "insurer"]
        individual_agents_df["Insurance_Provider"] = individual_agents_df["Insurance_Provider"].apply(lambda x:x.replace("\n"," "))
        individual_agents_df["Insurance_Provider"] = individual_agents_df["Insurance_Provider"].apply(lambda x: x.title())
        individual_agents_df = individual_agents_df[['Insurance_Provider','Addition_Upto_Relevant_Date', 'Deletion_Upto_Relevant_Date', 'Net_No_Of_Agents_On_Relevant_Date','No_Of_Agents_Start_Of_FY','Month', 'Year']]
        individual_agents_df['Relevant_Date'] = None
        for year in year_list:
            year = year.text
            individual_agents_df['Relevant_Date'] = np.where(((individual_agents_df['Month'].str.startswith('Ja'))& (individual_agents_df["Year"] == year)) , str(year)+'-01-31' , individual_agents_df['Relevant_Date'])
            if(int(year)%4==0):
                individual_agents_df['Relevant_Date'] = np.where(((individual_agents_df['Month'].str.startswith('F')) & (individual_agents_df["Year"] == year)) , str(year)+'-02-29' , individual_agents_df['Relevant_Date'])
            else:
                individual_agents_df['Relevant_Date'] = np.where(((individual_agents_df['Month'].str.startswith('F')) & (individual_agents_df["Year"] == year)) , str(year)+'-02-28' , individual_agents_df['Relevant_Date'])
            individual_agents_df['Relevant_Date'] = np.where(((individual_agents_df['Month'].str.startswith('Ma')) & (individual_agents_df["Year"] == year)) , str(year)+'-03-31' , individual_agents_df['Relevant_Date'])
            individual_agents_df['Relevant_Date'] = np.where(((individual_agents_df['Month'].str.startswith('Ap')) & (individual_agents_df["Year"] == year)) , str(year)+'-04-30' , individual_agents_df['Relevant_Date'])
            individual_agents_df['Relevant_Date'] = np.where(((individual_agents_df['Month'].str.startswith('May'))& (individual_agents_df["Year"] == year)) , str(year)+'-05-31' , individual_agents_df['Relevant_Date'])
            individual_agents_df['Relevant_Date'] = np.where(((individual_agents_df['Month'].str.startswith('Jun')) & (individual_agents_df["Year"] == year)) , str(year)+'-06-30' , individual_agents_df['Relevant_Date'])
            individual_agents_df['Relevant_Date'] = np.where(((individual_agents_df['Month'].str.startswith('Jul')) & (individual_agents_df["Year"] == year)) , str(year)+'-07-31' , individual_agents_df['Relevant_Date'])
            individual_agents_df['Relevant_Date'] = np.where(((individual_agents_df['Month'].str.startswith('Aug')) & (individual_agents_df["Year"] == year)) , str(year)+'-08-31' , individual_agents_df['Relevant_Date'])
            individual_agents_df['Relevant_Date'] = np.where(((individual_agents_df['Month'].str.startswith('Sep')) & (individual_agents_df["Year"] == year)) , str(year)+'-09-30' , individual_agents_df['Relevant_Date'])
            individual_agents_df['Relevant_Date'] = np.where(((individual_agents_df['Month'].str.startswith('Oct')) & (individual_agents_df["Year"] == year)) , str(year)+'-10-31' , individual_agents_df['Relevant_Date'])
            individual_agents_df['Relevant_Date'] = np.where(((individual_agents_df['Month'].str.startswith('Nov')) & (individual_agents_df["Year"] == year)) , str(year)+'-11-30' , individual_agents_df['Relevant_Date'])
            individual_agents_df['Relevant_Date'] = np.where(((individual_agents_df['Month'].str.startswith('De')) & (individual_agents_df["Year"] == year)) , str(year)+'-12-31' , individual_agents_df['Relevant_Date'])
        individual_agents_df["Relevant_Date"] = individual_agents_df["Relevant_Date"].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
        individual_agents_df["Runtime"] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
        individual_agents_max_rel_date = individual_agents_df["Relevant_Date"].max()
        df=pd.read_sql("SELECT * FROM AdqvestDB.GENERIC_LOOK_UP_TABLE_STATIC",engine)
        individual_agents_df=individual_agents_df.merge(df[['Raw_Data','Clean_Data']],left_on='Insurance_Provider',right_on='Raw_Data',how='left')
        individual_agents_df.drop('Raw_Data',axis=1,inplace=True)
        individual_agents_df.rename(columns={'Clean_Data':'Insurance_Provider_Clean'},inplace=True)
        individual_agents_df = individual_agents_df[individual_agents_df["Insurance_Provider"].str.lower().str.contains("total")==False]
        db_max_rel_date = pd.read_sql("select max(Relevant_Date) as Max from LIFE_INSURANCE_COUNCIL_INDIVIDUAL_AGENTS_MONTHLY_DATA",engine)
        db_max_rel_date = db_max_rel_date["Max"][0]
        if(individual_agents_max_rel_date > db_max_rel_date):
            print("New Data")
            individual_agents_df.to_sql(name = "LIFE_INSURANCE_COUNCIL_INDIVIDUAL_AGENTS_MONTHLY_DATA",if_exists="append",con = engine,index = False)
            CH_max_rel_dt = client.execute("SELECT Relevant_Date FROM AdqvestDB.LIFE_INSURANCE_COUNCIL_INDIVIDUAL_AGENTS_MONTHLY_DATA Order by Relevant_Date desc Limit 1")
            clickdate = CH_max_rel_dt.pop(0)
            click = clickdate[0]
            inag_df = pd.read_sql("select * from AdqvestDB.LIFE_INSURANCE_COUNCIL_INDIVIDUAL_AGENTS_MONTHLY_DATA WHERE Relevant_Date > '"+click.strftime('%Y-%m-%d')+"'",con=engine)
            client.execute("INSERT INTO LIFE_INSURANCE_COUNCIL_INDIVIDUAL_AGENTS_MONTHLY_DATA VALUES", inag_df.values.tolist())
        else:
            print("Data Already Present")
        log.job_end_log(table_name,job_start_time,no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_type = str(re.search("'(.+?)'", str(sys.exc_info()[0])).group(1))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        error_msg = str(sys.exc_info()[1]) + " line " + str(exc_tb.tb_lineno)
        print(error_msg)

        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
