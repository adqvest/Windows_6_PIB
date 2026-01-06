import datetime
import os
import re
import sys
import time
import warnings
import requests
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
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'LIFE_INSURANCE_COUNCIL_NEW_BUSINESS_PERFORMANCE_MONTHLY_DATA'
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

        url = 'https://www.lifeinscouncil.org/industry%20information/nbp.aspx'

        headers = {"User-Agent": "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.2.8) Gecko/20100722 Firefox/3.6.8 GTB7.1 (.NET CLR 3.5.30729)"}

        proxy_host = "proxy.crawlera.com"
        proxy_port = "8011"
        proxy_auth = "5c2fcd5a03ad47a8b87f3cc83450c4a7:"  # Make sure to include ':' at the end
        proxies = {"http": "http://{}@{}:{}/".format(proxy_auth, proxy_host, proxy_port)}

        r = requests.get(url, proxies=proxies)
        soup = BeautifulSoup(r.content, 'lxml')

        cookie = [k + "=" + v for k, v in r.cookies.get_dict().items()]
        headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                   'Accept-Encoding': 'gzip, deflate, br',
                   'Content-Type': 'application/x-www-form-urlencoded',
                   "User-Agent": "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.2.8) Gecko/20100722 Firefox/3.6.8 GTB7.1 (.NET CLR 3.5.30729)",
                   'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
                   'Connection': 'keep-alive',
                   'Cookie' : cookie[0],
                   'Host': 'www.lifeinscouncil.org',
                   'Origin': 'https://www.lifeinscouncil.org',
                   'Referer': 'https://www.lifeinscouncil.org/industry%20information/nbp.aspx',
                   'Sec-Fetch-Site': 'same-origin'
                   }

        year_list = soup.findAll("select", attrs={"id": "MainContent_drplistyear"})[0].findAll("option")[4:]
        year = soup.findAll("select", attrs={"id": "MainContent_drplistyear"})[0].findAll("option")[-1]['value']

        payload_soup = soup.findAll('input', attrs={'type':'hidden'})
        payload_dict = {'__EVENTTARGET':'ctl00$MainContent$drplistyear',
                        '__EVENTARGUMENT':'',
                        '__LASTFOCUS':'',
                        'ctl00$txtusername':'',
                        'ctl00$txtpwd':'',
                        'ctl00$MainContent$drplistyear': year,
                        'ctl00$MainContent$drplistmonth':''
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

        r = requests.post(url,data=payload_dict, headers=headers, allow_redirects=True)
        soup = BeautifulSoup(r.content, 'lxml')

        nbp_df = pd.read_html(r.content)[-1]
        nbp_df["Month"] = month_list[-1].text
        nbp_df["Year"] = year_list[-1].text

        nbp_df["Insurance_Provider"] = np.where((nbp_df[2].isna()), nbp_df[1], np.nan)
        nbp_df["Insurance_Provider"] = nbp_df["Insurance_Provider"].ffill()
        nbp_df = nbp_df[nbp_df[1].str.lower() != "grand total"]
        nbp_df[1] = np.where((nbp_df[1] == "private total"), "Total", nbp_df[1])
        nbp_df = nbp_df[nbp_df[1].str.lower() != "particulars"]
        nbp_df = nbp_df.replace("", np.nan)
        nbp_df = nbp_df[nbp_df[2].isnull() == False]
        nbp_df = nbp_df[nbp_df[1].isnull() == False]
        nbp_df.reset_index(drop=True, inplace=True)
        nbp_df["Insurance_Provider"] = nbp_df["Insurance_Provider"].apply(lambda x: x.replace("\n", " "))
        nbp_df = nbp_df.drop(columns=[0, 3, 4, 5, 6, 8, 9, 10, 11])
        nbp_df = nbp_df.rename(columns={1: "Policy_Type", 2: "Premium_In_Crs_INR", 7: "No_Of_Policies_And_Schemes"})
        nbp_df["Insurance_Provider"] = nbp_df["Insurance_Provider"].apply(lambda x: x.title())

        nbp_df['Relevant_Date'] = None
        for year in year_list:
            year = year.text
            nbp_df['Relevant_Date'] = np.where(((nbp_df['Month'].str.startswith('Ja'))& (nbp_df["Year"] == year)) , str(year)+'-01-31' , nbp_df['Relevant_Date'])
            if(int(year)%4==0):
                nbp_df['Relevant_Date'] = np.where(((nbp_df['Month'].str.startswith('F')) & (nbp_df["Year"] == year)) , str(year)+'-02-29' , nbp_df['Relevant_Date'])
            else:
                nbp_df['Relevant_Date'] = np.where(((nbp_df['Month'].str.startswith('F')) & (nbp_df["Year"] == year)) , str(year)+'-02-28' , nbp_df['Relevant_Date'])
            nbp_df['Relevant_Date'] = np.where(((nbp_df['Month'].str.startswith('Ma')) & (nbp_df["Year"] == year)) , str(year)+'-03-31' , nbp_df['Relevant_Date'])
            nbp_df['Relevant_Date'] = np.where(((nbp_df['Month'].str.startswith('Ap')) & (nbp_df["Year"] == year)) , str(year)+'-04-30' , nbp_df['Relevant_Date'])
            nbp_df['Relevant_Date'] = np.where(((nbp_df['Month'].str.startswith('May'))& (nbp_df["Year"] == year)) , str(year)+'-05-31' , nbp_df['Relevant_Date'])
            nbp_df['Relevant_Date'] = np.where(((nbp_df['Month'].str.startswith('Jun')) & (nbp_df["Year"] == year)) , str(year)+'-06-30' , nbp_df['Relevant_Date'])
            nbp_df['Relevant_Date'] = np.where(((nbp_df['Month'].str.startswith('Jul')) & (nbp_df["Year"] == year)) , str(year)+'-07-31' , nbp_df['Relevant_Date'])
            nbp_df['Relevant_Date'] = np.where(((nbp_df['Month'].str.startswith('Aug')) & (nbp_df["Year"] == year)) , str(year)+'-08-31' , nbp_df['Relevant_Date'])
            nbp_df['Relevant_Date'] = np.where(((nbp_df['Month'].str.startswith('Sep')) & (nbp_df["Year"] == year)) , str(year)+'-09-30' , nbp_df['Relevant_Date'])
            nbp_df['Relevant_Date'] = np.where(((nbp_df['Month'].str.startswith('Oct')) & (nbp_df["Year"] == year)) , str(year)+'-10-31' , nbp_df['Relevant_Date'])
            nbp_df['Relevant_Date'] = np.where(((nbp_df['Month'].str.startswith('Nov')) & (nbp_df["Year"] == year)) , str(year)+'-11-30' , nbp_df['Relevant_Date'])
            nbp_df['Relevant_Date'] = np.where(((nbp_df['Month'].str.startswith('De')) & (nbp_df["Year"] == year)) , str(year)+'-12-31' , nbp_df['Relevant_Date'])

        nbp_df["Relevant_Date"] = nbp_df["Relevant_Date"].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d").date())
        nbp_df["Runtime"] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
        nbp_df = nbp_df[['Insurance_Provider','Policy_Type','Premium_In_Crs_INR','No_Of_Policies_And_Schemes','Month','Year','Relevant_Date','Runtime']]

        nbp_df = nbp_df[nbp_df["Insurance_Provider"] != "Private"]
        nbp_df = nbp_df[nbp_df["Policy_Type"] != "Total"]
        nbp_max_rel_date = nbp_df["Relevant_Date"].max()
        db_max_rel_date = pd.read_sql("select max(Relevant_Date) as Max from LIFE_INSURANCE_COUNCIL_NEW_BUSINESS_PERFORMANCE_MONTHLY_DATA",engine)
        db_max_rel_date = db_max_rel_date["Max"][0]

        df=pd.read_sql("SELECT * FROM AdqvestDB.GENERIC_LOOK_UP_TABLE_STATIC",engine)
        final_df=nbp_df.merge(df[['Raw_Data','Clean_Data']],left_on='Insurance_Provider',right_on='Raw_Data',how='left')
        final_df.drop('Raw_Data',axis=1,inplace=True)
        final_df.rename(columns={'Clean_Data':'Insurance_Provider_Clean'},inplace=True)
        final_df=final_df[['Insurance_Provider', 'Insurance_Provider_Clean', 'Policy_Type','Premium_In_Crs_INR', 'No_Of_Policies_And_Schemes', 'Month', 'Year','Relevant_Date', 'Runtime']]
        print(final_df.head())

        final_df['Policy_Type']=final_df['Policy_Type'].str.replace('Single Premium','Single')
        final_df['Policy_Type']=final_df['Policy_Type'].str.replace('Non-Single','Non Single')
        final_df['Policy_Type']=final_df['Policy_Type'].str.replace('Group Yearly Renewable Premium','Group Yearly Renewable')
        final_df['Policy_Type']=final_df['Policy_Type'].apply(lambda x: x.strip())
        final_df['Premium_In_Crs_INR'] = final_df['Premium_In_Crs_INR'].apply(lambda x: float(x))
        final_df['No_Of_Policies_And_Schemes'] = final_df['No_Of_Policies_And_Schemes'].apply(lambda x: float(x))
        final_df['Year'] = final_df['Year'].apply(lambda x: int(x))

        if(nbp_max_rel_date > db_max_rel_date):
            print("New Data")
            final_df.to_sql(name = "LIFE_INSURANCE_COUNCIL_NEW_BUSINESS_PERFORMANCE_MONTHLY_DATA",if_exists="append",con = engine,index = False)
            click_max_date = client.execute("select max(Relevant_Date) from LIFE_INSURANCE_COUNCIL_NEW_BUSINESS_PERFORMANCE_MONTHLY_DATA")
            click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])

            query = 'select * from AdqvestDB.LIFE_INSURANCE_COUNCIL_NEW_BUSINESS_PERFORMANCE_MONTHLY_DATA where Relevant_Date > "' + click_max_date + '"'
            df = pd.read_sql(query,engine)
            client.execute("INSERT INTO LIFE_INSURANCE_COUNCIL_NEW_BUSINESS_PERFORMANCE_MONTHLY_DATA VALUES",df.values.tolist())
        else:
            print("Data Already Present")
        log.job_end_log(table_name,job_start_time,no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        error_msg = str(sys.exc_info()[1]) + " line " + str(exc_tb.tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
