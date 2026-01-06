# -*- coding: utf-8 -*-
"""
Created on Tue Nov  9 16:22:07 2021

@author: Abhishek Shankar
"""
import pandas as pd
import os
from dateutil import parser
import requests
from bs4 import BeautifulSoup
import re
import datetime as datetime
from pytz import timezone
import sys
import warnings
import time
from requests_html import HTMLSession
warnings.filterwarnings('ignore')
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from statistics import mean
import signal
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
import warnings
from dateutil import parser
warnings.filterwarnings('ignore')
from adqvest_robotstxt import Robots
robot = Robots(__file__)

# code you want to evaluate
#****   Date Time *****

engine = adqvest_db.db_conn()
connection = engine.connect()
#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days

def drop_duplicates(df):
    columns = df.columns.to_list()
    columns.remove('Runtime')
    df = df.drop_duplicates(subset=columns, keep='last')
    return df

    """Parses a html segment started with tag <table> followed
    by multiple <tr> (table rows) and inner <td> (table data) tags.
    It returns a list of rows with inner columns.
    Accepts only one <th> (table header/data) in the first row.
    """

def get_request_session(url, headers):
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    r=session.get(url,headers=headers)
    return r    

def get_data_from_lnk(lnk,df):
    
    r1=requests.post(lnk,verify=False,timeout=120)
    soup=BeautifulSoup(r1.content)
    
    try:
        district = soup.select('label:contains(District)')[0].find_next('div').text.strip()
        buldup=soup.find_all('div',class_="x_content label-block")
        d1=[i.find_all('div',class_="col-md-3 col-sm-3") for i in buldup]
        d2=[i for i in d1[-1]]
        d3=[i.find('label') for i in d2]
        d4=[i.text.replace('\n','').replace('\r','').strip() for i in d2]
        d5=[p for p,e in enumerate(d4) if e=='District']
        buldup=d4[d5[0]:d5[0]+2][1]
        # print(d4[d5[0]:d5[0]+2])
    except:
        district = None
    # print(district)
    
    # df = pd.read_html(link)
    df1 = [x for x in df if "number of apartment" in [y.lower() for y in x.columns]]
    if df1 != []:
        total_project=len(df1)
        df1=pd.concat(df1)
        # print("reached here")
        total_apt = sum(df1[[x for x in df1.columns if x.lower() == 'number of apartment'][0]])
        total_floors = len(set(df1[[x for x in df1.columns if 'floor' in x.lower()][0]]))
        total_booked = sum(df1[[x for x in df1.columns if 'booked' in x.lower()][0]])
        total_area = sum(df1[[x for x in df1.columns if 'saleable' in x.lower()][0]])

        df2 = [x for x in df if "Percentage of Work" in [y for y in x.columns]]
    else:

        total_apt = None
        total_floors = None
        total_booked = None
        total_area = None
        df2 = [x for x in df if "Percentage of Work" in [y for y in x.columns]]
    if df2 != []:
        # print("reached here 2")
        df2=pd.concat(df2)

        # df2 = df2[0]
        percentage_work = mean(df2['Percentage of Work'])
        df = pd.DataFrame({"Total_Apt": total_apt,
                           "Total_Num_Project":total_project,
                           "Total_Floors": total_floors,
                           "Total_Booked": total_booked,
                           "Percentage_Completed": percentage_work,
                           "Total_Area_In_Sqmts": total_area,
                           "Total_Area_In_Sqft": total_area * 10.7639,
                           "District": district}, index=[0])

    else:
        percentage_work = None
        df = pd.DataFrame({"Total_Apt": total_apt,
                           "Total_Num_Project":total_project,
                           "Total_Floors": total_floors,
                           "Total_Booked": total_booked,
                           "Percentage_Completed": percentage_work,
                           "Total_Area_In_Sqmts": total_area,
                           "Total_Area_In_Sqft": total_area * 10.7639,
                           "District": district}, index=[0])

    return df,district


def get_date(x):

  try:
    return parser.parse(x).date()
  except:
    return None

headers1 = {
    'authority': 'rerait.telangana.gov.in',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'max-age=0',
    # 'cookie': 'ASP.NET_SessionId=age0hiigzd2lzsrlx03thzoi; __RequestVerificationToken=HJcRx3_Q7HOdXgCQRplqE0SmsEqaZgFkCR0wfqb_wtk-C3v131EoLql2hp7_2rahz4VOLDT7wirGQvhpNGrqUjcVyxpbWO2anYwe073Sls41',
    'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Microsoft Edge";v="120"',
    'sec-ch-ua-mobile': '?1',
    'sec-ch-ua-platform': '"Android"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'none',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36 Edg/120.0.0.0',
}

cookies = {
    'ASP.NET_SessionId': 'age0hiigzd2lzsrlx03thzoi',
    '__RequestVerificationToken': 'HJcRx3_Q7HOdXgCQRplqE0SmsEqaZgFkCR0wfqb_wtk-C3v131EoLql2hp7_2rahz4VOLDT7wirGQvhpNGrqUjcVyxpbWO2anYwe073Sls41',
}
def get_link_df(req,total_records,num_pages,num_record,i):
     df_f_page=pd.DataFrame()
     data = [('__RequestVerificationToken', f'{req}'),
             ('Type', 'Promoter'),
             ('ID', '0'),
             ('pageTraverse', '0'),
             ('RoleIDPageload', '1'),
             ('Project', ''),
             ('hdnProject', ''),
             ('Promoter', ''),
             ('hdnPromoter', ''),
             ('AgentName', ''),
             ('hdnAgent', ''),
             ('CertiNo', ''),
             ('hdnCertiNo', ''),
             ('District', ''),
             ('hdnDivision', ''),
             ('hdnDistrict', ''),
             ('hdnProject', ''),
             ('hdnDTaluka', ''),
             ('hdnVillage', ''),
             ('hdnState', ''),
             ('Taluka', ''),
             ('Village', ''),
             ('CompletionDate_From', ''),
             ('hdnfromdate', ''),
             ('CompletionDate_To', ''),
             ('hdntodate', ''),
             ('PType', ''),
             ('hdnPType', ''),
             ('TotalRecords', f'{total_records}'),
             ('CurrentPage', f'{i}'),
             ('TotalPages', f'{num_pages}'),
             ('Command', 'Next'),
             ('PageSize', f'{num_record}'),]
     
     r = requests.post('https://rerait.telangana.gov.in/SearchList/Search', cookies=cookies, headers=headers1, data=data,verify=False)
     soup=BeautifulSoup(r.content,'lxml')
     page=[i for  i in soup.find_all(class_="grid-row")]
     page2=[i.find_all('td',class_="grid-cell") for i in page]
     
     com_dict={}
     for i in page2:
         # print('-------------')
         # print(i[4].find('a'))
         # print(i[5])
         com_dict["Sr_No"]=i[0].text 
         com_dict["Project"]=i[1].text 
         com_dict["Name"]=i[2].text 
         com_dict["Last_Modified_Date"]=i[3].text 
         com_dict["View_Details"]='https://rerait.telangana.gov.in'+i[4].find('a')['href']
         
         df1=pd.DataFrame.from_dict([com_dict])
         df_f_page=pd.concat([df_f_page,df1])
         df_f_page.reset_index(drop=True,inplace=True)
     
     return df_f_page

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir')   
    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'TELANGANA_RERA_DISTRICT_WISE_PIT_WEEKLY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        max_date_count= pd.read_sql('select Relevant_Date,count(*) as count from TELANGANA_RERA_DISTRICT_WISE_PIT_WEEKLY_DATA  group by Relevant_Date order by Relevant_Date desc;',engine)
        max_date=max_date_count['Relevant_Date'][0]
        today_count=max_date_count['count'][0]

        tdy=today.date()
        print(tdy)
        day=(tdy - max_date).days
        print(day)

        r=requests.get('https://rerait.telangana.gov.in/SearchList/Search',headers=headers1,verify=False)
        soup = BeautifulSoup(r.text,'lxml')

        total_records = soup.find("input",{"id":'TotalRecords'})['value']
        num_record=99
        num_pages=int(int(total_records)/num_record)+1
        req = soup.find("input",{"name":'__RequestVerificationToken'})['value']

        if (day >= 7)|((today_count<5000) & (max_date<=today.date())):
        
            if day >= 7:
                i = 0
                relevant_date = today.date()

            else:
                if today_count<5000:
                    print('Today data Collected')
                    query2="""select distinct Sr_No as Sr_No from TELANGANA_RERA_DISTRICT_WISE_PIT_WEEKLY_DATA where Relevant_Date='""" + str(max_date) + """';"""
                    uploded_sl= pd.read_sql(query2,engine)
                    uploded_sl=uploded_sl.Sr_No.tolist()
                    uploded_sl=[str(int(i)) for i in uploded_sl]
                    max_uplod_sl=uploded_sl[-1]
                    print(uploded_sl)
                    i=int(float(max_uplod_sl)/num_record)
                    relevant_date = max_date
        
            finished_sl=[]
            outer_loop=True
            while ((i<num_pages) & (outer_loop == True)): 
                # print(i)
                print(f"Working on page--{i}")
                pg_df=get_link_df(req,total_records,num_pages,num_record,i)
                # print(pg_df)
                if len(pg_df)>0:
                    uplod_list=[]
                    if len(finished_sl)>0:
                        print(finished_sl)
                        pg_df=pg_df[pg_df['Sr_No'].isin(finished_sl)==False]
                        pg_df.reset_index(drop=True,inplace=True)
                    
                    if max_date==today.date():
                        pg_df=pg_df[pg_df['Sr_No'].isin(uploded_sl)==False]
                        pg_df.reset_index(drop=True,inplace=True)
                    
                    for j in range(pg_df.shape[0]):
                        serial_no=pg_df['Sr_No'][j]
                        link=pg_df['View_Details'][j]
                        start_time = time.time()
                        try:
                            df = pd.read_html(link)
                            if len(df)>0:
                                df2,district=get_data_from_lnk(link,df)
                                if district==None:
                                    outer_loop=False
                                    break
                         
                                df_f_upload=df2
                                df_f_upload.reset_index(drop=True,inplace=True)
                                
                                df_f_upload['Last_Modified_Date'] = pg_df['Last_Modified_Date'][j]
                                df_f_upload['Project_Name'] = pg_df['Project'][j]
                                df_f_upload['Promoter_Name'] = pg_df['Name'][j]
                                df_f_upload['Sr_No'] = pg_df['Sr_No'][j]
                                
                                df_f_upload['Relevant_Date'] = relevant_date
                                df_f_upload['Runtime'] = datetime.datetime.now()
                                df_f_upload['Last_Modified_Date'] = df_f_upload['Last_Modified_Date'].apply(lambda x : get_date(x))
                                df_f_upload=drop_duplicates(df_f_upload)
                                print(f"Sl number collected-->{serial_no}")
                                uplod_list.append(df_f_upload)
                                finished_sl.append(str(serial_no))
                            else:
                                outer_loop=False
                                break
                                
                        except Exception as e:
                            if 'No tables found'== e:
                                print(e)
                                outer_loop=False
                                break
                        # outer_loop=False
                        # finished_sl.append(str(serial_no))
                        # break
                    if len(uplod_list) != 0:
                        df_upload=pd.concat(uplod_list)
                        # engine = adqvest_db.db_conn() 
                        connection = engine.connect() 
                        df_upload.to_sql(name='TELANGANA_RERA_DISTRICT_WISE_PIT_WEEKLY_DATA',con=engine,if_exists='append',index=False)            
                        print(df_upload.info())
                        connection.close()

                if not outer_loop:  #Check the condition
                    print("Condition satisfied, going back to the start of the outer loop")
                    outer_loop = True  # Reset the flag
                    i=i+0   
                else:
                    i=i+1
                    finished_sl=[]
                    time.sleep(3)
        else:
           print('Not in date interval')
        log.job_end_log(table_name,job_start_time,no_of_ping)
    except:
        try:
            connection.close()
        except:
            pass
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
