import sqlalchemy
import pandas as pd
from pandas.io import sql
import calendar
import os
import requests
from bs4 import BeautifulSoup
import re
import datetime as datetime
from pytz import timezone
import sys
import warnings
import time
warnings.filterwarnings('ignore')
import numpy as np
import json
from selenium import webdriver
import math
# import ssl
# from requests.exceptions import ConnectionError
# ssl._create_default_https_context = ssl._create_unverified_context


sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
# sys.path.insert(0, '/home/ubuntu/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
import adqvest_s3
import ClickHouse_db
from adqvest_robotstxt import Robots
robot = Robots(__file__)

engine = adqvest_db.db_conn()
# client = ClickHouse_db.db_conn()
#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
# days       = datetime.timedelta(1)
  

headers1 = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Cookie': 'JSESSIONID=BFF0B7452D36ECFD55F19067969CA375; SERVERID=aishe5; _gid=GA1.3.1908484132.1723447408; _ga=GA1.1.1897730041.1723447408; _ga_BD6Y88TZEC=GS1.1.1723453384.2.1.1723454184.0.0.0',
    'Origin': 'https://aishe.nic.in',
    'Referer': 'https://aishe.nic.in/aishe/institutionAisheCode',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
}

def get_main_pg_info_and_links(st_id,inst_id,ut_id,it_id,standalone):
    # st_id='16'
    # inst_id='university'
    # it_id='04'
    
    if standalone==True:
        params = {
            'flag': 'true',
            'institutionType': inst_id,
            'stateId': st_id,
            'districtCode': '-1',
            'universitytypeId': ut_id,
            'universityId': '-1',
            'instituteTypeId': '-1',
        }
    else:
        params = {
            'flag': 'true',
            'institutionType': inst_id,
            'stateId': st_id,
            'districtCode': '-1',
            'universitytypeId': ut_id,
            'universityId': '-1',
            'instituteTypeId': '-1',
        }
        
    # print(params)
    # print(inst_id,ut_id)
    # data = pd.DataFrame()
    max_retries = 3
    retries = 0

    while retries < max_retries:
        try:
            r = requests.get('https://aishe.nic.in/aishe/institutionAisheCode', params=params, 
                              headers=headers1,timeout=120,verify=False)

            if r.status_code!=200:
                data=pd.DataFrame()
                break
            # else:
            #     soup=BeautifulSoup(r.content)
            #     data=pd.read_html(r.content)
            #     print(data)
            #     break
            soup=BeautifulSoup(r.content)
            data=pd.read_html(r.content)
            print(data)
            break
        except ConnectionError as e:
            print(f"Connection error: {e}")
            data=pd.DataFrame()
            retries += 1
            
   
    if len(data)>0:
        # print('DATA')
        data=pd.concat([i for i in data])
        total_record=soup.find_all(class_="pagebanner")[0].text.split('items found')[0].strip()
        cookie_id=r.cookies.values()[1]
    else:
        data=pd.DataFrame()
        cookie_id=''
        total_record=0
        
    if len(data)>30:
        page_links=extract_other_pg_links_frm_main_pg(r,cookie_id)
       
    else:
        page_links=[]
   
    return data,page_links,total_record

def extract_other_pg_links_frm_main_pg(r,cookie_id):
       soup=BeautifulSoup(r.content)
       df=[i.find_all('a',href=True) for i in soup.find_all(class_="pagelinks")][0]
       df1=[i for i in df]
       df1=[i for i in df if 'Go to page' in str(i)]
   
       page_links=["https://aishe.gov.in"+str(i).split('title')[0].split("href=")[1].replace('"','').replace('amp;','').strip() for i in df1]
       page_links=[i.replace(';jsessionid='+r.cookies.values()[1],'') for i in page_links]
       
       return page_links
   
def get_pages_data(lnk):
    max_retries = 3
    retries = 0
    
    while retries < max_retries:
        try:
            r1=requests.get(lnk,headers=headers1,timeout=50,verify=False)
            # print('Failed Here')
            # if r1.status_code!=200:
            #     dfl=pd.DataFrame()
            #     break

            dfl=pd.read_html(r1.content)
            dfl=[i for i in dfl]
            dfl=pd.concat(dfl)
            break  
        except ConnectionError as e:
            print(f"Connection error: {e}")
            dfl=pd.DataFrame()
            retries += 1
            
    if len(dfl)>30:
          page_links=extract_other_pg_links_frm_main_pg(r1,cookie_id=r1.cookies.values()[1])
    else:
        page_links=[]
      
    return dfl,page_links

def get_data_from_links(st_id,inst_id,ut_id,it_id,standalone):
    df=pd.DataFrame()
    df,links,record_count=get_main_pg_info_and_links(st_id,inst_id,ut_id,it_id,standalone)
    if len(links)>0:
        new_links=[]
        done_links=[]
     
        while len(links)>0:
            for lnk in links:
                # print(lnk)
                dfl,add_links=get_pages_data(lnk)   
                robot.add_link(lnk)                             
                df=pd.concat([df,dfl])
                df.dropna(axis=0,inplace=True)
                time.sleep(10)
                done_links.append(lnk)
                new_links=add_links+new_links
            
            links=list(set(new_links+links))
            links=[i for i in links if i not in done_links]

    else:
        df.dropna(axis=0,inplace=True)
    df=df.drop_duplicates(keep="first")
    return df

def update_status_table(update_query):
        print(update_query)
        engine.execute(update_query)
        


def run_program(run_by='Adqvest_Bot', py_file_name=None):
    # os.chdir('/home/ubuntu/AdQvestDir')
    os.chdir('C:/Users/Administrator/AdQvestDir')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'AISHE_CODES_MONTHLY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
            
        db_max_date=pd.read_sql("select max(Relevant_Date) as Relevant_Date from AISHE_CODES_MONTHLY_DATA", engine)['Relevant_Date'][0]
        
        headers = {
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
                    'Connection': 'keep-alive',
                    'Cookie': 'JSESSIONID=BFF0B7452D36ECFD55F19067969CA375; SERVERID=aishe5; _gid=GA1.3.1908484132.1723447408; _gat_gtag_UA_250704415_1=1; _ga=GA1.1.1897730041.1723447408; _ga_BD6Y88TZEC=GS1.1.1723453384.2.1.1723453704.0.0.0',
                    'Referer': 'https://aishe.nic.in/aishe/home',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'same-origin',
                    'Sec-Fetch-User': '?1',
                    'Upgrade-Insecure-Requests': '1',
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
                    'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"macOS"',
                }


        # if today.date()-db_max_date>= datetime.timedelta(30):
            # update_status_table(f"UPDATE AISHE_CODES_MONTHLY_DATA_STATUS_TABLE_Temp_Push set Relevant_Date='{str(pd.to_datetime(today.date(), format='%Y-%m-%d').date())}', Col_Status=NULL;")

        
        status_tbl=pd.read_sql("select State,Max(Relevant_Date) as Relevant_Date from  AISHE_CODES_MONTHLY_DATA_STATUS_TABLE_Temp_Push where Col_Status is Null group by State;",engine)
        print(status_tbl)
        if len(status_tbl)>0:
            date=status_tbl['Relevant_Date'][0]
            state_to_be_col=status_tbl.State.to_list()
            state_to_be_col=[i.lower() for i in state_to_be_col]
            # r=requests.get("https://aishe.gov.in/aishe/institutionAisheCode?universityId=-1&flag=true&districtCode=-1&universitytypeId=-1&stateId=29&instituteTypeId=-1&institutionType=college&d-4030398-p=1",headers=headers,verify=False,timeout=60)
            r=requests.get("https://aishe.nic.in/aishe/aisheCode",headers=headers,verify=False,timeout=60)
            print(r)
            soup=BeautifulSoup(r.content)
            state_code=soup.findAll('select',{'name':'stateId'})
            state_code=[i.find_all('option') for i in state_code][0]
            state_code1={i.text:i.get('value') for i in state_code if 'Select State' not in i.text and i.text.lower()  in state_to_be_col}
            # state_code1={i.text:i.get('value') for i in state_code if 'Select State' not in i.text}

            university_code=soup.findAll('select',{'name':'universitytypeId'})
            university_code=[i.find_all('option') for i in university_code][0]
            university_code={i.text:i.get('value') for i in university_code if 'Select State' not in i.text and i.get('value')!='-1'}
            
            institution_code=soup.findAll('select',{'name':'institutionType'})
            institution_code=[i.find_all('option') for i in institution_code][0]
            institution_code={i.text:i.get('value') for i in institution_code if 'Select State' not in i.text and i.get('value')!='-1'}
            
            institution_ty_code=soup.findAll('select',{'name':'instituteTypeId'})
            institution_ty_code=[i.find_all('option') for i in institution_ty_code][0]
            institution_ty_code={i.text:i.get('value') for i in institution_ty_code if 'Select State' not in i.text and i.get('value')!='-1'}
            
            
            for st,st_id in state_code1.items():
                print(f'Working on State-->{st}')
                
                df_st=pd.DataFrame()
            
                for inst,inst_id in institution_code.items():
                    if inst_id!='standalone':
                        for uni_type,ut_id in university_code.items():
                            df_clg=pd.DataFrame()
                            try:
                              df_clg=get_data_from_links(st_id,inst_id,ut_id,it_id='-1',standalone=False)
                            except:
                              pass

                            df_clg=df_clg.rename(columns={'State Name':'State','District Name':'District_Name','University Name':'University_Name','College Name':'College_Institution_Name','AISHE Code':'Aishe_Code','Status':'Status'})
                            df_clg.drop(columns=[col for col in df_clg.columns if 'University Type' in col], inplace=True)
                            df_clg['University_Institution_Type']=uni_type
                            df_clg['Institution']=inst
                            df_st=pd.concat([df_st,df_clg])
                                
                    else:
                        for inst_type,it_id in institution_ty_code.items():
                            ut_id='-1'
                            df_stand=pd.DataFrame()
                            df_stand=get_data_from_links(st_id,inst_id,ut_id,it_id,standalone=True)
                            df_stand=df_stand.rename(columns={'State Name':'State','District Name':'District_Name','Institution Name':'College_Institution_Name','AISHE Code':'Aishe_Code','Status':'Status'})
                            df_stand.drop(columns=[col for col in df_stand.columns if 'Institution Type' in col], inplace=True)
                            df_stand['University_Institution_Type']=inst_type
                            df_stand['Institution']=inst
                            df_st=pd.concat([df_st,df_stand])
                
                df_st['Relevant_Date']=date
                df_st['Runtime']=datetime.datetime.now()
                df_st['College_Institution_Name']=df_st['College_Institution_Name'].apply(lambda x:str(x).title())
                df_st['University_Institution_Type']=df_st['University_Institution_Type'].apply(lambda x:str(x).title())
                df_st['University_Name']=df_st['University_Name'].apply(lambda x:str(x).title())
                
                
                df_st.reset_index(drop=True,inplace=True)
                df_st=df_st.drop_duplicates(keep="first")
                

                count_df = pd.DataFrame()
                count_df['State']=[st.title()]
                count_df['Collected_Count'] = len(df_st)
                count_df['Relevant_Date']=date
                count_df['Runtime']=datetime.datetime.now()
                
                engine = adqvest_db.db_conn()
                connection = engine.connect()
                if len(df_st)>0:
                    df_st.to_sql('AISHE_CODES_MONTHLY_DATA_Temp_Push',index=False, if_exists='append', con=engine)
                    update_status_table(f"UPDATE AISHE_CODES_MONTHLY_DATA_STATUS_TABLE_Temp_Push set Col_Status='Done' where State='{st.title()}';")
                    print(df_st.info())
                time.sleep(10)
                
        
            else:
                print('Data Already Available')

        log.job_end_log(table_name,job_start_time, no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)


if(__name__=='__main__'):
    run_program(run_by='manual')
