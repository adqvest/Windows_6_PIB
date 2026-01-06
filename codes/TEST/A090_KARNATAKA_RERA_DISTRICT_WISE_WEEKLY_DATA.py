# -*- coding: utf-8 -*-
"""
Created on Fri Sep 17 14:30:44 2021

@author: Abhishek Shankar
"""



import sqlalchemy
import secrets
import pandas as pd
from pandas.io import sql
import os
import requests
import numpy as np
import json
import time
import calendar
from dateutil import parser
from bs4 import BeautifulSoup
import re
import datetime as datetime
from pytz import timezone
import sys
import warnings
warnings.filterwarnings('ignore')
#sys.path.insert(0, '/home/ubuntu/AdQvestDir/Adqvest_Function')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
#sys.path.insert(0, r'C:\Adqvest')
import adqvest_db
import JobLogNew as log
from adqvest_robotstxt import Robots
robot = Robots(__file__)

os.chdir('C:/Users/Administrator/AdQvestDir/')
engine = adqvest_db.db_conn()
#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days

## job log details
job_start_time = datetime.datetime.now(india_time)
table_name = 'KARNATAKA_RERA_DISTRICT_WISE_WEEKLY_DATA_2_TABLES'
no_of_ping = 0


def run_program(run_by='Adqvest_Bot', py_file_name=None):
    global no_of_ping
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''


    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)


        engine = adqvest_db.db_conn()
        db_max_rel_date = pd.read_sql("select max(Relevant_Date) as Max from KARNATAKA_RERA_DISTRICT_WISE_WEEKLY_DATA_PIT",engine)
        db_max_rel_date = db_max_rel_date["Max"][0]

        if (today.date() - db_max_rel_date).days  >= 7:
            #requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = 'ALL:@SECLEVEL=1'
            # proxy_host = "proxy.crawlera.com"
            # proxy_port = "8011"
            # proxy_auth = "5c2fcd5a03ad47a8b87f3cc83450c4a7" # Make sure to include ':' at the end
            # proxies = {"https": "https://{}@{}:{}/".format(proxy_auth, proxy_host, proxy_port),
            #       "https": "https://{}@{}:{}/".format(proxy_auth, proxy_host, proxy_port)}
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"}

            url1 = 'https://rera.karnataka.gov.in/viewAllProjects'
            r = requests.get(url1,verify=False,headers=headers, timeout =120)
            robot.add_link(url1)

            soup = BeautifulSoup(r.text,'html')


            final_df = pd.DataFrame()

            district = soup.find("select",{"name":"district"}).find_all("option")
            district_code = [x['value'] for x in district if x['value'] != '0']
            district = [x.text for x in district if x['value'] != '0']

            for dname in district_code:
                time.sleep(1.5)
                url2 = 'https://rera.karnataka.gov.in/projectViewDetails'

                #dname = secrets.choice(district_code)

                pdata1 = {
                         "project": "",
                        "firm": "",
                        "appNo": "",
                        "regNo": "",
                        "district": dname,
                        "btn1": "Search"

                        }
                try:
                  r1 = requests.post(url2,data = pdata1,headers=headers,verify=False, timeout = 60)
                  robot.add_link(url2)
                  a = pd.read_html(r1.text)[0]
                except:
                  time.sleep(60)
                  try:
                    r1 = requests.post(url2,data = pdata1,headers=headers,verify=False, timeout = 60)
                    robot.add_link(url2)
                    a = pd.read_html(r1.text)[0]
                  except:
                    a = pd.DataFrame()
                    print(f'No tables Found for district:{dname}')
                

                if len(a) != 0:
                    soup1 = BeautifulSoup(r1.text,'html')
                    # Added | Pushkar | 8 Jun 2023
                    try:
                        project_id = soup1.find_all("a", {"onclick": "return showFileComplaintPreview(this);"})[0:len(a)]
                        project_id = [x['id'] for x in project_id]
                        a['Project_Id'] = project_id

                    except:
                        project_id = []
                        for i in soup1.find_all("tr"):
                            if i.findAll('td') != []:
                                if i.findAll('td')[-1].find('a')!= None:
                                    project_id.append(i.findAll('td')[-1].find('a')['id'])
                                else:
                                    project_id.append(np.nan)
                                    # break
                        a['Project_Id'] = project_id
                    # END | 8 Jun 2023

                    # print(dname)
                    for i in range(len(project_id)):
                        pid = project_id[i]
                        if type(pid) != float:
                            time.sleep(1.5)
                            url3 = 'https://rera.karnataka.gov.in/projectDetails'
                            # random1 = secrets.choice(project_id)
                            row = a[a['Project_Id'] == pid]
                            print(a[a['Project_Id'] == pid].iloc[:, [4, 5, 7]])
                            try:
                                r2 = requests.post(url3, data={"action": pid}, verify=False, timeout = 60)
                            except:
                                time.sleep(60)
                                try:
                                    r2 = requests.post(url3, data={"action": pid}, verify=False, timeout = 60)
                                except:
                                    continue

                            soup2 = BeautifulSoup(r2.text, 'html')

                            details = soup2.find_all("div", {"class": "row"})
                            details = [x.find_all("p") for x in details]
                            reqd = []
                            for stuff in details:
                                if len(stuff) == 5:
                                    c = {stuff[0].text: stuff[1].text, stuff[3].text: stuff[4].text}
                                    if (("inventory" in stuff[0].text.lower()) | ("inventory" in stuff[4].text.lower())):
                                        reqd.append(c)
                                elif len(stuff) == 4:
                                    c = {stuff[0].text: stuff[1].text, stuff[2].text: stuff[3].text}
                                    if (("inventory" in stuff[0].text.lower()) | ("inventory" in stuff[2].text.lower())):
                                        reqd.append(c)
                                elif len(stuff) == 2:
                                    c = {stuff[0].text: stuff[1].text}
                                    if (("inventory" in stuff[0].text.lower())):
                                        reqd.append(c)
                                else:
                                    print(len(stuff))

                            totals = []
                            if reqd != []:
                                for vals in reqd:
                                    for k, value in vals.items():
                                        if "no of inventory" in k.lower():
                                            totals.append(value)
                            if totals != []:
                                totals = sum(list(set([float(x) for x in totals])))
                            else:
                                totals = None

                            row['Total_Inventory'] = totals
                            row.columns = [x.replace(" ", "_").title() for x in list(row.columns)]
                            row = row.dropna(how='all', axis=1)
                            row['Relevant_Date'] = today.date()
                            row['Runtime'] = datetime.datetime.now()
                            final_df = pd.concat([final_df, row])
                            # row.to_sql(name='KARNATAKA_RERA_DISTRICT_WISE',con=engine,if_exists='append',index=False)
                            print(totals)
                        else:
                            row = a.iloc[i:i+1,:]
                            print(a.iloc[i:i+1,:])
                            row['Total_Inventory'] = np.nan
                            row.columns = [x.replace(" ", "_").title() for x in list(row.columns)]
                            row = row.dropna(how='all', axis=1)
                            row['Relevant_Date'] = today.date()
                            row['Runtime'] = datetime.datetime.now()
                            final_df = pd.concat([final_df, row])
                else:
                    print(f'Data unavailable for {dname.upper()}')
            print(final_df)
            
            # Added | Pushkar | 17 May 2023
            final_df['Covid-19_Extension_Date_1'] = [i[0] if type(i) != float else i for i in final_df['Covid-19_Extension_Date'].str.split()]
            final_df['Covid-19_Extension_Date_2'] = [i[-1] if type(i) != float else i for i in final_df['Covid-19_Extension_Date'].str.split()]
            final_df['Covid-19_Extension_Date_2'] = np.where(final_df['Covid-19_Extension_Date_2'] == '-', None, final_df['Covid-19_Extension_Date_2'])

            for col in ['Approved_On', 'Completion_Date', 'Covid-19_Extension_Date_1', 'Covid-19_Extension_Date_2']:
                if col in final_df.columns:
                    final_df[col] = np.where(final_df[col].isna(), None, final_df[col])
                    final_df[col] = [parser.parse(x).date() if x != None else None for x in final_df[col]]

            final_df = final_df.drop(columns = ['Covid-19_Extension_Date', 'Section_6_Extension_Date'])
            final_df.to_excel('rera_karnataka.xlsx')

            # end #

            if(final_df.shape[0]==0):
                print("No new data")
            else:
                engine = adqvest_db.db_conn()
                final_df.to_sql(name='KARNATAKA_RERA_DISTRICT_WISE_WEEKLY_DATA_NO_PIT',con=engine,if_exists='append',index=False)
            try:
                query = 'Select max(Relevant_Date) as RD from AdqvestDB.KARNATAKA_RERA_DISTRICT_WISE_WEEKLY_DATA_PIT'
                RD = pd.read_sql(query,con=engine)['RD'].iloc[0]
                query1 = "Delete from AdqvestDB.KARNATAKA_RERA_DISTRICT_WISE_WEEKLY_DATA_PIT where Relevant_Date = '"+str(RD)+"';"
                engine = adqvest_db.db_conn()
                final_df.to_sql(name='KARNATAKA_RERA_DISTRICT_WISE_WEEKLY_DATA_PIT',con=engine,if_exists='append',index=False)
            except:
                engine = adqvest_db.db_conn()
                final_df.to_sql(name='KARNATAKA_RERA_DISTRICT_WISE_WEEKLY_DATA_PIT',con=engine,if_exists='append',index=False)
        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
