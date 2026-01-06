# -*- coding: utf-8 -*-
"""
Created on Mon Jan 17 13:06:40 2022

@author: Abhishek Shankar
"""


import sqlalchemy
import secrets
import pandas as pd
from pandas.io import sql
import os
import requests
from bs4 import BeautifulSoup
import re
import datetime as datetime
from pytz import timezone
import sys
from dateutil import parser
import warnings
warnings.filterwarnings('ignore')
#sys.path.insert(0, '/home/ubuntu/AdQvestDir/Adqvest_Function')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
#sys.path.insert(0, r'C:\Adqvest')
import adqvest_db
import JobLogNew as log
import numpy as np
import json
import time
import calendar

def get_date(x):
  try:
    return parser.parse(x).date()
  except:
    return None
#os.chdir('C:/Users/Administrator/AdQvestDir/')
engine = adqvest_db.db_conn()
#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days

## job log details
job_start_time = datetime.datetime.now(india_time)
table_name = 'KARNATAKA_RERA_DISTRICT_WISE'
no_of_ping = 0

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    #os.chdir('/home/ubuntu/AdQvestDir')
    engine = adqvest_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name =  'KARNATAKA_RERA_DISTRICT_WISE'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0
    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"}

        url1 = 'https://rera.karnataka.gov.in/viewAllProjects'
        r = requests.get(url1,verify=False,headers=headers)
        soup = BeautifulSoup(r.text,'html')


        #        final_df = pd.DataFrame()

        district = soup.find("select",{"name":"district"}).find_all("option")
        district_code = [x['value'] for x in district if x['value'] != '0']
        district = [x.text for x in district if x['value'] != '0']

        #district_code = ['Bengaluru Urban', 'Bengaluru  Rural']
        for dname in district_code:
            print(dname)
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
              r1 = requests.post(url2,data = pdata1,headers=headers,verify=False)
            except:
              time.sleep(60)
              try:
                r1 = requests.post(url2,data = pdata1,headers=headers,verify=False)
              except:
                continue
            a = pd.read_html(r1.text)[0]

            soup1 = BeautifulSoup(r1.text,'html')
        #    project_id = soup1.find_all("a",{"title":"View Project Details"})#[0:len(a)]
        #    project_id = [x['id'] for x in project_id]
            col = []
            for row in soup1.findAll('table')[0].tbody.findAll('tr'):
            #    first_column = row.findAll('th')[0].contents
                third_column = row.findAll('td')[3].contents
                print (third_column)
                col.append(third_column)
            project_id = []
            for vals in col:
              try:
                print(vals[0].find("a")['id'])
                project_id.append(vals[0].find("a")['id'])
              except:
                print("NONE")
                project_id.append(None)
            a['Project_Id'] = project_id
            project_id = a['Project_Id']
            if a.empty:
              continue
        #      print(b)

            for pid in project_id:
                print(pid)
                if pid==None:
                  continue
                time.sleep(1.5)
        #        datan = {"project": "Sobha City Athena",
        #        "firm": "",
        #        "appNo": "",
        #        "regNo": "",
        #        "district": "0",
        #        "btn1": "Search"}
                url3 = 'https://rera.karnataka.gov.in/projectDetails'

        #        url3 = 'https://rera.karnataka.gov.in/projectViewDetails'

                #random1 = secrets.choice(project_id)
                row = a[a['Project_Id']==pid]
                print(a[a['Project_Id']==pid].iloc[:,[4,5,7]])
                row[[x for x in row.columns if 'approved' in x.lower()][0]] = row[[x for x in row.columns if 'approved' in x.lower()][0]].apply(lambda x : get_date(x))

                row[[x for x in row.columns if 'date' in x.lower()][0]] = row[[x for x in row.columns if 'date' in x.lower()][0]].apply(lambda x : get_date(x))
                headers['Referer'] = 'https://rera.karnataka.gov.in/projectViewDetails'
                try:
                  r2 = requests.post(url3,data = {"action": str(pid)},verify=False,headers=headers)
                except:
                  time.sleep(60)
                  try:
                    r2 = requests.post(url3,data = {"action": str(pid)},verify=False)
                  except:
                    continue

                soup2 = BeautifulSoup(r2.text,'html')

                #Inventory

                details = soup2.find_all("div",{"class":"row"})
                details = [x.find_all("p") for x in details]
                reqd = []
                for stuff in details:
                    if len(stuff)==5:
                      c = {stuff[0].text:stuff[1].text,stuff[3].text:stuff[4].text}
                      if(("inventory" in stuff[0].text.lower()) | ("inventory" in stuff[4].text.lower())):
                            reqd.append(c)
                    elif len(stuff)==4:
                      c = {stuff[0].text:stuff[1].text,stuff[2].text:stuff[3].text}
                      if(("inventory" in stuff[0].text.lower()) | ("inventory" in stuff[2].text.lower())):
                          reqd.append(c)
                    elif len(stuff)==2:
                      c = {stuff[0].text:stuff[1].text}
                      if(("inventory" in stuff[0].text.lower())):
                          reqd.append(c)
                    else:
                      print(len(stuff))

                totals = []
                if reqd != []:
                  for vals in reqd:
                    for k,value in vals.items():
                      if "no of inventory" in k.lower():
                          totals.append(value)
                if totals != []:
                  totals = sum(list(set([float(x) for x in totals])))
                else:
                  totals = None

                #PROJECT DATES
                reqd = []
                for stuff in details:
                    if len(stuff)==5:
                      c = {stuff[0].text:stuff[1].text,stuff[3].text:stuff[4].text}
                      if(("project start date" in stuff[0].text.lower()) | ("project start date" in stuff[4].text.lower())):
                            reqd.append(c)
                    elif len(stuff)==4:
                      c = {stuff[0].text:stuff[1].text,stuff[2].text:stuff[3].text}
                      if(("project start date" in stuff[0].text.lower()) | ("project start date" in stuff[2].text.lower())):
                          reqd.append(c)
                    elif len(stuff)==2:
                      c = {stuff[0].text:stuff[1].text}
                      if(("project start date" in stuff[0].text.lower())):
                          reqd.append(c)
                    else:
                      print(len(stuff))

                if reqd != []:
                  reqd = reqd[0]
                  reqd = {key.strip().replace(" ","_").replace(":",""): parser.parse(value.strip()).date() for key, value in reqd.items()}
                  if "Project_Start_Date" not in reqd.keys():
                      reqd['Project_Start_Date'] = None
                  elif "Project_End_Date" not in reqd.keys():
                      reqd['Project_End_Date'] = None
                else:
                  reqd = {"Project_Start_Date":None,"Project_End_Date":None}


                pgt_dates = reqd

                #LAND AREA

                reqd = []
                for stuff in details:
                    if len(stuff)==5:
                      c = {stuff[0].text:stuff[1].text,stuff[3].text:stuff[4].text}
                      if(("total area of land" in stuff[0].text.lower()) | ("total area of land" in stuff[4].text.lower())):
                            reqd.append(c)
                    elif len(stuff)==4:
                      c = {stuff[0].text:stuff[1].text,stuff[2].text:stuff[3].text}
                      if(("total area of land" in stuff[0].text.lower()) | ("total area of land" in stuff[2].text.lower())):
                          reqd.append(c)
                    elif len(stuff)==2:
                      c = {stuff[0].text:stuff[1].text}
                      if(("total area of land" in stuff[0].text.lower())):
                          reqd.append(c)
                    else:
                      print(len(stuff))

                if reqd != []:
                  reqd = reqd[0]
                  reqd = {k:v for k,v in reqd.items() if "total area of land" in k.lower()}
                  reqd = {k.strip().replace(" ","_").replace(":","").replace("(","").replace(")",""):float(v) for k,v in reqd.items()}
                else:
                  reqd = {"Total_Area_Of_Land_Sq_Mtr":None}

                land_area = reqd

                #CONSTRUCTION COST
                reqd = []
                for stuff in details:
                    if len(stuff)==5:
                      c = {stuff[0].text:stuff[1].text,stuff[3].text:stuff[4].text}
                      if(("estimated cost of construction" in stuff[0].text.lower()) | ("estimated cost of construction" in stuff[4].text.lower())):
                            reqd.append(c)
                    elif len(stuff)==4:
                      c = {stuff[0].text:stuff[1].text,stuff[2].text:stuff[3].text}
                      if(("estimated cost of construction" in stuff[0].text.lower()) | ("estimated cost of construction" in stuff[2].text.lower())):
                          reqd.append(c)
                    elif len(stuff)==2:
                      c = {stuff[0].text:stuff[1].text}
                      if(("estimated cost of construction" in stuff[0].text.lower())):
                          reqd.append(c)
                    else:
                      print(len(stuff))

                if reqd != []:
                  reqd = reqd[0]
                  reqd = {k:v for k,v in reqd.items() if "estimated cost of construction" in k.lower()}
                  reqd = {k.strip().replace(" :","").replace(" ","_").strip().replace("(","").replace(")",""):float(v) for k,v in reqd.items()}
                else:

                  reqd = {"Estimated_Cost_of_Construction_INR":None}

                const_cost = reqd

                #LAND COST
                reqd = []
                for stuff in details:
                    if len(stuff)==5:
                      c = {stuff[0].text:stuff[1].text,stuff[3].text:stuff[4].text}
                      if(("cost of land" in stuff[0].text.lower()) | ("cost of land" in stuff[4].text.lower())):
                            reqd.append(c)
                    elif len(stuff)==4:
                      c = {stuff[0].text:stuff[1].text,stuff[2].text:stuff[3].text}
                      if(("cost of land" in stuff[0].text.lower()) | ("cost of land" in stuff[2].text.lower())):
                          reqd.append(c)
                    elif len(stuff)==2:
                      c = {stuff[0].text:stuff[1].text}
                      if(("cost of land" in stuff[0].text.lower())):
                          reqd.append(c)
                    else:
                      print(len(stuff))

                if reqd != []:
                  reqd = reqd[0]
                  reqd = {k:v for k,v in reqd.items() if "cost of land" in k.lower()}
                  reqd = {k.strip().replace(":","").replace(" ","_").strip().replace("(","").replace(")",""):float(v) for k,v in reqd.items()}
                else:

                  reqd = {"Cost_of_Land_INR":None}

                land_cost = reqd

                #DEVELOPMENT
                reqd = []
                for stuff in details:
                    if len(stuff)==5:
                      c = {stuff[0].text:stuff[1].text,stuff[3].text:stuff[4].text}
                      if(("extent of development carried" in stuff[0].text.lower()) | ("extent of development carried" in stuff[4].text.lower())):
                            reqd.append(c)
                    elif len(stuff)==4:
                      c = {stuff[0].text:stuff[1].text,stuff[2].text:stuff[3].text}
                      if(("extent of development carried" in stuff[0].text.lower()) | ("extent of development carried" in stuff[2].text.lower())):
                          reqd.append(c)
                    elif len(stuff)==2:
                      c = {stuff[0].text:stuff[1].text}
                      if(("extent of development carried" in stuff[0].text.lower())):
                          reqd.append(c)
                    else:
                      print(len(stuff))

                if reqd != []:
                  reqd = reqd[0]
                  reqd = {k:v for k,v in reqd.items() if "extent of development carried" in k.lower()}
                  try:
                    reqd = {k.strip().replace(":","").title().replace(" ","_").strip().replace("(","").replace(")","")+"_percent".title():float(re.findall(r'\d+', v)[0]) for k,v in reqd.items()}
                  except:
                    reqd = {k.strip().replace(":","").title().replace(" ","_").strip().replace("(","").replace(")","")+"_percent".title():None for k,v in reqd.items()}

                else:

                  reqd = {"Extent_Of_Development_Carried_Till_Date_Percent":None}

                pct_development = reqd

                #MONEY COLLECTED FROM ALLOTEEE
                reqd = []
                for stuff in details:
                    if len(stuff)==5:
                      c = {stuff[0].text:stuff[1].text,stuff[3].text:stuff[4].text}
                      if(("total amount of money collected from allottee" in stuff[0].text.lower()) | ("total amount of money collected from allottee" in stuff[4].text.lower())):
                            reqd.append(c)
                    elif len(stuff)==4:
                      c = {stuff[0].text:stuff[1].text,stuff[2].text:stuff[3].text}
                      if(("total amount of money collected from allottee" in stuff[0].text.lower()) | ("total amount of money collected from allottee" in stuff[2].text.lower())):
                          reqd.append(c)
                    elif len(stuff)==2:
                      c = {stuff[0].text:stuff[1].text}
                      if(("total amount of money collected from allottee" in stuff[0].text.lower())):
                          reqd.append(c)
                    else:
                      print(len(stuff))

                if reqd != []:
                  reqd = reqd[0]
                  reqd = {k:v for k,v in reqd.items() if "total amount of money collected from allottee" in k.lower()}
                  try:
                    reqd = {k.strip().replace(":","").title().replace(" ","_").strip().replace("(","").replace(")","")+"_INR":float(re.findall(r'\d+', v)[0]) for k,v in reqd.items()}
                  except:
                    reqd = {k.strip().replace(":","").title().replace(" ","_").strip().replace("(","").replace(")","")+"_INR":None for k,v in reqd.items()}

                else:

                  reqd = {"Total_Amount_Of_Money_Collected_From_Allottee_INR":None}


                money_collected = reqd

                #TOTAL AMOUNT OF MONEY USED FROM DEVELOPMENT OF PROJECT
                reqd = []
                for stuff in details:
                    if len(stuff)==5:
                      c = {stuff[0].text:stuff[1].text,stuff[3].text:stuff[4].text}
                      if(("total amount of money used from development of project" in stuff[0].text.lower()) | ("total amount of money used from development of project" in stuff[4].text.lower())):
                            reqd.append(c)
                    elif len(stuff)==4:
                      c = {stuff[0].text:stuff[1].text,stuff[2].text:stuff[3].text}
                      if(("total amount of money used from development of project" in stuff[0].text.lower()) | ("total amount of money used from development of project" in stuff[2].text.lower())):
                          reqd.append(c)
                    elif len(stuff)==2:
                      c = {stuff[0].text:stuff[1].text}
                      if(("total amount of money used from development of project" in stuff[0].text.lower())):
                          reqd.append(c)
                    else:
                      print(len(stuff))

                if reqd != []:
                  reqd = reqd[0]
                  reqd = {k:v for k,v in reqd.items() if "total amount of money used from development of project" in k.lower()}
                  try:
                    reqd = {k.strip().replace(":","").title().replace(" ","_").strip().replace("(","").replace(")","")+"_INR":float(re.findall(r'\d+', v)[0]) for k,v in reqd.items()}
                  except:
                    reqd = {k.strip().replace(":","").title().replace(" ","_").strip().replace("(","").replace(")","")+"_INR":None for k,v in reqd.items()}

                else:

                  reqd = {"Total_Amount_Of_Money_Used_From_Development_Of_Project_INR":None}


                money_used = reqd

                #CARPET AREA
                reqd = []
                for stuff in details:
                    if len(stuff)==5:
                      c = {stuff[0].text:stuff[1].text,stuff[3].text:stuff[4].text}
                      if(("carpet area" in stuff[0].text.lower()) | ("carpet area" in stuff[4].text.lower())):
                            reqd.append(c)
                    elif len(stuff)==4:
                      c = {stuff[0].text:stuff[1].text,stuff[2].text:stuff[3].text}
                      if(("carpet area" in stuff[0].text.lower()) | ("carpet area" in stuff[2].text.lower())):
                          reqd.append(c)
                    elif len(stuff)==2:
                      c = {stuff[0].text:stuff[1].text}
                      if(("carpet area" in stuff[0].text.lower())):
                          reqd.append(c)
                    else:
                      print(len(stuff))

                if reqd != []:
                  reqd = reqd[0]
                  reqd = {k:v for k,v in reqd.items() if "carpet area" in k.lower()}
                  try:
                    reqd = {k.strip().replace(":","").title().replace(" ","_").strip().replace("(","").replace(")",""):float(re.findall(r'\d+', v)[0]) for k,v in reqd.items()}
                  except:
                    reqd = {k.strip().replace(":","").title().replace(" ","_").strip().replace("(","").replace(")",""):None for k,v in reqd.items()}

                else:

                  reqd = {"Carpet_Area_Sq_Mtr":None}


                carpet_area = reqd


                #AREA OF EXCLUSIVE BALCONY/VERANDAH
                reqd = []
                for stuff in details:
                    if len(stuff)==5:
                      c = {stuff[0].text:stuff[1].text,stuff[3].text:stuff[4].text}
                      if(("area of exclusive" in stuff[0].text.lower()) | ("area of exclusive" in stuff[4].text.lower())):
                            reqd.append(c)
                    elif len(stuff)==4:
                      c = {stuff[0].text:stuff[1].text,stuff[2].text:stuff[3].text}
                      if(("area of exclusive" in stuff[0].text.lower()) | ("area of exclusive" in stuff[2].text.lower())):
                          reqd.append(c)
                    elif len(stuff)==2:
                      c = {stuff[0].text:stuff[1].text}
                      if(("area of exclusive" in stuff[0].text.lower())):
                          reqd.append(c)
                    else:
                      print(len(stuff))

                if reqd != []:
                  reqd = reqd[0]
                  reqd = {k:v for k,v in reqd.items() if "area of exclusive" in k.lower()}
                  try:
                    reqd = {k.strip().replace(":","").title().replace("/"," ").replace(" ","_").strip().replace("(","").replace(")",""):float(re.findall(r'\d+', v)[0]) for k,v in reqd.items()}
                  except:
                    reqd = {k.strip().replace(":","").title().replace("/"," ").replace(" ","_").strip().replace("(","").replace(")",""):None for k,v in reqd.items()}

                else:

                  reqd = {"Area_Of_Exclusive_Balcony_Verandah_Sq_Mtr":None}


                excl_area = reqd


                inventory = {"Total_Inventory":totals}


                merge = {**inventory, **excl_area, **carpet_area,**money_used,**money_collected,**pct_development,**land_cost,**pgt_dates,**land_area,**const_cost}


        #        row['Total_Inventory'] = totals
                row1 = pd.DataFrame(merge,index=[0])
                row = row.reset_index(drop=True)
                row = pd.concat([row,row1],axis=1)
                row.columns = [x.replace(" ","_").title() for x in list( row.columns)]
        #        row = row.dropna(how='all',axis=1)


                row['Relevant_Date']=today.date()
                row['Runtime']=datetime.datetime.now()
                try:
                    row.to_sql(name='KARNATAKA_RERA_DISTRICT_WISE_WEEKLY_DATA_TEMP',con=engine,if_exists='append',index=False)
                    print("Data Uploaded")
                except:
                    continue
        #        op = pd.concat([row,op])
        log.job_end_log(table_name,job_start_time, no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
