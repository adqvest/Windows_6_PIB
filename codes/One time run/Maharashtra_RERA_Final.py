# -*- coding: utf-8 -*-
"""
Created on Thu Nov 11 14:57:31 2021

@author: Abhishek Shankar
"""



# -*- coding: utf-8 -*-
"""
Created on Thu Nov 11 13:26:33 2021

@author: Abhishek Shankar
"""


import sqlalchemy
import pandas as pd
from pandas.io import sql
import os
from dateutil import parser
import requests
from bs4 import BeautifulSoup
import re
import datetime as datetime
from pytz import timezone
import sys
import warnings
import json
import time
warnings.filterwarnings('ignore')
import numpy as np
from selenium import webdriver
from selenium.webdriver.support.select import Select
import pandas as pd
from dateutil import parser
import datetime as datetime
from datetime import date
import timeit
import io
import numpy as np
from pytz import timezone
from selenium import webdriver
from selenium.webdriver.support.select import Select
import pandas as pd
from dateutil import parser
import datetime as datetime
from datetime import date
import timeit
import io
import numpy as np
from pytz import timezone
import time
import numpy as np
import re
import itertools
import requests
import sqlalchemy
from pandas.io import sql
import os
from bs4 import BeautifulSoup
from dateutil import parser
import sys
import warnings
warnings.filterwarnings('ignore')
import csv
import calendar
import pdb
import json
from dateutil import parser
warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function/')
#sys.path.insert(0, r'C:\Adqvest')
#import adqvest_db
import JobLogNew as log
import adqvest_db


#start_time = timeit.default_timer()
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday =  today - days

engine = adqvest_db.db_conn()
connection = engine.connect()


def tableDataText(table):
    """Parses a html segment started with tag <table> followed
    by multiple <tr> (table rows) and inner <td> (table data) tags.
    It returns a list of rows with inner columns.
    Accepts only one <th> (table header/data) in the first row.
    """
    def rowgetDataText(tr, coltag='td'): # td (data) or th (header)
        return [td.get_text(strip=True) for td in tr.find_all(coltag)]
    rows = []
    trs = table.find_all('tr')
    headerow = rowgetDataText(trs[0], 'th')
    if headerow: # if there is a header row include first
        rows.append(headerow)
        trs = trs[1:]
    for tr in trs: # for every table row
        rows.append(rowgetDataText(tr, 'td') ) # data row
    return rows

def tableDataText(table):
    """Parses a html segment started with tag <table> followed
    by multiple <tr> (table rows) and inner <td> (table data) tags.
    It returns a list of rows with inner columns.
    Accepts only one <th> (table header/data) in the first row.
    """
    def rowgetDataText(tr, coltag='td'): # td (data) or th (header)
        return [td.get_text(strip=True) for td in tr.find_all(coltag)]
    rows = []
    trs = table.find_all('tr')
    headerow = rowgetDataText(trs[0], 'th')
    if headerow: # if there is a header row include first
        rows.append(headerow)
        trs = trs[1:]
    for tr in trs: # for every table row
        rows.append(rowgetDataText(tr, 'td') ) # data row
    return rows

def get_l2(link,headers):

  from statistics import mean
  url = link#"http://rerait.telangana.gov.in/PrintPreview/PrintPreview?q=w9SYNyyxyASIO4zt3wiHRa5QdKQNN00TpoY%2b89X4DgLIPQ9wuWqh45rxGQy6jB22Ss%2b0eDU4s5Ys3XbzivbHxjIWKacsXXpm70ll1gpuncb6OxrXHqzUEKiQbC7cfgM7%2f6dl0inim41EyS6XCD1ODbk1kWlT1gA7VB5w9IHXuZ4%3d"
  try:
    r = requests.get(url,headers=headers,verify=False)
    soup = BeautifulSoup(r.text,'lxml')
  except:
    time.sleep(5)
    r = requests.get(url,headers=headers,verify=False,timeout=10)
    soup = BeautifulSoup(r.text,'lxml')
  try:
    district = soup.select('label:contains(District)')[0].find_next('div').text.strip()
  except:
    district = None
  print(district)
  try:
    df = pd.read_html(r.text)

    df1 = [x for x in df if "number of apartment" in [y.lower() for y in x.columns]]
    if df1 != []:
       df1 = df1[0]
       try:
         total_apt = sum(df1[[x for x in df1.columns if x.lower()=='number of apartment'][0]])
       except:
         total_apt = None
       try:
         total_floors = len(set(df1[[x for x in df1.columns if 'floor' in x.lower()][0]]))
       except:
         total_floors = None
       try:
         total_booked = sum(df1[[x for x in df1.columns if 'booked' in x.lower()][0]])
       except:
         total_booked = None
       try:
          total_area = sum(df1[[x for x in df1.columns if 'area' in x.lower()][0]])
       except:
        total_area = None
    else:
       total_apt = None
       total_floors = None
       total_booked = None
       total_area = None
    df2 = [x for x in df if "Percentage of Work" in [y for y in x.columns]]

    if df2!=[]:
       df2 = df2[0]
       percentage_work = mean(df2['Percentage of Work'])

    else:
       percentage_work = None

    df = pd.DataFrame({"Total_Apt":total_apt,
                       "Total_Floors":total_floors,
                       "Total_Booked":total_booked,
                       "Percentage_Completed":percentage_work,
                       "Total_Area_In_Sqmts":total_area,
                       "Total_Area_In_Sqft":total_area*10.7639,
                       "District":district},index=[0])

  except:
    df =  pd.DataFrame({"Total_Apt":None,
                       "Total_Floors":None,
                       "Total_Booked":None,
                       "Percentage_Completed":None,
                       "Total_Area_In_Sqmts":None,
                       "Total_Area_In_Sqft":None,
                       "District":None},index=[0])


  return df

def get_date(x):

  try:
    return parser.parse(x).date()
  except:
    return None


def run_program(run_by='Adqvest_Bot', py_file_name=None):
#    os.chdir('/home/ubuntu/AdQvestDir')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'MAHARASHTRA_RERA_DISTRICT_WISE_PIT'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

    #%%
        url = 'https://maharerait.mahaonline.gov.in/SearchList/Search'
        headers = {
           "user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"
                   }
        #url = 'http://rerait.telangana.gov.in/SearchList/Search'
        headers['dnt']= '1'
        headers['referer'] = url
        r = requests.get(url,headers=headers,verify=False,timeout=10)
        print(r)
        soup = BeautifulSoup(r.text,'lxml')
        req_token = soup.find("input",{"name":'__RequestVerificationToken'})['value']
        cookies = '; '.join([x.name + '=' + x.value for x in r.cookies])
        headers['cookie'] = cookies
        headers['Content-Type']= 'application/json'
        headers['dnt']= '1'
        headers['referer'] = url
        req_token = soup.find("input",{"name":'__RequestVerificationToken'})['value']
        pmt = soup.find_all("select",{"id":"PType"})[0].find_all("option")

        pmt = [x for x in pmt if x.text.lower()=='residential']

        type_id = [x['value'] for x in pmt if "select" not in x.text.lower()]
        type_name = [x.text for x in pmt if "select" not in x.text.lower()]

        for id1,name in zip(type_id,type_name):
          print(name)
          headers = {
           "user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
           "Connection": "keep-alive"
                   }

          headers['cookie'] = cookies
          headers['Connection']= 'keep-alive'
          headers['dnt']= '1'
          #headers['Content-Length']= '478'
          headers['Content-Type'] = 'application/x-www-form-urlencoded'
          headers['Referer'] = url
          #req_token = soup.select('input[name=__RequestVerificationToken')[0]['value']__RequestVerificationToken: guuGHkqt58BnoxYh7-HXIl3SJlBZ_3Us3q-vrv-xPviztc6g28Muoa1qJeF8meprUTzPrQGEJeOWHwIyXObBW0upV6C7OquFtNYaKWQTpRs1
          data = {
          "__RequestVerificationToken": req_token,
          "Type": "Promoter",
          "ID": "0",
          "pageTraverse": "1",
          "Project": "",
          "hdnProject": "",
          'Promoter': "",
          "hdnPromoter": "",
          "AgentName": "",
          "hdnAgent": "",
          "CertiNo": "",
          "hdnCertiNo": "",
          "State": "",
          "hdnDivision": '',
          "hdnDistrict": "",
          "hdnProject": "",
          "hdnDTaluka": "",
          "hdnVillage": '',
          "hdnState": "",
          "District": "",
          "hdnState": "",
          "PinCode": "",
          "hdnPincode": "",
          "CompletionDate_From": "",
          'hdnfromdate': "",
          "CompletionDate_To": "",
          "hdntodate": "",
          "PType": str(id1),
          "hdnPType": str(id1),
          "btnSearch": "Search",
          }


          try:
            url = 'https://maharerait.mahaonline.gov.in/SearchList/Search'
            r1 = requests.post(url,headers=headers,data=data,verify=False)
            print(r1)
          except:
            time.sleep(5)
            url = 'https://maharerait.mahaonline.gov.in/SearchList/Search'
            r1 = requests.post(url,headers=headers,data=data,verify=False,timeout=10)
            print(r1)
          try:
            df = pd.read_html(r1.text)
            soup = BeautifulSoup(r1 .text,'lxml')
            total = soup.find("input",{"id":'TotalRecords'})['value']

            total_pages = soup.find("input",{"id":'TotalPages'})['value']
            current_page = soup.find("input",{"id":'CurrentPage'})['value']
            print("################GOT IT############")
          except:
            print("################SORRY############")

          req_token = soup.find("input",{"name":'__RequestVerificationToken'})['value']

          output = pd.DataFrame()
          #for dt,dt_code in zip(district[0:2],district_code[0:2]):
          #  print(dt,dt_code)
          for i in range(0,int(total_pages)):#total_pages



              data2 = {
               "__RequestVerificationToken": req_token,
              "Type": "Promoter",
              "ID": "0",
              "pageTraverse": "1",
              "Project": '',
              'hdnProject': '',
              'Promoter': '',
              "hdnPromoter": '',
              "AgentName": "",
              "hdnAgent": "",
              'CertiNo': "",
              "hdnCertiNo": "",
              "State": "",
              "hdnDivision": "",
              "hdnDistrict": '',
              "hdnProject": '',
              "hdnDTaluka": "",
              "hdnVillage": "",
              "hdnState": '',
              "District": "",
              "hdnState": "",
              "PinCode": "",
              "hdnPincode": "",
              "CompletionDate_From": "",
              "hdnfromdate": "",
              "CompletionDate_To": "",
              "hdntodate": "",
              "PType": str(id1),
              "hdnPType": str(id1),
              'TotalRecords': total,
              "CurrentPage":str(i),
              "TotalPages": total_pages,
              "Command": "Next",
               }

              headers['cookie'] = cookies
              headers['Referer'] = url
              try:
                url = 'https://maharerait.mahaonline.gov.in/SearchList/Search'
                r1 = requests.post(url,headers=headers,data=data2,verify=False)
                print(r1)
              except:
                time.sleep(5)
                url = 'https://maharerait.mahaonline.gov.in/SearchList/Search'
                r1 = requests.post(url,headers=headers,data=data2,verify=False,timeout=10)
                print(r1)
              try:
                soup = BeautifulSoup(r1 .text,'lxml')
                try:
                  next1 = soup.find("button",{"id":"btnNext"})['value']
                  if next1==None:
                     break
                except:
                  break
                df = pd.read_html(r1.text)
                if df==[]:
                  continue
                df = df[0]
                soup = BeautifulSoup(r1 .text,'lxml')

                total = soup.find("input",{"id":'TotalRecords'})['value']
                total_pages = soup.find("input",{"id":'TotalPages'})['value']
                current_page = soup.find("input",{"id":'CurrentPage'})['value']
                print("################GOT IT############")
              except:
                break
                print("################SORRY############")

              soup = BeautifulSoup(r1.text,'lxml')
            #  req = soup.find("input",{"name":'__RequestVerificationToken'})['value']
              table = soup.find('table')
              links = []
              for tr in table.findAll("tr"):
                  trs = tr.findAll("td")
                  for each in trs:
                      try:
                          link = each.find('a')['href']
                          links.append("https://maharerait.mahaonline.gov.in"+link)
                      except:
                          pass


              df.columns = [x.title().replace(".","").replace(" ","_") for x in df.columns]
              df = df.dropna(how='all',axis=1)
              df['View_Details'] = links
              headers = {
               "user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
               "Connection": "keep-alive"
                       }
              headers['cookie'] = '; '.join([x.name + '=' + x.value for x in r1.cookies])
              sub = pd.DataFrame()

              for l,row  in df.iterrows():
                time.sleep(1.5)
                url = row['View_Details']
                try:
                  l2 = get_l2(url,headers)
                except:
                  time.sleep(5)
                  l2 = get_l2(url,headers)

                row = row.to_frame().T

                row = pd.concat([row.reset_index(drop=True),l2],axis=1)

                sub = pd.concat([sub,row])


              df = sub


              req_token = soup.find("input",{"name":'__RequestVerificationToken'})['value']
              print(df.iloc[:,0])
              df['Last_Modified_Date'] = df['Last_Modified_Date'].apply(lambda x : get_date(x))

          #    df['District_2'] = dt
              df['Type'] = name#'Residential'
              df['Relevant_Date'] = today.date()
              df['Runtime'] = datetime.datetime.now()
#              output = pd.concat([output,df])

                  #output['Relevant_Date'] = today.date()
                  #output['Runtime'] = datetime.datetime.now()
              df = df[[x for x in df.columns if x != 'View_Details']]
              #df = df[[x for x in df.columns if x != 'Sr_No']]

              df.to_sql(name='MAHARASHTRA_RERA_DISTRICT_WISE_PIT_V2',con=engine,if_exists='append',index=False)
              print("Uploaded")
        #  output = pd.concat([output,df])

        #output['Relevant_Date'] = today.date()
        #output['Runtime'] = datetime.datetime.now()
        log.job_end_log(table_name,job_start_time,no_of_ping)

    except:
        try:
            connection.close()
        except:
            pass
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])

        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
