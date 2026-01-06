# -*- coding: utf-8 -*-
"""
Created on Fri Dec 10 10:57:00 2021

@author: Abhishek Shankar
"""


import sqlalchemy
import pandas as pd
from pandas.io import sql
import os
from dateutil import parser
import requests
from bs4 import BeautifulSoup
import urllib
import re
import datetime as datetime
from pytz import timezone
import sys
import warnings
import json
import time
warnings.filterwarnings('ignore')
import numpy as np
import time
warnings.filterwarnings('ignore')
#sys.path.insert(0, '/home/ubuntu/AdQvestDir/Adqvest_Function')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
#sys.path.insert(0, r'C:\Adqvest')
import numpy as np
import adqvest_db
from selenium.webdriver.support.ui import Select

from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import JobLogNew as log
requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = 'ALL:@SECLEVEL=1'
from csv import reader
import pandas as pd
engine = adqvest_db.db_conn()
#import datetime
d = datetime.datetime.now()

# code you want to evaluate
#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days



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


def run_program(run_by='Adqvest_Bot', py_file_name=None):

    #os.chdir('/home/ubuntu/AdQvestDir')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'CIBIL_SUIT_FILED_CASES_QUARTERLY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        '''

        Step 1

        '''
        headers = {
                   "user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36",
                   }
        url = 'https://suit.cibil.com/loadSuitFiledDataSearchAction'

        r = requests.get(url,headers=headers)
        print(r.status_code)
        soup = BeautifulSoup(r.text,'html')

        st_ut = soup.find("select",{"id":'croreAccount'}).find_all("option")
        st_ut_str = [x.text for x in st_ut if 'state' in x.text.lower()]
        st_ut_code = [x['value'] for x in st_ut if 'state' in x.text.lower()]

        tp = soup.find("select",{"id":'quarterIdCrore'}).find_all("option")

        tp = [x for x in tp if "select" not in x.text.lower()]
        tp = [x for x in tp if int(parser.parse(x.text).date().strftime("%y")) >= 15]
        tp_str = [x.text for x in tp]
        tp_code = [x['value'] for x in tp]

        cookie = '; '.join([x.name + '=' + x.value for x in r.cookies])

        r = requests.Session()


        #import random
        #n = random.randint(1, 5)
        #tp_str = [tp_str[n]]
        #tp_code = [tp_code[n]]

        for tp,tp_id in zip(tp_str,tp_code):

            time.sleep(1)
            '''

            Step 2

            '''

            purl = 'https://suit.cibil.com/loadSuitFiledDataSearchAction'

            data = {"quarterIdSummary": '0',
                    "quarterIdGrantors": '0',
                    "croreAccount": '3',
                    "quarterIdCrore": tp_id,
                    "lakhAccount": '0',
                    "quarterIdLakh": '0',
                    "quarterDateStr": tp,
                    "fileType": "1",
                    "searchMode": '3'}

            r1 = r.post(purl,data=data,headers=headers)
            print(r1.status_code)
            cookie = '; '.join([x.name + '=' + x.value for x in r.cookies])

            soup1 = BeautifulSoup(r1.text,'html')
            a =str(soup1)
            a = a.split("var json = ")[-1]
            a = a.split(";")[0]
            a = eval(a)
            a = json.loads(a)['rows']
            states = [x['stateBean'] for x in a]
            states_id = [x['stateId'] for x in states]
            state_names= [x['stateName'] for x in states]


            headers['Cookie'] = cookie
        #    states_id = [states_id[n]]
        #    state_names = [state_names[n]]
            for st,st_id in zip(state_names,states_id):
                time.sleep(1)
                '''

                Step 3

                '''

                purl = 'https://suit.cibil.com/getCreditGrantorsSummaryAction'

                data = {"fileType":"2",
                        "stateName": "",
                        "suitSearchBean.quarterBean.quarterId": tp_id,
                        "suitSearchBean.stateBean.stateId": st_id,
                        "summaryState": "1",
                        "suitSearchBean.summaryType": "2"}


                r2 = r.post(purl,data=data,headers=headers)
                print(r2.status_code)


                soup2 = BeautifulSoup(r2.text,'html')
                a =str(soup2)
                a = a.split("var json = ")[-1]
                a = a.split(";")[0]
                a = eval(a)
                a = json.loads(a)['rows']

                banks = [x['bankBean'] for x in a]
                bank_names = [x['bankName'] for x in banks]
                bank_id = [x['bankId'] for x in banks]
                bank_records = [x['bankNoRecords'] for x in banks]
                cookie = '; '.join([x.name + '=' + x.value for x in r.cookies])

        #        bank_names = [bank_names[n]]
        #        bank_id = [bank_id[n]]
        #        bank_records = [bank_records[n]]
                for bk,bk_id,bk_r in zip(bank_names,bank_id,bank_records):
                    time.sleep(1)
                    d = datetime.datetime.now()
                    unixtime = int(time.mktime(d.timetuple()))

                    data = {"fileType": "2",
                            "suitSearchBeanJson": '{"borrowerName":"","costAmount":"","stateName":"","directorName":"","branchBean":null,"dunsNumber":"","city":"","bankBean":{"bankId":'+str(bk_id)+',"bankName":"","categoryBean":{"categoryId":0,"categoryName":"","categoryAllotedId":"","active":0,"enable":false},"bankNoRecords":0,"bankTotalAmount":"","enable":false,"active":0},"quarterBean":{"quarterId":'+str(tp_id)+',"quarterDate":null,"quarterDateStr":"","quarterName":"","quarterMonthStr":"","quarterYearStr":"","isPush":0},"stateBean":{"stateId":'+str(st_id)+',"stateName":"","stateNoRecords":0,"stateTotalAmount":"","category":"","enable":false,"isActive":0},"borrowerAddress":null,"borrowerId":0,"sort":0,"totalRecords":0,"totalAmount":"","quarterCol":"","categoryBean":null,"noOFCGs1Cr":0,"records1Cr":0,"noOFCGs25Lac":0,"records25Lac":0,"cat":"","catGroup":"","fromQuarterId":0,"toQuarterId":0,"partyTypeId":0,"quarterId":0,"srNo":"","userComments":"","rejected":0,"rejectComment":"","lastLimit":0,"firstLimit":0,"reject":null,"edit":null,"modify":null,"editedTotalAmount":null,"editedDirectorNames":null,"rejectComments":null,"updateReject":"","userId":0,"isReview":"","sortBy":null,"sortOrder":null,"summaryState":"1","summaryType":"2","directorId":0,"directorSuffix":"","dinNumber":"","editedDirectorDin":null,"dirPan":"","editedDirectorPan":null,"title":"","directorBean":null,"user":null,"importDataBean":null,"uploadBatchBean":null}',
                          "_search": False,
                          "nd": str(unixtime),
                          "rows": bk_r,
                          "page": "1",
                          "sidx": "",
                          "sord": "asc"}


                    query_parameters = urllib.parse.urlencode(data)
                    url = 'https://suit.cibil.com/loadSearchResultPage?'+query_parameters

                    final = json.loads(requests.get(url).content)

                    data = pd.DataFrame(final['rows'])
                    data['Registered_Address'] = data['importDataBean'].apply(lambda x : x['regaddr'])
                    data['Outstanding_Amount_In_Lacs'] = pd.to_numeric(data['totalAmount'],errors='ignore')
                    data['State'] = data['stateName']
                    data['Party'] = data['borrowerName']
                    data['Director_Name'] = data['directorName']
                    data['Bank'] = bk
                    data['Relevant_Date'] = tp
                    data['Quarter'] = tp
                    data['Relevant_Date'] = data['Relevant_Date'].apply(lambda x : parser.parse(x).date())
                    data['Source'] = 'CIBIL'
                    data['Category'] = '1 Cr and above'
                    data = data[['State','Registered_Address','Bank','Party','Director_Name','Outstanding_Amount_In_Lacs','Category','Source','Relevant_Date']]
                    data['Runtime'] = datetime.datetime.now()

                    data.to_sql(name='CIBIL_EQUIFAX_CRIF_DEFAULTERS_MONTHLY_QUARTERLY_DATA',con=engine,if_exists='append',index=False)
                    print("Uploaded--",st,"--",bk,'--',tp)


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
