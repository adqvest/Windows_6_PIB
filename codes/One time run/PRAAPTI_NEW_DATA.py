# -*- coding: utf-8 -*-
"""
Created on Thu Dec  3 15:50:45 2020

@author: abhis
"""


import sys
import re
import os
from io import StringIO
import sqlalchemy
from dateutil import parser
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
import datetime
from pytz import timezone
import time
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import JobLogNew as log
import adqvest_db


def last_day_of_month(any_day):
    # this will never fail
    # get close to the end of the month for any day, and add 4 days 'over'
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
    # subtract the number of remaining 'overage' days to get last day of current month, or said programattically said, the previous day of the first of next month
    return next_month - datetime.timedelta(days=next_month.day)

def date(x):

    try:
        date1 = parser.parse(x).date()
    except:
        date1 = ''

    return date1

def clean_state(st):

    st1 = st
    st1 = re.sub('[^a-zA-Z0-9 \n\.]', ' ', st1)
    st1 = st1.title()

    return st1

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
    table_name = 'PRAAPTI_NEW_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        start = time.time()

        india_time = timezone('Asia/Kolkata')
        today      = datetime.datetime.now(india_time)
        days       = datetime.timedelta(1)
        yesterday = today - days
        #os.chdir(r"C:\Adqvest\Selenium Extension\chromedriver.exe")
        headers = {
                   "user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
                   'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'
                   }


        url1 = 'https://praapti.in/'

        r = requests.get(url1,headers=headers)

        no_of_ping += 1

        soup1 = BeautifulSoup(r.text,'lxml')

        links = soup1.find_all('a',{'class':"sidebar"},href=True)

        urls = [x['href'] for x in links]
        states = [x.text for x in links]

        codes = soup1.find_all("div",class_="outer_div")
        #codes = codes.find_all('input')
        codes = [x.find_all('input') for x in codes]
        codes = sum(codes,[])
        codes1 = [x['value'] for x in codes]

        st_codes = [x['id'] for x in codes]
        st_codes = [x.split('StateCodeget_')[1] for x in st_codes]

        #st_codes = st_codes[1:5]
        #codes1 = codes1[1:5]

        main = pd.DataFrame()
        nas = pd.DataFrame()


        years = [2017,2018,2019,2020]
        years = [str(x) for x in years]

        for y in years:

            for i in range(1,13):

                i = str(i)
                if len(i)==1:
                    i = str(0)+i
                else:
                    i = i


                for st,co in zip(st_codes,codes1):
                    print(st,co)
                    #Avaialble Discoms
                    time.sleep(0.5)
                    sample = 'https://praapti.in/state-dashboard/'+st

                    r2 = requests.get(sample,headers=headers)

                    no_of_ping += 1

                    soup2 = BeautifulSoup(r2.text,'lxml')

                    print(st , r2.status_code,"First Request")

                    headers = {
                           "user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
                           'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                           'referer':sample
                           }

                    headers['cookie'] = '; '.join([x.name + '=' + x.value for x in r2.cookies])
                    headers['content-type'] = 'application/x-www-form-urlencoded'

                    data1 = {'_token':soup2.find('input',attrs = {'name':'_token'})['value'],'Datepick': i+'-'+y,'IsDetailsPost':1}

                    time.sleep(0.5)

                    r3 = requests.post(sample,data=data1,headers=headers,verify=False)

                    no_of_ping += 1

                    print(st,r3.status_code,"Second Request")

                    soup3 = BeautifulSoup(r3.content,'html')

                    print(soup3.find_all("div",class_='col-md-12 StatePage_Heading')[0].text.strip("\r\n").strip())

                    scraped_date = soup3.find_all("div",class_='col-md-12 StatePage_Heading')[0].text.strip("\r\n").strip()

                    scraped_date = parser.parse(scraped_date.split("on")[1].strip()).date()

                    scraped_date = last_day_of_month(scraped_date)

                    headers['cookie'] = '; '.join([x.name + '=' + x.value for x in r3.cookies])

                    time.sleep(0.5)

                    dis_st = 'https://praapti.in/DiscomWise_FCFS_Table/'+co

                    r = requests.get(dis_st,headers=headers)

                    no_of_ping += 1

                    print(st,r.status_code,"Third Request")

                    soup4 = BeautifulSoup(r.text,'html.parser')

                    discom_combo = soup4.find_all("p",{'class':'GenCoText'})
                    discoms = [x['onclick'] for x in discom_combo]
                    discoms = [x.split("getDiscomCode1")[1].split(";")[0] for x in discoms]
                    #    discoms = [x.split('(', 1)[1].split(')')[0] for x in discoms]
                    for vals in discoms:
                        print(vals)
                        discom1 = eval(vals)[0]
                        discom2 = eval(vals)[1]
                        print(st)

                        time.sleep(0.5)

                        url1 = 'https://praapti.in/FCFSTableExportExcel'

                        print("Getting Discomm Data",st)

                        data2 = {
                                "_token":soup2.find('input',attrs = {'name':'_token'})['value'],
                                 "GencoPendings_StateCode_SelectList_exl": co,
                                 "GencoPendings_DiscomCode_SelectList_exl": discom1,
                                 "GencoPendings_GenCoCode_SelectList_exl": discom2,
                                 "GencoPendings_InvoiceType_exl": 2,
                                 "GencoPendings_Currency_SelectList_exl": 7,
                                 "GencoPendings_ExcludingDisputedVal_SelectList_exl": 1
                                 }


                        try:
                            time.sleep(1)
                            r1 = requests.post(url1,data=data2,headers=headers)

                            no_of_ping += 1

                            print(st,r1.status_code,"final Request")
                        except:
                            time.sleep(1)
                            r1 = requests.post(url1,data=data2,headers=headers)

                            no_of_ping += 1

                            print(st,r1.status_code,"final Request")

                        cols = ['Invoice_No', 'Invoice_Date', 'Amount_Billed_To_Discom',
                               'Pending_Amount_Excluding_Disputed_Amount', 'Disputed_Amount',
                               'Percentage_Pending_Payment_Status', 'Due_Since_Days', 'State', 'State_Code',
                               'Discom', 'Company_Organisation']

                        data = r1.text
                        if data != '':
                            TESTDATA = StringIO(data)
                            df = pd.read_csv(TESTDATA, sep=",")
                            st1 = st
                            st1 = re.sub('[^a-zA-Z0-9 \n\.]', ' ', st1)
                            st1 = st1.title()
                            df['State'] = st1
                            df['State_Code'] = co
                            df['Discomm'] = discom1
                            df['Company_Organisation'] = discom2
                            df = df[~df[df.columns.to_list()[0]].str.contains("Total ")]
                            df.columns = cols
                            df['Invoice_Date'] = df['Invoice_Date'].apply(lambda x: date(x))
                            df['Relevant_Date'] = scraped_date
                            df['Runtime'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
                            main = pd.concat([main,df])

                        else:
                            print("NA",st)
                            df1 = pd.DataFrame({"State":st,"Discom":discom1,"Company":discom2,"Date":scraped_date},index=[0])
                            nas = pd.concat([nas,df1])



                main1 = main.copy()
                main1['State'] = main1['State'].apply(lambda x: clean_state(x))

                main1[['Amount_Billed_To_Discom',
                       'Pending_Amount_Excluding_Disputed_Amount']] = main1[['Amount_Billed_To_Discom',
                       'Pending_Amount_Excluding_Disputed_Amount']]/10000000
                #main1['']


                main1[['Amount_Billed_To_Discom',
                       'Pending_Amount_Excluding_Disputed_Amount']] = main1[['Amount_Billed_To_Discom',
                       'Pending_Amount_Excluding_Disputed_Amount']].round(2)

                cols = ['Invoice_No', 'Invoice_Date', 'Amount_Billed_To_Discom_Cr',
                       'Pending_Amount_Excluding_Disputed_Amount_Cr', 'Disputed_Amount',
                       'Percentage_Pending_Payment_Status', 'Due_Since_Days', 'State', 'State_Code',
                       'Discom', 'Company_Organisation','Relevant_Date','Runtime']

                main1.columns = cols


                main1.to_sql("PRAAPTI_NEW_DATA",con=engine,if_exists='replace',index=False)

                del main1

                del main


                end = time.time()
                print(end - start)

        connection.close()
        ##%%
        log.job_end_log(table_name,job_start_time,no_of_ping)

    except:
        try:
            connection.close()
        except:
            pass
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
