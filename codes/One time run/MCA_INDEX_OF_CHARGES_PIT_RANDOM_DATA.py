from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import datetime as datetime
import re
import os
# import requests
import warnings
warnings.filterwarnings('ignore')
import sqlalchemy
from selenium import webdriver
from pytz import timezone
import time
import pytesseract
from PIL import Image
import sys
from selenium.common.exceptions import ElementNotInteractableException,NoSuchElementException
from selenium.webdriver.common.proxy import Proxy, ProxyType
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
import ClickHouse_db



def run_program(run_by='Adqvest_Bot', py_file_name=None):

    engine = adqvest_db.db_conn()
    connection = engine.connect()
    client = ClickHouse_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'MCA_INDEX_OF_CHARGES_PIT_RANDOM_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
        query = "select Distinct Cin from MCA_INDEX_OF_CHARGES_PIT_RANDOM_DATA"
        cin = pd.read_sql(query, con=engine)
        count = 0
        for i in range(len(cin)):
            print(cin['Cin'][i])
            query = 'select * from MCA_INDEX_OF_CHARGES_PIT_RANDOM_DATA where Cin = "' + cin['Cin'][i] + '"'
            pit = pd.read_sql(query, con=engine)
            company_df = pd.DataFrame()
            print(count, i)
            for i in pit['Runtime'].unique():
                run = pit[pit['Runtime']==i]
                for j in run['Charge_Id'].unique():
                    if (len(run[run['Charge_Id']==j]) >1):
                        modify = run[run['Charge_Id']==j]
                        pit.drop(index = modify[:-1].index[0], inplace=True)
            count = count + 1
            pit.to_sql("MCA_CHARGES_STAGING_MONTHLY_DATA_Temp_Tharani", index=False, if_exists='append', con=engine)
                            # company_df = company_df.append(modify)
        #                 else:
        #                     modify['Date_Of_Creation'][1:] = modify['Date_Of_Modification'][1:]
        #                     modify['Date_Of_Satisfaction'][:-1] = modify['Date_Of_Modification'][1:]
        #                     company_df = company_df.append(modify)
        #             else:
        #                 modify = run[run['Charge_Id']==j]
        #                 company_df = company_df.append(modify)
        #     company_df['Runtime'] = company_df['Runtime'].apply(lambda x : x.date())
        #     DOC = company_df[company_df['Date_Of_Creation']!='0000-00-00']
        #     DOC['Date_Of_Creation'] = DOC['Date_Of_Creation'].apply(lambda x : datetime.datetime.strptime(str(x), '%Y-%m-%d').date().replace(day=1))
        #     DOC = DOC[['Company_Name', 'Date_Of_Creation', 'Amount', 'Runtime']]
        #     DOC = DOC.groupby(by=['Company_Name', 'Runtime', 'Date_Of_Creation'], as_index=False).sum()
        #     DOC.columns = ['Company_Name', 'Runtime', 'Relevant_Date', 'Amount']
        #     DOS = company_df[company_df['Date_Of_Satisfaction']!='0000-00-00']
        #     DOS['Date_Of_Satisfaction'] = DOS['Date_Of_Satisfaction'].apply(lambda x : datetime.datetime.strptime(str(x), '%Y-%m-%d').date().replace(day=1))
        #     DOS = DOS[['Company_Name', 'Date_Of_Satisfaction', 'Amount', 'Runtime']]
        #     DOS = DOS.groupby(by=['Company_Name', 'Runtime', 'Date_Of_Satisfaction'], as_index=False).sum()
        #     DOS.columns = ['Company_Name', 'Runtime', 'Relevant_Date', 'Amount']
        #     df = pd.merge(DOC, DOS, on=['Company_Name', 'Relevant_Date', 'Runtime'], how='outer')
        #     df = df[['Company_Name', 'Runtime', 'Amount_x', 'Amount_y']]
        #     df = df.groupby(by=['Company_Name', 'Runtime'], as_index=False).sum()
        #     df['Amount'] = df['Amount_x'] - df['Amount_y']
        #     df['Runtime'] = df['Runtime'].apply(lambda x : x.replace(day=1))
        #     df = df.groupby(by=['Company_Name', 'Runtime'], as_index=False).max()
        #     mca_df = mca_df.append(df)
        #
        #
        # mca_df.to_sql("MCA_CHARGES_STAGING_MONTHLY_DATA_Temp_Tharani", index=False, if_exists='append', con=engine)


        log.job_end_log(table_name, job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'", str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name, job_start_time, error_type, error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
