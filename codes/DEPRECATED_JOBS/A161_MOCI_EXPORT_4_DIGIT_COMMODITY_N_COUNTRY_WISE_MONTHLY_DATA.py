import requests
import sqlalchemy
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import datetime as datetime
from pytz import timezone
import sys
import re
import time
import warnings
import os
from calendar import monthrange
import datetime as datetime
warnings.filterwarnings('ignore')
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dateutil.relativedelta import *
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
from adqvest_robotstxt import Robots
robot = Robots(__file__)

engine = adqvest_db.db_conn()
connection = engine.connect()
import MySql_To_Clickhouse as MySql_CH
import JobLogNew as log
from dateutil.relativedelta import relativedelta
#%%

def end_date(year,month):
    end_dt = datetime.datetime(year, month, monthrange(year, month)[1]).date()
    return end_dt

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
    table_name = 'MOCI_EXPORT_4_DIGIT_COMMODITY_N_COUNTRY_WISE_MONTHLY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]

    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
        engine = adqvest_db.db_conn()
        connection = engine.connect()
        todate=pd.to_datetime('now').date()
        prev_month = todate.replace(day=1)
        prev_month=prev_month - datetime.timedelta(days=1)
        def set_null(HSCodes):
            for code in HSCodes:
                query="update MOCI_EXPORT_4_DIGIT_COMMODITY_N_COUNTRY_WISE_MONTHLY_DATA_STATUS set Status='NULL' where HSCode='"+str(code)+"'"
                engine.execute(query)


        def get_months_year(max_rel_date):
            Latest_Date = max_rel_date + datetime.timedelta(1)
            print(Latest_Date)
            Latest_Month = Latest_Date.month
            Latest_Year = Latest_Date.year
            Current_Month = max_rel_date.month
            Current_Year = max_rel_date.year
            Prev_Date = max_rel_date + relativedelta(months=-1)
            Prev_Month = Prev_Date.month
            Prev_Year = Prev_Date.year
            return (Latest_Month,Latest_Year,Current_Month,Current_Year,Prev_Date,Prev_Month,Prev_Year)

        def table_reinstate():
            engine = adqvest_db.db_conn()
            max_date_act_table=pd.read_sql('select max(Relevant_Date) as Max from MOCI_EXPORT_4_DIGIT_COMMODITY_N_COUNTRY_WISE_MONTHLY_DATA',engine)['Max'][0]
            max_date_com_table=pd.read_sql('select max(Relevant_Date) as Max from MOCI_EXPORT_4_DIGIT_COMMODITY_WISE_MONTHLY_DATA',engine)['Max'][0]
            if max_date_act_table< max_date_com_table:
                status_df=pd.read_sql("select distinct HSCode as HSCode,Commodity as Commodity from MOCI_EXPORT_4_DIGIT_COMMODITY_WISE_MONTHLY_DATA where Relevant_Date>'"+str(max_date_act_table)+"'",engine)
                status_df['Code_Status']='FALSE'
                status_df['Status']='NULL'
                status_df['Table_Run_Status']='Wait'
                status_df['Max_Relevant_Date_DB']=max_date_act_table
                status_df['Relevant_Date']=pd.to_datetime('now').date()
                status_df['Runtime']=pd.to_datetime('now')

                #### DELETING STATUS TABLE AND REINSTATING ####

                connection.execute("Delete from MOCI_EXPORT_4_DIGIT_COMMODITY_N_COUNTRY_WISE_MONTHLY_DATA_STATUS")
                connection.execute('commit')
                engine = adqvest_db.db_conn()
                status_df.to_sql("MOCI_EXPORT_4_DIGIT_COMMODITY_N_COUNTRY_WISE_MONTHLY_DATA_STATUS", if_exists="append", index=False, con=engine)
            else:
                status_df=pd.DataFrame()
            return len(status_df)

        def update_status_table(status_code):
            query="update MOCI_EXPORT_4_DIGIT_COMMODITY_N_COUNTRY_WISE_MONTHLY_DATA_STATUS set Table_Run_Status='Ran' where HSCode='"+str(status_code)+"'"
            engine.execute(query)
            max_relevant_date=pd.read_sql("select max(Relevant_Date) as MAX from AdqvestDB.MOCI_EXPORT_4_DIGIT_COMMODITY_N_COUNTRY_WISE_MONTHLY_DATA where HSCode='"+str(status_code)+"'",engine)['MAX'][0]
            # print(max_relevant_date)
            query="update MOCI_EXPORT_4_DIGIT_COMMODITY_N_COUNTRY_WISE_MONTHLY_DATA_STATUS set Max_Relevant_Date_DB='"+str(max_relevant_date)+"' where HSCode='"+str(status_code)+"'"
            engine.execute(query)
            date=pd.to_datetime('now').date()
            now=pd.to_datetime('now')
            query="update MOCI_EXPORT_4_DIGIT_COMMODITY_N_COUNTRY_WISE_MONTHLY_DATA_STATUS set Relevant_Date='"+str(date)+"' where HSCode='"+str(status_code)+"'"
            engine.execute(query)
            query="update MOCI_EXPORT_4_DIGIT_COMMODITY_N_COUNTRY_WISE_MONTHLY_DATA_STATUS set Runtime='"+str(now)+"' where HSCode='"+str(status_code)+"'"
            engine.execute(query)
            try:
                prev_month = date.replace(day=1)
                prev_month=prev_month - datetime.timedelta(days=1)
                if prev_month==max_relevant_date:
                    query="update MOCI_EXPORT_4_DIGIT_COMMODITY_N_COUNTRY_WISE_MONTHLY_DATA_STATUS set Status='DONE' where HSCode='"+str(status_code)+"'"
                    engine.execute(query)
            except:
                print('Data has not been collected for this hscode')

        def date_to_be_collected(max_rel_date,export_200):
            Latest_Month,Latest_Year,Current_Month,Current_Year,Prev_Date,Prev_Month,Prev_Year=get_months_year(max_rel_date)
            print('-----Trying to collect for latest date------>',Latest_Year,Latest_Month)
            value_latest=collect_data(export_200,Latest_Year,Latest_Month)
            if value_latest == 'NOT DONE':
                print('Site is down')
            else:
                print('-----Trying to collect for current date------>',Current_Year,Current_Month)
                value_current=collect_data(export_200,Current_Year,Current_Month)

                print('-----Trying to collect for previous date------>',Prev_Year,Prev_Month)
                value_previous=collect_data(export_200,Prev_Year,Prev_Month)
                if value_latest==value_current==value_previous=='DONE':
                    print('-----------------------CODE RAN SUCCESSFULLY FOR GIVEN 200 HSCODE-----------------------')
                elif value_latest==value_current==value_previous=='NOT DONE':
                    print('-----------------------SITE IS DOWN TRYING AGAIN AFTER SOMETIME--------------------------')
                else:
                    raise ValueError('Something went wrong check collect_data function')

        def collect_data(export_200,yr,mon):
            print('Data_collection')
            engine = adqvest_db.db_conn()
            connection = engine.connect()
            print('data collecton2')
            count=0
            str_return='NOT DONE'
            session=requests.Session()

            #Added | Pushkar | 20 July 2023 | Retries
            retry = Retry(total=10, backoff_factor=5)
            adapter = HTTPAdapter(max_retries=retry)
            session.mount('http://', adapter)
            session.mount('https://', adapter)


            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.100 Safari/537.36"}   
            try:
                r=session.get("https://tradestat.commerce.gov.in/meidb/comcntq.asp?ie=e",verify=False,timeout=80)
            except:
                r=session.get("https://tradestat.commerce.gov.in/meidb/comcntq.asp?ie=e",verify=False,timeout=80)
            main_soup=BeautifulSoup(r.content)
            #added by kama
            df_final=pd.DataFrame()
            if 'error' in main_soup.text.lower():
                return 'NOT DONE'
            else:
            #ended
                
                years = [int(i.text) for i in main_soup.find_all('select', {'name':'yy1'})[0].find_all('option') if int(i.text) >= yr]
                months = [int(i['value']) for i in main_soup.find_all('select', {'name':'Mm1'})[0].find_all('option') if int(i['value']) == mon]
                if ((len(years) != 0) & (len(months) != 0)):
                    l1=main_soup.findAll("input")
                    for i in range(len(export_200)):
                        print('code--->',export_200['HSCode'].iloc[i]) 
                        # session=requests.Session()
                        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.100 Safari/537.36"}
                        print(yr)
                        print(mon)
                        data1={l1[1]['name']:l1[1]['value'],
                        'Mm1':str(mon),'yy1':str(yr),'hscode':str(export_200['HSCode'].iloc[i]),'sort':'0',l1[3]['name']:l1[3]['value'],
                        l1[6]['name']:l1[6]['value']}
                        time.sleep(5)
                        try:
                            req=session.post("https://tradestat.commerce.gov.in/meidb/comcnt.asp?ie=e",data=data1,headers=headers,verify=False)
                        #                          no_of_ping +=1
                        except:
                            req=session.post("https://tradestat.commerce.gov.in/meidb/comcnt.asp?ie=e",data=data1,headers=headers,verify=False)
                        inr_soup=BeautifulSoup(req.text,'lxml')
                        if 'error' in inr_soup.text.lower():
                            return 'NOT DONE'
                        else:
                            q=1
                            try:
                                inr=pd.read_html(str(inr_soup))[0]
                            except:
                                q=0
                            if q==1:
                                inr=pd.read_html(str(inr_soup))[0]
                                inr.drop(columns=['%Growth','%Growth.1','S.No.'],axis=1,inplace=True)
                                inr=inr.iloc[:,:3]
                                inr.columns=['Country','Monthly_Value_INR_In_Lakhs_Last_Year','Monthly_Value_INR_In_Lakhs_Current_Year']

                                data2={l1[1]['name']:l1[1]['value'],
                                'Mm1':str(mon),'yy1':str(yr),'hscode':str(export_200['HSCode'].iloc[i]),'sort':'0',l1[4]['name']:l1[4]['value'],
                                l1[6]['name']:l1[6]['value']}
                                time.sleep(5)
                                try:
                                    req=session.post("https://tradestat.commerce.gov.in/meidb/comcnt.asp?ie=e",data=data2,headers=headers,verify=False)
                                except:
                                    req=session.post("https://tradestat.commerce.gov.in/meidb/comcnt.asp?ie=e",data=data2,headers=headers,verify=False)
                                #   

                                usd_soup=BeautifulSoup(req.text,'lxml')
                                if 'error' in usd_soup.text.lower():
                                    return 'NOT DONE'
                                else: 
                                    usd=pd.read_html(str(usd_soup))[0]
                                    usd.drop(columns=['%Growth','%Growth.1','S.No.'],axis=1,inplace=True)
                                    usd=usd.iloc[:,:3]
                                    usd.columns=['Country','Monthly_Value_USD_In_Million_Last_Year','Monthly_Value_USD_In_Million_Current_Year']


                                    data3={l1[1]['name']:l1[1]['value'],
                                    'Mm1':str(mon),'yy1':str(yr),'hscode':str(export_200['HSCode'].iloc[i]),'sort':'0',l1[5]['name']:l1[5]['value'],
                                    l1[6]['name']:l1[6]['value']}
                                    time.sleep(5)
                                    req=session.post("https://tradestat.commerce.gov.in/meidb/comcnt.asp?ie=e",data=data3,headers=headers,verify=False)
                                    #                          no_of_ping +=1
                                    qty_soup=BeautifulSoup(req.text,'lxml')
                                    if 'error' in qty_soup.text.lower():
                                        return 'NOT DONE'
                                    else:
                                        qty=pd.read_html(str(qty_soup))[0]
                                        qty.drop(columns=['%Growth','%Growth.1','S.No.'],axis=1,inplace=True)
                                        qty=qty.iloc[:,:3]
                                        qty.columns=['Country','Monthly_Volume_In_Thousands_Last_Year','Monthly_Volume_In_Thousands_Current_Year']

                                        df_final1=pd.merge(inr,usd,on='Country',how='left')
                                        df_final=pd.merge(df_final1,qty,on='Country',how='left')
                                        df_final['HSCode']=str(export_200['HSCode'].iloc[i])
                                        df_final['Commodity']=export_200['Commodity'].iloc[i]
                                        df_final=df_final[['HSCode','Commodity','Country','Monthly_Value_INR_In_Lakhs_Last_Year','Monthly_Value_INR_In_Lakhs_Current_Year',
                                                        'Monthly_Value_USD_In_Million_Last_Year','Monthly_Value_USD_In_Million_Current_Year',
                                                        'Monthly_Volume_In_Thousands_Last_Year','Monthly_Volume_In_Thousands_Current_Year']]
                                        df_final['Commodity']=df_final['Commodity'].apply(lambda x: x.replace("''",""))
                                        df_final['Relevant_Date']=end_date(int(yr),int(mon))
                                        df_final['Runtime']=datetime.datetime.now()
                                        df_final=df_final[df_final['Country']!='Total']
                                        df_final.fillna(0, inplace=True)
                                        if ((df_final['Monthly_Value_INR_In_Lakhs_Current_Year'] == 0).all()) or (df_final['Monthly_Value_INR_In_Lakhs_Current_Year'].isnull().all()) and ((df_final['Monthly_Volume_In_Thousands_Current_Year'].isnull().all()) or (df_final['Monthly_Volume_In_Thousands_Current_Year'].all() == 0)) :
                                            count+=1
                                            print('All the value for '+str(export_200['HSCode'].iloc[i])+' are zero')
                                            update_status_table(export_200['HSCode'].iloc[i])

                                        else:
                                            df_mapping=pd.read_sql("Select * from GENERIC_DICTIONARY_TABLE where Input_Table ='MOCI_4_AND_6_DIGIT_COMMODITY_DATA'", engine)
                                            print('Data looks fine......attempting to push it to Db')
                                            df_final = pd.merge(df_final, df_mapping, left_on='HSCode', right_on='Input', how='left')
                                            df_final=df_final[['HSCode','Commodity','Output_2','Output','Country','Monthly_Value_INR_In_Lakhs_Last_Year','Monthly_Value_INR_In_Lakhs_Current_Year','Monthly_Value_USD_In_Million_Last_Year','Monthly_Value_USD_In_Million_Current_Year','Monthly_Volume_In_Thousands_Last_Year','Monthly_Volume_In_Thousands_Current_Year','Relevant_Date_x','Runtime_x']]
                                            df_final.columns=['HSCode','Commodity','Commodity_Clean_1','Commodity_Clean_2','Country','Monthly_Value_INR_In_Lakhs_Last_Year','Monthly_Value_INR_In_Lakhs_Current_Year','Monthly_Value_USD_In_Million_Last_Year','Monthly_Value_USD_In_Million_Current_Year','Monthly_Volume_In_Thousands_Last_Year','Monthly_Volume_In_Thousands_Current_Year','Relevant_Date','Runtime']
                                            df_final['Commodity_Clean_2']=df_final['Commodity_Clean_2'].str.upper()
                                            df_final.drop_duplicates(inplace=True)
                                            count+=1
                                            engine = adqvest_db.db_conn()
                                            connection = engine.connect()

                                            connection.execute("Delete from MOCI_EXPORT_4_DIGIT_COMMODITY_N_COUNTRY_WISE_MONTHLY_DATA where Relevant_Date='"+str(end_date(int(yr),int(mon)))+"' and HSCode='"+str(export_200['HSCode'].iloc[i])+"'")
                                            connection.execute('commit')
                                            time.sleep(3)
                                            df_final['Country']= df_final['Country'].str.replace("'","")
                                            engine = adqvest_db.db_conn()
                                            df_final.to_sql("MOCI_EXPORT_4_DIGIT_COMMODITY_N_COUNTRY_WISE_MONTHLY_DATA", if_exists="append", index=False, con=engine)

                                            print('----------DONE FOR HSCODE---------------')
                                            print(str(export_200['HSCode'].iloc[i]))
                                            print('done for these much HSCode:',count)
                                            time.sleep(1)
                                            
                                            update_status_table(export_200['HSCode'].iloc[i])
                            else:
                                count+=1
                                print('No data in website')
                                update_status_table(export_200['HSCode'].iloc[i])

                            str_return='DONE'
                            
                            connection.close()
                else:
                    print('Latest Date Dropdown option unavailable')
            return str_return

        done_data=pd.read_sql("Select max(Max_Relevant_Date_DB) as MAX from MOCI_EXPORT_4_DIGIT_COMMODITY_N_COUNTRY_WISE_MONTHLY_DATA_STATUS where Status='DONE'",engine)['MAX'][0]
        if done_data != None:   # Added | Pushkar | 1 Mar 2024
            if done_data<prev_month:
                HSCodes=pd.read_sql("Select distinct HSCode as HSCode from MOCI_EXPORT_4_DIGIT_COMMODITY_N_COUNTRY_WISE_MONTHLY_DATA_STATUS where Status ='DONE'",engine)
                HSCodes=HSCodes['HSCode']
                HSCodes=list(HSCodes)
                set_null(HSCodes)
        max_rel_date=pd.read_sql('select max(Relevant_Date) as Max from MOCI_EXPORT_4_DIGIT_COMMODITY_N_COUNTRY_WISE_MONTHLY_DATA',engine)['Max'][0]
        print(max_rel_date)
        HSCode_200=pd.read_sql("select distinct HSCode as HSCode,Commodity from MOCI_EXPORT_4_DIGIT_COMMODITY_N_COUNTRY_WISE_MONTHLY_DATA_STATUS where Table_Run_Status='Wait' limit 200",engine)
        
        if HSCode_200.empty:
            print('ALL the code ran')
            print('collected for all hscode----> making changes in status')

            len_status=table_reinstate()
            if len_status>0:
                print('Start_collecting_new_data')
                engine = adqvest_db.db_conn()
                max_run_date=pd.read_sql('select max(Relevant_Date) as Max from MOCI_EXPORT_4_DIGIT_COMMODITY_N_COUNTRY_WISE_MONTHLY_DATA_STATUS',engine)['Max'][0]
                robot.add_link("https://tradestat.commerce.gov.in/meidb/comcnt.asp?ie=i")

                date_to_be_collected(max_rel_date,HSCode_200)
            else:
                print('Data in the table is up to date as in overall commodity wise table')
                log.job_end_log(table_name, job_start_time, no_of_ping)
        else:
            print('Collecting for 500 hscode')
            today=pd.to_datetime('now')
            engine = adqvest_db.db_conn()
            run_status=pd.read_sql('select * from MOCI_EXPORT_4_DIGIT_COMMODITY_N_COUNTRY_WISE_MONTHLY_DATA_STATUS',engine)
            max_run_date=pd.read_sql('select max(Relevant_Date) as Max from MOCI_EXPORT_4_DIGIT_COMMODITY_N_COUNTRY_WISE_MONTHLY_DATA_STATUS',engine)['Max'][0]
            robot.add_link("https://tradestat.commerce.gov.in/meidb/comcnt.asp?ie=i")

            if (run_status['Table_Run_Status'] == 'Wait').all() and (today.date() - max_run_date).days > 5:
                date_to_be_collected(max_rel_date,HSCode_200)
            elif (run_status['Table_Run_Status']  != 'Wait').any():
                date_to_be_collected(max_rel_date,HSCode_200)
            else:
                print('----- Data will be collected after few days -----')
                date_to_be_collected(max_rel_date,HSCode_200)
        MySql_CH.ch_truncate_and_insert(table_name)
        print('Data pushed into clickhouse')      
        log.job_end_log(table_name, job_start_time, no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'", str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name, job_start_time, error_type, error_msg, no_of_ping)


if(__name__=='__main__'):
    run_program(run_by='manual')
