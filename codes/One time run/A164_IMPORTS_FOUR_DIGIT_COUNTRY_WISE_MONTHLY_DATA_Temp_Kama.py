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
from dateutil.relativedelta import *
import urllib3
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNewWithReconnect as log
from dateutil.relativedelta import relativedelta
import ClickHouse_db
#%%

def end_date(year,month):
    end_dt = datetime.datetime(year, month, monthrange(year, month)[1]).date()
    return end_dt

def ldm(any_day):
    # this will never fail
    # get close to the end of the month for any day, and add 4 days 'over'
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
    # subtract the number of remaining 'overage' days to get last day of current month, or said programattically said, the previous day of the first of next month
    return next_month - datetime.timedelta(days=next_month.day)

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
    table_name = 'EXPORTS_FOUR_DIGIT_COUNTRY_WISE_MONTHLY_DATA'
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

        client = ClickHouse_db.db_conn()
        engine = adqvest_db.db_conn()
        connection = engine.connect()
        todate=pd.to_datetime('now').date()
        prev_month = todate.replace(day=1)
        prev_month=prev_month - datetime.timedelta(days=1)
        def get_import_hscode():
            client = ClickHouse_db.db_conn()
            engine = adqvest_db.db_conn()
            connection = engine.connect()
            q = "Select Distinct Code as HSCode,Description as Commodity from EXIM_HS_CODE_COMMODITY_PAIR_STATIC  where `Type`='HSN' and LENGTH(toString(Code))=4"
            import_top_500,cols = client.execute(q,with_column_types=True)
            import_top_500 = pd.DataFrame(import_top_500, columns=[tuple[0] for tuple in cols])
            import_top_500.reset_index(drop=True,inplace=True)
            # import_top_500=import_top_500.iloc[477:]
            import_top_500 = import_top_500[import_top_500.iloc[:,0]!='']
            return import_top_500
        def set_null(HSCodes):
            for code in HSCodes:
                query="update IMPORTS_FOUR_DIGIT_COUNTRY_WISE_HSCODE_STATUS set Status='NULL' where HSCode='"+str(code)+"'"
                engine.execute(query)
        # last_date=pd.read_sql("select min(Relevant_Date) as Min from IMPORTS_FOUR_DIGIT_COUNTRY_WISE",engine)['Min'][0]
        done_data=pd.read_sql("Select max(Max_Relevant_Date_DB) as MAX from IMPORTS_FOUR_DIGIT_COUNTRY_WISE_HSCODE_STATUS where Status='DONE'",engine)['MAX'][0]


        if str(done_data)<str(prev_month):
            HSCodes=pd.read_sql("Select distinct HSCode as HSCode from IMPORTS_FOUR_DIGIT_COUNTRY_WISE_HSCODE_STATUS where Status ='DONE'",engine)
            HSCodes=HSCodes['HSCode']
            HSCodes=list(HSCodes)
            set_null(HSCodes)
        q = "Select Distinct Four_Digit_HSCode as HSCode,Four_Digit_Commodity as Commodity from IMPORTS_CLEAN_DATA_HSN_MONTHLY"
    
        # date1 = last_date
        date2 = pd.read_sql("select max(Relevant_Date) as Max from IMPORTS_DATA_HSN_MONTHLY",engine)['Max'][0]
        connection.close()
        max_date = []
        max_db_date=pd.to_datetime('2018-02-28').date()
        today=pd.read_sql("select min(Relevant_Date) as Max from IMPORTS_FOUR_DIGIT_COUNTRY_WISE_MONTHLY_DATA_Temp_Kama",engine)['Max'][0]
        dates=pd.date_range(max_db_date,today,
                        freq='MS').strftime("%Y-%m-%d").tolist()
        print(dates)
        n=len(dates)
        dates=dates[:(n-1)]
        print(dates)
        dates_list = [pd.datetime.strptime(date, "%Y-%m-%d").date() for date in dates]
        all_dates=dates_list
        
        for date1 in all_dates:
            comm = []
            HScode = []


            date=date1
            max_rel_date = pd.to_datetime(date).date()
            print(max_rel_date)
            Latest_Date = max_rel_date + datetime.timedelta(1)
            print(Latest_Date)
            Latest_Month = Latest_Date.month
            Latest_Year = Latest_Date.year
            Current_Month = max_rel_date.month
            Current_Year = max_rel_date.year
            Prev_Date = max_rel_date + relativedelta(months=-1)
            Prev_Month = Prev_Date.month
            Prev_Year = Prev_Date.year

            session = requests.Session()
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.100 Safari/537.36"}

            r = session.get("https://tradestat.commerce.gov.in/meidb/comq.asp?ie=i", verify=False)
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            main_soup = BeautifulSoup(r.content)
            l1 = main_soup.findAll("input")
            main_data = {l1[1]['name']: l1[1]['value'],
                         'Mm1': str(Latest_Month), 'yy1': str(Latest_Year), 'hslevel': '4', 'sort': '0',
                         l1[3]['name']: l1[3]['value'], l1[4]['name']: l1[4]['value'],
                         l1[6]['name']: l1[6]['value'],
                         l1[7]['name']: l1[7]['value']}

            req = session.post("https://tradestat.commerce.gov.in/meidb/com.asp?ie=i", data=main_data, headers=headers,
                               verify=False)
            master_soup = BeautifulSoup(req.text, 'lxml')
            master = pd.read_html(str(master_soup))[0]
            master.drop(columns=['%Growth', '%Growth.1', 'S.No.'], axis=1, inplace=True)
            master = master.iloc[:, :2]
            master['Relevant_Date']=end_date(Latest_Year,Latest_Month)
            master.columns = ['HSCode', 'Commodity','Relevant_Date']
            print('this is length of/ distinct Hscode present in data for '+str(Latest_Date)+'----->',len(master))
            import_top_500=get_import_hscode()
            print(len(import_top_500))
            New_HSCode=list(master.HSCode)
            HSCode=list(import_top_500.HSCode)
            HSCode=[float(x) for x in HSCode]
            New_HS=[]
            for x in New_HSCode:
                if len(str(x))>5:
                    New_HS.append(x) 
            final_New_HSCode=set(New_HS).symmetric_difference(HSCode)
            new_df=master[master['HSCode'].apply(lambda x : x in final_New_HSCode)]
            if len(new_df)>0:
                print('There are new HSCode for '+str(date)+' and its length is '+str(len(new_df))+' ')
                new_df.rename(columns={'HSCode':'Code','Commodity':'Description'},inplace=True)
                new_df['Type']='HSN'
                new_df['Runtime']=pd.to_datetime('now')
                new_df['Last_Updated']=pd.to_datetime('now')
                new_df.to_sql("EXIM_HS_CODE_COMMODITY_PAIR_STATIC", if_exists="append", index=False, con=engine)
                new_df_click=pd.read_sql("select * from EXIM_HS_CODE_COMMODITY_PAIR_STATIC where Relevant_Date='"+str(end_date(Latest_Year,Latest_Month))+"'",engine)
                client.execute("INSERT INTO AdqvestDB.EXIM_HS_CODE_COMMODITY_PAIR_STATIC VALUES",
                               new_df_click.values.tolist())
                print("Data Loaded Succesfully in click house ".format(len(new_df)))
            count=0
            time.sleep(3)
            import_top_500=get_import_hscode()
            print(len(import_top_500))
            print('Running for Date : '+str(date)+' ')
            for code in range(import_top_500.shape[0]):
                data_date=end_date(Latest_Year,Latest_Month)
                print('Checking for date '+str(data_date)+'')
                print('checking for HSCode ---->' +str(import_top_500['HSCode'].iloc[code]))
                data=pd.read_sql("select * from IMPORTS_FOUR_DIGIT_COUNTRY_WISE_MONTHLY_DATA where HSCode='"+str(import_top_500['HSCode'].iloc[code])+"' and Relevant_Date='"+str(data_date)+"'",engine)
                if data.empty:
                    
                
                    connection.close()
                    print(import_top_500['HSCode'].iloc[code])
                    the_code = import_top_500['HSCode'].iloc[code]
                    session=requests.Session()
                    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.100 Safari/537.36"}
                    try:
                        r=session.get("https://tradestat.commerce.gov.in/meidb/comcntq.asp?ie=i",verify=False)
                    except:
                        r=session.get("https://tradestat.commerce.gov.in/meidb/comcntq.asp?ie=i",verify=False)
                    #            no_of_ping +=1
                    main_soup=BeautifulSoup(r.content)
                    l1=main_soup.findAll("input")
                    main_data={l1[1]['name']:l1[1]['value'],
                    'Mm1':str(Latest_Month),'yy1':str(Latest_Year),'hscode':str(import_top_500['HSCode'].iloc[code]),'sort':'0',l1[3]['name']:l1[3]['value'],
                    l1[6]['name']:l1[6]['value']}
                    try:
                        req=session.post("https://tradestat.commerce.gov.in/meidb/comcnt.asp?ie=i",data=main_data,headers=headers,verify=False)
                    except:
                        req=session.post("https://tradestat.commerce.gov.in/meidb/comcnt.asp?ie=i",data=main_data,headers=headers,verify=False)
                    #            no_of_ping +=1
                    master_soup=BeautifulSoup(req.text,'lxml')
                    flag=0
                    try:
                        master=pd.read_html(str(master_soup))[0]
                    except:
                        print('No new data')
                        flag=1
                    if flag!=1:
                        master.drop(columns=['%Growth','%Growth.1','S.No.'],axis=1,inplace=True)
                        master=master.iloc[:,:3]
                        master.columns=['Country','Monthly_Value_INR_In_Lakhs_Last_Year','Monthly_Value_INR_In_Lakhs_Current_Year']
                        #if master['Monthly_Value_INR_In_Lakhs_Current_Year'].isnull().all()!=True:
                        #                if today.day>15:

                        for yr in [Latest_Year]:

                            for mon in [Latest_Month]:
                                engine = adqvest_db.db_conn()
                                connection = engine.connect()
                                client = ClickHouse_db.db_conn()
                                print(yr)
                                print(mon)
                                data1={l1[1]['name']:l1[1]['value'],
                                'Mm1':str(mon),'yy1':str(yr),'hscode':str(import_top_500['HSCode'].iloc[code]),'sort':'0',l1[3]['name']:l1[3]['value'],
                                l1[6]['name']:l1[6]['value']}
                                time.sleep(5)
                                try:
                                    req=session.post("https://tradestat.commerce.gov.in/meidb/comcnt.asp?ie=i",data=data1,headers=headers,verify=False)
                                #                          no_of_ping +=1
                                except:
                                    req=session.post("https://tradestat.commerce.gov.in/meidb/comcnt.asp?ie=i",data=data1,headers=headers,verify=False)
                                inr_soup=BeautifulSoup(req.text,'lxml')
                                inr=pd.read_html(str(inr_soup))[0]
                                inr.drop(columns=['%Growth','%Growth.1','S.No.'],axis=1,inplace=True)
                                inr=inr.iloc[:,:3]
                                inr.columns=['Country','Monthly_Value_INR_In_Lakhs_Last_Year','Monthly_Value_INR_In_Lakhs_Current_Year']

                                data2={l1[1]['name']:l1[1]['value'],
                                'Mm1':str(mon),'yy1':str(yr),'hscode':str(import_top_500['HSCode'].iloc[code]),'sort':'0',l1[4]['name']:l1[4]['value'],
                                l1[6]['name']:l1[6]['value']}
                                time.sleep(5)
                                try:
                                    req=session.post("https://tradestat.commerce.gov.in/meidb/comcnt.asp?ie=i",data=data2,headers=headers,verify=False)
                                except:
                                    req=session.post("https://tradestat.commerce.gov.in/meidb/comcnt.asp?ie=i",data=data2,headers=headers,verify=False)
                                #                          no_of_ping +=1
                                usd_soup=BeautifulSoup(req.text,'lxml')
                                usd=pd.read_html(str(usd_soup))[0]
                                usd.drop(columns=['%Growth','%Growth.1','S.No.'],axis=1,inplace=True)
                                usd=usd.iloc[:,:3]
                                usd.columns=['Country','Monthly_Value_USD_In_Million_Last_Year','Monthly_Value_USD_In_Million_Current_Year']


                                data3={l1[1]['name']:l1[1]['value'],
                                'Mm1':str(mon),'yy1':str(yr),'hscode':str(import_top_500['HSCode'].iloc[code]),'sort':'0',l1[5]['name']:l1[5]['value'],
                                l1[6]['name']:l1[6]['value']}
                                time.sleep(5)
                                req=session.post("https://tradestat.commerce.gov.in/meidb/comcnt.asp?ie=i",data=data3,headers=headers,verify=False)
                                #                          no_of_ping +=1
                                qty_soup=BeautifulSoup(req.text,'lxml')
                                qty=pd.read_html(str(qty_soup))[0]
                                qty.drop(columns=['%Growth','%Growth.1','S.No.'],axis=1,inplace=True)
                                qty=qty.iloc[:,:3]
                                qty.columns=['Country','Monthly_Volume_In_Thousands_Last_Year','Monthly_Volume_In_Thousands_Current_Year']

                                df_final1=pd.merge(inr,usd,on='Country',how='left')
                                df_final=pd.merge(df_final1,qty,on='Country',how='left')
                                df_final['HSCode']=str(import_top_500['HSCode'].iloc[code])
                                df_final['Commodity']=import_top_500['Commodity'].iloc[code]
                                df_final=df_final[['HSCode','Commodity','Country','Monthly_Value_INR_In_Lakhs_Last_Year','Monthly_Value_INR_In_Lakhs_Current_Year',
                                                'Monthly_Value_USD_In_Million_Last_Year','Monthly_Value_USD_In_Million_Current_Year',
                                                'Monthly_Volume_In_Thousands_Last_Year','Monthly_Volume_In_Thousands_Current_Year']]
                                df_final['Commodity']=df_final['Commodity'].apply(lambda x: x.replace("''",""))
                                df_final['Relevant_Date']=end_date(int(yr),int(mon))
                                df_final['Runtime']=datetime.datetime.now()
                                df_final=df_final[df_final['Country']!='Total']
                                df_final.fillna(0, inplace=True)
                                # print(df_final)

                                if (df_final['Monthly_Value_INR_In_Lakhs_Current_Year'] == 0).all():
                                    print('All the value for '+str(import_top_500['HSCode'].iloc[code])+' are zero')
                                    connection.close()
                                else:

                                    print('Data looks fine......attempting to push it to Db')
                                    count+=1
                                    client = ClickHouse_db.db_conn()
                                    engine = adqvest_db.db_conn()
                                    connection = engine.connect()
                                    # client.execute("INSERT INTO "+table_name+" VALUES", df_final.values.tolist())
                                    # client.execute(query)
                                    connection.execute("Delete from IMPORTS_FOUR_DIGIT_COUNTRY_WISE_MONTHLY_DATA where Relevant_Date='"+str(end_date(int(Latest_Year),int(Latest_Month)))+"' and HSCode='"+str(import_top_500['HSCode'].iloc[code])+"'")
                                    connection.execute('commit')
                                    print('deleted the data for hscode '+str(import_top_500['HSCode'].iloc[code])+' and date -->',str(end_date(int(Latest_Year),int(Latest_Month))))
                                    time.sleep(2)
                                    df_final.to_sql("IMPORTS_FOUR_DIGIT_COUNTRY_WISE_MONTHLY_DATA", if_exists="append", index=False, con=engine)
                                    print(df_final.head(2))
                                    # df_final.to_sql(table_name, if_exists="append", index=False, con=engine)
                                    print('done for:',import_top_500['HSCode'].iloc[code])
                                    print('done for these much HSCode:',count)
                                    time.sleep(3)
                    else:
                        print('No data')
                else:
                    print('Data is already there for HSCode '+str(import_top_500['HSCode'].iloc[code])+' for ' +str(data_date))                    
                            
              # Latest_Date=Latest_Date+relativedelta(months=1)
              #     Latest_Month=Latest_Date.month
              #     Latest_Year=Latest_Date.year

  #%%
        log.job_end_log(table_name, job_start_time, no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'", str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name, job_start_time, error_type, error_msg, no_of_ping)


if(__name__=='__main__'):
    run_program(run_by='manual')
