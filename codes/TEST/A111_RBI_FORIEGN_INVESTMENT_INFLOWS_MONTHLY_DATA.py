import calendar
import datetime
import os
import re
import ssl
import sys
import warnings
import requests
import numpy as np
import pandas as pd
import regex as re
import time
from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta
from pytz import timezone
from playwright.sync_api import sync_playwright
warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import JobLogNew as log
import adqvest_db
import ClickHouse_db
import adqvest_s3
import boto3
ssl._create_default_https_context = ssl._create_unverified_context

def run_program(run_by = 'Adqvest_Bot', py_file_name = None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    client = ClickHouse_db.db_conn()

    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday =  today - days

    job_start_time = datetime.datetime.now(india_time)
    table_name = "RBI_FORIEGN_INVESTMENT_INFLOWS_MONTHLY_DATA"
    scheduler = ''
    no_of_ping = 0

    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]

    try:
        if(run_by == 'Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        #>>>>>>>>>>>>>>>>>>>>>>> functions <<<<<<<<<<<<<<<<<<<<<<
        def get_date2(month1,year):
            month1=month1.replace(".","").strip()
            if month1=='Sept':
                month1='Sep'
            try:
                month_num=datetime.datetime.strptime(month1, '%b').month
            except:
                month_num=datetime.datetime.strptime(month1, '%B').month
            date1=str(year)+'-'+str(month_num)+'-'+str(calendar.monthrange(int(year),int(month_num))[1])
            try:
                date1=datetime.datetime.strptime(date1,"%Y-%m-%d").date()
            except:
                date1=datetime.datetime.strptime(date1,"%y-%m-%d").date()

            return date1

        def bracket_remove(x):
            x=str(x)
            x=re.sub("\(.*?\)",'',x)
            x=x.strip()
            return x
        #>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>><<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
        max_rel_date = pd.read_sql("select max(Relevant_Date) as RD from RBI_FORIEGN_INVESTMENT_INFLOWS_MONTHLY_DATA;",con=engine)
        max_rel_date = max_rel_date['RD'][0]
        print(max_rel_date)
        # headers ={"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36" ,
        #           'referer':'https://rbi.org.in/Scripts/Statistics.aspx'}

        headers = {
            # 'authority': 'www.google-analytics.com',
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            # 'content-length': '0',
            'content-type': 'text/plain',
            'origin': 'https://www.rbi.org.in',
            'referer': 'https://www.rbi.org.in/',
            'sec-ch-ua': '"Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'cross-site',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
        }
        r = requests.get('https://www.rbi.org.in/Scripts/BS_ViewBulletin.aspx',headers=headers)
        soup = BeautifulSoup(r.content, 'lxml')
        print('yess')

        __VIEWSTATE = soup.find_all(attrs={"name" : "__VIEWSTATE"})[0]['value']
        __VIEWSTATEGENERATOR = soup.find_all(attrs={"name" : "__VIEWSTATEGENERATOR"})[0]['value']
        __EVENTVALIDATION = soup.find_all(attrs={"name" : "__EVENTVALIDATION"})[0]['value']


        year = datetime.datetime.today().year
        year_list=sorted(list(range(year, year - 1, -1)),reverse=True)
        print(year_list)
        month_list=sorted(list(range(1,13)),reverse=True)
        print(month_list)
        final_link=[]

        for y in year_list:
            print(y)
            for mon in month_list:
                print(mon)
                payload = {
                        "__VIEWSTATE":__VIEWSTATE,
                        "__VIEWSTATEGENERATOR":__VIEWSTATEGENERATOR,
                        "__EVENTVALIDATION":__EVENTVALIDATION,
                        "hdnYear":y,
                        "hdnMonth":mon
                        }

                r_post = requests.post('https://www.rbi.org.in/Scripts/BS_ViewBulletin.aspx',
                                  data = payload, headers = headers,verify=False)
                soup=BeautifulSoup(r_post.content,"html.parser")
                link2=soup.find_all(class_="link2")
                for i in link2:
                    if 'Investment Inflows' in str(i):
                        print("Found:",i)
                        page_link='https://www.rbi.org.in/Scripts/'+i['href']
                        # page_link = i
                        print(page_link)
                        # header2={"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36" ,
                        #          'referer':'https://rbi.org.in/Scripts/Statistics.aspx'}
                        excel_request=requests.post(page_link, headers=headers,verify=False)
                        soup2=BeautifulSoup(excel_request.content,"html.parser")
                        link_2=soup2.find_all(class_="tableheader")
                        for m in link_2:
                           link=m.find("a")
                           if link!=None:
                                final_link.append(link['href'])
                        break
                    else:
                        print("Link not found")

                try:

                    final_df=pd.DataFrame()
                    for link in final_link:
                        r = requests.get(link, headers=headers, verify=False)
                        with open(f"RBI_FORIEGN_INVESTMENT_INFLOWS_MONTHLY_DATA_{date_cur_mon}.XLSX", 'wb') as f:
                            f.write(r.content)
                        filename = f"RBI_FORIEGN_INVESTMENT_INFLOWS_MONTHLY_DATA_{date_cur_mon}.XLSX"

                        data=pd.read_excel(filename,sheet_name=0)
                        df1=data.copy()
                        df1.dropna(thresh=5,inplace=True,axis=1)
                        date_index=df1[df1.iloc[:,0].str.lower().str.contains('item')==True].index[0]
                    #     print(date_index)

                        #>>>>>>>> date formatting <<<<<<<<<<
                        try:
                            prev_year=df1.iloc[date_index,4]
                    #         print(prev_year)
                            prev_mon=df1.iloc[date_index+1,4]
                    #         print(prev_mon)
                            current_year=df1.iloc[date_index,5]
                            cur_year_prev_mon=df1.iloc[date_index+1,5]
                            current_month=df1.iloc[date_index+1,6]
                            date_prev_year=get_date2(prev_mon,prev_year)
                            print(date_prev_year)
                            current_month=current_month.replace(".","").strip()
                            if current_month=='Jan' or current_month=='January':
                                date_prev_mon=get_date2(cur_year_prev_mon,prev_year)
                            else:
                                date_prev_mon=get_date2(cur_year_prev_mon,current_year)
                            print(date_prev_mon)
                            date_cur_mon=get_date2(current_month,current_year)
                            print(date_cur_mon)
                        except:
                            prev_year=df1.iloc[date_index,4]
                    #         print(prev_year)
                            prev_mon=df1.iloc[date_index+1,4]
                            current_year=df1.iloc[date_index,6]
                            cur_year_prev_mon=df1.iloc[date_index+1,5]
                            current_month=df1.iloc[date_index+1,6]
                            date_prev_year=get_date2(prev_mon,prev_year)
                            print(date_prev_year)
                            date_prev_mon=get_date2(cur_year_prev_mon,current_year)
                            current_month=current_month.replace(".","").strip()
                            print(current_month)
                            if current_month=='Jan' or current_month=='January':
                                date_prev_mon=get_date2(cur_year_prev_mon,prev_year)
                            else:
                                date_prev_mon=get_date2(cur_year_prev_mon,current_year)
                            print(date_prev_mon)
                            date_cur_mon=get_date2(current_month,current_year)
                            print(date_cur_mon)


                        start_index=df1[df1.iloc[:,0].str.lower().str.contains('direct')==True].index[0]
                        try:
                            end_index=df1[df1.iloc[:,0].str.lower().str.contains('estimates')==True].index[0]
                        except:
                            pass
                        try:
                            df1=df1[start_index:end_index]
                        except:
                            df1=df1[start_index:]
                        df_cur_mon=df1.iloc[:,[0,-1]]
                        df_prev_mon=df1.iloc[:,[0,-2]]
                        df_prev_year=df1.iloc[:,[0,-3]]

                        df_cur_mon.columns=['Item','Amount_USD_Mn']
                        df_prev_mon.columns=['Item','Amount_USD_Mn']
                        df_prev_year.columns=['Item','Amount_USD_Mn']

                        df_cur_mon.reset_index(drop=True,inplace=True)
                        df_prev_mon.reset_index(drop=True,inplace=True)
                        df_prev_year.reset_index(drop=True,inplace=True)

                        df_cur_mon[['Item_No','Item']] = df_cur_mon["Item"].str.split(" ", 1, expand=True)
                        df_prev_mon[['Item_No','Item']] = df_prev_mon["Item"].str.split(" ", 1, expand=True)
                        df_prev_year[['Item_No','Item']] = df_prev_year["Item"].str.split(" ", 1, expand=True)

                        df_cur_mon['Item']=df_cur_mon['Item'].apply(lambda x:bracket_remove(x))
                        df_prev_mon['Item']=df_prev_mon['Item'].apply(lambda x:bracket_remove(x))
                        df_prev_year['Item']=df_prev_year['Item'].apply(lambda x:bracket_remove(x))

                        df_cur_mon=df_cur_mon[['Item_No','Item','Amount_USD_Mn']]
                        df_prev_mon=df_prev_mon[['Item_No','Item','Amount_USD_Mn']]
                        df_prev_year=df_prev_year[['Item_No','Item','Amount_USD_Mn']]

                        df_cur_mon['Relevant_Date']=date_cur_mon
                        df_cur_mon['Amount_USD_Mn']=df_cur_mon['Amount_USD_Mn'].replace("–",np.nan)
                        df_cur_mon['Amount_USD_Mn']=df_cur_mon['Amount_USD_Mn'].replace("-",np.nan)

                        df_prev_mon['Relevant_Date']=date_prev_mon
                        df_prev_mon['Amount_USD_Mn']=df_prev_mon['Amount_USD_Mn'].replace("–",np.nan)
                        df_prev_mon['Amount_USD_Mn']=df_prev_mon['Amount_USD_Mn'].replace("-",np.nan)

                        df_prev_year['Relevant_Date']=date_prev_year
                        df_prev_year['Amount_USD_Mn']=df_prev_year['Amount_USD_Mn'].replace("–",np.nan)
                        df_prev_year['Amount_USD_Mn']=df_prev_year['Amount_USD_Mn'].replace("-",np.nan)

                        df_all_dates=pd.DataFrame()
                        df_all_dates=pd.concat([df_cur_mon,df_prev_mon,df_prev_year])
                        df_all_dates.reset_index(drop=True,inplace=True)
                        df_all_dates['Runtime']=datetime.datetime.now()

                        final_df=pd.concat([final_df,df_all_dates])
                        print("Done for link:",link)

                         #>>>>>>>>>>>>>>>>> s3 bucket upload <<<<<<<<<<<<<<<<<<<<<<<<<<<
                        print("s3 upload")
                        ACCESS_KEY_ID = adqvest_s3.s3_cred()[0]
                        ACCESS_SECRET_KEY = adqvest_s3.s3_cred()[1]
                        BUCKET_NAME = 'adqvests3bucket'
                        s3 = boto3.resource(
                               's3',
                               aws_access_key_id=ACCESS_KEY_ID,
                               aws_secret_access_key=ACCESS_SECRET_KEY,
                               config=boto3.session.Config(signature_version='s3v4',region_name = 'ap-south-1')
                         )
                        data_s3 =  open(filename, 'rb')
                        s3.Bucket(BUCKET_NAME).put_object(Key='RBI/RBI_FORIEGN_INVESTMENT_INFLOWS_MONTHLY_DATA/'+filename, Body=data_s3)
                        data_s3.close()
                        os.remove(filename)
                        print("Data pushed to S3")

                    final_df.reset_index(drop=True,inplace=True)
                    final_df.drop_duplicates(keep="first",inplace = True,subset=["Relevant_Date","Item_No","Item","Amount_USD_Mn"])
                    final_df2= final_df[final_df['Relevant_Date']>max_rel_date]
                    try:
                        check_date=final_df2['Relevant_Date'][0]
                        print(check_date)
                    except:
                        pass
                    if final_df2.shape[0]==0:
                        print("No new data came")
                    else:

                        #>>>>>>>>>>.. Deleting and reinserting the data for previous month and year <<<<<<<<<<<<<<<<<<<<
                        prev_mon_date=check_date-relativedelta(months=1)
                        prev_mon_date= datetime.date(prev_mon_date.year, prev_mon_date.month, calendar.monthrange(prev_mon_date.year, prev_mon_date.month)[1])
                        print(prev_mon_date)
                        prev_year_date=check_date-relativedelta(years=1)
                        print(prev_year_date)

                        df_prev_mon=final_df[final_df['Relevant_Date']==prev_mon_date]
                        df_prev_mon.reset_index(drop=True,inplace=True)
                        df_prev_mon.drop_duplicates(keep="first",inplace = True,subset=["Relevant_Date","Item_No","Item"])
                        print("No of records in prev month:",df_prev_mon.shape[0])

                        df_prev_year=final_df[final_df['Relevant_Date']==prev_year_date]
                        df_prev_year.reset_index(drop=True,inplace=True)
                        df_prev_year.drop_duplicates(keep="first",inplace = True,subset=["Relevant_Date","Item_No","Item"])
                        print("No of records in prev year:",df_prev_year.shape[0])

                        df_cur_mon=final_df[final_df['Relevant_Date']==check_date]
                        df_cur_mon.reset_index(drop=True,inplace=True)
                        df_cur_mon.drop_duplicates(keep="first",inplace = True,subset=["Relevant_Date","Item_No","Item"])
                        print("No of records in current month:",df_cur_mon.shape[0])

                        count1=pd.read_sql("SELECT count(*) as Count FROM AdqvestDB.RBI_FORIEGN_INVESTMENT_INFLOWS_MONTHLY_DATA where  Relevant_Date = '"+str(prev_mon_date)+"'",engine)['Count'][0]
                        print(count1)
                        query1 = "Delete from AdqvestDB.RBI_FORIEGN_INVESTMENT_INFLOWS_MONTHLY_DATA where  Relevant_Date = '"+str(prev_mon_date)+"';"
                        engine.execute(query1)
                        df_prev_mon.to_sql(name = "RBI_FORIEGN_INVESTMENT_INFLOWS_MONTHLY_DATA",con = engine,if_exists = 'append',index = False)

                        count2=pd.read_sql("SELECT count(*) as Count FROM AdqvestDB.RBI_FORIEGN_INVESTMENT_INFLOWS_MONTHLY_DATA where  Relevant_Date = '"+str(prev_year_date)+"'",engine)['Count'][0]
                        print(count2)
                        query2= "Delete from AdqvestDB.RBI_FORIEGN_INVESTMENT_INFLOWS_MONTHLY_DATA where  Relevant_Date = '"+str(prev_year_date)+"';"
                        engine.execute(query2)
                        df_prev_year.to_sql(name = "RBI_FORIEGN_INVESTMENT_INFLOWS_MONTHLY_DATA",con = engine,if_exists = 'append',index = False)

                        df_cur_mon.to_sql(name = "RBI_FORIEGN_INVESTMENT_INFLOWS_MONTHLY_DATA",con = engine,if_exists = 'append',index = False)


                        #>>>>>>>>>ClickHouse delete and re-upload <<<<<<<<<<<<<<<<<<<<<<<<<<
                        print("Cickhouse delete and data re upload")

                        click_max_date = client.execute("select max(Relevant_Date) from RBI_FORIEGN_INVESTMENT_INFLOWS_MONTHLY_DATA")
                        click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])

                        query1="ALTER TABLE AdqvestDB.RBI_FORIEGN_INVESTMENT_INFLOWS_MONTHLY_DATA DELETE where Relevant_Date = '"+str(prev_mon_date)+"'"
                        client.execute(query1)
                        query1_2 = 'select * from AdqvestDB.RBI_FORIEGN_INVESTMENT_INFLOWS_MONTHLY_DATA where Relevant_Date = "' + str(prev_mon_date)+ '"'
                        df1 = pd.read_sql(query1_2,engine)
                        client.execute("INSERT INTO RBI_FORIEGN_INVESTMENT_INFLOWS_MONTHLY_DATA VALUES", df1.values.tolist())

                        query2="ALTER TABLE AdqvestDB.RBI_FORIEGN_INVESTMENT_INFLOWS_MONTHLY_DATA DELETE where Relevant_Date = '"+str(prev_year_date)+"'"
                        client.execute(query2)
                        query2_2 = 'select * from AdqvestDB.RBI_FORIEGN_INVESTMENT_INFLOWS_MONTHLY_DATA where Relevant_Date = "' + str(prev_year_date) + '"'
                        df2 = pd.read_sql(query2_2,engine)
                        client.execute("INSERT INTO RBI_FORIEGN_INVESTMENT_INFLOWS_MONTHLY_DATA VALUES", df2.values.tolist())

                        query3 = 'select * from AdqvestDB.RBI_FORIEGN_INVESTMENT_INFLOWS_MONTHLY_DATA where Relevant_Date = "' + str(check_date) + '"'
                        df3 = pd.read_sql(query3,engine)
                        client.execute("INSERT INTO RBI_FORIEGN_INVESTMENT_INFLOWS_MONTHLY_DATA VALUES", df3.values.tolist())
                except:
                    pass
                    print("No new links found")
                time.sleep(5)




        log.job_end_log(table_name,job_start_time, no_of_ping)


    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
