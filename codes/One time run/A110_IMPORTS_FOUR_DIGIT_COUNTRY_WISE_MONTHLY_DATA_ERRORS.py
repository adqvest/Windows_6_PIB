import requests
import sqlalchemy
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import datetime as datetime
from pytz import timezone
import sys
import warnings
import os
from calendar import monthrange
import datetime as datetime
warnings.filterwarnings('ignore')
from dateutil.relativedelta import *
#sys.path.insert(0, '/home/ubuntu/AdQvestDir/Adqvest_Function')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
from dateutil.relativedelta import relativedelta
import ClickHouse_db

def ldm(any_day):
    # this will never fail
    # get close to the end of the month for any day, and add 4 days 'over'
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
    # subtract the number of remaining 'overage' days to get last day of current month, or said programattically said, the previous day of the first of next month
    return next_month - datetime.timedelta(days=next_month.day)

def end_date(year,month):
    end_dt = datetime.datetime(year, month, monthrange(year, month)[1]).date()
    return end_dt
#%%
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
    table_name = 'IMPORTS_FOUR_DIGIT_COUNTRY_WISE_MONTHLY_DATA'
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
#        from clickhouse_driver import Client
        client = ClickHouse_db.db_conn()
        last_date=pd.read_sql("select min(Relevant_Date) as Min from IMPORTS_DATA_HSN_MONTHLY",engine)['Min'][0]
#        exports=pd.read_sql("select HSCode,Commodity,sum(Monthly_Value_INR_In_Lakhs_Current_Year) as Value from EXPORTS_DATA_HSN_MONTHLY where Relevant_Date>='"+str(last_date+relativedelta(years=-3))+"' and HSCode not in ('72091630','27111990','72106100','64031920') group by HSCode",engine)
#        export_top_500=exports.sort_values(by='Value',ascending=False).iloc[1:501]
        q = "Select Distinct Four_Digit_HSCode as HSCode,Four_Digit_Commodity as Commodity from IMPORTS_CLEAN_DATA_HSN_MONTHLY"
        q = "Select Distinct Code as HSCode,Description as Commodity from EXIM_HS_CODE_COMMODITY_PAIR_STATIC  where `Type`='HSN' and LENGTH(toString(Code))=4"
        export_top_500,cols = client.execute(q,with_column_types=True)
        export_top_500 = pd.DataFrame(export_top_500, columns=[tuple[0] for tuple in cols])
        export_top_500.reset_index(drop=True,inplace=True)
        # export_top_500=export_top_500.iloc[477:]
        export_top_500 = export_top_500[export_top_500.iloc[:,0]!='']
#        print(export_top_500)
        date1 = last_date
        date2 = pd.read_sql("select max(Relevant_Date) as Max from IMPORTS_DATA_HSN_MONTHLY",engine)['Max'][0]
        max_date = []
        comm = []
        HScode = []
#        for code,row in export_top_500.iterrows():
#              q = "select max(Relevant_Date) as MAX from AdqvestDB.EXPORTS_FOUR_DIGIT_COUNTRY_WISE_MONTHLY_DATA where HSCode='"+str(export_top_500['HSCode'].iloc[code])+"'"
##              max_rel_date=pd.read_sql(q,engine)['MAX'][0]
#              max_rel_date,cols = client.execute(q,with_column_types=True)
#              max_rel_date = pd.DataFrame(max_rel_date, columns=[tuple[0] for tuple in cols]).iloc[0,0]
##              max_rel_date.reset_index(drop=True,inplace=True)
#
#              max_date.append(max_rel_date)
#              comm.append(export_top_500['Commodity'].iloc[code])
#              HScode.append(export_top_500['HSCode'].iloc[code])
        q = "Select max(Relevant_Date) as Relevant_Date,HSCode,Commodity from IMPORTS_FOUR_DIGIT_COUNTRY_WISE_MONTHLY_DATA group by HSCode,Commodity"
        export_top_500,cols = client.execute(q,with_column_types=True)
        export_top_500 = pd.DataFrame(export_top_500, columns=[tuple[0] for tuple in cols])
        export_top_500.reset_index(drop=True,inplace=True)
#        export_top_500 = pd.DataFrame({"Relevant_Date":max_date,"Commodity":comm,"HSCode":HScode})
        print("DataFrame with Updated Codes is ready")
        export_top_500  = export_top_500.sort_values("Relevant_Date",ascending=False)

        '''

        Similar Code as Exports

        '''

        import_top_500 = export_top_500.copy()


#        all_dates = []
#        while date1<=date2:
#          print(ldm(date1))
#          all_dates.append(date1)
#          date1+=relativedelta(months=1)
        # import_top_500=import_top_500.iloc[396:]
        max_date = pd.read_sql("Select max(Relevant_Date) as RD from AdqvestDB.IMPORTS_DATA_HSN_MONTHLY",engine)['RD'][0]
        if date2 == max_date:
            print("Data Upto Date")
        else:
            all_dates = [date2]

            for date1 in all_dates:
              for code in range(import_top_500.shape[0]):
                  the_code = export_top_500['HSCode'].iloc[code]

                  print(import_top_500['HSCode'].iloc[code])
      #            max_rel_date=pd.read_sql("select max(Relevant_Date) as MAX from AdqvestDB.EXPORTS_DATA_COUNTRY_WISE_MONTHLY where HSCode="+export_top_500['HSCode'].iloc[code],engine)['MAX'][0]
                  max_rel_date = date1#import_top_500['Relevant_Date'].iloc[0]
                  date1 = max_rel_date
                  if(max_rel_date.year)<today.year:
                    continue
                  Latest_Date=max_rel_date#+relativedelta(months=1)
      #            Latest_Date = date1+relativedelta(months=1)
                  Latest_Month=Latest_Date.month
                  Latest_Year=Latest_Date.year
                  Current_Month = date1#max_rel_date.month
                  Current_Year = date1#max_rel_date.year
                  Prev_Date = date1
                  Prev_Month = Prev_Date.month
                  Prev_Year = Prev_Date.year
                  session=requests.Session()
                  headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.100 Safari/537.36"}

                  r=session.get("https://tradestat.commerce.gov.in/meidb/comcntq.asp?ie=i",verify=False)
    #              no_of_ping +=1
                  main_soup=BeautifulSoup(r.content)
                  l1=main_soup.findAll("input")
                  main_data={l1[1]['name']:l1[1]['value'],
                    'Mm1':str(Latest_Month),'yy1':str(Latest_Year),'hscode':str(import_top_500['HSCode'].iloc[code]),'sort':'0',l1[3]['name']:l1[3]['value'],
                   l1[6]['name']:l1[6]['value']}
                  req=session.post("https://tradestat.commerce.gov.in/meidb/comcnt.asp?ie=i",data=main_data,headers=headers,verify=False)
    #              no_of_ping +=1
                  master_soup=BeautifulSoup(req.text,'lxml')
                  try:
                      master=pd.read_html(str(master_soup))[0]
                      master.drop(columns=['%Growth','%Growth.1','S.No.'],axis=1,inplace=True)
                      master=master.iloc[:,:3]
                      master.columns=['Country','Monthly_Value_INR_In_Lakhs_Last_Year','Monthly_Value_INR_In_Lakhs_Current_Year']
                      #if master['Monthly_Value_INR_In_Lakhs_Current_Year'].isnull().all()!=True:
    #                  if today.day>15:
                      for yr in [Latest_Year]:
                          for mon in [Latest_Month]:
                              print(yr)
                              print(mon)
                              data1={l1[1]['name']:l1[1]['value'],
                                'Mm1':str(mon),'yy1':str(yr),'hscode':str(import_top_500['HSCode'].iloc[code]),'sort':'0',l1[3]['name']:l1[3]['value'],
                               l1[6]['name']:l1[6]['value']}
                              req=session.post("https://tradestat.commerce.gov.in/meidb/comcnt.asp?ie=i",data=data1,headers=headers,verify=False)
    #                          no_of_ping +=1
                              inr_soup=BeautifulSoup(req.text,'lxml')
                              inr=pd.read_html(str(inr_soup))[0]
                              inr.drop(columns=['%Growth','%Growth.1','S.No.'],axis=1,inplace=True)
                              inr=inr.iloc[:,:3]
                              inr.columns=['Country','Monthly_Value_INR_In_Lakhs_Last_Year','Monthly_Value_INR_In_Lakhs_Current_Year']

                              data2={l1[1]['name']:l1[1]['value'],
                                'Mm1':str(mon),'yy1':str(yr),'hscode':str(import_top_500['HSCode'].iloc[code]),'sort':'0',l1[4]['name']:l1[4]['value'],
                               l1[6]['name']:l1[6]['value']}
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
                              df_final['Relevant_Date']=end_date(int(yr),int(mon))
                              df_final['Runtime']=datetime.datetime.now()
                              df_final=df_final[df_final['Country']!='Total']
                              df_final.fillna(0, inplace=True)
                              client.execute("INSERT INTO "+table_name+" VALUES", df_final.values.tolist())
                              #client.execute(query)
                              df_final.to_sql(table_name, if_exists="append", index=False, con=engine)

    #                          try:
    #                            try:
    #                                q = "Select max(Runtime) as RT from AdqvestDB."+table_name+" where Relevant_Date ='"+str(Latest_Date)+"' and HSCode ='"+the_code+"';"
    #                                max_rt,cols = client.execute(q,with_column_types=True)
    #                                max_rt = pd.DataFrame(max_rt, columns=[tuple[0] for tuple in cols]).iloc[0,0]
    #    #                            max_rt.reset_index(drop=True,inplace=True)
    #                                query="ALTER TABLE AdqvestDB."+table_name+" delete  where Runtime = '" +str(max_rt)+"' and  Relevant_Date ='"+str(Latest_Date)+"';"
    #                                client.execute("INSERT INTO "+table_name+" VALUES", df_final.values.tolist())
    #                                client.execute(query)
    #                            except:
    #                                raise Exception("Data not updated on CH")
    #
    #                            q = "Select max(Runtime) as RT from AdqvestDB."+table_name+" where Relevant_Date ='"+str(Latest_Date)+"' and HSCode ='"+the_code+"';"
    #                            max_rt = pd.read_sql(q,con=engine).iloc[0,0]
    #    #                        query="ALTER TABLE AdqvestDB."+table_name+" delete  where Runtime = '" +str(max_rt)+"'"
    #                            query = "DELETE from AdqvestDB."+table_name+" where Runtime = '" +str(max_rt)+"' and  Relevant_Date ='"+str(Latest_Date)+"';"
    #                            df_final.to_sql(table_name, if_exists="append", index=False, con=engine)
    #                            connection.execute(query)
    #                            connection.execute("commit")
    #                            print(df_final.head(2))
    #                          except:
    #                            break
    #                else:
    #                      for prev_yr in [Prev_Year]:
    #                          for prev_mon in [Prev_Month]:
    #
    #                              print(prev_yr)
    #                              print(mon)
    #                              data1={l1[1]['name']:l1[1]['value'],
    #                                'Mm1':str(prev_mon),'yy1':str(prev_yr),'hscode':str(import_top_500['HSCode'].iloc[code]),'sort':'0',l1[3]['name']:l1[3]['value'],
    #                               l1[6]['name']:l1[6]['value']}
    #                              req=session.post("https://tradestat.commerce.gov.in/meidb/comcnt.asp?ie=i",data=data1,headers=headers,verify=False)
    #                              no_of_ping +=1
    #                              inr_soup=BeautifulSoup(req.text,'lxml')
    #                              inr=pd.read_html(str(inr_soup))[0]
    #                              inr.drop(columns=['%Growth','%Growth.1','S.No.'],axis=1,inplace=True)
    #                              inr=inr.iloc[:,:3]
    #                              inr.columns=['Country','Monthly_Value_INR_In_Lakhs_Last_Year','Monthly_Value_INR_In_Lakhs_Current_Year']
    #
    #                              data2={l1[1]['name']:l1[1]['value'],
    #                                'Mm1':str(prev_mon),'yy1':str(prev_yr),'hscode':str(import_top_500['HSCode'].iloc[code]),'sort':'0',l1[4]['name']:l1[4]['value'],
    #                               l1[6]['name']:l1[6]['value']}
    #                              req=session.post("https://tradestat.commerce.gov.in/meidb/comcnt.asp?ie=i",data=data2,headers=headers,verify=False)
    #                              no_of_ping +=1
    #                              usd_soup=BeautifulSoup(req.text,'lxml')
    #                              usd=pd.read_html(str(usd_soup))[0]
    #                              usd.drop(columns=['%Growth','%Growth.1','S.No.'],axis=1,inplace=True)
    #                              usd=usd.iloc[:,:3]
    #                              usd.columns=['Country','Monthly_Value_USD_In_Million_Last_Year','Monthly_Value_USD_In_Million_Current_Year']
    #
    #
    #                              data3={l1[1]['name']:l1[1]['value'],
    #                                'Mm1':str(prev_mon),'yy1':str(prev_yr),'hscode':str(import_top_500['HSCode'].iloc[code]),'sort':'0',l1[5]['name']:l1[5]['value'],
    #                               l1[6]['name']:l1[6]['value']}
    #                              req=session.post("https://tradestat.commerce.gov.in/meidb/comcnt.asp?ie=i",data=data3,headers=headers,verify=False)
    #                              no_of_ping +=1
    #                              qty_soup=BeautifulSoup(req.text,'lxml')
    #                              qty=pd.read_html(str(qty_soup))[0]
    #                              qty.drop(columns=['%Growth','%Growth.1','S.No.'],axis=1,inplace=True)
    #                              qty=qty.iloc[:,:3]
    #                              qty.columns=['Country','Monthly_Volume_In_Thousands_Last_Year','Monthly_Volume_In_Thousands_Current_Year']
    #
    #                              df_final1=pd.merge(inr,usd,on='Country',how='left')
    #                              df_final=pd.merge(df_final1,qty,on='Country',how='left')
    #                              df_final['HSCode']=str(import_top_500['HSCode'].iloc[code])
    #                              df_final['Commodity']=import_top_500['Commodity'].iloc[code]
    #                              df_final=df_final[['HSCode','Commodity','Country','Monthly_Value_INR_In_Lakhs_Last_Year','Monthly_Value_INR_In_Lakhs_Current_Year',
    #                                                'Monthly_Value_USD_In_Million_Last_Year','Monthly_Value_USD_In_Million_Current_Year',
    #                                                'Monthly_Volume_In_Thousands_Last_Year','Monthly_Volume_In_Thousands_Current_Year']]
    #                              df_final['Relevant_Date']=end_date(int(prev_yr),int(prev_mon))
    #                              df_final['Runtime']=datetime.datetime.now()
    #                              df_final=df_final[df_final['Country']!='Total']
    #                              try:
    #                                try:
    #                                    q = "Select max(Runtime) as RT from AdqvestDB."+table_name+" where Relevant_Date ='"+str(Prev_Date)+"' and HSCode ='"+the_code+"';"
    #                                    max_rt,cols = client.execute(q,with_column_types=True)
    #                                    max_rt = pd.DataFrame(max_rt, columns=[tuple[0] for tuple in cols]).iloc[0,0]
    #        #                            max_rt.reset_index(drop=True,inplace=True)
    #                                    query="ALTER TABLE AdqvestDB."+table_name+" delete  where Runtime = '" +str(max_rt)+"' and  Relevant_Date ='"+str(Prev_Date)+"';"
    #                                    print(query)
    #                                    client.execute("INSERT INTO "+table_name+" VALUES", df_final.values.tolist())
    #                                    client.execute(query)
    #                                except:
    #                                    raise Exception("Data not updated on CH")
    #
    #                                q = "Select max(Runtime) as RT from AdqvestDB."+table_name+" where Relevant_Date ='"+str(Prev_Date)+"' and HSCode ='"+the_code+"';"
    #                                max_rt = pd.read_sql(q,con=engine).iloc[0,0]
    #        #                        query="ALTER TABLE AdqvestDB."+table_name+" delete  where Runtime = '" +str(max_rt)+"'"
    #                                query = "DELETE from AdqvestDB."+table_name+" where Runtime = '" +str(max_rt)+"' and  Relevant_Date ='"+str(Prev_Date)+"';"
    #                                print(query)
    #                                df_final.to_sql(table_name, if_exists="append", index=False, con=engine)
    #                                connection.execute(query)
    #                                connection.execute("commit")
    #                                print(df_final.head(2))
    #                              except:
    #                                break
    #                              print(df_final.head(2))
    #                  else:
    #                      print("No Data Available")
    #                      break
                  except ValueError:
                      print("No Tables Found")


                # Latest_Date=Latest_Date+relativedelta(months=1)
                # Latest_Month=Latest_Date.month
                # Latest_Year=Latest_Date.year

#%%
        log.job_end_log(table_name, job_start_time, no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'", str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name, job_start_time, error_type, error_msg, no_of_ping)


if(__name__=='__main__'):
    run_program(run_by='manual')
