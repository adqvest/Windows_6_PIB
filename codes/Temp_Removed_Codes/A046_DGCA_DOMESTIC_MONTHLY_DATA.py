# -*- coding: utf-8 -*-
"""

@author: Rahul
"""

import pandas as pd
import os
from pandas.tseries.offsets import MonthEnd
import requests
import camelot

from bs4 import BeautifulSoup
import re
import datetime as datetime
from pytz import timezone
import sys
import warnings
import time
from selenium.webdriver.common.by import By
warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
import numpy as np
import dbfunctions
from selenium import webdriver
import ClickHouse_db
from adqvest_robotstxt import Robots
robot = Robots(__file__)

def airline_mapping(df,mapping_df):
    df.insert(1,'Airline_Clean','')
    df['Airline_Temp']=df['Airline']
    df['Airline_Temp']=df['Airline_Temp'].str.replace(r'\s\s+',' ')
    df['Airline_Temp']=df['Airline_Temp'].apply(lambda x:x.replace(' ',''))
    df['Airline_Temp']=df['Airline_Temp'].str.strip()
    df['Airline_Temp'] = df['Airline_Temp'].apply(str.upper)
    df['Airline_Clean']=df.merge(mapping_df,how='left',left_on='Airline_Temp',right_on='Input')['Output']
    del df['Airline_Temp']

    return df

def get_desired_table(tables,search_str):
    final_df=pd.DataFrame()
    for st in search_str:
        for i in range(tables.n):
            for col in tables[i].df.columns:
                if tables[i].df[col].str.lower().str.contains(st.lower(),regex=True).any():
                    tb=i
                    rcol=col
                    df=tables[i].df.copy()
                    final_df=pd.concat([final_df,df])
                    final_df.drop_duplicates(keep='first', inplace=True)
                    break
    return final_df

def get_desired_table2(tables,search_str):
    final_df=pd.DataFrame()
    for st in search_str:
        for i in range(tables.n):
            for col in tables[i].df.columns:
                if tables[i].df[col].str.lower().str.contains(st.lower(),regex=True).any():
                    tb=i
                    rcol=col
                    df=tables[i+1].df.copy()
                    final_df=pd.concat([final_df,df])
                    final_df.drop_duplicates(keep='first', inplace=True)
                    break
    return final_df



no_of_ping = 0
def run_program(run_by='Adqvest_Bot', py_file_name=None):
    global no_of_ping
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    engine = adqvest_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'DGCA_DOMESTIC_MONTHLY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''


    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        max_rel_date = pd.read_sql('SELECT MAX(Relevant_Date) as max_date FROM DGCA_DOMESTIC_MONTHLY_DATA', con=engine)['max_date'][0]
        print(max_rel_date)

        url='https://www.dgca.gov.in/digigov-portal/?page=4264/4206/sericename'
        options = webdriver.ChromeOptions()

        options.add_argument("--disable-infobars")
        options.add_argument("start-maximized")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-notifications")
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--no-sandbox')
        
        chrome_driver = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
        driver = webdriver.Chrome(executable_path=chrome_driver, chrome_options=options)

        driver.get(url)
        robot.add_link(url)
        driver.maximize_window()
        time.sleep(10)
        soup=BeautifulSoup(driver.page_source)

        time.sleep(5)

        driver.close()

        table_soup=soup.find_all('table',attrs={'id':'DataTables_Table_0'})[0]

        year=table_soup.find_all('tr',attrs={'role':'row'})[0].find('td',attrs={'rowspan':'1'}).get_text()

        soup2=table_soup.find_all('tr',attrs={'role':'row'})
        soup2=soup2[:-1]

        months=[]
        links=[]
        for i in soup2:
            link=i.find_all('td',attrs={'font face':''})[1].find('a')['data-url']
            print(link)
            links.append(link)
            month=i.find_all('td',attrs={'font face':''})[1].get_text()
            months.append(month)

        links = ["https://public-prd-dgca.s3.ap-south-1.amazonaws.com" + x.split("dgca")[-1] for x in links if (".pdf" in x)]
        print(links[-1])
        links_df=pd.DataFrame(list(zip(months,links)),columns=['Month','Link'])

        links_df['Year']=links_df['Link'].apply(lambda x:x.split("/")[-1].split(".pdf")[0])

        links_df['Year']=links_df['Year'].apply(lambda x:re.findall(r'\d+',x)[0])

        years=["20"+yr if len(yr)<4 else yr for yr in links_df['Year']]

        links_df['Year']=years

        # links_df['Year']=year

        links_df['Relevant_Date']=links_df['Month']+"-"+links_df['Year']
        links_df['Relevant_Date']=pd.to_datetime(links_df['Relevant_Date'],format="%B-%Y")+ MonthEnd(1)

        links_df['Relevant_Date']=links_df['Relevant_Date'].apply(lambda x:x.date())

        del links_df['Month']
        del links_df['Year']
        print(links_df)

        links_df=links_df[links_df['Relevant_Date']>max_rel_date]
        print("------------")
        print(links_df)

        if links_df.empty:
            print("No new data")

        else:
            

            max_link_date=max(links_df['Relevant_Date'])

            links_df=links_df[links_df['Relevant_Date']==max_link_date]



        airline_mapping_df = pd.read_sql("select * FROM GENERIC_DICTIONARY_TABLE where Input_Table='DGCA_DOMESTIC_MONTHLY_DATA';", engine)
        if links_df.empty:
            print("No new data")
        else:

            for _,i in links_df.iterrows():
                rel_date=i['Relevant_Date']
                print(rel_date)
                link=i['Link']
                link=link.replace(" ","%20")
                print(link)
                r=requests.get(link)
                os.chdir(r'C:\Users\Administrator\Junk2\\')
                path=os.getcwd()
                file_name = 'DGCA_DOMESTIC_MONTHLY_DATA' + str(rel_date).replace('-', '_') + '.pdf'
                print(file_name)
                with open(path+file_name, 'wb') as f:
                    f.write(r.content)
                    f.close()

                key = 'DGCA/DOMESTIC/'
                file_path=path+file_name
                dbfunctions.to_s3bucket(file_path, key)

                tables=camelot.read_pdf(link,pages='1-end',process_background=True,line_scale=40,shift_text=[''],strip_text=[',','-','\n','%'])

                df=get_desired_table(tables,['market share'])
                index=[]
                for i in range(df.shape[1]):
                    # if df.iloc[:,i].str.lower().str.contains('month &').any():
                    if df.iloc[:,i].str.lower().str.contains('month').any():
                        col_index=i
                        # row_index=df[df.iloc[:, col_index].str.lower().str.contains('month &') == True].index[0]
                        row_index=df[df.iloc[:, col_index].str.lower().str.contains('month') == True].index[0]
                        index.append(row_index)
                        index.append(col_index)
                        
                        break
                        
                col_idx=index[1]
                row_idx=index[0]
                df=df.iloc[row_idx:,col_idx:]   

                # df=df.replace('',np.nan)
                df[df.columns[0]]=df[df.columns[0]].replace('',np.nan)
                df[df.columns[0]]=df[df.columns[0]].fillna(method='bfill',limit=1)
                df[df.columns[0]]=df[df.columns[0]].fillna(method='ffill',limit=1)
                df.reset_index(drop=True,inplace=True)
                df.iloc[row_idx-1,col_idx+1]='Variable'

                df.dropna(thresh=7,inplace=True,axis=0)
                df.columns=df.iloc[1,:]
                df=df.iloc[2:,:]
                df.reset_index(drop=True,inplace=True)

                new_col=[]
                for col in list(df.columns):
                    if col=='':
                        col='Variable'
                    col=col.replace('Month & Year','Relevant_Date')
                    col=col.replace("Month &",'Relevant_Date')
                    col=col.replace("Year",'Relevant_Date')
                    new_col.append(col)
                    
                df.columns=new_col
                df['Relevant_Date'] = df['Relevant_Date'].fillna(method='bfill').fillna(method='ffill') 

                str_list=['qtr','total']
                drop_idx=[]
                for i in str_list:
                    idx=df[df.iloc[:,col_idx].str.lower().str.contains(i.lower(),regex=True)==True].index
                    for j in idx:
                        drop_idx.append(j)
                   
                df.drop(index=drop_idx,inplace=True)
                df.reset_index(drop=True,inplace=True)

                df1=df[df['Variable']=='Pax Carried']
                df2=df[df['Variable']=='Market Share']

                df1=pd.melt(df1,[df1.columns[0],df1.columns[1]],value_vars=df1.columns[2:],var_name='Airline', value_name='Pax_Carried_Lakhs')
                df2=pd.melt(df2,[df2.columns[0],df2.columns[1]],value_vars=df2.columns[2:],var_name='Airline', value_name='Market_Share_Pct')

                df1.drop(columns=['Variable'],inplace=True)
                df2.drop(columns=['Variable'],inplace=True)

              
                df_final=pd.merge(df1,df2,on=['Relevant_Date','Airline'],how='inner')
                os.chdir(r'C:\Users\Administrator\Junk2\\')
                df_final.to_excel('Market_share.xlsx')

                #---Load factor
                df=get_desired_table2(tables,['denied boarding'])
                df.reset_index(drop=True,inplace=True)

                index=[]
                for i in range(df.shape[1]):
                    if df.iloc[:,i].str.lower().str.contains('month').any():
                        col_index=i
                        row_index=df[df.iloc[:, col_index].str.lower().str.contains('month') == True].index[0]
                        index.append(row_index)
                        index.append(col_index)
                        
                        break
                        
                col_idx=index[1]
                row_idx=index[0]
                df=df.iloc[row_idx:,col_idx:]

                df=df.replace('',np.nan)
                df.reset_index(drop=True,inplace=True)

                df.dropna(thresh=7,inplace=True,axis=0)
                df.columns=df.iloc[0,:]
                df=df.iloc[1:,:]
                df.reset_index(drop=True,inplace=True)
                df=df.rename(columns={'Month':'Relevant_Date'})

                df1=pd.melt(df,[df.columns[0]],value_vars=df.columns[1:],var_name='Airline', value_name='Passenger_Load_Factor_Pct')
                
                df_final2=pd.merge(df_final,df1,on=['Relevant_Date','Airline'],how='inner')

                df_final2['Corridor']='Domestic'
                df_final2['Runtime']=datetime.datetime.now()

                df_final2['Airline']=df_final2['Airline'].str.replace('\*','')
                df_final2=df_final2.rename(columns={'Pax_Carried_Lakh':'Pax_Carried_Lakhs'})

                df_final2['Relevant_Date']=df_final2['Relevant_Date']+"-"+str(year)
                try:
                    df_final2['Relevant_Date']=pd.to_datetime(df_final2['Relevant_Date'],format="%B-%Y")+ MonthEnd(1)
                except:
                    df_final2['Relevant_Date']=pd.to_datetime(df_final2['Relevant_Date'],format="%b-%Y")+ MonthEnd(1)
                
                df_final2['Relevant_Date']=df_final2['Relevant_Date'].apply(lambda x:x.date())
                print(df_final2.shape)
                print(max(df_final2['Relevant_Date']))
                



                final_clean=airline_mapping(df_final2,airline_mapping_df)

                final_clean=final_clean[final_clean['Relevant_Date']>max_rel_date]
                final_clean.drop_duplicates(keep='last')

                final_clean.to_sql("DGCA_DOMESTIC_MONTHLY_DATA", index=False, if_exists='append', con=engine)

                # os.remove(file_path)





        log.job_end_log(table_name,job_start_time, no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)


if(__name__=='__main__'):
    run_program(run_by='manual')
