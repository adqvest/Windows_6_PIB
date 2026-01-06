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
requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = 'ALL:@SECLEVEL=1'
from csv import reader
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
#%%
warnings.filterwarnings('ignore')
#%%
# sys.path.insert(0, '/home/ubuntu/AdQvestDir/Adqvest_Function')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
#sys.path.insert(0, r'C:\Adqvest')
import numpy as np
import adqvest_db
import JobLogNew as log
from adqvest_robotstxt import Robots

robot = Robots(__file__)
from Cleaner_cibil_crif_equifax import full_clean
from Cleaner_cibil_crif_equifax import clean_company
from Cleaner_cibil_crif_equifax import clean_location
from Cleaner_cibil_crif_equifax import clean_bnk_br_st_ad


engine = adqvest_db.db_conn()
# d = datetime.datetime.now()
#%%
engine = adqvest_db.db_conn()
connection = engine.connect()
#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday =  today - days

#%%
my_dictionary={
     'lacs':{'st_ut':'lakhAccount','dt_rng':'quarterIdLakh','clk':4,'cat':'25 Lacs and above','file_type':"1"},
     'crore':{'st_ut':'croreAccount','dt_rng':'quarterIdCrore','clk':3,'cat':'1 Cr and above','file_type':"2"}
     }

def drop_duplicates(df):
    columns = df.columns.to_list()
    columns.remove('Runtime')
    df = df.drop_duplicates(subset=columns, keep='last')
    return df

def clean_values(x):
    x=float(str(x).replace(',',''))
    return x

def Upload_Data_mysql(table_name, data, category):
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    
    query=f"select max(Relevant_Date) as Max from {table_name} where Category='{category}'"
    db_max_date = pd.read_sql(query,engine)["Max"][0]

    if db_max_date==None:
        db_max_date=pd.to_datetime('2014-01-31', format='%Y-%m-%d').date()
        
    data=data.loc[data['Relevant_Date'] > db_max_date]
    data.to_sql(table_name, con=engine, if_exists='append', index=False)
    print('Data loded in mysql')

def get_page_content(url,cate_type='',layer_1=False,layer_2=False,layer_3=False,layer_4=False,rel_month='',st_row='',bnk_id=''):
    driver_path = r'C:\Users\Administrator\AdQvestDir\chromedriver.exe'
    download_path = r"C:\Users\Administrator\Junk_One_Time"
    
    prefs = {
        "download.default_directory": download_path,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True
    }

    options = webdriver.ChromeOptions()
    options.add_argument("start-maximized")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--disable-blink-features")
    options.add_argument("--disable-blink-features=AutomationControlled")
    driver = webdriver.Chrome(executable_path=driver_path, chrome_options=options)
    driver.get(url)
    time.sleep(5)

    if layer_1==True:
       soup=BeautifulSoup(driver.page_source)
       return soup
    if layer_2==True:
        elem1=driver.find_element(By.XPATH, f'//*[@id="{my_dictionary[cate_type]["st_ut"]}"]/option[3]')
        time.sleep(3)
        elem1.click()

        dropdown_element = driver.find_element(By.ID, f'{my_dictionary[cate_type]["dt_rng"]}') 
        dropdown = Select(dropdown_element)
        dropdown.select_by_value(rel_month)
        
        elem3=driver.find_element(By.XPATH, f'//*[@id="loadSuitFiledDataSearchAction"]/div[1]/div[{my_dictionary[cate_type]["clk"]}]/div[4]/img')
        time.sleep(3)
        elem3.click()
        time.sleep(5)
        soup1=BeautifulSoup(driver.page_source)
        df=pd.read_html(driver.page_source)
       
        return df,soup1

def update_status_table(record_df):
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    for index,row in record_df.iterrows():
        update_query = f"""UPDATE CIBIL_INST_WISE_DEFAULTERS_CNT_N_OS_AMOUNT_MONTHLY_RAW_DATA
               set Category='{row['Category']}',Record_No='{row['Record_No']}',
               Credit_Institution_Type='{row['Credit_Institution_Type']}',Credit_Institution='{row['Credit_Institution']}',
               Runtime='{row['Runtime']}',Outstanding_Amt_Lacs='{row['Outstanding_Amt_Lacs']}',
               WHERE Relevant_Date ='{row['Relevant_Date']}'"""

        engine.execute(update_query)
                          
#%%
def run_program(run_by='Adqvest_Bot', py_file_name=None):

    # os.chdir('/home/ubuntu/AdQvestDir')
    os.chdir('C:/Users/Administrator/AdQvestDir')
    

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'CIBIL_INST_WISE_DEFAULTERS_CNT_N_OS_AMOUNT_MONTHLY_RAW_DATA'
    scheduler = ''
    no_of_ping = 0

    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)\

        
        os.chdir('C:/Users/Administrator/CIBIL_EQUIFAX_CRIF')
        driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
        download_path=os.getcwd()
        url = 'https://suit.cibil.com/loadSuitFiledDataSearchAction'
        robot.add_link(url)
        print('-----------------------------')


#%%
        soup=get_page_content(url,layer_1=True)
       
      
        #%%
        
        for ct in my_dictionary.keys():
            engine = adqvest_db.db_conn()
            connection = engine.connect()
            #%%
            # print(my_dictionary[ct]['cat'])
            print(f"Working on--{my_dictionary[ct]['cat']}")
            
            if today.day==14:
                Revise_old_records=True
            else:
                Revise_old_records=False


            alreday_col_dates = pd.read_sql(f"select Distinct Relevant_Date as Relevant_Date from CIBIL_INST_WISE_DEFAULTERS_CNT_N_OS_AMOUNT_MONTHLY_RAW_DATA where  Category ='{my_dictionary[ct]['cat']}' order by Relevant_Date desc limit 5",engine)
            alreday_col_dates['Relevant_Date']=alreday_col_dates['Relevant_Date'].apply(lambda x:str(x))
            base_date=pd.to_datetime('2014-01-31',format='%Y-%m-%d').date()
            
            db_max_dt = pd.read_sql(f"select max(Relevant_Date) as Max from CIBIL_INST_WISE_DEFAULTERS_CNT_N_OS_AMOUNT_MONTHLY_RAW_DATA where Category ='{my_dictionary[ct]['cat']}'",engine)["Max"][0]

            date_id = soup.find("select",{"id":f"{my_dictionary[ct]['dt_rng']}"}).find_all("option")
            date_id = [x for x in date_id if "select" not in x.text.lower()]
            

            if Revise_old_records==True:
                date_id_val={x['value']:pd.to_datetime(x.text).date() for x in date_id if pd.to_datetime(x.text).date()>base_date}

            else:
                # date_id_val={x['value']:pd.to_datetime(x.text).date() for x in date_id if ((pd.to_datetime(x.text).date()>=db_max_dt))}
                date_id_val={x['value']:pd.to_datetime(x.text).date() for x in date_id if (str(pd.to_datetime(x.text).date())  in alreday_col_dates.Relevant_Date.to_list() or  (pd.to_datetime(x.text).date()>db_max_dt))}

            print(date_id_val)
            #%%
            if len(date_id_val)>0:
                for dt_id,dt_val in date_id_val.items():
                    #%%
                    dt_val=date_id_val[dt_id]
                    
                    print(f"Working on--{dt_val}")

                    df=pd.DataFrame()
                    page_obj=get_page_content(url,cate_type=ct,layer_2=True,rel_month=dt_id)
                    soup1=page_obj[1]
                    print(soup1)
            
                    if (('No Records' in str(soup1)) & (today.day<10)):
                        break

                    df=page_obj[0][1]
                    # df=get_page_content(url,cate_type=ct,layer_2=True,rel_month=dt_id)[0][1]

                    
                    df['Credit_Institution_Type']=df.iloc[:,-1]
                    df.dropna(axis=0,how='all',inplace=True)
                    df['Credit_Institution_Type']=df['Credit_Institution_Type'].ffill(axis=0)
                    df.drop(df.columns[[3,4,5]], axis=1, inplace=True)
                    df.columns=['Credit_Institution','Record_No','Outstanding_Amt_Lacs','Credit_Institution_Type']
                    df.drop(df[(df['Credit_Institution'] == df['Record_No'])].index, inplace = True)
                    df.drop(df[(df['Credit_Institution'] == 'Total')].index, inplace = True)
                    df['Outstanding_Amt_Lacs']=df['Outstanding_Amt_Lacs'].apply(lambda x:clean_values(x))
                    df['Credit_Institution_Type']=np.where(df['Credit_Institution']=='Grand Total',np.nan,df['Credit_Institution_Type'])

                    df['Relevant_Date'] = dt_val
                    df['Category'] = str(my_dictionary[ct]['cat'])
                    df["Runtime"] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
                    # print(df)
                    
                    #%%
                    if str(dt_val) in alreday_col_dates.Relevant_Date.to_list():
                         engine.execute(f"Delete from CIBIL_INST_WISE_DEFAULTERS_CNT_N_OS_AMOUNT_MONTHLY_RAW_DATA where Relevant_Date='{dt_val}' and Category ='{my_dictionary[ct]['cat']}'")
                         engine.execute(f"Delete from CIBIL_INST_WISE_DEFAULTERS_CNT_N_OS_AMOUNT_MONTHLY_CLEAN_DATA where Relevant_Date='{dt_val}' and Category ='{my_dictionary[ct]['cat']}'")
                         print(f"Delete from CIBIL_INST_WISE_DEFAULTERS_CNT_N_OS_AMOUNT_MONTHLY_CLEAN_DATA where Relevant_Date='{dt_val}' and Category ='{my_dictionary[ct]['cat']}'")

                    
                    df.to_sql('CIBIL_INST_WISE_DEFAULTERS_CNT_N_OS_AMOUNT_MONTHLY_RAW_DATA', con=engine, if_exists='append', index=False)
                    print(df)
                        
            # print('--------------------')
            #%%
            raw_df = pd.read_sql(f"select * from CIBIL_INST_WISE_DEFAULTERS_CNT_N_OS_AMOUNT_MONTHLY_RAW_DATA where  Category ='{my_dictionary[ct]['cat']}'",engine)
            raw_df['Credit_Institution']=raw_df['Credit_Institution'].apply(lambda x:str(x).upper().strip())

            bank_lookup= pd.read_sql('Select distinct Credit_Institution,Credit_Institution_Clean from CIBIL_EQUIFAX_CRIF_DEFAULTERS_MAPPING_STATIC where Credit_Institution is not null',engine)
            raw_df=pd.merge(raw_df, bank_lookup[['Credit_Institution','Credit_Institution_Clean']],on='Credit_Institution',how='left')
            raw_df['Credit_Institution_Clean']=np.where((raw_df['Credit_Institution_Clean'].isna()),raw_df['Credit_Institution'].apply(lambda x:clean_bnk_br_st_ad(str(x))),raw_df['Credit_Institution_Clean'])

            clean_df = raw_df[['Credit_Institution_Type','Credit_Institution','Credit_Institution_Clean','Record_No','Outstanding_Amt_Lacs', 'Category', 'Relevant_Date', 'Runtime']]
            clean_df=drop_duplicates(clean_df)
            Upload_Data_mysql('CIBIL_INST_WISE_DEFAULTERS_CNT_N_OS_AMOUNT_MONTHLY_CLEAN_DATA',clean_df,str(my_dictionary[ct]['cat']))


#%%
        log.job_end_log(table_name,job_start_time,no_of_ping)

    except:
        try:
            connection.close()
        except:
            pass
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)


        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
