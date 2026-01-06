

import pandas as pd
import camelot
import warnings
warnings.filterwarnings('ignore')
from dateutil import parser
import numpy as np
import sqlalchemy
from pandas.io import sql
from bs4 import BeautifulSoup
import calendar
import datetime as datetime
from pytz import timezone
import requests
import sys
import time
import os
import re
import sqlalchemy
import numpy as np
from selenium import webdriver
from pandas.core.common import flatten
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
engine = adqvest_db.db_conn()
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

def get_table_max_date_and_links(Table_Name,Company_Name):
    max_date=pd.read_sql("SELECT max(Relevant_Date) as Max from "+Table_Name+" where Bank='"+Company_Name+"'",engine)['Max'][0]
    links = pd.read_sql("SELECT * FROM BANK_BASEL_III_QUARTERLY_LINKS WHERE Relevant_Date > '"+str(max_date)+"' and Bank ='"+Company_Name+"' order by Relevant_Date",engine)

    return max_date,links

def extract_tables_from_pdf(pdf_url, new_line,flavor, row_tol, edge_tol,line_scale):
    if flavor=='stream' and new_line==False:
        tables = camelot.read_pdf(pdf_url,pages='1-end', flavor=flavor, row_tol=row_tol, edge_tol=edge_tol,strip_text=[',','-','\n','%',''])
        return tables
    elif flavor=='stream' and new_line==True:
        tables = camelot.read_pdf(pdf_url,pages='1-end', flavor=flavor, row_tol=row_tol, edge_tol=edge_tol,strip_text=[',','-','%',''])
        return tables
    else:
        tables=camelot.read_pdf(pdf_url,pages='1-end',line_scale=line_scale,strip_text=[',','-','\n','%',''])
        return tables
    
def clean_table(df):
    try:

        df['Industry']=df['Industry'].str.replace("^[A-Z]\.","")
        df['Sub_Industry']=df['Sub_Industry'].str.replace("^[A-Z]\.","")
        df['Industry']=df['Industry'].str.replace("^[a-z]\.","")
        df['Sub_Industry']=df['Sub_Industry'].str.replace("^[a-z]\.","")
        df['Industry']=df['Industry'].str.replace("\.","")
        df['Sub_Industry']=df['Sub_Industry'].str.replace("\.","")
        df['Industry']=df['Industry'].str.replace("\d+","")
        df['Sub_Industry']=df['Sub_Industry'].str.replace("\d+","")
        df['Industry']=df['Industry'].str.lower().str.replace('of which',' ')
        df['Sub_Industry']=df['Sub_Industry'].str.lower().str.replace('of which',' ')
        df['Sub_Industry']=df['Sub_Industry'].str.replace(r'\s\s+',' ')
        df['Sub_Industry']=df['Sub_Industry'].str.strip()

    except:
        pass
    col=df.columns[1]
    df[col]=df[col].str.lower().str.replace('\)',' ').str.replace('\(',' ')
    df[col]=df[col].str.lower().str.replace('\*',' ')
    df[col]=df[col].str.lower().str.replace('\#',' ').str.replace('\+',' ')
    df[col]=df[col].str.lower().str.replace('\^',' ')
    df[col]=df[col].str.strip()
    df[col]=df[col].str.lower().str.replace('&',' and ').str.replace('/',' or ').str.replace('\*',' ')
    df[col]=df[col].str.capitalize()
    df[col]=df[col].str.replace(r'\s\s+',' ')
    df[col]=df[col].str.strip()
    return df
def search(heading,end_text,file_name,sheets):
    df = pd.DataFrame()
    cond = False
    for sheet in sheets:
            data = pd.read_excel(file_name,sheet_name = sheet,header=None)
            text = data.fillna('').to_string()
            text = re.sub(r'  +',' ',text).strip()
            if(re.search(heading, text, re.IGNORECASE)):
                data_rows = data.shape[0]
                data.reset_index(inplace=True,drop=True)
                data = data.replace('',np.nan)
                data.dropna(axis=1,how='all',inplace=True)
                df = pd.concat([df,data])
                cond = True
                continue
            if(cond):
                data_rows = data.shape[0]
                data.reset_index(inplace=True,drop=True)
                data = data.replace('',np.nan)
                if(re.search(end_text, text, re.IGNORECASE)):
                    print(sheet)
                    print("stopped")
                    break
                data.dropna(axis=1,how='all',inplace=True)
                df = pd.concat([df,data])
    return df

def read_data_using_ilovepdf(link,bank):
    headers={'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36'}
    r = requests.get(link, headers = headers, verify = False, timeout = 60)
    print(r)
    bank=bank.replace(' ','_')
    print(bank)
    file_name_pdf=r'C:\Users\Administrator\Junk_One_Time\BANK_'+str(bank)+'.pdf'
    file_name_xlsx=r'C:\Users\Administrator\Junk_One_Time\BANK_'+str(bank)+'.xlsx'
    os.chdir(r"C:\Users\Administrator\Junk_One_Time")
    try:
        os.remove(file_name_pdf)
        os.remove(file_name_xlsx)
    except:
        pass
    # driver_path = r"D:\Adqvest\chromedriver.exe"
    # download_path = r"D:\Adqvest\DATA_COLLECTIONS"
    driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
    download_path = r"C:\Users\Administrator\Junk_One_Time"
    # driver_path = r"D:\Adqvest\chrome_path\chromedriver.exe"
    # download_path = r"D:\Adqvest\Junk"
    prefs = {
        "download.default_directory": download_path,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True
        }

    options = webdriver.ChromeOptions()
    options.add_experimental_option('prefs', prefs)
    driver = webdriver.Chrome(executable_path=driver_path,chrome_options=options)


    # driver.get("https://www.epfindia.gov.in" + link.split("..")[-1])
    time.sleep(5)
    driver.get("https://www.ilovepdf.com/")
    driver.maximize_window()
    time.sleep(2)
    driver.find_element(By.XPATH,"//*[contains(text(),'Login')]").click()
    time.sleep(2)
    email = driver.find_element(By.XPATH,"//*[@id='loginEmail']")
    email.send_keys("kartmrinal101@outlook.com")
    password = driver.find_element(By.XPATH,"//*[@id='inputPasswordAuth']")
    password.send_keys("zugsik-zuqzuH-jyvno4")
    time.sleep(1)
    driver.find_element(By.XPATH,"//*[@id='loginBtn']").click()
    time.sleep(1)
    driver.find_element(By.XPATH,"//*[contains(text(),'PDF to Excel')]").click()
    time.sleep(1)

    r = requests.get(link, headers = headers, verify = False, timeout = 60)
    #     robot.add_link(link)
    print(r)
    file_name='BANK_'+str(bank)+'.pdf'
    with open(file_name,'wb') as f:
        f.write(r.content)
        f.close()


    input_element = driver.find_element(By.XPATH,"//*[@type='file']")
    input_element.send_keys(os.getcwd()+"\\"+file_name)
    time.sleep(3)
    driver.find_element(By.XPATH,"//*[@id='processTask']").click()
    time.sleep(30)
    driver.quit()
    time.sleep(10)
    xls = pd.ExcelFile(file_name_xlsx)
    sheets = xls.sheet_names
    xls_dict = pd.read_excel(file_name_xlsx,sheet_name = None,header=None)
    
    return file_name_xlsx,sheets


def assign_value(df,frnt,behd,col,row,values):
    done_value=[]
    act_col=col+1
    strt_frnt=act_col-frnt
    strt_end=act_col+behd
    i=0
    j=len(values)-1
    
    while frnt>=1:
        df[strt_frnt][row]=values[i]
        frnt-=1
        strt_frnt+=1
        done_value.append(values[i])
        i+=1
        
    while j>=len(values)-behd:
        df[strt_end][row]=values[j]
        done_value.append(values[j])
        j-=1
        
    print(done_value)
    col_val=(val for val in values if val not in done_value)
    col_val=list(col_val)
    df[act_col][row]=col_val[0]
    return df
def get_num_front_num_behind(df,col,row,values,position):
    num_front=0
    num_behind=0
    if position=='behind':
        col_static=col+1
        i=col
        num_front=0
        num_behind=0
        j=col_static+1
        for val in range(len(values)):
            if df[i][row]=='' and i>0:
                num_front+=1
                i-=1
            try:
                if df[j][row]=='' and j<len(df.columns):
                    num_behind+=1
                    j+=1
            except:
                val+=1
    elif position=='front':
        col_static=col-1
        i=col
        for val in range(len(values)):
            if df[i][row]=='' and i<len(df.columns):
                num_front+=1
                i+=1
    print(num_front,num_behind)
    return num_front,num_behind
def column_values_clean(df_final):
    for col in range(len(df_final.columns)):
        if pd.to_numeric(df_final[col], errors='coerce').notna().any():
            print(col)
            for val in range(len(df_final)):
                value=[]
                position=''
                if df_final[col][val]=='':
                    try:
                        value=df_final[col+1][val].split()
                        position='behind'
                        try:
                            if len(value)==1:
                                value=(df_final[col-1][val].split())
                                position='front'
                            else:
                                value=value
                        except:
                            continue
                    except:
                        try:
                            value=(df_final[col-1][val].split())
                            position='front'
                        except:
                            continue
                    value_series = pd.Series(value)

                    if pd.to_numeric(value_series, errors='coerce').notna().any():
                        if (len(value)!=0 and position=='') or len(value)<=1:
                            
                            val+=1
                        elif len(value)==2:
                            if position=='behind':
                                
                                df_final[col][val]=value[0]
                                df_final[col+1][val]=value[1]
                            elif position=='front':
                                df_final[col][val]=value[1]
                                df_final[col-1][val]=value[0]
                        elif len(value)>2:
                            frnt,behd=get_num_front_num_behind(df_final,col,val,value,position)
                            df_final=assign_value(df_final,frnt,behd,3,37,value)
    print(df_final)
    return df_final

def clean_row_col(l_col,df):
    dict_col={}
    final_data=[]
    for row in range(len(df)):
        rows=[]
        cols=[]
        data=[]
        for col in range(len(df.columns)):
            try:
                for i in l_col:
                    if i.lower() in df.iloc[row,col].lower():
                        rows.append(row)
                        cols.append(col)
                    elif i.lower() in df.iloc[row-1,col].lower():
                        rows.append(row)
                        cols.append(col)
            except:
                continue
        
        if len(cols) <= len(l_col):
            continue
        else:
            print(len(cols),len(l_col))
            final_data.append((set(rows),set(cols)))
    dict_data = {list(rows)[0]: list(cols) for rows, cols in final_data}
    print(dict_data)
    df_check=df.copy()
    df_check.reset_index(drop=True,inplace=True)
    df_check=df_check.iloc[:,:-(len(l_col))]
    for i, (key, value) in enumerate(dict_data.items()):
        df_check=df_check.iloc[:key]
        df_c=df.iloc[key:,[i for i in value]]
        df_c.reset_index(drop=True,inplace=True)
        df_c.columns=range(len(l_col))
        df_check=df_check.append(df_c)

    return df_check


