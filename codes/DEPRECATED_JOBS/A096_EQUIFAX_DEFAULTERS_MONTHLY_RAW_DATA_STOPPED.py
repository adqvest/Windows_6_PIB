import pandas as pd
import os
import requests
from bs4 import BeautifulSoup
import re
import datetime as datetime
from pytz import timezone
import sys
import warnings
warnings.filterwarnings('ignore')
import numpy as np
import time
import warnings
from datetime import datetime as dt
from pandas.tseries.offsets import MonthEnd
import glob
from selenium import webdriver
import boto3
#%%
warnings.filterwarnings('ignore')
# sys.path.insert(0, '/home/ubuntu/AdQvestDir/Adqvest_Function')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import JobLogNew as log
import adqvest_db
from adqvest_robotstxt import Robots
import adqvest_s3
robot = Robots(__file__)

engine = adqvest_db.db_conn()
connection = engine.connect()
#%%
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days
#%%
my_dictionary={
     'lacs':{'cat':'25 lacs and above','file_type':"25l"},
     'crore':{'cat':'1 Cr and above','file_type':"1cr"}
     }


def clean_values(x):
    
    if 'Cr' in x:
        x=float(str(x).replace(',','').replace('Cr',''))*100
    else:
        x=float(str(x).replace(',',''))
    return x
def convert_date_format(input_date,output_format='%Y-%m-%d',input_format='%b %Y',Month_end=True):
    try:
       input_datetime = dt.strptime(str(input_date),input_format)
       output_date = input_datetime.strftime(output_format)
    except:
       input_datetime = dt.strptime(str(input_date),"%b '%y")
       output_date = input_datetime.strftime(output_format)

    
    if Month_end==True:
        output_date=pd.to_datetime(str(output_date), format=output_format)+ MonthEnd(1)
        
    output_date=pd.to_datetime(str(output_date), format='%Y-%m-%d').date()
    return output_date
  

def row_col_index_locator(df,l1,take_min=False):
    df.reset_index(drop=True,inplace=True)
    df.fillna('#', inplace=True)
    
    index2=[]
    touple_list=[]
    dict1={}
    for j in l1:
        tpl=()
        for i in range(df.shape[1]):
            if df.iloc[:,i].str.lower().str.contains(j.lower()).any()==True:
                # print(f"Found--{j}")
                # print(f"Column--{i}")
                index2.append(i)
                
                row_index=df[df.iloc[:, i].str.lower().str.contains(j.lower()) == True].index[0]
                # print(f"Row--{row_index}")
                index2.append(row_index)
                tpl=(i,row_index)
                break
        touple_list.append(tpl)
   
    
    if take_min==True:
        touple_list = [t for t in touple_list if t]
        min_tuple = min(touple_list, key=lambda x: x[0])
        return min_tuple
    else:
        return index2

def get_renamed_columns(df,col_dict):
    df.fillna('#', inplace=True)
    for k,v in col_dict.items():
        # print(k)
        try:
            ele=[item for item in df.columns if k.lower() in item.lower()]
            k1=df.columns.to_list().index(ele[0])
            df=df.rename(columns={f"{df.columns[k1]}":f"{col_dict[k]}"})
        except:
            pass
    
    df.reset_index(drop=True,inplace=True)
    df=df.replace('#',np.nan)
    return df

def S3_upload(filename,bucket_folder):


    ACCESS_KEY_ID = adqvest_s3.s3_cred()[0]
    ACCESS_SECRET_KEY = adqvest_s3.s3_cred()[1]

    BUCKET_NAME = 'adqvests3bucket'
    s3 = boto3.resource(
         's3',
         aws_access_key_id=ACCESS_KEY_ID,
         aws_secret_access_key=ACCESS_SECRET_KEY,
         config=boto3.session.Config(signature_version='s3v4',region_name = 'ap-south-1'))
    data_s3 =  open(filename, 'rb')
    s3.Bucket(BUCKET_NAME).put_object(Key=f'{bucket_folder}/'+filename, Body=data_s3)
    data_s3.close()
    print("Data uploaded to S3")
    
def read_link(link,file_name,s3_folder):
    os.chdir('C:/Users/Administrator/CIBIL_EQUIFAX_CRIF')
    path=os.getcwd()
    r=requests.get(link,verify=False)
    r.raise_for_status()
    with open(file_name, 'wb') as f:
        f.write(r.content)
    files = glob.glob(os.path.join(path, "*.csv"))
    file=files[0]   
    S3_upload(file,s3_folder)
    print(f'This File loded In S3--->{file_name}')
    os.remove(file)
    return file

def drop_duplicates(df):
    columns = df.columns.to_list()
    columns.remove('Runtime')
    df = df.drop_duplicates(subset=columns, keep='last')
    return df

def Upload_Data_mysql(table_name, data, category):
    engine = adqvest_db.db_conn()
    connection = engine.connect()


    # try:
    #    data=data[['Category_Of_Bank_FI'
    #             'Party','Bank','State','Branch','Registered_Address',
    #             'Outstanding_Amount_In_Lacs','Asset_Classification','Date_Of_Classification','Suit','Other_Bank','Director_Name','Director_1','Pan_Dir1',
    #             'Director_1_DIN','Director_2','Pan_Dir2','Director_2_DIN','Director_3','Pan_Dir3','Director_3_DIN','Director_4','Pan_Dir4','Director_4_DIN',
    #             'Director_5','Pan_Dir5','Director_5_DIN','Director_6','Pan_Dir6','Director_6_DIN','Director_7','Pan_Dir7','Director_7_DIN','Director_8','Pan_Dir8','Director_8_DIN','Director_9','Pan_Dir9','Director_9_DIN','Director_10','Pan_Dir10','Director_10_DIN',
    #             'Director_11','Pan_Dir11','Director_11_DIN','Director_12','Pan_Dir12','Director_12_DIN',
    #             'Director_13','Pan_Dir13','Director_13_DIN','Director_14','Pan_Dir14','Director_14_DIN','Director_15','Pan_Dir15','Director_15_DIN',
    #             'Director_16','Pan_Dir16','Director_16_DIN','Director_17','Pan_Dir17','Director_17_DIN','Director_18','Pan_Dir18','Director_18_DIN',
    #             'Director_19','Pan_Dir19','Director_19_DIN','Director_20','Pan_Dir20','Director_20_DIN','Source','Category','Relevant_Date','Runtime']]
    # except Exception as e:
    #    exception_variable =ast.literal_eval(e.args[0].replace('not in index','')) 
    #    for col in exception_variable:
    #        data[col]=np.nan

    query=f"select max(Relevant_Date) as Max from {table_name} where Category='{category}' and Source ='EQUIFAX'"
    db_max_date = pd.read_sql(query,engine)["Max"][0]
    data=data.loc[data['Relevant_Date'] > db_max_date]
    data.to_sql(table_name, con=engine, if_exists='append', index=False)
    print('Data loded in mysql')

def get_page_content(url):
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
    time.sleep(30)
    soup=BeautifulSoup(driver.page_source)
    return soup

def process_eqifax_data(link,category,present_date):
    # r1 = requests.get(link, timeout = 120)
    # soup1 = BeautifulSoup(r1.content,'lxml')
    # time.sleep(60)
    soup1=get_page_content(link)
    page_count=soup1.find_all(id='count')[0].text
    page_count_df=pd.DataFrame.from_dict([{'Category':category,'Records':int(page_count),'Source':'EQUIFAX','Relevant_Date':present_date,'Runtime':datetime.datetime.now(india_time).strftime('%Y-%m-%d %H:%M:%S')}])
    print(page_count_df)

    links_cr = soup1.findAll('a',attrs = {'class':"btn btn-primary btn-sm"},href = True)
    csv_link=[i['href'] for i in links_cr][0]
    
    file_name=f"EQUIFAX_DEFAULTERS_{category.strip().replace(' ','_')}_{present_date}.csv"
    s3_folder='CIBIL_EQUIFAX_CRIF/EQUIFAX'
    read_link(csv_link,file_name,s3_folder)
    
    # df=pd.read_csv(csv_link,header=None, error_bad_lines=False)
    df=pd.read_csv(csv_link)

    if [col for col in  df.columns if col in ['Branch','State']]:
         print('Column Present')
    else:
         try:
             si=row_col_index_locator(df,['Category of bank'],take_min=True)[1]
             if si>3:
                 si=0
             df=df.iloc[si:,:]
             df.reset_index(drop=True,inplace=True)
             df.columns=df.iloc[row_col_index_locator(df,['Category of bank'],take_min=True)[1],:]
             df=df.drop(index=[row_col_index_locator(df,['Category of bank'],take_min=True)[1]])
             df.reset_index(drop=True,inplace=True)
         except:
             try:
                 si=row_col_index_locator(df,['SCTG'],take_min=True)[1]
                 if si>3:
                     si=0
                 df=df.iloc[si:,:]
                 df.reset_index(drop=True,inplace=True)
                 df.columns=df.iloc[row_col_index_locator(df,['SCTG'],take_min=True)[1],:]
                 df=df.drop(index=[row_col_index_locator(df,['SCTG'],take_min=True)[1]])
                 df.reset_index(drop=True,inplace=True)
             except:
                 si=0
                 df=df.iloc[si:,:]
                 df.reset_index(drop=True,inplace=True)
                 print('Column Present')

        
    df.reset_index(drop=True,inplace=True)
    renamed_col={
         'Category of bank/FI':'Category_Of_Bank_FI','Name of bank/FI':'Bank', 
         'Branch':'Branch',
         'Sr.No.':'Sr_No','Name of Party':'Party',
         'Registered Address':'Registered_Address',
         'Outstanding Amount in Rs. lakhs ':'Outstanding_Amount_In_Lacs', 
         'Asset Classification':'Asset_Classification', 
         'Date of\nClassification':'Date_Of_Classification',
         'Suit':'Suit', 
         'Name of other banks/ FIs':'Other_Bank'}
    df=get_renamed_columns(df,renamed_col)
    df.drop(columns=[col for col in df.columns if col in ['#']], inplace=True)
    if (category=='25 lacs and above'):
        df.columns =['Category_Of_Bank_FI','Credit_Institution', 'Branch','State','Sr_No','Party','Registered_Address','Outstanding_Amt_Lacs','Suit', 'Other_Bank', 'Director_1','Director_1_DIN', 'Director_2', 'Director_2_DIN', 'Director_3','Director_3_DIN', 'Director_4','Director_4_DIN', 'Director_5','Director_5_DIN', 'Director_6','Director_6_DIN', 'Director_7','Director_7_DIN', 'Director_8','Director_8_DIN', 'Director_9','Director_9_DIN', 'Director_10','Director_10_DIN', 'Director_11', 'Director_11_DIN', 'Director_12','Director_12_DIN','Director_13','Director_13_DIN','Director_14','Director_14_DIN']
        df['Asset_Classification']=np.nan
        df['Date_Of_Classification']=np.nan
        print('Here')

    if (category=='1 Cr and above'):
       df.columns = ['Category_Of_Bank_FI','Credit_Institution', 'Branch','State','Sr_No','Party','Registered_Address','Outstanding_Amt_Lacs','Asset_Classification','Date_Of_Classification','Suit', 'Other_Bank', 'Director_1','Director_1_DIN', 'Director_2', 'Director_2_DIN', 'Director_3','Director_3_DIN', 'Director_4','Director_4_DIN', 'Director_5','Director_5_DIN', 'Director_6','Director_6_DIN', 'Director_7','Director_7_DIN', 'Director_8','Director_8_DIN', 'Director_9','Director_9_DIN', 'Director_10','Director_10_DIN', 'Director_11', 'Director_11_DIN', 'Director_12','Director_12_DIN','Director_13','Director_13_DIN','Director_14','Director_14_DIN'] 

    
    # df['Source'] = 'EQUIFAX'
    df['Category'] = category
    df["Relevant_Date"] = present_date
    df['Runtime'] = datetime.datetime.now(india_time).strftime('%Y-%m-%d %H:%M:%S')
    
    df['Credit_Institution']=df['Credit_Institution'].apply(lambda x:str(x).upper().strip())
    df['Branch']=df['Branch'].apply(lambda x:str(x).upper().strip())
    df['State']=df['State'].apply(lambda x:str(x).upper().strip())
    df['Party']=df['Party'].apply(lambda x:str(x).upper().strip())
    
    return df,page_count_df
        
#%%
def run_program(run_by = 'Adqvest_Bot', py_file_name = None):
    # os.chdir('/home/ubuntu/AdQvestDir')
    os.chdir('C:/Users/Administrator/AdQvestDir')

    
    job_start_time = datetime.datetime.now(india_time)
    table_name = "EQUIFAX_DEFAULTERS_MONTHLY_RAW_DATA_STOPPED"
    scheduler = ''
    no_of_ping = 0

    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]

    try :
        if(run_by == 'Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)


            os.chdir('C:/Users/Administrator/CIBIL_EQUIFAX_CRIF')
            url = 'https://www.equifax.co.in/legal_notice/suit_filed_details/en_in'
            src = requests.get(url, timeout = 120)
            robot.add_link(url)
            content = src.text
            soup = BeautifulSoup(content,'lxml')
            links = soup.findAll('a',attrs = {'class':"namelg"},href = True)
            links_date={l['href']:convert_date_format(l.text.replace('\n\t', '').strip().replace('Sept','Sep')) for l in links} 
            #%%
            for ct in my_dictionary.keys():
                org_tbl_col_dt = pd.read_sql(f"select Distinct Relevant_Date as Relevant_Date from EQUIFAX_DEFAULTERS_MONTHLY_RAW_DATA_STOPPED where  Category ='{my_dictionary[ct]['cat']}'",engine)
                org_tbl_col_dt['Relevant_Date']=org_tbl_col_dt['Relevant_Date'].apply(lambda x:str(x))
                
                base_date=pd.to_datetime('2016-01-31',format='%Y-%m-%d').date()
                link_date={li: dt for li,dt  in links_date.items() if ((my_dictionary[ct]['file_type'] in str(li).lower()))}
                link_date={li:dt for li,dt  in link_date.items() if (str(dt) not in org_tbl_col_dt.Relevant_Date.to_list() and (dt>base_date))}

                df_bank=pd.DataFrame()
                df_bank_count=pd.DataFrame()
                if len(link_date)>0:
                    for link,date in link_date.items():
                        try:
                            df_1,df_count=process_eqifax_data(link,my_dictionary[ct]['cat'],date)
                            time.sleep(1)
                            df_bank=pd.concat([df_bank,df_1])
                            df_bank_count=pd.concat([df_bank_count,df_count])
                           
                        except:
                            print(f'Having issue in-->{date}')
            #%%
   
                if (len(df_bank)>0 or len(df_bank_count)>0):
                    Upload_Data_mysql('EQUIFAX_DEFAULTERS_MONTHLY_RAW_DATA_STOPPED',df_bank,str(my_dictionary[ct]['cat']))
                    Upload_Data_mysql('CIBIL_EQUIFAX_CRIF_DEFAULTERS_MONTHLY_QTRLY_DATA_STATUS_TABLE',df_bank_count,str(my_dictionary[ct]['cat']))



                    
        log.job_end_log(table_name,job_start_time, no_of_ping)

    except:
        try:
            connection.close()
        except:
            pass
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
       

        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by = 'manual')
