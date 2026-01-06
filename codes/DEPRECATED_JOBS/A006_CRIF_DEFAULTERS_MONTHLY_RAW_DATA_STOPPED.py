import pandas as pd
import datetime as datetime
import numpy as np
from pytz import timezone
import glob
import time
import re
import requests
import os
from bs4 import BeautifulSoup
import pdfplumber
import sys
import boto3
import warnings
from pandas.tseries.offsets import MonthEnd
warnings.filterwarnings('ignore')
import ssl
ssl._create_default_https_context = ssl._create_unverified_context


#%%
# sys.path.insert(0, 'D:\Adqvest')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import adqvest_s3
import dbfunctions
import JobLogNew as log
import ClickHouse_db
from adqvest_robotstxt import Robots
robot = Robots(__file__)
#%%
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday =  today - days

def pdfplumber_table_extractor(f,custom_columns):
    # print(datetime.datetime.now(india_time))
    t1=datetime.datetime.now(india_time)
    df_final=pd.DataFrame()
    with pdfplumber.open(f) as pdf:
        for i in range(len(pdf.pages)):
           print(i)
           try:
               table = pdf.pages[i].extract_tables()[0]
               if i==0:
                   df = pd.DataFrame(table[1:],columns=custom_columns)
               else:
                   df = pd.DataFrame(table[:],columns=custom_columns)
               df_final=pd.concat([df_final,df])
           except:
               pass
        return df_final
           
my_dictionary={
     'lacs':{'cat':'25 Lacs and above','file_type':"suit filed accounts rs 25 lakhs and above"},
     'crore':{'cat':'1 Cr and above','file_type':"suit filed accounts rs 1 crore and above"}
     }

def clean_location(text):
    text = text.lower()
    text = text.replace(',',' ')
    text = text.replace('|',' ').replace('*','').replace(']','').replace('[','').replace("`",' ')
    text = re.sub(r'#',' ',text) # Removing hash(#) symbol .
    text = re.sub(r'@',' ',text) # replace @ with space
    text = re.sub(r'\$',' ',text) # replace $ with space
    text = re.sub(r'%',' ',text) # replace % with space
    text = re.sub(r'\^',' ',text) # replace ^ with space
    text = re.sub(r'&',' ',text) # replace & with space
    text = re.sub(r'_',' ',text) # replace & with space
    text = re.sub(r'\d+ml', ' ', text)
    text = re.sub(r'[-]', ' ', text)
    text = text.upper()
    return text

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
    
    files = glob.glob(os.path.join(path, "*.pdf"))

    # file=files[0]  
    file=[i for i in files if file_name in i][0]
    # split_pdf(file_name, file_name.split('.pdf')[0], 10)
    S3_upload(file,s3_folder)
    print(f'This File loded In S3--->{file_name}')
    return file

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

def process_crif_data(df,dt_val,category):
    # renamed_col={
    #     'SRNO':'Sr_No','PARTY':'Party', 'CREDIT GRANTOR':'Bank', 'STATE':'State', 'CREDIT GRANTOR BRANCH':'Branch','REGISTERED ADDRESS':'Registered_Address', 'OUTSTANDING AMOUNT IN LACS':'Outstanding_Amt_Lacs',
    #     'ASSET CLASSIFICATION':'Asset_Classification', 'CLASDSAIFTIEC AOTFI ON':'Date_Of_Classification', 'SUIT':'Suit', 'OTHER BANK':'Other_Bank','DIRECTOR 1':'Director_1', 'PAN_DIR1':'Pan_Dir1', 
    #     'DIN FOR DIRECTOR 1':'Director_1_DIN', 'DIRECTOR 2':'Director_2','PAN_DIR2':'Pan_Dir2', 'DIN FOR DIRECTOR 2':'Director_2_DIN', 'DIRECTOR 3':'Director_3', 
    #     'PAN_DIR3':'Pan_Dir3','DIN FOR DIRECTOR 3':'Director_3_DIN', 'DIRECTOR 4':'Director_4', 'PAN_DIR4':'Pan_Dir4', 'DIN FOR DIRECTOR 4':'Director_4_DIN','DIRECTOR 5':'Director_5', 
    #     'PAN_DIR5':'Pan_Dir5', 'DIN FOR DIRECTOR 5':'Director_5_DIN', 'DIRECTOR 6':'Director_6','PAN_DIR6':'Pan_Dir6', 'DIN FOR DIRECTOR 6':'Director_6_DIN', 'DIRECTOR 7':'Director_7', 
    #     'PAN_DIR7':'Pan_Dir7','DIN FOR DIRECTOR 7':'Director_7_DIN', 'DIRECTOR 8':'Director_8', 'PAN_DIR8':'Pan_Dir8', 'DIN FOR DIRECTOR 8':'Director_8_DIN','DIRECTOR 9':'Director_9', 
    #     'PAN_DIR9':'Pan_Dir9', 'DIN FOR DIRECTOR 9':'Director_9_DIN', 'DIRECTOR 10':'Director_10','PAN_DIR10':'Pan_Dir10', 'DIN FOR DIRECTOR 10':'Director_10_DIN','DIRECTOR 11':'Director_11', 
    #     'PAN_DIR11':'Pan_Dir11','DIN FOR DIRECTOR 11':'Director_11_DIN', 'DIRECTOR 12':'Director_12', 'PAN_DIR12':'Pan_Dir12','DIN FOR DIRECTOR 12':'Director_12_DIN', 
    #     'DIRECTOR 13':'Director_13', 'PAN_DIR13':'Pan_Dir13','DIN FOR DIRECTOR 13':'Director_13_DIN', 'DIRECTOR 14':'Director_14', 'PAN_DIR14':'Pan_Dir14',
    #     'DIN FOR DIRECTOR 14':'Director_14_DIN', 'DIRECTOR 15':'Director_15', 'PAN_DIR15':'Pan_Dir15','DIN FOR DIRECTOR 15':'Director_15_DIN', 'DIRECTOR 16':'Director_16', 
    #     'PAN_DIR16':'Pan_Dir16','DIN FOR DIRECTOR 16':'Director_16_DIN', 'DIRECTOR 17':'Director_17', 'PAN_DIR17':'Pan_Dir17','DIN FOR DIRECTOR 17':'Director_17_DIN', 
    #     'DIRECTOR 18':'Director_18', 'PAN_DIR18':'Pan_Dir18','DIN FOR DIRECTOR 18':'Director_18_DIN', 'DIRECTOR 19':'Director_19', 'PAN_DIR19':'Pan_Dir19','DIN FOR DIRECTOR 19':'Director_19_DIN'}
    # df=get_renamed_columns(df,renamed_col)
    renamed_columns=['Sr_No', 'Party', 'Credit_Institution', 'State', 'Branch', 'Registered_Address','Outstanding_Amt_Lacs', 'Asset_Classification',
                      'Date_Of_Classification', 'Suit', 'Other_Bank', 'Director_1',
                      'Pan_Dir1', 'Director_1_DIN', 'Director_2', 'Pan_Dir2',
                      'Director_2_DIN', 'Director_3', 'Pan_Dir3', 'Director_3_DIN',
                      'Director_4', 'Pan_Dir4', 'Director_4_DIN', 'Director_5', 'Pan_Dir5',
                      'Director_5_DIN', 'Director_6', 'Pan_Dir6', 'Director_6_DIN',
                      'Director_7', 'Pan_Dir7', 'Director_7_DIN', 'Director_8', 'Pan_Dir8',
                      'Director_8_DIN', 'Director_9', 'Pan_Dir9', 'Director_9_DIN',
                      'Director_10', 'Pan_Dir10', 'Director_10_DIN', 'Director_11',
                      'Pan_Dir11', 'Director_11_DIN', 'Director_12', 'Pan_Dir12',
                      'Director_12_DIN', 'Director_13', 'Pan_Dir13', 'Director_13_DIN',
                      'Director_14', 'Pan_Dir14', 'Director_14_DIN', 'Director_15',
                      'Pan_Dir15', 'Director_15_DIN', 'Director_16', 'Pan_Dir16',
                      'Director_16_DIN', 'Director_17', 'Pan_Dir17', 'Director_17_DIN',
                      'Director_18', 'Pan_Dir18', 'Director_18_DIN', 'Director_19',
                      'Pan_Dir19', 'Director_19_DIN','Director_20','Pan_Dir20','Director_20_DIN']
    
    df.columns=renamed_columns[:len(df.columns)]
    df['Relevant_Date'] = dt_val
    # df['Source'] = 'CRIF'
    df['Category'] = category
    df['Runtime'] = datetime.datetime.now(india_time).strftime('%Y-%m-%d %H:%M:%S')
    
    df['Credit_Institution']=df['Credit_Institution'].apply(lambda x:str(x).upper().replace('BANK NA', 'BANK').replace('.', '').strip())
    df['Branch']=df['Branch'].apply(lambda x:clean_location(str(x)).upper().strip())
    df['State']=df['State'].apply(lambda x:clean_location(str(x)).upper().strip())
    df['Party']=df['Party'].apply(lambda x:clean_location(str(x)).upper().replace('.', '').strip())
    df['Registered_Address']=df['Registered_Address'].apply(lambda x:str(x).upper().strip().replace('"','').replace('#', '').replace('“','').replace('“','').replace('•	','').replace('” ',''))
    
    return df
def Upload_Data_mysql(table_name, data, category):
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    
    query=f"select max(Relevant_Date) as Max from {table_name} where Category='{category}' and Source ='CRIF'"
    db_max_date = pd.read_sql(query,engine)["Max"][0]
    # data=data.loc[data['Relevant_Date'] > db_max_date]
    data.to_sql(table_name, con=engine, if_exists='append', index=False)
    print('Data loded in mysql')
    
#%%
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
    table_name = "CRIF_DEFAULTERS_MONTHLY_RAW_DATA_STOPPED"
    scheduler = ''
    no_of_ping = 0

    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]

    try :
        if(run_by == 'Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        #%%
        os.chdir('C:/Users/Administrator/CIBIL_EQUIFAX_CRIF')
        url = 'https://www.crifhighmark.com/suit-filed-cases/suit-filed-cases-list'
        r1 = requests.get(url, timeout = 120)
        robot.add_link(url)
        content = r1.text
        soup = BeautifulSoup(content,'lxml')
        links = soup.findAll(class_='col-sm-8 article-single mob-social-linked')
        links_text=[i.find('p') for i in links]
        links_text={i.get('title').lower():'https://www.crifhighmark.com'+i['href'] for i in links_text[0].find_all('a')}
     
       #%%
        for ct in my_dictionary.keys():
            engine = adqvest_db.db_conn()
            connection = engine.connect()
            
            org_tbl_col_dt = pd.read_sql(f"select Distinct Relevant_Date as Relevant_Date from CRIF_DEFAULTERS_MONTHLY_RAW_DATA_STOPPED where Category ='{my_dictionary[ct]['cat']}'",engine)
            base_date=max(org_tbl_col_dt['Relevant_Date'])
            org_tbl_col_dt['Relevant_Date']=org_tbl_col_dt['Relevant_Date'].apply(lambda x:str(x))

            # base_date=pd.to_datetime('2014-04-30',format='%Y-%m-%d').date()
            
            src = requests.get(links_text[my_dictionary[ct]['file_type']], timeout = 120)
            robot.add_link(links_text[my_dictionary[ct]['file_type']])
            content = src.text
            soup = BeautifulSoup(content,'lxml')
            pdf_links = soup.findAll(class_="col-sm-8 article-single mob-social-linked")
            pdf_links2=[i.find_all('p') for i in pdf_links][0]
            date_link={'https://www.crifhighmark.com'+i.find('a')['href']:pd.to_datetime(i.text.split('Cons_')[0]).date() for i in pdf_links2 if pd.to_datetime(i.text.split('Cons_')[0]).date()>base_date}
            date_link={li:dt for li,dt  in date_link.items() if (str(dt) not in org_tbl_col_dt.Relevant_Date.to_list())}
    
            print(date_link.values())
            for link,current_date in date_link.items():
                # print(link)
                print(current_date)
                file_name=f"CRIF_DEFAULTERS_{my_dictionary[ct]['cat'].strip().replace(' ','_')}_{str(current_date)}.pdf"
                s3_folder='CIBIL_EQUIFAX_CRIF/CRIF'
                pdf_file=read_link(link,file_name,s3_folder)
                print(pdf_file)
    
                t=[pd.DataFrame(pdfplumber.open(pdf_file).pages[i].extract_tables()[0][1:],columns=pdfplumber.open(pdf_file).pages[i].extract_tables()[0][0])  for i in [0]]
                column_list=t[0].columns
                
                df_final=pd.DataFrame()
                pages_scrapped=0
                with pdfplumber.open(pdf_file) as pdf:
                    for i in range(len(pdf.pages)):
                       print(i)
                       try:
                           table = pdf.pages[i].extract_tables()[0]
                           if i==0:
                               df = pd.DataFrame(table[1:],columns=column_list)
                           else:
                               df = pd.DataFrame(table[:],columns=column_list)
                           df_final=pd.concat([df_final,df])
                       except:
                           pass
                       pages_scrapped +=1
                       
                       if pages_scrapped==100:
                            if len(df_final)>0:
                               df_bnk=process_crif_data(df_final,current_date,category=str(my_dictionary[ct]['cat']))
                               Upload_Data_mysql('CRIF_DEFAULTERS_MONTHLY_RAW_DATA_STOPPED',df_bnk,str(my_dictionary[ct]['cat']))
                               df_final=pd.DataFrame()
                            pages_scrapped=0
                    
                    if len(df_final)>0:
                        df_bnk=process_crif_data(df_final,current_date,category=str(my_dictionary[ct]['cat']))
                        Upload_Data_mysql('CRIF_DEFAULTERS_MONTHLY_RAW_DATA_STOPPED',df_bnk,str(my_dictionary[ct]['cat']))
                        df_final=pd.DataFrame()
                                               
                os.remove(pdf_file)
                
                
       #%%
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
