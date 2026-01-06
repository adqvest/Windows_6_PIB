import datetime
import os
import re
import sys
import warnings
import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil import parser
from playwright.sync_api import sync_playwright
from pytz import timezone
import time
import glob
from selenium import webdriver
from datetime import datetime as dt
from pandas.tseries.offsets import MonthEnd
import numpy as np
warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import adqvest_s3
import boto3
import ClickHouse_db
import JobLogNew as log
from adqvest_robotstxt import Robots
robot = Robots(__file__)

engine = adqvest_db.db_conn()
client = ClickHouse_db.db_conn()
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/State_Function')
import state_rewrite
#%%
def Upload_Data(table_name, data, db: list):
    query=f"select max(Relevant_Date) as Max from {table_name}"
    db_max_date = pd.read_sql(query,engine)["Max"][0]
    data=data.loc[data['Relevant_Date'] > db_max_date]
    
    if 'MySQL' in db:
        data.to_sql(table_name, con=engine, if_exists='append', index=False)
        print(data.info())

    if 'Clickhouse' in db:

        click_max_date = client.execute(f"select max(Relevant_Date) from {table_name}")
        click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])
        query2 =f"select * from {table_name} WHERE Relevant_Date > '" + click_max_date +"';" 

        df = pd.read_sql(query2,engine)
        client.execute(f"INSERT INTO {table_name} VALUES",df.values.tolist())
        print("Data uplodedin Ch")
def City_state_mapping(df,column,new_column):
    raw=df[str(column)]
    dic ={}
    state = []
    for i in raw:
        print(f'-------------->{i}')
        if ((i !=None) and (i !='-') and (i!='Others')):
            try:
                states =  state_rewrite.state((i.lower()))
                print(states)
                dic[i.lower()] = states.split('|')[-1].upper()
                print(states.split('|')[-1].upper())
            except:
                dic[i.lower()] = None
        else:
            dic[i] = None

    clean = []
    for i in df[str(column)]:
        try:
           clean.append(dic[i.lower()])
        except:
           clean.append(dic[i])
    
    df[str(new_column)] = clean
    return df


def pdf_to_excel(file_path,key_word="",OCR_doc=False):
    
    os.chdir(file_path)
    path=os.getcwd()
    download_path=os.getcwd()
    pdf_list = glob.glob(os.path.join(path, "*.pdf"))
    print(pdf_list)

    matching = [s for s in pdf_list if key_word in s]
    print('Matching')
    print(matching)
                   
    
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=10000,
                                    chromium_sandbox=True,
                                    downloads_path=download_path)
        page = browser.new_page()
        page.goto("https://www.ilovepdf.com/",timeout=30000*5)

        page.locator("//*[contains(text(),'Login')]").click()
        email = page.locator("//*[@id='loginEmail']")
        email.fill("kartmrinal101@outlook.com")
        password = page.locator("//*[@id='inputPasswordAuth']")
        password.fill("zugsik-zuqzuH-jyvno4")
        page.locator("//*[@id='loginBtn']").click()
        page.get_by_title("PDF to Excel").click()

        for i in matching:
            with page.expect_file_chooser() as fc_info:
                page.get_by_text("Select PDF file").click()
                file_chooser = fc_info.value
                file_chooser.set_files(i)
                if OCR_doc==True:
                    page.get_by_text("Continue without OCR").click()
                    
                page.locator("//*[@id='processTask']").click()
                with page.expect_download() as download_info:
                    page.get_by_text("Download EXCEL").click()
                # Wait for the download to start
                download = download_info.value
                # Wait for the download process to complete
                print(download.path())
                file_name = download.suggested_filename
                # wait for download to complete
                download.save_as(os.path.join(download_path, file_name))
                page.go_back()

def driver_parameter():
    prefs = {
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True
        }

    options = webdriver.ChromeOptions()
    options.add_experimental_option('prefs', prefs) 
    options.add_experimental_option('excludeSwitches', ['enable-automation']) 

    options.add_argument("--disable-infobars")
    options.add_argument("start-maximized")
    options.add_argument("--disable-extensions")
    options.add_argument('--incognito')
    options.add_argument("--disable-notifications")
    options.add_argument('--ignore-certificate-errors')
    options.add_argument("--use-fake-ui-for-media-stream")
    options.add_argument("--use-fake-device-for-media-stream")
    options.add_experimental_option("prefs", prefs)
    
def get_page_content(url,driver_path):
    
    options=driver_parameter()
    driver = webdriver.Chrome(executable_path=driver_path,chrome_options=options)
    driver.get(url)
    time.sleep(10)
    driver.implicitly_wait(10)
    soup=BeautifulSoup(driver.page_source)
    return soup

def get_desire_table(link,serach_str):
     xls = pd.ExcelFile(link)
     sheet_names = xls.sheet_names
     print(sheet_names)
     for i in range(len(sheet_names)):
         df=pd.DataFrame()
         df = pd.read_excel(link, sheet_name=sheet_names[i], header=None)
         df=df.replace(np.nan,'')
         print('Done ',sheet_names[i])
         if type(df.iloc[0,0])!=int:
             if serach_str in df.iloc[0,0].lower():
                 print('Sheet Found')
                 break
             else:
                 df=pd.DataFrame()
             
     return df
def row_col_index_locator(df,l1):
    df.reset_index(drop=True,inplace=True)
    df.fillna('#', inplace=True)
    
    index2=[]
    dict1={}
    for j in l1:
        for i in range(df.shape[1]):
            if df.iloc[:,i].str.lower().str.contains(j.lower()).any()==True:
                print(f"Found--{j}")
                print(f"Column--{i}")
                index2.append(i)
                row_index=df[df.iloc[:, i].str.lower().str.contains(j.lower()) == True].index[0]
                print(f"Row--{row_index}")
                index2.append(row_index)
                break
                
    return index2

def row_modificator(df,l1,col_idx,row_del=False,keep_row=False,update_row=False):
  from collections import ChainMap
  keep_inx=[]
  print(type(l1[0]))
  if isinstance(l1[0],dict):
      l1=dict(ChainMap(*l1))
  else:
      l1=dict.fromkeys(l1,np.nan)
    
  for i in l1.keys():
    df = df.reset_index(drop=True)
    r=df[df.iloc[:,col_idx].str.lower().str.contains(i.lower())==True].index.to_list()
    if (keep_row==True):
        keep_inx.append(r[0])
    
    if row_del==True:
        df.drop(index=r,inplace=True)
        df[df.columns[col_idx]]=df[df.columns[col_idx]].str.strip()
    else:
        if (update_row==True):
            for j in r:
                   print(r)
                   df.iloc[j,col_idx]=l1[i]
                   df[df.columns[col_idx]]=df[df.columns[col_idx]].str.strip()
            
  if(keep_row==True):
        drop_list=[i for i in df.index if i not in keep_inx]
        df.drop(index=drop_list,inplace=True)
               
  df.reset_index(drop=True,inplace=True)    
  return df  

def convert_date_format(input_date,output_format='%Y-%m-%d',input_format='%B%Y'):
    try:
        input_datetime = dt.strptime(str(input_date),'%B%Y')
        output_date = input_datetime.strftime(output_format)
    except:
        input_datetime = dt.strptime(str(input_date),'%b%Y')
        output_date = input_datetime.strftime(output_format)
   
    output_date=pd.to_datetime(str(output_date), format='%Y-%m-%d')+ MonthEnd(1)
    output_date=output_date.date()
    output_date=pd.to_datetime(str(output_date), format='%Y-%m-%d').date()
    return output_date
        
def get_date_from_string(input_txt):
    from calendar import day_name
    from calendar import month_name
    l1=['Webupdate','Websiteupdate','procfigures','WebsiteUpdate','websiteupdate']
    for j in l1:
        if j in input_txt:
            txt=input_txt.strip().split('/')[-1].split('.pdf')[0].split(j)[1]
            print(txt)
            dt=[txt for x in month_name if x in txt][0]
   
    date=convert_date_format(dt)
    print(date)
    return date

def get_request_session(url):
    
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    r=session.get(url)
    return r

def S3_upload(filename,bucket_folder):
    ACCESS_KEY_ID = 'AKIAYCVSRU2U3XP2DB75'
    ACCESS_SECRET_KEY ='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'
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
    os.chdir('C:/Users/Administrator/FCI')
    path=os.getcwd()
    r = get_request_session(link)
    with open(file_name, 'wb') as f:
        f.write(r.content)
    files = glob.glob(os.path.join(path, "*.pdf"))
    print(files)
    file=files[0]   
    S3_upload(file_name,s3_folder)
    return file
    

def convert_date_format_1(input_date,output_format='%Y-%m-%d',input_format='%d.%m.%Y'):
    input_datetime = dt.strptime(str(input_date), input_format)
    output_date = input_datetime.strftime(output_format)
    lnk_date=pd.to_datetime(str(output_date), format='%Y-%m-%d').date()
    return output_date

def drop_duplicates(df):
    columns = df.columns.to_list()
    columns.remove('Runtime')
    df = df.drop_duplicates(subset=columns, keep='last')
    return df

def get_sorted_links(link_date,max_rev_date):
    df1=pd.DataFrame()
    new_li=[]
    new_date=[]
    for k,v in link_date.items():
        if v>max_rev_date:
            new_li.append(k)
            new_date.append(v)

    df1['Links']=new_li
    df1['Date']=new_date
    return df1
#%%

#%%
def run_program(run_by = 'Adqvest_Bot', py_file_name = None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')

    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday =  today - days

    job_start_time = datetime.datetime.now(india_time)
    table_name = "FPD_FOODGRAIN_PROCUREMENT_MONTHLY_CUMU_DATA"
    scheduler = ''
    no_of_ping=0

    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]

    try :
        if(run_by == 'Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
            
            
        os.chdir('C:/Users/Administrator/FCI')
        driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
        download_path=os.getcwd()
        url='https://dfpd.gov.in/procurement-figures.htm'
        robot.add_link(url)
        max_rel_date = pd.read_sql("select max(Relevant_Date) as Date from FPD_FOODGRAIN_PROCUREMENT_MONTHLY_CUMU_DATA", con=engine)['Date'][0]
        max_rel_date=pd.to_datetime(str(max_rel_date), format='%Y-%m-%d').date()
        print(type(max_rel_date))

        delete_pdf=os.listdir(r"C:\Users\Administrator\FCI")
        for file in delete_pdf:
            os.remove(file)
        
        
        
        ############### Getting Page Links#################################################################
        soup=get_page_content(url,driver_path)
        page=[i for i in soup.find_all('tr')]
        page=[l.find_all('a', href=True,target="_blank") for l in soup.find_all('tr')]

        pg_link=[i[0]['href'] for i in page if (len(i)!=0 and re.findall('Webupdate',i[0]['href']))]
        pg_link=['https://dfpd.gov.in/writereaddata'+i.split('writereaddata')[1] for i in pg_link]
        link_date={i:get_date_from_string(i) for i in pg_link}
        
        link_df=get_sorted_links(link_date,max_rel_date)

        ##################################################################################################33

        if link_df.shape[0]==0:
            print("No new data available")
        else:
            for i in range(link_df.shape[0]):
                rel_date=link_df['Date'][i]
                link=link_df['Links'][i]
                robot.add_link(link)

                filename=f"FPD_FOOD_GRAIN_PROCUREMENT_{rel_date}.pdf"
                s3_folder='AGRI/PROCUREMENT'
                lnk=read_link(link,filename,s3_folder)

                pdf_to_excel('C:/Users/Administrator/FCI')
                time.sleep(10)

                os.chdir('C:/Users/Administrator/FCI')
                f1 = glob.glob(os.path.join( os.getcwd(), "*.xlsx"))
                print(f1)
                fs=f1[0]
                df_c=[]
                
                df1=get_desire_table(fs,'RICE PRODUCTION AND PROCUREMENT'.lower())
                if df1.empty==True:
                    df1=pd.read_excel(fs,sheet_name='Table 2')
                
                row_start=row_col_index_locator(df1,['As on','Upto'])
                # row_start=row_col_index_locator(df1,['Upto'])
                end_row=row_col_index_locator(df1,['ALL INDIA'])
            
                try:
                    date=df1.iloc[row_start[1],row_start[0]].split('Upto')[1].split('\n')[0].strip()
                except:
                    # date='30.11.2021'
                    date=df1.iloc[row_start[1],row_start[0]].split('As on')[1].replace(')','').strip()
                    
                df1=df1.iloc[row_start[1]+1:end_row[1],[0,-2,-1]]
                date=convert_date_format_1(date)
                std=pd.to_datetime(date, format='%Y-%m-%d').date()

                if df1.empty==True:
                    df1=pd.read_excel(fs,sheet_name='Table 2')
                    df1=df1.iloc[row_col_index_locator(df1,['Sates'])[1]+1:row_col_index_locator(df1,['ALL INDIA'])[1],[0,-2,-1]]
                    df1=df1.iloc[row_col_index_locator(df1,['prod'])[1]+1:,:]
                    df1=df1.replace('#',np.nan)
                    
                


                df1.columns=['State','Production_Lakh_MT','Procurement_Lakh_MT']
                df1['State']=df1['State'].apply(lambda x:x.replace('/','').strip())
                df1['State']=df1['State'].apply(lambda x:x.replace('.','').strip())
                df1=row_modificator(df1,['ALL INDIA','Neg-','WHEAT','Advance'],0,row_del=True)
                df1 = df1.reset_index(drop=True)
                df1=df1.replace("neg",np.nan)
                df1['Commodity']='Rice'


                #################################################################
                if std.month==9:
                    query='''select Relevant_Date,sum(Procurement_Lakh_MT) as Unit from FPD_FOODGRAIN_PROCUREMENT_MONTHLY_CUMU_DATA
                    where Commodity='Rice' and Relevant_Date=(select max(Relevant_Date) from FPD_FOODGRAIN_PROCUREMENT_MONTHLY_CUMU_DATA)
                    group by Relevant_Date,Commodity  Order by Relevant_Date desc;'''
                    sum_val = pd.read_sql(query,engine)["Unit"][0]
                    if df1.Procurement_Lakh_MT.sum()-sum_val<10:
                            raise Exception(f'Please Check Rice data for----> {date}')
                        
                df2=get_desire_table(fs,'WHEAT PRODUCTION AND PROCUREMT'.lower())
                if df2.empty==True:
                     df2=pd.read_excel(fs,sheet_name='Table 3')
                
            
                # row_start1=row_col_index_locator(df2,['RMS'])
                end_row1=row_col_index_locator(df2,['ALL INDIA'])
                df2=df2.iloc[:end_row1[1],[0,-3,-1]]

                df2.columns=['State','Production_Lakh_MT','Procurement_Lakh_MT']
                df2['State']=df2['State'].apply(lambda x:x.replace('/','').strip())
                df1['State']=df1['State'].apply(lambda x:x.replace('.','').strip())

                df2=row_modificator(df2,['ALL INDIA','Neg-','WHEAT','Advance'],0,row_del=True)
                df2 = df2.reset_index(drop=True)
                df2=df2.replace("neg",np.nan)
                df2['Commodity']='Wheat'
                #####################################################################################
                if std.month==3:
                    query='''select Relevant_Date,sum(Procurement_Lakh_MT) as Unit from FPD_FOODGRAIN_PROCUREMENT_MONTHLY_CUMU_DATA
                    where Commodity='Wheat' and Relevant_Date=(select max(Relevant_Date) from FPD_FOODGRAIN_PROCUREMENT_MONTHLY_CUMU_DATA)
                    group by Relevant_Date,Commodity  Order by Relevant_Date desc;'''
                    sum_val = pd.read_sql(query,engine)["Unit"][0]
                    if df2.Procurement_Lakh_MT.sum()-sum_val<10:
                            raise Exception(f'Please Check Wheat data for----> {date}')

                df_final=pd.concat([df1,df2])

                df_final=df_final.replace("#",np.nan)
                # df_final['Unit']='Lakh Tons'
                df_final['Relevant_Date']=date
                df_final["Runtime"] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))

                # df_c.append(df_final)
                #%%
                # df_final1=pd.concat(df_c)
                df_final = df_final.reset_index(drop=True)
                df_final=df_final[df_final.iloc[:,0].str.contains("SatesUts")==False]
                df_final=df_final[df_final.iloc[:,0].str.contains("In Lakh Tons")==False]

                df_final['Production_Lakh_MT']=np.where((df_final["Production_Lakh_MT"]==''),np.nan,df_final["Production_Lakh_MT"])
                df_final['Procurement_Lakh_MT']=np.where((df_final["Procurement_Lakh_MT"]==''),np.nan,df_final["Procurement_Lakh_MT"])
                df_final=df_final.replace("Neg",np.nan)
            

                df_final['State']=df_final['State'].apply(lambda x:x.replace('NET (Tirpura)','TRIPURA').strip())
                df_map=City_state_mapping(df_final,'State','State_Clean')
                output = pd.merge(df_final, df_map[['State','State_Clean']], on='State', how = 'left')

                output=output[['State','State_Clean_y','Production_Lakh_MT','Procurement_Lakh_MT','Commodity','Relevant_Date','Runtime']]
                output=output.rename(columns={'State_Clean_y':'State_Clean'})
                output['Relevant_Date'] = pd.to_datetime(output['Relevant_Date'],format='%Y-%m-%d')
                output['Relevant_Date']=output['Relevant_Date'].dt.date
                output=drop_duplicates(output)
                
                Upload_Data('FPD_FOODGRAIN_PROCUREMENT_MONTHLY_CUMU_DATA',output,['MySQL'])
                Upload_Data('FPD_FOODGRAIN_PROCUREMENT_MONTHLY_CUMU_DATA',output,['Clickhouse'])

                os.remove(f1[0])
                os.remove(lnk)
                
         
        log.job_end_log(table_name,job_start_time, no_of_ping)


    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
