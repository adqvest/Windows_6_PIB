import sqlalchemy
import pandas as pd
import os
import requests
from bs4 import BeautifulSoup
import re
import datetime as datetime
from pytz import timezone
import sys
import warnings
import time
import re
import io
import glob
import csv
import calendar
warnings.filterwarnings('ignore')
import numpy as np
from selenium import webdriver
import requests
from time import sleep
import re
import datetime as datetime
import numpy as np
import sys
import time
from PyPDF2 import PdfFileReader, PdfWriter
from playwright.sync_api import sync_playwright

from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.proxy import Proxy, ProxyType
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

#%%
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import JobLogNew as log
import adqvest_db
import ClickHouse_db
import boto3
#%%
from adqvest_robotstxt import Robots
robot = Robots(__file__)


def row_col_index_locator(df,l1):
    df.reset_index(drop=True,inplace=True)
    df.fillna('#', inplace=True)
    
    index2=[]
    dict1={}
    for j in l1:
        for i in range(df.shape[1]):
            if df.iloc[:,i].str.lower().str.contains(j).any()==True:
                print(f"Found--{j}")
                print(f"Column--{i}")
                index2.append(i)
                row_index=df[df.iloc[:, i].str.lower().str.contains(j) == True].index[0]
                print(f"Row--{row_index}")
                index2.append(row_index)
                break
                
    return index2



def pdf_to_excel(file_path,download_path):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=10000,
                                    chromium_sandbox=True,
                                    downloads_path=download_path)
        page = browser.new_page()
        page.goto("https://www.ilovepdf.com/",timeout=30000*5)

        page.locator("//*[contains(text(),'Log in')]").click()
        email = page.locator("//*[@id='loginEmail']")
        email.fill("kartmrinal101@outlook.com")
        password = page.locator("//*[@id='inputPasswordAuth']")
        password.fill("zugsik-zuqzuH-jyvno4")
        page.locator("//*[@id='loginBtn']").click()
        page.get_by_title("PDF to Excel").click()

        for i in file_path:
            with page.expect_file_chooser() as fc_info:
                page.get_by_text("Select PDF file").click()
                file_chooser = fc_info.value
                file_chooser.set_files(i)
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


def download(chrome):
    print('Inside Download(driver)')
    chrome.switch_to.default_content()
    time.sleep(3)
    chrome.switch_to.frame("reportFrame")

    time.sleep(3)
    chrome.switch_to.frame("openDocChildFrame")
    time.sleep(3)

    chrome.switch_to.frame("webiViewFrame")
    time.sleep(10)
    print(chrome.find_element(By.XPATH,'//*[@id="iconleft_iconMenu_arrow__dhtmlLib_239"]/div'))
    chrome.find_element(By.XPATH,'//*[@id="iconleft_iconMenu_arrow__dhtmlLib_239"]/div').click()
    time.sleep(10)
    c1=chrome.find_element(By.XPATH,'//*[@id="iconMenu_menu__dhtmlLib_239_item__menuAutoId_3"]')
    print(c1)
    c1.click()

    time.sleep(10)
    chrome.find_element(By.XPATH,'//*[@id="_dhtmlLib_244_span_text__menuAutoId_4"]').click()
    time.sleep(3)

    return chrome

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

def split_pdf_to_four(filename):
    from PyPDF2 import PdfReader
    # pdfReader = PdfFileReader(open(filename, "rb"))
    pdfReader1 = PdfReader(open(filename, "rb"))
    print(filename)
    # pages=pdfReader1.getNumPages()
    pages=len(pdfReader1.pages)
    
    #try:
    pdf_writer1 = PdfWriter()
    pdf_writer2 = PdfWriter()
    pdf_writer3 = PdfWriter()
    pdf_writer4 = PdfWriter()

    page_break = round(pages/4)
    

    for page in range(page_break):
        pdf_writer1.add_page(pdfReader1.pages[page])

    for page in range(page_break,page_break*2):
        pdf_writer2.add_page(pdfReader1.pages[page])

    for page in range(page_break*2,page_break*3):
        pdf_writer3.add_page(pdfReader1.pages[page])

    for page in range(page_break*3,len(pdfReader1.pages)):
        pdf_writer4.add_page(pdfReader1.pages[page])

    with open("part1.pdf", 'wb') as file1:
        pdf_writer1.write(file1)

    with open("part2.pdf", 'wb') as file2:
        pdf_writer2.write(file2)

    with open("part3.pdf", 'wb') as file3:
        pdf_writer3.write(file3)

    with open("part4.pdf", 'wb') as file4:
        pdf_writer4.write(file4)

def get_date(month1,year):
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

#%%

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    client = ClickHouse_db.db_conn()
    #****   Date Time ****
    import datetime
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    job_start_time = datetime.datetime.now(india_time)
    table_name = 'RBI_ALL_INDIA_RURAL_WAGES_MEN_MONTHLY_DATA'
    if(py_file_name is None):
       py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0
    try :

        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)



        if today.day==1 or today.day==16:                #Added condition | Joe
            '''Since we are not able to access site, this will be like an alert'''
            
            # os.chdir(r'C:\Users\Administrator\RBI_Wages')
            os.chdir(r'C:\Users\Administrator\RBI_Wages')
            delete_pdf=os.listdir(r"C:\Users\Administrator\RBI_Wages")
            for file in delete_pdf:
                os.remove(file)

            chrom_driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
            down_path =r"C:\Users\Administrator\RBI_Wages"
            #%%
            # chrom_driver_path="C:/Users/Santonu/Desktop/ADQvest/Chrome Driver/chromedriver.exe"
            # # os.chdir(r"C:\Users\Santonu\Desktop\ADQvest\Error files\today error")
            # down_path=os.getcwd()

            executable_path= chrom_driver_path
            os.chdir(down_path)
            options = webdriver.ChromeOptions()
            options.add_argument("--disable-infobars")
            options.add_argument("start-maximized")
            options.add_argument("--disable-extensions")

            # options.add_argument("--disable-software-rasterizer")
            options.add_argument("--disable-gpu")
            # options.add_argument("--headless")
            options.add_argument("--disable-notifications")
            options.add_argument('--ignore-certificate-errors')
            options.add_argument('--no-sandbox')
            download_file_path = down_path
            prefs = {
                "download.default_directory": download_file_path,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "plugins.always_open_pdf_externally": True,
                 # "safebrowsing.enabled": True
                }


            capabilities = dict(DesiredCapabilities.CHROME)
            options.add_experimental_option('prefs', prefs)
            max_rel_date = pd.read_sql("select max(Relevant_Date) as Max from RBI_ALL_INDIA_RURAL_WAGES_MEN_MONTHLY_DATA",engine)["Max"][0]
            print(max_rel_date)
            url = 'https://dbieold.rbi.org.in/DBIE/statistics.rbi?action=RL&f=30&sd=40'
            robot.add_link(url)
            driver = webdriver.Chrome(executable_path=executable_path,chrome_options=options)
            driver.get(url)
            time.sleep(10)
            data=pd.read_html(driver.page_source)
            df1=pd.concat(data)
            col=row_col_index_locator(df1,['to'])[0]
            row=row_col_index_locator(df1,['average daily wage rates'])[1]

            from datetime import datetime
            data_max_date=pd.to_datetime(str(datetime.strptime(df1.iloc[row,col].title(),'%d-%b-%Y')), format='%Y-%m-%d').date()
            if data_max_date>max_rel_date:
                import datetime
                ##################################################################################################
                driver.implicitly_wait(15)
                link_xpath='/html/body/div/ul/li[3]/table/tbody/tr/td[1]/a'

                driver.find_element(By.XPATH, link_xpath).click()
                driver.implicitly_wait(15)
                time.sleep(2)
                download(driver)
                time.sleep(2)
                time.sleep(80)
                #################################################################################################
                
                os.chdir(r'C:\Users\Administrator\RBI_Wages')
                path=os.getcwd()
                files = glob.glob(os.path.join(path, "*.pdf"))
                print(files)
                file=files[0]

                S3_upload('Average_Daily_Wage_Rates_(in_Rs.)_in_Rural_India_for_Men.pdf','RBI_ALL_INDIA_RURAL_WAGES_MEN_MONTHLY_DATA')
                print("File loded to s3")
                ####################################################################################################
                os.chdir(r'C:\Users\Administrator\RBI_Wages')
                split_pdf_to_four(file)
                ###################################################################################################
                os.chdir(r'C:\Users\Administrator\RBI_Wages')
                download_path=os.getcwd()
                pdf_list = glob.glob(os.path.join(path, "*.pdf"))
                print(pdf_list)

                matching = [s for s in pdf_list if "part" in s]
                print('Matching')
                print(matching)
                pdf_to_excel(matching,download_path)
                #################################################################################################

                File3_4=['part3.xlsx','part4.xlsx']

                con_data = []
                final_df=pd.DataFrame()
                for f1 in File3_4:
                    xl = pd.ExcelFile(f1)
                    sheets=[xl.sheet_names ]
                    c1=[]
                    for sheet in sheets:
                        if sheet=='Table 23':
                            pass
                        else:
                            df1=pd.DataFrame()
                            df1=pd.read_excel(f1,sheet_name=sheet) #Its acts as a dictionary

                    for v in df1.values():
                            c1.append(v)
                            # df1=pd.concat(c1)
                            # df1=df1.dropna(thresh=4,axis=1)
                            # df1=df1.dropna(thresh=3,axis=0)

                    for c in range(len(c1)):
                        print(f"Working on table--->{c}")
                        for i in range(2,len(c1[c].columns)):
                            try:
                                 data_1 = c1[c].melt(id_vars=["Month","State"], value_vars=[c1[c].columns[i]])
                            except:
                                pass
                            data_1["Runtime"] = datetime.datetime.now()
                            data_1=data_1.rename(columns={"variable":"Type"})
                            data_1=data_1.rename(columns={"value":"Wage_INR"})
                            con_data.append(data_1)

                data_final=pd.concat(con_data)
                # final_df= pd.concat([final_df,data_final])  ##Dont understand
                final_df= pd.concat([data_final])
                final_df=final_df.dropna(subset=['State'])
                final_df.reset_index(drop=True,inplace=True)
                final_df['Type']=final_df['Type'].replace(r"\n",'',regex=True)
                final_df['Month']=final_df['Month'].apply(lambda x:get_date(x.split("-")[0].strip(), x.split("-")[1].strip()))
                final_df['Wage_INR']=final_df['Wage_INR'].replace("-",np.nan)
                final_df.columns=['Relevant_Date','State','Type','Wage_INR','Runtime']
                final_df=final_df[['State','Type','Wage_INR','Relevant_Date','Runtime']]
                final_df['Wage_INR']=final_df['Wage_INR'].replace("-",np.nan)
                final_df['Type']=final_df['Type'].replace("Sweeper","Sweeping/ Cleaning Workers")
                final_df['Type']=final_df['Type'].replace("Sowing","Sowing (including Planting/ Transplanting/Weeding workers)")
                final_df['Type']=final_df['Type'].replace("Ploughing","Ploughing/Tilling Workers")
                final_df['Type']=final_df['Type'].replace("Picking","Picking Workers (includingTea, Cotton, Tobacco &other commercialcrops")
                final_df['Type']=final_df['Type'].replace("Harvesting","Harvesting/ Winnowing/ Threshing workers")
                final_df['Type']=final_df['Type'].replace("Blacksmit","Blacksmith")

                print(final_df)
                final_df=final_df[final_df['Relevant_Date']>max_rel_date]
                final_df.reset_index(drop=True,inplace=True)
                print(final_df.info())
                if final_df.shape[0]==0:
                    print("No new data")
                else:
                    final_df.to_sql(name='RBI_ALL_INDIA_RURAL_WAGES_MEN_MONTHLY_DATA',con=engine,if_exists='append',index=False)
                ########################################################################################################################

                File1_2=['part1.xlsx','part2.xlsx']
                con_data = []
                # for file in files2:
                final_df=pd.DataFrame()
                for f2 in File1_2:
                    xl = pd.ExcelFile(f2)
                    sheets=[xl.sheet_names]
                    c2=[]
                    for sheet in sheets:
                        if sheet=='Table 1':
                            pass
                        else:
                            df2=pd.read_excel(f2,sheet_name=sheet)
                    for v in df2.values():
                            c2.append(v)
                          

                    for c in range(len(c2)):
                        print(f"Working on table--->{c}")
                        for i in range(2,len(c2[c].columns)):
                            try:
                                 data_1 = c2[c].melt(id_vars=["Month","State"], value_vars=[c2[c].columns[i]])
                            except:
                                pass
                            data_1["Runtime"] = datetime.datetime.now()
                            data_1=data_1.rename(columns={"variable":"Type"})
                            data_1=data_1.rename(columns={"value":"Wage_INR"})
                            con_data.append(data_1)

                data_final=pd.concat(con_data)
                final_df= pd.concat([final_df,data_final])
                final_df=final_df.dropna(subset=['State'])
                data_final.reset_index(drop=True,inplace=True)
                final_df['Type']=final_df['Type'].replace(r"\n",'',regex=True)
                final_df['Month']=final_df['Month'].apply(lambda x:get_date(x.split("-")[0].strip(), x.split("-")[1].strip()))
                final_df['Wage_INR']=final_df['Wage_INR'].replace("-",np.nan)
                final_df.columns=['Relevant_Date','State','Type','Wage_INR','Runtime']
                final_df=final_df[['State','Type','Wage_INR','Relevant_Date','Runtime']]
                final_df['Wage_INR']=final_df['Wage_INR'].replace("-",np.nan)
                final_df['Type']=final_df['Type'].replace("Sweeper","Sweeping/ Cleaning Workers")
                final_df['Type']=final_df['Type'].replace("Sowing","Sowing (including Planting/ Transplanting/Weeding workers)")
                final_df['Type']=final_df['Type'].replace("Ploughing","Ploughing/Tilling Workers")
                final_df['Type']=final_df['Type'].replace("Picking","Picking Workers (includingTea, Cotton, Tobacco &other commercialcrops")
                final_df['Type']=final_df['Type'].replace("Harvesting","Harvesting/ Winnowing/ Threshing workers")
                final_df['Type']=final_df['Type'].replace("Blacksmit","Blacksmith")

                print(final_df)
                final_df=final_df[final_df['Relevant_Date']>max_rel_date]
                final_df.reset_index(drop=True,inplace=True)
                print(final_df)
                if final_df.shape[0]==0:
                    print("No new data")
                else:
                    final_df.to_sql(name='RBI_ALL_INDIA_RURAL_WAGES_MEN_MONTHLY_DATA',con=engine,if_exists='append',index=False)

                ##########################################################################################################################
            else:
                print('Data upto date')
        log.job_end_log(table_name,job_start_time,no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])
        error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)

        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
