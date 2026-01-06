from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import requests
import datetime as datetime
from pytz import timezone
import warnings
warnings.filterwarnings('ignore')
from pandas.core.common import flatten
import os
import re
import csv
import time
import io
import os
#os.chdir(r'D:\Adqvest\ncdex')
import sqlalchemy
import sys
import boto3
from botocore.config import Config
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import JobLogNew as log
import adqvest_db
import adqvest_s3

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

#%%


#%%


def run_program(run_by='Adqvest_Bot', py_file_name=None):

    os.chdir('C:/Users/Administrator/AdQvestDir/')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    #add table name
    table_name = 'ICRA_DAILY_RATINGS_RAW_DATA_Temp_Abdul'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        repeat=0
        check_df = pd.read_sql("SELECT * FROM ICRA_DAILY_FILES_LINKS_Temp_Abdul where Data_Scrap_Status = 'No' and Download_Status='Yes' and Comments = 'Error'",engine)
        if check_df.shape[0]>0:

            # while(repeat<5):


            os.chdir("C:/Users/Administrator/AdQvestDir/ICRA_DOWNLOAD_FOLDER")

            links = pd.read_sql("SELECT * FROM ICRA_DAILY_FILES_LINKS_Temp_Abdul where Data_Scrap_Status = 'No' and Download_Status='Yes' and Comments = 'Error'",engine)
            #links = pd.read_sql("SELECT * FROM ICRA_DAILY_FILES_LINKS_Temp_Abdul where Data_Scrap_Status is null and comments is not null and Relevant_Date>'2020-10-31' and Relevant_Date<'2021-01-01' order by Relevant_Date desc",engine)
            #links = pd.read_sql("SELECT * FROM ICRA_DAILY_FILES_LINKS_Temp_Abdul where Data_Scrap_Status is null and comments is not null and Relevant_Date='2021-02-01' order by Relevant_Date desc",engine)

            #chrome_driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
            download_file_path = r"C:\Users\Administrator\AdQvestDir\ICRA_DOWNLOAD_FOLDER"
            prefs = {
                "download.default_directory": download_file_path,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True
                }


            options = webdriver.ChromeOptions()
            #options.add_argument("--disable-infobars")
            options.add_argument("start-maximized")
            #options.add_argument("--disable-extensions")
            #options.add_argument("--disable-notifications")
            #options.add_argument('--ignore-certificate-errors')
            #options.add_argument('--no-sandbox')
            options.add_experimental_option('prefs', prefs)

            #driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options = options)
            driver = webdriver.Chrome(executable_path=r"C:\Users\Administrator\AdQvestDir\chromedriver.exe",chrome_options=options)
            #driver = webdriver.Chrome(executable_path=chrome_driver_path,chrome_options=options)


            driver.get("https://www.ilovepdf.com/")
            driver.maximize_window()

            driver.find_element_by_xpath("//*[contains(text(),'Log in')]").click()
            email = driver.find_element_by_xpath("//*[@id='loginEmail']")
            email.send_keys("kartmrinal101@outlook.com")
            password = driver.find_element_by_xpath("//*[@id='inputPasswordAuth']")
            password.send_keys("zugsik-zuqzuH-jyvno4")
            time.sleep(1)
            driver.find_element_by_xpath("//*[@id='loginBtn']").click()
            time.sleep(1)
            driver.find_element_by_xpath("//*[contains(text(),'PDF to Excel')]").click()
            time.sleep(1)


            # access_credentials = pd.read_sql("select * from AWS_S3_ACCESS_CREDENTIALS", engine)
            # ACCESS_KEY_ID = access_credentials["Acess_Key_ID"][0]
            # ACCESS_SECRET_KEY = access_credentials["Access_Secret_Key"][0]
            ACCESS_KEY_ID = adqvest_s3.s3_cred()[0]
            ACCESS_SECRET_KEY = adqvest_s3.s3_cred()[1]
            s3 = boto3.resource(
                's3',
                aws_access_key_id=ACCESS_KEY_ID,
                aws_secret_access_key=ACCESS_SECRET_KEY,
                config=Config(signature_version='s3v4', region_name='ap-south-1')
            )



            print(links.shape[0])
            for a,values in links.iterrows():


                try:

                    try:
                        time.sleep(1)
                        driver.find_element_by_class_name("downloader__extra").click()
                        time.sleep(1)
                    except:
                        pass


                    link=values['Links']
                    name=values['Company_Name']
                    date=values['Relevant_Date']
                    file_name=values['File_Name']

                    path = file_name.replace('pdf','xlsx')

                    os.chdir(r"C:/Users/Administrator/AdQvestDir/ICRA_DOWNLOAD_FOLDER")

                    try:
                        os.remove(file_name)
                        os.remove(path)
                    except:
                        pass

                    #no_of_ping += 1
                    #ro.r('download.file("'+url+'", destfile = "/home/ubuntu/crisil_data/' + row['Rating_File_Name'] + '", mode="wb")')
                    # r =  requests.get(link,verify = False,headers={"User-Agent": "XY"})
                    # time.sleep(3)
                    # with open(file_name,'wb') as f:
                    #     f.write(r.content)
                    #     f.close()
                    path = "C:/Users/Administrator/AdQvestDir/ICRA_DOWNLOAD_FOLDER/" + file_name

                    s3.Bucket('adqvests3bucket').download_file('ICRA/' + file_name, path)


                    print(link)
                    time.sleep(5)
                    input_element = driver.find_element_by_xpath("//*[@type='file']")
                    input_element.send_keys(os.getcwd()+"\\" + file_name)
                    time.sleep(10)

                    driver.find_element_by_xpath("//*[@id='processTask']").click()


                    time.sleep(35)

                    os.chdir("C:/Users/Administrator/AdQvestDir/ICRA_DOWNLOAD_FOLDER")

                    path = file_name.replace('pdf','xlsx')
                    with open(path, "rb") as f:
                        sheet = io.BytesIO(f.read())
                    xls = pd.ExcelFile(sheet,engine='openpyxl')
                    sheet_names = xls.sheet_names
                    df=pd.DataFrame()

                    for i in range(len(sheet_names)):
                        with open(path, "rb") as f:
                                file_io_obj = io.BytesIO(f.read())
                        df = pd.read_excel(file_io_obj, engine='openpyxl', sheet_name=sheet_names[i], header = None)
                        #df = pd.read_excel(path, sheet_name=sheet_names[i], header=None)

                        if df.shape[0]>1:
                            break
                        continue

                    os.remove(file_name)
                    os.remove(path)

                    df.dropna(inplace=True,how='all',axis=1)
                    df=df.replace(r'\n',' ',regex=True)
                    df=df.replace(r'#','',regex=True)
                    df=df.replace(r',','',regex=True)
                    df=df.replace(r'\^','',regex=True)
                    df=df.replace(r'\*','',regex=True)
                    df.reset_index(drop=True,inplace=True)


                    req_col_1 = None
                    req_col_2 = None
                    for col in df.columns:
                        if df[col].str.lower().str.contains('trust name').any() or df[col].str.lower().str.contains('transaction name').any():
                            req_col_1=col
                            break

                    for col in df.columns:
                        if df[col].str.lower().str.contains('instrument').any():
                            req_col_2=col
                            break

                    try:
                        df=df.loc[:,req_col_1:]
                    except:
                        df=df.loc[:,req_col_2:]

                    try:
                        try:
                            start_index=df[df.loc[:,req_col_1].str.lower().str.contains('trust name',na=False)==True].index[0]
                        except:
                            start_index=df[df.loc[:,req_col_1].str.lower().str.contains('transaction name',na=False)==True].index[0]
                        try:
                            end_index=df[df.loc[:,req_col_1].str.lower().str.contains('instrument details',na=False)==True].index[0]
                        except:
                            end_index=df[df.loc[:,req_col_1].str.contains('Total',na=False)==True].index[0]

                        df=df.loc[:,req_col_1+1:]

                    except:
                        start_index=df[df.loc[:,req_col_2].str.lower().str.contains('instrument',na=False)==True].index[0]
                        try:
                            end_index=df[df.loc[:,req_col_2].str.contains('Total',na=False)==True].index[0]
                        except:
                            end_index=df[df.loc[:,req_col_2].str.lower().str.contains('instrument details',na=False)==True].index[0]

                    df.columns=df.loc[start_index]
                    df=df.loc[start_index+1:end_index-1]
                    df=df.dropna(axis=1,how='all')
                    df=df.dropna(axis=0,how='all')



                    if df.shape[1]==4:
                        df.columns=['Facilities','Previous_Rated_Amount_Cr','Current_Rated_Amount_Cr','Ratings']
                    elif df.shape[1]==3 and 'Previous' not in df.columns:
                        df.columns=['Facilities','Current_Rated_Amount_Cr','Ratings']
                    elif df.shape[1]==5:
                        df.drop(df.columns[2], axis = 1, inplace = True)
                        df.columns=['Facilities','Previous_Rated_Amount_Cr','Current_Rated_Amount_Cr','Ratings']

                    df.reset_index(inplace = True, drop = True)

                    first_col_error = df[((df['Facilities'] == 'Instrument') | (df['Facilities'] == 'Trust Name')) & (df['Ratings'] == 'Rating Action')].index.to_list()


                    if first_col_error != []:
                        for i in first_col_error:
                            df.drop(i, inplace = True)
                            try:
                                df.drop(i-1, inplace = True)
                            except:
                                pass
                    else:
                        pass

                    df.reset_index(inplace = True, drop = True)

                    na_index_facilities = df[df['Facilities'].isnull()].index.tolist()
                    na_index_ratings = df[df['Ratings'].isnull()].index.tolist()

                    try:
                        if na_index_facilities != []:
                            for i in na_index_facilities:
                                df['Facilities'][i] = df['Facilities'][i - 1]
                        else:
                            pass

                        if na_index_ratings != []:
                            for i in na_index_ratings:
                                df['Ratings'][i] = df['Ratings'][i - 1]
                        else:
                            pass
                    except:
                        df.dropna(subset = ['Facilities'], inplace = True)

                    final_df=df.copy()
                    final_df['Company']=name
                    final_df=final_df.astype('str')
                    final_df['Relevant_Date']=date
                    final_df['Relevant_Date']=pd.to_datetime(final_df['Relevant_Date'])
                    final_df['Runtime']=datetime.datetime.now()
                    final_df['Links']=link
                    final_df.reset_index(drop=True,inplace=True)
                    #final_df=final_df[['Company','Links','Facilities','Previous_Rated_Amount_Cr','Current_Rated_Amount_Cr','Ratings','Relevant_Date','Runtime']]

                    if final_df.shape[0] == 0:
                        raise Exception()
                    else:
                        pass

                    driver.find_element_by_class_name("downloader__extra").click()
                    final_df.to_sql(name='ICRA_DAILY_RATINGS_RAW_DATA_Temp_Abdul',con=engine,if_exists='append',index=False)
                    print(final_df)

                    print('Uploaded')
                    connection.execute("update ICRA_DAILY_FILES_LINKS_Temp_Abdul set Data_Scrap_Status = 'Yes',comments=null where Links = '"+link+"'")

                    if no_of_ping!=0:
                        no_of_ping-=1


                except:

                    try:
                        driver.close()
                    except:
                        pass

                    error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
                    error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
                    print(error_type)
                    print(error_msg)

                    options = webdriver.ChromeOptions()

                    options.add_experimental_option('prefs', prefs)

                    #driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options = options)
                    driver = webdriver.Chrome(executable_path=r"C:\Users\Administrator\AdQvestDir\chromedriver.exe",chrome_options=options)
                    #driver = webdriver.Chrome(executable_path=chrome_driver_path,chrome_options=options)

                    driver.get("https://www.ilovepdf.com/")
                    driver.maximize_window()

                    driver.find_element_by_xpath("//*[contains(text(),'Log in')]").click()
                    email = driver.find_element_by_xpath("//*[@id='loginEmail']")
                    email.send_keys("kartmrinal101@outlook.com")
                    password = driver.find_element_by_xpath("//*[@id='inputPasswordAuth']")
                    password.send_keys("zugsik-zuqzuH-jyvno4")
                    time.sleep(1)
                    driver.find_element_by_xpath("//*[@id='loginBtn']").click()
                    time.sleep(1)
                    driver.find_element_by_xpath("//*[contains(text(),'PDF to Excel')]").click()
                    time.sleep(1)


                    ACCESS_KEY_ID = adqvest_s3.s3_cred()[0]
                    ACCESS_SECRET_KEY = adqvest_s3.s3_cred()[1]
                    s3 = boto3.resource(
                        's3',
                        aws_access_key_id=ACCESS_KEY_ID,
                        aws_secret_access_key=ACCESS_SECRET_KEY,
                        config=Config(signature_version='s3v4', region_name='ap-south-1')
                    )

                    try:
                        connection.execute(f'update ICRA_DAILY_FILES_LINKS_Temp_Abdul set Data_Scrap_Status = "No",comments= "{error_msg}" where Links = "{link}"')
                    except:
                        connection.execute("update ICRA_DAILY_FILES_LINKS_Temp_Abdul set Data_Scrap_Status = 'No',comments= 'Error Second run' where Links = '"+link+"'")
                    continue




                # driver.close()
                # repeat+=1

        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)



if(__name__=='__main__'):
    run_program(run_by='manual')
