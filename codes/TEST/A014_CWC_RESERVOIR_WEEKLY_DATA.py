import sqlalchemy
import pandas as pd
from pandas.io import sql
import os
import requests
from bs4 import BeautifulSoup
from dateutil import parser
import re
import datetime as datetime
from pytz import timezone
import sys
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
#sys.path.insert(0,r'C:\Adqvest')
import adqvest_db
import JobLogNew as log
import ClickHouse_db

import numpy as np
import time
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

pd.options.display.max_columns = None
pd.options.display.max_rows = None

#%%
def column_locator(df,l1):
    df.reset_index(drop=True,inplace=True)
    df.fillna('#', inplace=True)
    index2=[]
    dict1={}
    for j in l1:
        for i in range(df.shape[1]):
                if df.iloc[:,i].str.contains(j).any()==True:
                    print(f"found--{j}")
                    print(i)
                    dict1[i]=j
                    index2.append(i)
                    break

    for k,v in dict1.items():
          df=df.rename(columns={f"{df.columns[k]}":f"{v}"})
          # df.reset_index(drop=True,inplace=True)
          df=df.replace('#',None)
          
    return df    

def row_modificator(df,l1,col_idx,col_idx_2,row_del=False,first_pos=False):
  df.fillna('#', inplace=True)
  for i in l1:
    df = df.reset_index(drop=True)
    if first_pos==True:
        df = df.reset_index(drop=True)
        r1=df[df.iloc[:,col_idx].str.contains(i)==True].index.to_list()
        for k in r1:
                if (df.iloc[k,col_idx_2]=='#'):
                    df.drop(index=[k],inplace=True)
                    df[df.columns[col_idx]]=df[df.columns[col_idx]].str.strip()


  df.replace('#',None, inplace=True)                                      
  return df

  #%%

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

        ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'CWC_RESERVOIR_WEEKLY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        client = ClickHouse_db.db_conn()

        os.chdir('C:/Users/Administrator/Junk')

        for file in  os.listdir():
            os.remove(file)

        url_start='http://cwc.gov.in'
        page_url='http://cwc.gov.in/reservoir-level-storage-bulletin'

        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.79 Safari/537.36"}

        for i in range(0,1):
#            page_url=page_url+str(i)

            req = requests.get(page_url,headers = headers)
            no_of_ping += 1
            req.raise_for_status()
            soup = BeautifulSoup(req.content)
            print(page_url+str(i))
            for j in range(0,1):
                url_end=soup.findAll(class_='views-field views-field-field-upload-pdf')[j+1].findAll('a')[-1]['href'] #change to j+1
                url=url_start+url_end
                file=soup.findAll(class_='views-field views-field-field-upload-pdf')[j+1].findAll('a')[0]['href'].split("files/")[-1]
                print(file)#change to j+1

                title = soup.findAll(class_='newitem')[j]
                title = title.get_text()
                print(title)
                title =  title.lower().split("as on")[-1].strip()
                title=re.sub(r"[\([{})\]]", "", title)
                print(title)
                print(type(title))
                try:
                    date = datetime.datetime.strptime(title,"%d.%m.%y").date()
                except:
                    date = datetime.datetime.strptime(title,"%d.%m.%Y").date()
                print(date)

                db_rel_date = pd.read_sql('Select max(Relevant_Date) as db_rel_date from CWC_RESERVOIR_WEEKLY_DATA',engine)
                db_rel_date = db_rel_date['db_rel_date'][0]
                print(db_rel_date)



                if((date > db_rel_date)):

                    req = requests.get(url,verify=False,headers = headers)
                    no_of_ping += 1
                    print(url)
                    req.raise_for_status()
                    with open("downloaded_file.pdf",'wb') as f:
                        f.write(req.content)
                        f.close()


                    chrome_driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
                    download_file_path = r"C:\Users\Administrator\Junk"
                    prefs = {
                        "download.default_directory": download_file_path,
                        "download.prompt_for_download": False,
                        "download.directory_upgrade": True
                        }



                    #options = webdriver.ChromeOptions()


                    chrome_driver = "C:/Users/Administrator/AdQvestDir/chromedriver.exe"
                    options = webdriver.ChromeOptions()
                    options.add_argument("--disable-infobars")
                    options.add_argument("start-maximized")
                    options.add_argument("--disable-extensions")
                    options.add_argument("--disable-notifications")
                    options.add_argument('--ignore-certificate-errors')
                    options.add_argument('--no-sandbox')
                    options.add_experimental_option('prefs', prefs)
                    driver = webdriver.Chrome(executable_path=chrome_driver, chrome_options=options)

                    driver = webdriver.Chrome(executable_path=chrome_driver_path, chrome_options=options)

                    driver.get("https://www.ilovepdf.com/")
                    no_of_ping += 1
                    driver.maximize_window()

                    driver.find_element(By.XPATH,"//*[contains(text(),'Log in')]").click()
                    email = driver.find_element(By.XPATH,"//*[@id='loginEmail']")
                    email.send_keys("kartmrinal101@outlook.com")
                    password = driver.find_element(By.XPATH,"//*[@id='inputPasswordAuth']")
                    password.send_keys("zugsik-zuqzuH-jyvno4")
                    time.sleep(1)
                    driver.find_element(By.XPATH,"//*[@id='loginBtn']").click()
                    time.sleep(1)
                    driver.find_element(By.XPATH,"//*[contains(text(),'PDF to Excel')]").click()
                    time.sleep(1)

                    input_element = driver.find_element(By.XPATH,"//*[@type='file']")
                    input_element.send_keys(os.getcwd()+"\\downloaded_file.pdf")
                    time.sleep(1)
                    driver.find_element(By.XPATH,"//*[@id='processTask']").click()

                    time.sleep(30)


                    path = "downloaded_file.xlsx"


                    # heading  = 'WEEKLY REPORT OF (.*) IMPORTANT RESERVOIRS OF INDIA'
                    
                    heading = 'NAME OF RESERVOIR'
                    end_text = 'TOTAL FOR (.*) RESERVOIRS'
                    xls = pd.ExcelFile(path)
                    sheets = xls.sheet_names

                    all_data = []
                    cwc_df = pd.DataFrame()

                    try:
                        for i in range(len(sheets)):
                            sheet = sheets[i]
                            data = pd.read_excel(path, sheet_name=sheet)

                            text = data.fillna('').to_string()
                            text = re.sub(r'  +', ' ', text).strip()
                            if (re.search(heading, text, re.IGNORECASE)):
                                print(i)
                                all_data.append(i)
                                data_rows = data.shape[0]
                                data.reset_index(inplace=True, drop=True)
                                data = data.replace('', np.nan)
                                data.dropna(axis=1, thresh=data_rows / 2, inplace=True)
                                print(data.shape)
                                cwc_df = pd.concat([cwc_df, data])
                            if (re.search(end_text, text, re.IGNORECASE)):
                                print(sheet)
                                all_data.append(i)
                                print("stopped")
                                break

                        all_dfs = []
                        if cwc_df.empty == True:

                            reqd_sheets = sheets[all_data[0]:all_data[-1]]

                            for vals in reqd_sheets:
                                data = pd.read_excel(path, sheet_name=vals)
                                all_dfs.append(data)

                        if all_dfs != []:
                            all_dfs1 = [x for x in all_dfs if x.empty == False]
                        try:

                            #                          cwc_df  = pd.DataFrame()
                            cwc_df = pd.concat(all_dfs1)
                        except:
                            cwc_df = cwc_df
                    #                    cwc_df1 = cwc_df.copy()
                    except:
                        raise Exception("Unable to Capture data from Sheets")

                    # cwc_df.columns = [x for x in range(cwc_df.shape[1])]
                    # cwc_df.rename(columns={0:'S.No',
                    # 1:'SR',
                    # 2:'FRL',
                    # 3:'Current_Reservoir_Level',
                    # 4:'Live_Capacity_At_FRL_BCM',
                    # 5:'Current_Live_Storage',
                    # 6:'Date',
                    # 7:'Current_Year',
                    # 8:'Last_Year',
                    # 9:'Last_Ten_Years_Average_Percentage_Of_Live_Capacity_At_FRL',
                    # 10:'IRR',
                    # 11:'Hydel'},inplace=True)
                    cwc_df = cwc_df.drop(cwc_df.columns[[0]], axis=1)
                    print(cwc_df)
                    ###############################################################################################
                    cwc_df = column_locator(cwc_df, l1=['NAME OF  RESERVOIR', 'CURRENT RESERVO IR LEVEL', 'S. NO', 'LIVE CAPACIT Y',
                                                        'FRL', 'LIVE CAPACIT AT FRL', 'CURRENT LIVE STORAGE', 'DATE',
                                                        'CURREN T YEAR', 'LAST YEAR', 'LAST 10 YEARS AVERA GE', 'IRR', 'HYD EL'])

                    cwc_df = cwc_df[['S. NO', 'NAME OF  RESERVOIR', 'FRL', 'CURRENT RESERVO IR LEVEL',
                                     'LIVE CAPACIT Y', 'CURRENT LIVE STORAGE', 'DATE', 'CURREN T YEAR',
                                     'LAST YEAR', 'LAST 10 YEARS AVERA GE', 'IRR', 'HYD EL']]

                    # cwc_df.reset_index(drop=True,inplace=True)
                    print(cwc_df)
                    #######################################################################################################

                    # cwc_df.reset_index(drop=True,inplace=True)
                    cwc_df.columns = ['Sno', 'Name_Of_Reservoir', 'FRL', 'Current_Reservoir_Level',
                                      'Live_Capacity_At_FRL_BCM', 'Current_Live_Storage', 'Date', 'Current_Year',
                                      'Last_Year', 'Last_Ten_Years_Average_Percentage_Of_Live_Capacity_At_FRL', 'IRR', 'Hydel']

                    # cwc_df.reset_index(drop=True,inplace=True)
                    # cwc_df.rename(columns={'SR':'Name_Of_Reservoir','S.No':'Sno'},inplace=True)
                    cwc_df = cwc_df.replace("", None)
                    print(cwc_df)

                    cwc_df['Region'] = np.nan
                    # cwc_df['State']=np.nan

                    cwc_df["Name_Of_Reservoir"] = np.where((cwc_df["Name_Of_Reservoir"].isnull() & cwc_df["Sno"].notnull()), cwc_df["Sno"],
                                                           cwc_df["Name_Of_Reservoir"])

                    cwc_df['Sno'] = np.where((cwc_df['Sno'].isnull() == True), "", cwc_df['Sno'])
                    cwc_df['Sno'] = cwc_df['Sno'].apply(lambda x: str(x))
                    cwc_df = cwc_df[cwc_df['Sno'].str.lower().str.contains("week") == False]
                    cwc_df = cwc_df[cwc_df['Sno'].str.lower().str.contains("total") == False]

                    cwc_df['Name_Of_Reservoir'] = cwc_df['Name_Of_Reservoir'].apply(lambda x: str(x))
                    cwc_df['Region'] = np.where((cwc_df['Sno'].str.contains('REGION')), cwc_df['Sno'], np.nan)
                    cwc_df['Region'] = cwc_df['Region'].ffill()
                    cwc_df.fillna('#', inplace=True)
                    cwc_df = cwc_df[(cwc_df['Region'] == '#') == False]

                    # cwc_df = cwc_df[cwc_df["Region"].notnull()]
                    print(cwc_df)

                    ########################################################################################

                    l2 = ['NORTHERN REGION', 'EASTERN REGION', 'WESTERN REGION',
                          'CENTRAL REGION', 'WESTERN REGION', 'SOUTHERN REGION']

                    cwc_df = row_modificator(cwc_df, l2, col_idx=1, col_idx_2=2, row_del=True, first_pos=True)

                    cwc_df = cwc_df[cwc_df["Name_Of_Reservoir"] != '2']
                    cwc_df = cwc_df[cwc_df['Name_Of_Reservoir'].str.contains('NAME OF (.*)') == False]
                    cwc_df.reset_index(drop=True, inplace=True)
                    cwc_df = cwc_df.replace(to_replace='None', value=np.nan)
                    cwc_df.dropna(subset=['Name_Of_Reservoir'], inplace=True)
                    cwc_df.fillna('#', inplace=True)

                    cwc_df = cwc_df.replace("#", "")
                    # cwc_df=cwc_df.replace("",np.nan)

                    cwc_df['State'] = np.nan
                    cwc_df['State'] = np.where(((cwc_df["Sno"] == "") & (cwc_df["Name_Of_Reservoir"] != "")), cwc_df['Name_Of_Reservoir'],
                                               None)
                    cwc_df['State'] = np.where((cwc_df["FRL"] == ""), cwc_df['Name_Of_Reservoir'], np.nan)
                    cwc_df['State'] = cwc_df['State'].ffill()
                    # cwc_df.replace('#',np.nan, inplace=True)
                    cwc_df.fillna('#', inplace=True)
                    cwc_df = cwc_df[(cwc_df['FRL'] != '#')]
                    # cwc_df = cwc_df[cwc_df["FRL"].notnull()]

                    cwc_df["Date"] = cwc_df["Date"].apply(lambda x: pd.to_datetime(x).date())
                    print(cwc_df)

                    #######################################################################################
                    # cwc_df['State'] = np.where(((cwc_df["Sno"] == "") & (cwc_df["Name_Of_Reservoir"] != "")),cwc_df['Name_Of_Reservoir'],np.nan)
                    # cwc_df['State']=cwc_df['State'].ffill()
                    # cwc_df = cwc_df[cwc_df["FRL"].notnull()]

                    # cwc_df = cwc_df[cwc_df["Name_Of_Reservoir"] != '2']
                    # cwc_df = cwc_df[cwc_df['Name_Of_Reservoir'].str.contains('NAME OF (.*)') == False]
                    # cwc_df = cwc_df[cwc_df['FRL'].str.contains('NAME OF  RESERVOIR') == False]

                    # cwc_df["Date"] = cwc_df["Date"].apply(lambda x: pd.to_datetime(x).date())
                    # cwc_df['Date'] = pd.to_datetime(cwc_df['Date'], format='%Y-%d-%m')
                    # cwc_df['Date']=cwc_df['Date'].dt.date

                    ###################################################################################


                    cwc_df.rename(columns={'Sno': 'S_No',
                                           'FRL': 'FRL_M',
                                           'Current_Reservoir_Level': 'Current_Reservoir_Level_M',
                                           'Live_Capacity_At_FRL': 'Live_Capacity_At_FRL_BCM',
                                           'Current_Live_Storage': 'Current_Live_Storage_BCM',
                                           'Current_Year': 'Current_Year_Percentage_Of_Live_Capacity_At_FRL',
                                           "Last_Year": "Last_Year_Percentage_Of_Live_Capacity_At_FRL",
                                           "Last_Ten_Years_Average": "Last_Ten_Years_Average_Percentage_Of_Live_Capacity_At_FRL",
                                           "IRR": "IRR_TH_HA",
                                           "Hydel": "Hydel_MW"
                                           }, inplace=True)

                    cwc_df["Relevant_Date"] = date
                    cwc_df['Runtime'] = today.strftime("%Y-%m-%d %H:%M:%S")
                    cwc_df['File_Name'] = path

                    cwc_df["State"] = cwc_df["State"].apply(lambda x: x.strip())
                    cwc_df["State"] = cwc_df["State"].apply(lambda x: x.replace("\xa0", " "))
                    cwc_df["State"] = np.where((cwc_df["State"] == "CHHATTIS GARH"), "CHHATTISGARH", cwc_df["State"])
                    cwc_df["State"] = np.where((cwc_df["State"] == "UTTRAKHAND"), "UTTARAKHAND", cwc_df["State"])
                    cwc_df.reset_index(drop=True, inplace=True)
                    cwc_df = cwc_df[cwc_df.iloc[:, 2] != '']
                    cwc_df['S_No'] = [item for item in range(1, len(cwc_df) + 1)]
                    print(cwc_df)
                    if((cwc_df['Relevant_Date'].max() > db_rel_date)):
                        print("yes")

                        cwc_df.to_sql(name='CWC_RESERVOIR_WEEKLY_DATA',con=engine,if_exists='append',index=False)
                        print("Data Loaded Succesfully with {} rows".format(len(cwc_df)))
                        # ClickHouse
                        cwc_df.replace({np.nan: None}, inplace = True)
                        # client.execute("INSERT INTO CWC_RESERVOIR_WEEKLY_DATA VALUES", cwc_df.values.tolist())
                        click_max_date = client.execute("select max(Relevant_Date) from AdqvestDB.CWC_RESERVOIR_WEEKLY_DATA")
                        print(click_max_date)
                        click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])
                        query = 'select * from AdqvestDB.CWC_RESERVOIR_WEEKLY_DATA WHERE Relevant_Date > "' + click_max_date + '"'
                        df = pd.read_sql(query,engine)
                        client.execute("INSERT INTO CWC_RESERVOIR_WEEKLY_DATA VALUES",df.values.tolist())

        log.job_end_log(table_name,job_start_time, no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
