import datetime
import os
import re
import sys
import time
import warnings
import requests

import pandas as pd
from pytz import timezone
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import ClickHouse_db
import JobLogNew as log

from adqvest_robotstxt import Robots
robot = Robots(__file__)
from cleancompanies import comp_clean

def drop_duplicates(df):
    columns = df.columns.to_list()
    columns.remove('Runtime')
    df = df.drop_duplicates(subset=columns, keep='last')
    return df

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    engine = adqvest_db.db_conn()
    client = ClickHouse_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'NAUKRI_TOTAL_JOB_POSTING_COMPANY_WISE_WEEKLY_DATA'
    scheduler = ''
    
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]

    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        Rel_date = pd.read_sql("select max(Relevant_Date) as Max from NAUKRI_TOTAL_JOB_POSTING_COMPANY_WISE_WEEKLY_DATA",engine)
        last_rel_date = Rel_date["Max"][0]

        if(today.date()-last_rel_date >= datetime.timedelta(7)):
            chrome_driver_path = 'C:\\Users\\Administrator\\AdQvestDir\\chromedriver.exe'
            
            options = webdriver.ChromeOptions()
            
            options.add_experimental_option("excludeSwitches", ["enable-logging"])
            options.add_argument("--disable-infobars")
            # options.add_argument("start-maximized")
            options.add_argument("--disable-extensions")
            options.add_argument("--disable-notifications")
            options.add_argument('--ignore-certificate-errors')
            options.add_argument('--no-sandbox')
            driver = webdriver.Chrome(executable_path=chrome_driver_path, chrome_options=options)

            # driver.maximize_window()
            # driver.get("https://www.naukri.com/top-company-jobs")
            # driver.implicitly_wait(10)
            time.sleep(3)
            robot.add_link("https://www.naukri.com/top-company-jobs")

            response = requests.get("https://www.naukri.com/top-company-jobs")

            if response.status_code == 200:
                print('Connected')
            else:
                print('Unable to connect to Website')

            html = response.text
            soup = BeautifulSoup(html, 'html.parser')

            # Find all the <a> elements within the specified class
            data = soup.find('div', class_='multiColumn colCount_four').find_all('a')

            Company_Name=[]
            Company_URl=[]

            for p in data:
                Company_Name.append(p.text)
                Company_URl.append(p['href'])


            RunTime = pd.to_datetime('now').strftime("%Y-%m-%d %H:%M")
            Relevant_Date = pd.to_datetime('now').strftime("%Y-%m-%d")

            df2=pd.DataFrame({"Company_Name":Company_Name,
                              "Company_URL":Company_URl,
                              "Relevant_Date": "2022-02-13",
                              "Runtime":RunTime,
                             })

            b=df2.copy()
            output = pd.DataFrame()
            robot.add_link(Company_URl[0])


            for url ,C_name in zip(Company_URl,Company_Name):
                try:
                    limit = 0
                    while True:
                        try:

                            driver.get(url)
                            time.sleep(5)
                            xpath = '//*[@id="jobs-list-header"]/div[1]'
                            wait = WebDriverWait(driver, 10)
                            # names = driver.find_element(By.XPATH,xpath)
                            names = wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
                            reqd = []
                            break
                        except:
                            driver.get('data;')
                            time.sleep(2)
                            if limit > 3:
                                print('Error: ', C_name, '--->', url)

                    if names==[]:
                        for company in names:
                            reqd.append(company.text)
                    else:
                        reqd.append(names.text)

                    if len(reqd) < 5:
                        for i in range(1,6-len(reqd)):
                            reqd.append(None)
                    else:
                        reqd = reqd[0:5]

                    try:

                        job_count = "//*[@id='jobs-list-header']/div[1]/span"
                        count = driver.find_element(By.XPATH,job_count).text
                        count = re.findall(r'-?\d+\.?\d*', count.replace(',', '')).pop()
                        count = int(count)
                        
                    except:
                        count = None

                    #### Induja-- 10/10/23 -- added above line for company name cleaning and fetching####
                    company_name = "//*[@id='jobs-list-header']/div[1]"
                    name = driver.find_element(By.XPATH,company_name).text.split("\n")[1].strip().lower().title()
                    name = name.replace('Jobs','')
                    name = name.strip()
                    
                    company_first = "//*[@id='listContainer']/div[2]/div/div[1]/div/div[2]/span/a[1]"
                    name_first = driver.find_elements('xpath',company_first)[0].text.strip().lower().title()
                    reqd = [x.lower() if x!=None else None for x in reqd ]
                    name_list = list(set(reqd)-({None}))
                    name_list = [x.lower() if x!=None else None for x in name_list]

                    # if name in name_list:
                    #     boolean = 0
                    # else:
                    #     boolean = 1
                    if C_name.partition(' ')[0].lower() in name.lower() and name.lower() in name_first.lower():
                        value = 1
                    elif C_name == 'TCS Jobs' and  name == 'Tata Consultancy Services Tcs' and name.lower() in name_first.lower():
                        value = 1
                    else:
                        value = 0
                    data = pd.DataFrame({"Company_Main_Page":C_name,"Company":name,"First_Company_In_List": name_first,"Job_Posting":count,'Is_Valid':value},index=[0])
                    print(data)
                    output = pd.concat([output,data])
        
                    output["Relevant_Date"]=Relevant_Date
                    output["Runtime"]=RunTime

                except:
                    print('Error: ', C_name, '--->', url)
                    continue

            # Naukri_Data=output.loc[output['Data_Check'] == 1]
            # Naukri_Data=output.loc[output['Is_Valid'] == 1]
            # Naukri_Data=Naukri_Data.drop(columns=['Data_Check','Filter'])
            # Naukri_Data=output
            ##############################################################################################
            Naukri_Data,un_mapped = comp_clean(output, 'Company', 'internet', 'Company_Name_Clean')

            Naukri_Data=drop_duplicates(Naukri_Data)
            Naukri_Data.to_sql(name = "NAUKRI_TOTAL_JOB_POSTING_COMPANY_WISE_WEEKLY_DATA",if_exists = "append",con = engine,index = False)
            print("Done")
            ########################################################################################
            if len(un_mapped) > 0:
                um_mactch_df=Naukri_Data[(Naukri_Data["Company"].isin(un_mapped)) & (Naukri_Data["Is_Valid"]==1)]

                if um_mactch_df.empty == False:
                    unmapped=um_mactch_df.Company.tolist()
                    raise Exception(f'--------Complete Mapping Failed--------\nList of unmapped companies:\n{list(set(unmapped))}')
                
                

            click_max_date = client.execute("select max(Relevant_Date) from AdqvestDB.NAUKRI_TOTAL_JOB_POSTING_COMPANY_WISE_WEEKLY_DATA")
            click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])

            query = 'select * from AdqvestDB.NAUKRI_TOTAL_JOB_POSTING_COMPANY_WISE_WEEKLY_DATA where Relevant_Date > "' + click_max_date + '"'
            final_df = pd.read_sql(query,engine)

            client.execute("INSERT INTO AdqvestDB.NAUKRI_TOTAL_JOB_POSTING_COMPANY_WISE_WEEKLY_DATA VALUES", final_df.values.tolist())
            print("Data Loaded Succesfully in click house ".format(len(final_df)))
            print("All done")

        else:
            print("Data already updated")
        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')