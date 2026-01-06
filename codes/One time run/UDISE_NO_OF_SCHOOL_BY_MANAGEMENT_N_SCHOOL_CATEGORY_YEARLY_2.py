from selenium import webdriver
from selenium.webdriver.support.ui import Select
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import time
import datetime
from pytz import timezone
import pandas as pd
import sys
import re
import os
import numpy as np
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
from adqvest_robotstxt import Robots
robot = Robots(__file__)
engine = adqvest_db.db_conn()
connection = engine.connect()

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/')
from State_Function import state_rewrite

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/District_Function')
import district_rewrite

def fetch_state_alpha_codes(engine):
    query = "SELECT State, State_Alpha_Code FROM AdqvestDB.INDIA_STATE_N_STATE_ALPHA_CODE_GST_CODE_MAPPING"
    df = pd.read_sql(query, engine)
    return df

def date1(x):
    return datetime.date(int(str(20)+x[-2:]),3,31)

def check_for_rows(myrow, mylist):
    checklist = mylist.copy()
    for val in mylist:
        for cell in myrow:
            if type(cell) == str:
                if val in cell:
                    checklist.remove(val)
                    break
           
    if checklist == []:
        return True
    else:
        return False

def Clean_df(df):

    grade_row = df.apply(lambda row: check_for_rows(row,['Location','School Management']), axis=1)
    gender_row = df.apply(lambda row: check_for_rows(row,['PS (I-V)','UPS (I-VIII)','HSS (I-XII)','UPS (VI-VIII)','HSS (VI-XII)','SS (I-X)','SS (VI-X)','SS (IX-X)','HSS (IX-XII)','HSS (XI-XII)']), axis=1)
    cols = []
    for grade, gender in zip(df[grade_row].iloc[0,:], df[gender_row].iloc[0,:]):
        if any(pd.Series([grade,gender]).isna()):
            cols.append(gender.strip())
            continue
        else:
            cols.append((grade.strip() + ' ' + gender.strip()).replace(' ','_').replace('-','_').replace('_Enrolment',''))
    df.columns = cols
    df.drop([df.index[grade_row][0],df.index[gender_row][0]], inplace = True)
    
    start_idx = df.apply(lambda row: row.astype(str).str.contains('Academic')).any(axis=1).idxmax()
    df = df.iloc[start_idx + 1:].reset_index(drop=True)
    df.reset_index(inplace = True, drop = True)
    
    df.columns=['District','School_Mgmt','PS_I_V','UPS_I_VIII','HSS_I_XII','UPS_VI_VIII','HSS_VI_XII','SS_I_X','SS_VI_X','SS_IX_X','HSS_IX_XII','HSS_XI_XII','Total']
    df = df[df['District'] != 'Total']
    df = df[df['School_Mgmt'] != 'Overall']
    
    df['District_Clean'] = np.nan
    for i in range(len(df.District)):
        if pd.notnull(df['District'][i]) and df['District'][i] != '':
            district = district_rewrite.district(df['District'][i]).split('|')
            df['District_Clean'][i] = district[-1].title()

    df['State'] = state['Text']
    df['State_Clean'] = np.nan
    df['State_Clean']= df['State'].apply(lambda x:state_rewrite.state((x.lower())).split('|')[-1].title())
    state_alpha_df = fetch_state_alpha_codes(engine)
    df['State_Alpha_Code'] = df['State_Clean'].map(state_alpha_df.set_index('State')['State_Alpha_Code'])

    df['Academic_Year'] = academic_year
    df['Relevant_Date'] = relevant_date
    df=df[['State','State_Clean','State_Alpha_Code','District','District_Clean','School_Mgmt','PS_I_V','UPS_I_VIII','HSS_I_XII','UPS_VI_VIII','HSS_VI_XII','SS_I_X','SS_VI_X','SS_IX_X','HSS_IX_XII','HSS_XI_XII','Total','Academic_Year','Relevant_Date']]
    df['Runtime']=pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
    df['School_Mgmt_Map'] = np.nan
    return df

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    #add table name
    table_name = "UDISE_NO_OF_SCHOOL_BY_MANAGEMENT_N_SCHOOL_CATEGORY_YEARLY_2"
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        download_file_path = r"C:\Users\Administrator\Downloads"
        prefs = {
            "download.default_directory": download_file_path,
                            }
        # driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
        options = webdriver.ChromeOptions()
        options.add_argument("--disable-infobars")
        options.add_argument("start-maximized")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-notifications")
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--no-sandbox')            
        options.add_experimental_option('prefs', prefs)

        url = "https://dashboard.udiseplus.gov.in/#/reportDashboard/sReport"
        driver = webdriver.Chrome(executable_path=driver_path)
        driver.get(url)
        robot.add_link(url)

        time.sleep(2)
        element = driver.find_element(By.XPATH,'//tr[.//span[contains(text(),"1003")]]')
        element.find_element(By.XPATH, './/img[@style="cursor: pointer;"]').click()

        time.sleep(6)
        year_select = driver.find_element(By.XPATH, '//div[.//label[contains(text(),"Year")] and @class="form-group row"]//select')
        year_options = year_select.find_elements(By.XPATH, './/option')
        years = [option.get_attribute('value') for option in year_options]
        academic = [option.text for option in year_options]

        for year, academic_year in zip(years, academic):
            Select(year_select).select_by_value(year)
            time.sleep(8)

            relevant_date = date1(year)
            print(f"Collecting data for year: {relevant_date}")

            max_date = pd.read_sql("Select max(Relevant_Date) as MAX from UDISE_NO_OF_SCHOOL_BY_MANAGEMENT_N_SCHOOL_CATEGORY_YEARLY_2", engine)['MAX'][0]
            print(max_date)
            if relevant_date > max_date:
                print("New Data")
                Select(driver.find_element(By.XPATH, '//div[.//label[contains(text(),"India/State/UT")] and @class="form-group row"]//select')).select_by_index(0)
                states = [{'Text': j.text.strip(), 'Index' : i} for i,j in enumerate(driver.find_element(By.XPATH, '//div[.//label[contains(text(),"India/State/UT")] and @class="form-group row"]//select').find_elements(By.XPATH, './/option'))]         
                time.sleep(5)

                for state in states[2:]:
                    Select(driver.find_element(By.XPATH, '//div[.//label[contains(text(),"India/State/UT")] and @class="form-group row"]//select')).select_by_index(state['Index'])
                    time.sleep(5)
                
                    Select(driver.find_element(By.XPATH, '//div[.//label[contains(text(),"District")] and @class="form-group row"]//select')).select_by_index(1)
                    time.sleep(5)
                    
                    driver.find_element(By.XPATH, '//img[@class="mr-3" and contains(@src,"excel")]').click()
                    time.sleep(5)
                    
                    files = os.listdir(r"C:\Users\Administrator\Downloads")
                    paths = [os.path.join(r"C:\Users\Administrator\Downloads", basename) for basename in files]
                    latest_file = max(paths, key=os.path.getctime)
                    
                    with open(latest_file,"rb") as f:
                        df = pd.read_excel(latest_file)
                        df = Clean_df(df)

                    df.to_sql('UDISE_NO_OF_SCHOOL_BY_MANAGEMENT_N_SCHOOL_CATEGORY_YEARLY_2', index=False, if_exists='append', con=engine)
                    os.remove(latest_file)
            else:
                print('No New Data')
            log.job_end_log(table_name, job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'", str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name, job_start_time, error_type, error_msg, no_of_ping)
if(__name__=='__main__'):
    run_program(run_by='manual')               