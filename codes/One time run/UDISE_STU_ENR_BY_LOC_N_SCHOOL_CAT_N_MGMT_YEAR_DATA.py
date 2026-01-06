from selenium import webdriver
from selenium.webdriver.support.ui import Select
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from playwright.sync_api import sync_playwright
import time
import datetime
from pytz import timezone
import pandas as pd
import sys
import re
import os
import numpy as np
import warnings
import glob
warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import ClickHouse_db
import JobLogNew as log
from adqvest_robotstxt import Robots
robot = Robots(__file__)

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/')
from State_Function import state_rewrite

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/District_Function')
import district_rewrite

sys.path.insert(0,r'C:\Users\Administrator\AdQvestDir\Adqvest_Function')
import pdftoexcel

def fetch_state_alpha_codes(engine):
    query = "SELECT State, State_Alpha_Code FROM AdqvestDB.INDIA_STATE_N_STATE_ALPHA_CODE_GST_CODE_MAPPING"
    df = pd.read_sql(query, engine)
    return df

def pdf_to_excel(file_path,key_word="",OCR_doc=False):
    os.chdir(file_path)
    path=os.getcwd()
    download_path=os.getcwd()
    pdf_list = glob.glob(os.path.join(path, "*.pdf"))
    # print(pdf_list)
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
                time.sleep(30)    
                page.locator("//*[@id='processTask']").click()
                time.sleep(80)    
                with page.expect_download() as download_info:
                    page.get_by_text("Download EXCEL").click()
                # Wait for the download to start
                download = download_info.value
                # Wait for the download process to complete
                # print(download.path())
                file_name = download.suggested_filename
                # wait for download to complete
                download.save_as(os.path.join(download_path, file_name))
                page.go_back()

def date1(x):
    return datetime.date(int(str(20)+x[-2:]),3,31)

def clean_string(s):
    return re.sub(r'[^a-zA-Z0-9\s]', '', s)

def get_desired_table(link, search_str):
    xls = pd.ExcelFile(link)
    sheet_names = xls.sheet_names
    for sheet_name in sheet_names:
        df = pd.read_excel(link, sheet_name=sheet_name, header=None)
        df = df.replace(np.nan, '')
        print('Processed', sheet_name)
        for col in df.columns:
            for row in df.index:
                cell_value = str(df.iat[row, col])
                if clean_string(search_str.lower()) in clean_string(cell_value.lower()):
                    print('Sheet Found:', sheet_name)
                    return df
    
    print('Sheet not found for search string:', search_str)
    return None 
    
def check_for_rows(row, keywords):
    return any(keyword in str(item) for item in row for keyword in keywords)    

def Clean_df(df):

    df.iloc[0,:] = df.iloc[0,:].ffill()
    df = df.transpose()
    df.replace('', np.nan, inplace=True)
    df = df.ffill(axis=0)
    df = df.transpose()
    
    grade_row = df.apply(lambda row: check_for_rows(row,['Class','Primary']), axis=1)
    gender_row = df.apply(lambda row: check_for_rows(row,['Girls','Boys']), axis=1)
    cols = []
    for grade, gender in zip(df[grade_row].iloc[0,:], df[gender_row].iloc[0,:]):
        if any(pd.Series([grade,gender]).isna()):
            cols.append(gender.strip())
            continue
        else:
            cols.append((grade.strip() + ' ' + gender.strip()).replace('(','').replace('_Enrolment','').replace(')',''))
    
    df.columns = cols
    df.drop([df.index[grade_row][0],df.index[gender_row][0]], inplace = True)
    
    transformed_df = df.melt(id_vars=['Location', 'Rural/Urban', 'School Category', 'School Management'], 
                         var_name='Class and Gender', 
                         value_name='Value')
    
    transformed_df = transformed_df.sort_values(by=["School Category",'School Management','Rural/Urban','Location'])
    transformed_df.reset_index(inplace = True, drop = True)            
    transformed_df.rename(columns = {'Location' : 'District','School Category':'School_Cat','School Management' : 'School_Mgmt','Rural/Urban' : 'Location'}, inplace = True)
    transformed_df['District'] = transformed_df['District'].str.title()

    mapper = {'Class-XII': 'Class 12', 'Class-XI': 'Class 11', 'Class-X': 'Class 10', 'Class-IX': 'Class 9', 'Class-VIII': 'Class 8', 'Class-VII': 'Class 7', 'Class-VI': 'Class 6', 
          'Class-V': 'Class 5', 'Class-IV': 'Class 4', 'Class-III': 'Class 3', 'Class-II': 'Class 2', 'Class-I': 'Class 1'}
    for val, map_ in mapper.items():
        transformed_df['Class and Gender'] = transformed_df['Class and Gender'].str.replace(val, map_)

    transformed_df['Class'] = transformed_df['Class and Gender'].str.replace('Boys|Girls','', regex = True).str.replace('_',' ').str.strip()
    transformed_df['Class'] = transformed_df['Class'].apply(lambda x: ' '.join(x.split()[:-1]) if x.split()[-1] == 'Total' else x)
    
    transformed_df['Gender'] = transformed_df['Class and Gender'].apply(lambda x: x.split()[-1])
    
    vals = {"Class 1":"Primary" , "Class 2":"Primary" , "Class 3":"Primary", "Class 4":"Primary", 
            "Class 5":"Primary", "Class 6":"Upper Primary", "Class 7":"Upper Primary", 
            "Class 8":"Upper Primary", "Class 9":"Secondary", "Class 10":"Secondary",
            "Class 11":"Higher Secondary", "Class 12":"Higher Secondary",
            "Pre-Primary":"Class Pre Primary"}
    transformed_df['Type'] = transformed_df['Class'].map(vals)
    
    return transformed_df

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    
    engine = adqvest_db.db_conn()
    client = ClickHouse_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days
    # ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'UDISE_STU_ENR_BY_LOC_N_SCHOOL_CAT_N_MGMT_YEAR_DATA'
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
        os.chdir(download_file_path)

        os.chdir(r"C:\Users\Administrator\Downloads")
        download=os.getcwd()
        delete_pdf=os.listdir(r"C:\Users\Administrator\Downloads")
        # for file in delete_pdf:
        #         os.remove(file)
        
        prefs = {
            "download.default_directory": download_file_path,
                            }
        driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
        options = webdriver.ChromeOptions()
        options.add_argument("--disable-infobars")
        options.add_argument("start-maximized")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-notifications")
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--no-sandbox')            
        options.add_experimental_option('prefs', prefs)
        connection = engine.connect()

        query = "SELECT State FROM UDISE_STU_ENR_BY_LOC_N_SCHOOL_STATUS_DATA where Status is Null;"
        results = pd.read_sql(query,engine)
        states_to_collect = results['State'].tolist()

        url = 'https://dashboard.udiseplus.gov.in/#/reportDashboard/sReport'
        driver = webdriver.Chrome(executable_path=driver_path)
        driver.get(url)
        robot.add_link(url)
        
        time.sleep(2)
        element = driver.find_element(By.XPATH,'//tr[.//span[contains(text(),"4002")]]')
        element.find_element(By.XPATH, './/img[@style="cursor: pointer;"]').click()
        time.sleep(8)
        year_select = driver.find_element(By.XPATH, '//div[.//label[contains(text(),"Year")] and @class="form-group row"]//select')
        year_options = year_select.find_elements(By.XPATH, './/option')
        years = [option.get_attribute('value') for option in year_options]
        academic = [option.text for option in year_options]

        for year, academic_year in zip(years, academic):
            Select(year_select).select_by_value(year)
            time.sleep(10)

            relevant_date = date1(year)
            print(f"Collecting data for year: {relevant_date}")
       
            # max_date = pd.read_sql("Select max(Relevant_Date) as MAX from UDISE_STU_ENR_BY_LOC_N_SCHOOL_CAT_N_MGMT_YEAR_DATA", engine)['MAX'][0]
            # print(max_date)
            # if relevant_date > max_date:
        
            Select(driver.find_element(By.XPATH, '//div[.//label[contains(text(),"India/State/UT")] and @class="form-group row"]//select')).select_by_index(0)
            states = [{'Text': j.text.strip(), 'Index' : i} for i,j in enumerate(driver.find_element(By.XPATH, '//div[.//label[contains(text(),"India/State/UT")] and @class="form-group row"]//select').find_elements(By.XPATH, './/option'))]            
            print(states_to_collect)

            state_element = driver.find_element(By.CSS_SELECTOR, 'div.col-sm-8 select.form-control')
            dropdown = Select(state_element)

            for state in states_to_collect:
                state_found = False
                for state_option in states:
                    if state == state_option['Text']:
                        state_found = True
                        dropdown.select_by_visible_text(state)    
                        print('Selected State:', state)
                        time.sleep(5)

                        Select(driver.find_element(By.XPATH, '//div[.//label[contains(text(),"District")] and @class="form-group row"]//select')).select_by_index(1)
                        time.sleep(5)
                        
                        driver.find_element(By.XPATH, '//img[@class="mr-3" and contains(@src,"pdf")]').click()
                        time.sleep(20)

                        files = os.listdir(r"C:\Users\Administrator\Downloads")
                        pdf_files = [file for file in files if file.lower().endswith('.pdf')]
                        paths = [os.path.join(r"C:\Users\Administrator\Downloads", basename) for basename in pdf_files]
                        latest_file_path = max(paths, key=os.path.getctime)   
                        latest_file = os.path.basename(latest_file_path)
                        print(latest_file)
                        
                        pdf_to_excel(r"C:\Users\Administrator\Downloads")

                        # files = os.listdir(r"C:\Users\Administrator\Downloads")
                        # xl_files = [file for file in files if file.lower().endswith('.xlsx')]
                        # xl_paths = [os.path.join(r"C:\Users\Administrator\Downloads", basename) for basename in xl_files]
                        # latest_xl_file = (max(xl_paths, key=os.path.getctime))
                        # print(latest_xl_file)

                        os.chdir("C:/Users/Administrator/Downloads")
                        excel_files=glob.glob(os.path.join(os.getcwd(), "*.xlsx"))
                        print(excel_files)
                        for f in excel_files:
                            search_str = 'Primary'
                            df = get_desired_table(f, search_str)
                            df = Clean_df(df)

                            df['District_Clean'] = np.nan
                            for i in range(len(df.District)):
                                if pd.notnull(df['District'][i]) and df['District'][i] != '':
                                    district = district_rewrite.district(df['District'][i]).split('|')
                                    df['District_Clean'][i] = district[-1].title()
                                
                            df['State'] = state
                            df['State_Clean'] = np.nan
                            df['State_Clean']= df['State'].apply(lambda x:state_rewrite.state((x.lower())).split('|')[-1].title())
                            # for i in range(len(df.State)):
                            #     state = state_rewrite.state(df['State'][i]).split('|')
                            #     df['State_Clean'][i] = state[-1].title()

                            df['Academic_Year'] = academic_year
                            df['Relevant_Date'] = relevant_date
                            df['Runtime']=pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
                            df.drop(columns=['Class and Gender'], inplace=True)
                            df = df[df['Gender'] != 'Total']
                            state_alpha_df = fetch_state_alpha_codes(engine)
                            df['State_Alpha_Code'] = df['State_Clean'].map(state_alpha_df.set_index('State')['State_Alpha_Code'])

                            df = df[['State','State_Clean','State_Alpha_Code','District','District_Clean','Location','School_Cat','School_Mgmt','Type','Class','Gender','Value'] + [col for col in df.columns if col not in ['State','State_Clean','State_Alpha_Code','District','District_Clean','Location','School_Cat','School_Mgmt','Type','Class','Gender','Value']]]
                            df.to_sql("UDISE_STU_ENR_BY_LOC_N_SCHOOL_CAT_N_MGMT_YEAR_DATA", index=False, if_exists='append', con=engine)
                            os.remove(f)

                            update_query = f'UPDATE UDISE_STU_ENR_BY_LOC_N_SCHOOL_STATUS_DATA SET Status = "Yes" , Runtime = "{today}" WHERE State = "{state}";'
                            connection.execute(update_query)
                            print("Table has been updated for - ",state)  # Updates the status for all the collected companies 

                        os.remove(latest_file_path) 
                if not state_found:
                    print(f"{state} not found in dropdown. Skipping...")
                    continue
        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        # error_msg = str(sys.exc_info()[1])
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')