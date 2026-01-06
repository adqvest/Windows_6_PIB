import warnings
warnings.filterwarnings('ignore')
import datetime as datetime
import re
import os
import sys
import time
import calendar
import numpy as np
import pandas as pd
from pytz import timezone
from twocaptcha import solver
from playwright.sync_api import sync_playwright
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import ClickHouse_db
import JobLogNew as log
from adqvest_robotstxt import Robots
robot = Robots(__file__)

def clean_txt(text):
    text = text.upper()
    text = text.replace('  ',' ').replace(',','').replace('-','').replace('(','').replace(')','').replace('\n','').replace('.','')
    text = text.replace('LTD','').replace('LIMITED','')
    text = text.replace('THE ','').replace('&','AND')
    text = re.sub(r'  +',' ', text).strip()
    return text

def get_captcha(path):
    api_key = os.getenv('APIKEY_2CAPTCHA', '368c8fa1c31062b71eab0e93f3e41d01')
    solver_inst = solver.TwoCaptcha(api_key)
    balance = solver_inst.balance()
    if balance != 0:
        text = solver_inst.normal(path)
        text = text['code'].upper()
        if '-' in text:
            values = text.split('-') # type: ignore
            ans = int(values[0]) + int(values[1])
        elif '+' in text:
            values = text.split('+') # type: ignore
            ans = int(values[0]) + int(values[1])
        else:
            ans = text
        return ans
    else:
        raise Exception('Balance 0 please add funds into 2captcha account!')

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    client = ClickHouse_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'MCA_INDEX_OF_CHARGES_PIT_RANDOM_DATA, MCA_INDEX_OF_CHARGES_NO_PIT_RANDOM_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        max_date_db = pd.read_sql('SELECT MAX(Relevant_Date) as max_date FROM MCA_INDEX_OF_CHARGES_WEEKLY_STATUS', con=engine)['max_date'][0]
        df_mapping=pd.read_sql("Select * from GENERIC_DICTIONARY_TABLE where Input_Table ='MCA_INDEX_OF_CHARGES_PIT_RANDOM_DATA'", engine)
        mid_month = int(calendar.monthrange(today.year, today.month)[1]/2)        
        url = 'https://www.mca.gov.in/content/mca/global/en/mca/master-data/MDS.html'
        
        if (today.date()-max_date_db).days >= mid_month:    # Condition to check the status bi-monthly
            status_df=pd.read_sql("select Cin,Company_Name,Status from AdqvestDB.MCA_INDEX_OF_CHARGES_WEEKLY_STATUS ",engine)
            if status_df['Status'].isnull().sum()==0: # Condition when 14 days period is passed still All companies data is collected
                status_df['Status']=None
                connection.execute("UPDATE AdqvestDB.MCA_INDEX_OF_CHARGES_WEEKLY_STATUS set Status=NULL")
                cin_list=pd.read_sql("select Cin,Company_Name,Status from AdqvestDB.MCA_INDEX_OF_CHARGES_WEEKLY_STATUS where Status is null",engine)
                batch = len(cin_list) // 14
                if len(cin_list) > batch:
                    cin_list = cin_list.iloc[:batch, :]
                else:
                    pass
                cin_list1 = cin_list['Cin']
                cin_company = cin_list['Company_Name']
            else:     # Condition when 14 days period is passed still some companies data is not collected
                cin_list=pd.read_sql("select Cin,Company_Name,Status from AdqvestDB.MCA_INDEX_OF_CHARGES_WEEKLY_STATUS where Status is null",engine)
                batch = len(cin_list) // 14
                if len(cin_list) > batch:
                    cin_list = cin_list.iloc[:batch, :]
                else:
                    pass
                cin_list1 = cin_list['Cin']
                cin_company = cin_list['Company_Name'] 
              
        else:   # If above condition is invalid then check for the companies where status is null
            cin_list=pd.read_sql("select Cin,Company_Name,Status from AdqvestDB.MCA_INDEX_OF_CHARGES_WEEKLY_STATUS where Status is null",engine)
            batch = len(cin_list) // 14
            if len(cin_list) > batch:
                cin_list = cin_list.iloc[:batch, :]
            else:
                pass
            cin_list1 = cin_list['Cin']
            cin_company = cin_list['Company_Name']
        
        if len(cin_list1) != 0:
            print(f'Collection for {len(cin_list1)} companies')
            for cin,company in zip(cin_list1,cin_company):
                print(cin,company)
                pw = sync_playwright().start()
                browser = pw.firefox.launch(headless = False, ignore_default_args=["start-maximized"])
                context = browser.new_context(java_script_enabled = True,bypass_csp=True)
                page = context.new_page()
                page.goto(url, wait_until='networkidle', timeout = 30000*5)
                time.sleep(2)
                page.locator('//input[@id = "masterdata-search-box"]').fill(cin)
                time.sleep(.5)
                page.keyboard.press('Enter')
                time.sleep(1)
                page.query_selector('//canvas[@id = "captchaCanvas"]').screenshot(path='C:/Users/Administrator/Junk/captcha.png') # type: ignore
                
                # After enetring on company name in search area
                limit = 0
                while limit < 5:
                    ans = get_captcha('C:/Users/Administrator/Junk/captcha.png')
                    time.sleep(5)
                    page.locator('//input[@id = "customCaptchaInput"]').fill(str(ans))
                    time.sleep(5)
                    
                    try:
                        page.locator('//button[@class = "btn btn-primary subm"]').click() 
                        check = page.locator('//*[contains(text(),"The captcha entered is incorrect. Please retry.")]').inner_text()
                    except:
                        check = None
                    if check != None:
                        page.locator('//input[@id = "customCaptchaInput"]').clear()
                        print(check)
                        page.query_selector('//canvas[@id = "captchaCanvas"]').screenshot(path='C:/Users/Administrator/Junk/captcha.png') # type: ignore   
                        time.sleep(5)
                        limit+=1
                    else:
                        print('Captcha correct')   
                        break
                    
                page.locator('//table[@class = "table two-masterdata table-borderless"]/tbody').hover()
                cond = page.locator('//table[@class = "table two-masterdata table-borderless"]/tbody').inner_text()
                if 'No Results Found' in cond:
                    print('Error No Cin for data')
                    connection.execute("Update AdqvestDB.MCA_INDEX_OF_CHARGES_WEEKLY_STATUS set Status='No Cin',Runtime = '"+datetime.datetime.now(india_time).strftime("%Y-%m-%d %H:%M:%S")+"',Relevant_Date = '"+datetime.datetime.now(india_time).strftime("%Y-%m-%d")+"' where Cin='"+cin+"'")
                else:
                    retry = 0
                    while retry < 2:
                        page.locator('//table[@class = "table two-masterdata table-borderless"]/tbody').click()
                        time.sleep(5)
                        page.query_selector('//canvas[@id = "captchaCanvas"]').screenshot(path='C:/Users/Administrator/Junk/captcha.png') # type: ignore
                    
                        # After clicking on record
                        limit = 0
                        while limit < 5:
                            ans = get_captcha('C:/Users/Administrator/Junk/captcha.png')
                            time.sleep(5)
                            page.locator('//input[@id = "customCaptchaInput"]').fill(str(ans))
                            time.sleep(5)
                            page.locator('//button[@class = "btn btn-primary subm"]').click() 
                            try:
                                check = page.locator('//*[contains(text(),"The captcha entered is incorrect. Please retry.")]').inner_text()
                            except:
                                check = None
                            if check != None:
                                limit+=1
                                page.locator('//input[@id = "customCaptchaInput"]').clear()
                                print(check)
                                page.query_selector('//canvas[@id = "captchaCanvas"]').screenshot(path='C:/Users/Administrator/Junk/captcha.png') # type: ignore   
                            else:
                                print('Captcha correct')   
                                break
                        try:        
                            cond1 = page.locator('//*[contains(text(), "Index of Charges")]')
                        except:
                            cond1 = None
                        if cond1 != None:
                            break
                        else:
                            retry=retry+1
                    try:
                        page.locator('//*[contains(text(), "Index of Charges")]').click()
                        cond2 = page.locator('//*[contains(text(), "Index of Charges")]')
                    except:
                        cond2 = None
                        page.locator('//table[@class = "table two-masterdata table-borderless"]/tbody').click()
                        time.sleep(2)
                        page.query_selector('//canvas[@id = "captchaCanvas"]').screenshot(path='C:/Users/Administrator/Junk/captcha.png') # type: ignore
                    
                        # After clicking on record
                        limit = 0
                        while limit < 5:
                            ans = get_captcha('C:/Users/Administrator/Junk/captcha.png')
                            time.sleep(2)
                            page.locator('//input[@id = "customCaptchaInput"]').fill(str(ans))
                            time.sleep(2)
                            page.locator('//button[@class = "btn btn-primary subm"]').click() 
                            try:
                                check = page.locator('//*[contains(text(),"The captcha entered is incorrect. Please retry.")]').inner_text()
                            except:
                                check = None
                            if check != None:
                                limit+=1
                                page.locator('//input[@id = "customCaptchaInput"]').clear()
                                print(check)
                                page.query_selector('//canvas[@id = "captchaCanvas"]').screenshot(path='C:/Users/Administrator/Junk/captcha.png') # type: ignore   
                            else:
                                print('Captcha correct')   
                                break

                    if cond2 == None:
                        page.locator('//*[contains(text(), "Index of Charges")]').click()
                        
                        time.sleep(5)


                        tables = pd.read_html(page.content())
                        if len(tables) == 0:
                            connection.execute("Update AdqvestDB.MCA_INDEX_OF_CHARGES_WEEKLY_STATUS set Status='No Data',Runtime = '"+datetime.datetime.now(india_time).strftime("%Y-%m-%d %H:%M:%S")+"',Relevant_Date = '"+datetime.datetime.now(india_time).strftime("%Y-%m-%d")+"' where Cin='"+cin+"'")
                            df = pd.DataFrame()
                        else:
                            for i in tables:
                                if 'Asset Holder Name' in i.columns:
                                    df = i.copy()
                                    break

                            df['Company_Name'] = company
                            df['Cin'] = cin
                            df['Date of Creation'] = [datetime.datetime.strptime(str(x),'%d/%m/%Y').date() for x in df['Date of Creation']]
                            df['Date of Modification'] = [datetime.datetime.strptime(str(x),'%d/%m/%Y').date() if x != '-' else np.nan for x in df['Date of Modification']]
                            df['Date of Satisfaction'] = [datetime.datetime.strptime(str(x),'%d/%m/%Y').date() if x != '-' else np.nan for x in df['Date of Satisfaction']]
                            df.columns = [i.strip().title().replace(' ', '_').strip() for i in df.columns]
                            df = df.drop(['Sr._No', 'Asset_Holder_Name', 'Whether_Charge_Registered_By_Other_Entity'], axis = 1)
                           
                            df['Charge_Holder_Clean_Name'] = df['Charge_Holder_Name'].apply(lambda x:clean_txt(x))
                            df_no_pit = pd.merge(df, df_mapping, left_on='Company_Name', right_on='Input', how='left')
                            df_no_pit=df_no_pit[['Cin','Srn','Company_Name','Output','Charge_Id','Charge_Holder_Name','Charge_Holder_Clean_Name','Date_Of_Creation','Date_Of_Modification','Date_Of_Satisfaction','Amount','Address']]
                            df_no_pit.columns=['Cin','Srn','Company_Name','Company_Name_Clean','Charge_Id','Charge_Holder_Name','Charge_Holder_Clean_Name','Date_Of_Creation','Date_Of_Modification','Date_Of_Satisfaction','Amount','Address']
                            df_no_pit['Type_of_Company_Nature'] = np.where(pd.isna(df_no_pit['Company_Name_Clean']), 'Others', 'NBFC')
                            df_no_pit['Runtime'] = datetime.datetime.now(india_time)
                            connection.execute("Update AdqvestDB.MCA_INDEX_OF_CHARGES_WEEKLY_STATUS set Status='Yes',Runtime = '"+datetime.datetime.now(india_time).strftime("%Y-%m-%d %H:%M:%S")+"',Relevant_Date = '"+datetime.datetime.now(india_time).strftime("%Y-%m-%d")+"' where Cin='"+cin+"'")
                            connection.execute("Delete from AdqvestDB.MCA_INDEX_OF_CHARGES_NO_PIT_RANDOM_DATA  where Cin='"+cin+"'")
                            df_no_pit.to_sql('MCA_INDEX_OF_CHARGES_NO_PIT_RANDOM_DATA',index=False, if_exists='append', con=engine)
                            
                            df_pit = pd.read_sql("select * from AdqvestDB.MCA_INDEX_OF_CHARGES_NO_PIT_RANDOM_DATA where Cin = '"+cin+"'",con=engine)
                            df_pit['Date_Of_Creation'] = df_pit['Date_Of_Creation'].apply(lambda x:str(x))
                            df_pit['Date_Of_Modification'] = df_pit['Date_Of_Modification'].apply(lambda x:str(x))
                            df_pit['Date_Of_Satisfaction'] = df_pit['Date_Of_Satisfaction'].apply(lambda x:str(x))
                            df_pit=df_pit[['Cin', 'Srn', 'Company_Name', 'Company_Name_Clean', 'Charge_Id','Charge_Holder_Name', 'Charge_Holder_Clean_Name', 'Date_Of_Creation','Date_Of_Modification', 'Date_Of_Satisfaction', 'Type_of_Company_Nature', 'Amount', 'Address','Runtime']]
                                
                            client.execute("INSERT INTO MCA_INDEX_OF_CHARGES_PIT_RANDOM_DATA VALUES", df_pit.values.tolist())
                            print('collection done')
                pw.stop()

        log.job_end_log(table_name, job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)
if(__name__=='__main__'):
    run_program(run_by='manual')