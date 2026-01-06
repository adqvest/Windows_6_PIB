import warnings
from bs4 import BeautifulSoup
warnings.filterwarnings('ignore')
import datetime as datetime
import re
import sys
import time
import calendar
import numpy as np
import pandas as pd
from pytz import timezone
from playwright.async_api import async_playwright
import asyncio
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import ClickHouse_db
import twocaptchasolver
import JobLogNew as log
from adqvest_robotstxt import Robots
robot = Robots(__file__)

async def get_data(url,cin):
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    df = None

    pw = await async_playwright().start()
    browser = await pw.firefox.launch(headless = False, ignore_default_args=["start-maximized"])
    context = await browser.new_context(java_script_enabled = True,bypass_csp=True)
    page = await context.new_page()
    await page.goto(url, wait_until='networkidle', timeout = 30000*5)
    time.sleep(2)
    await page.locator('//input[@id = "masterdata-search-box"]').fill(cin)
    time.sleep(.5)
    await page.keyboard.press('Enter')
    time.sleep(1)
    await page.locator('//canvas[@id = "captchaCanvas"]').screenshot(path='/users/nidhigoel/documents/junk/captcha.png') # type: ignore

    await solve_cap(page)

    await page.locator('//table[@class = "table two-masterdata table-borderless"]/tbody').hover()
    cond = await page.locator('//table[@class = "table two-masterdata table-borderless"]/tbody').inner_text()
    if 'No Results Found' in cond:
        print('Error No Cin for data')
        connection.execute("Update AdqvestDB.MCA_INDEX_OF_CHARGES_WEEKLY_STATUS set Status='No Cin',Runtime = '"+datetime.datetime.now(india_time).strftime("%Y-%m-%d %H:%M:%S")+"',Relevant_Date = '"+datetime.datetime.now(india_time).strftime("%Y-%m-%d")+"' where Cin='"+cin+"'")
    else:
        retry = 0
        while retry < 2:
            await page.locator('//table[@class = "table two-masterdata table-borderless"]/tbody').click()
            time.sleep(2)
            await solve_cap(page)


            try:
                cond1 = page.locator('//*[contains(text(), "Index of Charges")]')
            except:
                cond1 = None
            if cond1 != None:
                break
            else:
                retry=retry+1

        try:
            await page.locator('//*[contains(text(), "Index of Charges")]').click()
            cond2 = page.locator('//*[contains(text(), "Index of Charges")]')
        except:
            cond2 = None
            await page.locator('//table[@class = "table two-masterdata table-borderless"]/tbody').click()
            time.sleep(2)

        if cond2 == None:
            page.locator('//*[contains(text(), "Index of Charges")]').click()

        tables = pd.read_html(await page.content())
        if len(tables) == 0:
            connection.execute("Update AdqvestDB.MCA_INDEX_OF_CHARGES_WEEKLY_STATUS set Status='No Data',Runtime = '"+datetime.datetime.now(india_time).strftime("%Y-%m-%d %H:%M:%S")+"',Relevant_Date = '"+datetime.datetime.now(india_time).strftime("%Y-%m-%d")+"' where Cin='"+cin+"'")
            df = pd.DataFrame()
        else:
            for i in tables:
                if 'Asset Holder Name' in i.columns:
                    df = i.copy()
                    break
        if(df.empty):
            connection.execute("Update AdqvestDB.MCA_INDEX_OF_CHARGES_WEEKLY_STATUS set Status='No Data',Runtime = '"+datetime.datetime.now(india_time).strftime("%Y-%m-%d %H:%M:%S")+"',Relevant_Date = '"+datetime.datetime.now(india_time).strftime("%Y-%m-%d")+"' where Cin='"+cin+"'")
    await page.close()
    await context.close()
    await browser.close()
    await pw.stop()
    if(df.empty == False):
        return df
    else:
        return None

def clean_txt(text):
    text = text.upper()
    text = text.replace('  ',' ').replace(',','').replace('-','').replace('(','').replace(')','').replace('\n','').replace('.','')
    text = text.replace('LTD','').replace('LIMITED','')
    text = text.replace('THE ','').replace('&','AND')
    text = re.sub(r'  +',' ', text).strip()
    return text

def calc(captcha):
    if ('+' in captcha):
        digit1 = int(captcha.split('+')[0].strip())
        digit2 = int(captcha.split('+')[-1].strip())
        return digit1+digit2
    elif ('-' in captcha):
        digit1 = int(captcha.split('+')[0].strip())
        digit2 = int(captcha.split('+')[-1].strip())
        return digit1-digit2
    elif len(captcha) > 3:
        digit1 = str(captcha)[0:2]
        digit2 = str(captcha)[2:]
        return int(digit1)+int(digit2)
    else:
        return captcha

async def solve_cap(page):
    captchaSolved = False
    while captchaSolved == False:
        await page.locator('//canvas[@id = "captchaCanvas"]').screenshot(path='/users/nidhigoel/documents/junk/captcha.png')
        captcha = twocaptchasolver.solve_captcha('/users/nidhigoel/documents/junk/', 'captcha.png',1,1)
        captcha_ans = calc(captcha[1])
        await page.locator('//input[@id = "customCaptchaInput"]').fill(str(captcha_ans))
        await page.locator('//button[@class = "btn btn-primary subm"]').click()
        time.sleep(2)
        soup = BeautifulSoup(await page.content(),'html.parser')
        if 'The captcha entered is incorrect. Please retry.' in soup.text:
            captchaSolved = False
            twocaptchasolver.report_bad_captcha(captcha[0])
        else:
            captchaSolved = True

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    client = ClickHouse_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)

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

        if len(cin_list1) != 0:
            print(f'Collection for {len(cin_list1)} companies')
            for cin,company in zip(cin_list1,cin_company):
                print(cin,company)
                # nest_asyncio.apply()
                loop = asyncio.get_event_loop()
                df = loop.run_until_complete(get_data(url,cin))  

                if df is not None:
                    if(df.empty):
                        connection.execute("Update AdqvestDB.MCA_INDEX_OF_CHARGES_WEEKLY_STATUS set Status='No Data',Runtime = '"+datetime.datetime.now(india_time).strftime("%Y-%m-%d %H:%M:%S")+"',Relevant_Date = '"+datetime.datetime.now(india_time).strftime("%Y-%m-%d")+"' where Cin='"+cin+"'")
                    else:
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
                        engine = adqvest_db.db_conn()
                        df_pit = pd.read_sql("select * from AdqvestDB.MCA_INDEX_OF_CHARGES_NO_PIT_RANDOM_DATA where Cin = '"+cin+"'",con=engine)
                        df_pit['Date_Of_Creation'] = df_pit['Date_Of_Creation'].apply(lambda x:str(x))
                        df_pit['Date_Of_Modification'] = df_pit['Date_Of_Modification'].apply(lambda x:str(x))
                        df_pit['Date_Of_Satisfaction'] = df_pit['Date_Of_Satisfaction'].apply(lambda x:str(x))
                        df_pit=df_pit[['Cin', 'Srn', 'Company_Name', 'Company_Name_Clean', 'Charge_Id','Charge_Holder_Name', 'Charge_Holder_Clean_Name', 'Date_Of_Creation','Date_Of_Modification', 'Date_Of_Satisfaction', 'Type_of_Company_Nature', 'Amount', 'Address','Runtime']]

                        client.execute("INSERT INTO MCA_INDEX_OF_CHARGES_PIT_RANDOM_DATA VALUES", df_pit.values.tolist())
                        print('collection done')
                

        log.job_end_log(table_name, job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)
if(__name__=='__main__'):
    run_program(run_by='manual')
