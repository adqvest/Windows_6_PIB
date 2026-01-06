import datetime
import os
import re
import sys
import time
import warnings
warnings.filterwarnings('ignore')
import pandas as pd
from playwright.sync_api import sync_playwright
from pytz import timezone

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import ClickHouse_db
import JobLogNew as log

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir(r'C:/Users/Administrator/AdQvestDir/')
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
    table_name = 'NSE_DAILY_FII_DII_TRADING_ACTIVITY_ON_NSE_BSE_AND_MSEI'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
        
        max_date = pd.read_sql("Select max(Relevant_Date) as RD from AdqvestDB.NSE_DAILY_FII_DII_TRADING_ACTIVITY_ON_NSE_BSE_AND_MSEI",con=engine)['RD'][0]
        mkt_holidays = pd.read_sql('select Holiday, Relevant_Date from NSE_BSE_MARKET_HOLIDAYS_YEARLY_DATA', engine)
        mkt_holidays = mkt_holidays.drop_duplicates('Relevant_Date').reset_index(drop=True)

        if mkt_holidays.Relevant_Date.isin([max_date]).any() == True:
            holiday_idx = mkt_holidays[mkt_holidays.Relevant_Date.isin([max_date])]['Holiday'].index[0]
            error_type = 'Market Holiday'
            error_msg = 'Market Holiday on account of ' + mkt_holidays['Holiday'][holiday_idx]
            print(error_msg)
            log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

        elif  ((max_date.weekday() == 5) | (max_date.weekday() == 6)):
            print ("BSE_EQ_DERV_DATA")
            print("Data not available for",max_date.strftime("%Y-%m-%d"))
            print("Weekend , no data")

        else:
            url_ffi = 'https://www.nseindia.com/reports/fii-dii'
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=False)
                page = browser.new_page()
                time.sleep(5)

                page.goto(url_ffi, wait_until='networkidle')
                time.sleep(5)
                pg_content = page.content()
                ffi = pd.read_html(pg_content)[0]
                ffi.columns = ['Category', 'Relevant_Date', 'Buy_Value_INR_Cr', 'Sale_Value_INR_Cr', 'Net_Value_INR_Cr']
                ffi['Category'] = ffi.Category.str.replace('*','').str.strip()
                ffi['Relevant_Date'] = ffi['Relevant_Date'].apply(lambda x: datetime.datetime.strptime(x, '%d-%b-%Y').date())

                ffi['Runtime'] = datetime.datetime.now()

                if max_date == ffi['Relevant_Date'].unique()[0]:
                  print('data already available')
                else:
                  ffi.to_sql("NSE_DAILY_FII_DII_TRADING_ACTIVITY_ON_NSE_BSE_AND_MSEI", index=False, if_exists='append',con=engine)

        click_max_date = client.execute("select max(Relevant_Date) from NSE_DAILY_FII_DII_TRADING_ACTIVITY_ON_NSE_BSE_AND_MSEI")
        click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])
        query = 'select * from AdqvestDB.NSE_DAILY_FII_DII_TRADING_ACTIVITY_ON_NSE_BSE_AND_MSEI where Relevant_Date > "' + click_max_date + '"'
        df = pd.read_sql(query, engine)
        client.execute("INSERT INTO NSE_DAILY_FII_DII_TRADING_ACTIVITY_ON_NSE_BSE_AND_MSEI VALUES", df.values.tolist())

        log.job_end_log(table_name,job_start_time,no_of_ping)

    except:
        
        try:
            connection.close()
        except:
            pass
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')


