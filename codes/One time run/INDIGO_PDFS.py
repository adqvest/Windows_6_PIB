from datetime import datetime, timedelta
from pytz import timezone
import sys
import re
import pandas as pd
import os
from playwright.sync_api import sync_playwright
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
from adqvest_robotstxt import Robots
robot = Robots(__file__)
import dbfunctions

def download_file(today):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless = False, ignore_default_args=["start-maximized"])
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        
        base_url = 'https://www.goindigo.in/content/dam/skyplus6e/in/en/assets/global/documents/IndiGo_Tariff_Sheet_{date}.pdf'
            
        start_date = datetime.strptime('2023-10-31', '%Y-%m-%d')
        today_date = datetime.today()
        num_weeks = (today_date - start_date).days // 7 + 1
        
        links_data = []
        for i in range(num_weeks):
            new_date = start_date + timedelta(weeks=i)
            date = new_date.strftime('%Y-%m-%d')
            print(date)
            new_url = base_url.format(date=date)
            print(new_url)
            page.wait_for_timeout(20000)  
            page.goto(new_url)
            page.wait_for_timeout(180000)

            if "Whoops! You seem to have lost your way." in page.content():
                print(f"File not found for date: {date}. Skipping.")
                links_data.append({'Relevant_Date': date, 'URL': new_url,'Status' : None})
                print('-----------------------------------------')
                continue
            
            filename = os.path.join(r"C:\Users\Administrator\AdQvestDir\INDIGO_JUNK", f"Indigo_Tariff_Sheet_{date}.pdf")
            print(filename)
            
            with page.expect_download() as download_info:
                page.evaluate(f'''() => {{
                    const link = document.createElement('a');
                    link.href = '{new_url}';
                    link.download = '';
                    document.body.appendChild(link);
                    link.click();
                    document.body.removeChild(link);
                }}''')
                download = download_info.value
                download.save_as(filename)

                key = 'INDIGO_TARIFF_SHEETS/'    
                dbfunctions.to_s3bucket(filename, key)  
                page.wait_for_timeout(60000)  
                
                links_data.append({'Relevant_Date': date, 'URL': new_url,'Status' : 'Yes'})
                print('-----------------------------------------')  
                page.wait_for_timeout(120000) 

        df = pd.DataFrame(links_data)
        df['Source'] = 'Indigo'
        df['Relevant_Date'] = pd.to_datetime(df['Relevant_Date']).dt.date
        df['Runtime'] = today
        
        df = df[['Source', 'URL', 'Status','Relevant_Date','Runtime']] 

        browser.close()
        return df


def run_program(run_by='Adqvest_Bot', py_file_name=None):    
    engine = adqvest_db.db_conn()
    
    india_time = timezone('Asia/Kolkata')
    today      = datetime.now(india_time)

    # job log details
    job_start_time = datetime.now(india_time)
    table_name = 'INDIGO_TARIFF_SHEET_LINKS_ONE_TIME_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
            
        df = download_file(today)
        print(df)
        df.to_sql(table_name, con=engine, if_exists='append', index=False)
        
        log.job_end_log(table_name,job_start_time,no_of_ping) 

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')