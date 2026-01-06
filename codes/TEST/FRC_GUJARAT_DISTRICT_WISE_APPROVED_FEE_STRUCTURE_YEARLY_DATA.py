from math import ceil
import sys
from pytz import timezone
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import datetime as datetime
import warnings
import re
warnings.filterwarnings('ignore')
import time
from playwright.sync_api import sync_playwright
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
from adqvest_robotstxt import Robots
robot = Robots(__file__)
    

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    engine = adqvest_db.db_conn()
    connection = engine.connect()

    india_time = timezone('Asia/Kolkata')
    today = datetime.datetime.now(india_time)
    days = datetime.timedelta(1)
    yesterday = today - days
    
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'FRC_GUJARAT_DIST_SCHOOL_WISE_FEE_STRUCTURE_YEARLY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
            
        url = 'http://www.frcgujarat.org/'
        pw = sync_playwright().start()
        browser = pw.firefox.launch(headless = True, ignore_default_args=["start-maximized"])
        context = browser.new_context(java_script_enabled = True,bypass_csp=True)
        page = context.new_page()
        page.goto(url, wait_until='networkidle')
        page.locator('//*[contains(text(), "Browse All")]').click()
        districts = pd.read_sql('SELECT District from FRC_GUJARAT_DIST_SCHOOL_WISE_FEE_STRUCTURE_YEARLY_DATA_STATUS WHERE Status is nulL', con = engine)['District']
        final_fee_df = pd.DataFrame()
        for dist in districts:
            try:
                print(dist)
                final_df = pd.DataFrame()
                page.select_option('//select[@id = "currentDistrict"]', label=dist)
                page.locator('//button[@id="submit"]').click()
                time.sleep(5)
                soup2 = BeautifulSoup(page.content())
                total_entries = int(soup2.find('div', class_ = 'showing-entry col-md-6').text.replace('\n', '').split('of')[-1].replace('Entries', '').strip())
                print('Total records: ', total_entries)
                to_collect = round(total_entries*.1)
                pages = ceil(to_collect/10)
                for pg in range(0, pages+1):
                    print(f'Page: {pg}')
                    soup3 = BeautifulSoup(page.content())
                    df = pd.read_html(str(soup3.find('div', class_ = 'table-responsive')))[0]
                    # print(df)
                    final_fee_df = pd.DataFrame()
                    for i in range(len(df.iloc[:, :])):
                        page.keyboard.press('Escape')
                        page.locator('//div[@class = "btn-group btn-group-circle"]').nth(i).click()
                        time.sleep(5)
                        soup4 = BeautifulSoup(page.content())
                        fee_stat = soup4.find('td', {'id': 'thFee_status'}).text.split(':')[-1].strip()
                        fee_df = pd.read_html(page.content(), header=None)[-1]
                        fee_df.columns = [i[-1].split('(')[-1].replace(')', '') for i in fee_df.columns]
                        fee_df['School_Name'] = df['School Name'][i]
                        fee_df['Medium'] = df['Medium'][i]
                        fee_df['Board'] = df['Board'][i]
                        fee_df['Fee_Status'] = fee_stat
                        fee_df['District'] = df['District'][i]
                        fee_df['Runtime'] = datetime.datetime.now(india_time)
                        final_fee_df = pd.concat([final_fee_df, fee_df])
                        time.sleep(5)
                        page.keyboard.press('Escape')
                    page.locator(f'//a[@rel="next"]').click()
                    time.sleep(5)
                    final_df = pd.concat([final_df, final_fee_df])
                final_df = final_df[['District','School_Name', 'Medium', 'Board','Fee_Status', 'Standard', '2017 - 2018', '2018 - 2019', '2019 - 2020', '2020 - 2021','2021 - 2022', '2022 - 2023', '2023 - 2024', '2024 - 2025', 'Runtime']]
                req_entries = round(total_entries/10)

                ## Denormalization part 
                val_vars = final_df.columns[6:-1]
                df_denorm = pd.melt(final_df, id_vars=['District','School_Name', 'Medium', 'Board','Fee_Status','Standard','Runtime'], value_vars=val_vars, var_name='Year', value_name='Approved_Fees_INR')
                df_denorm['Relevant_Date'] = df_denorm.Year.apply(lambda x : datetime.date(int(x.split('-')[-1].strip()), 3, 31))
                df_denorm['School_Number'] = df_denorm.School_Name.apply(lambda x: x.split('(')[-1].replace(')', '').strip())
                df_denorm['School_Name'] = df_denorm.School_Name.apply(lambda x: x.split('(')[0].replace('-', '').strip())
                df_denorm = df_denorm.replace('-', np.nan)

                school_list = df_denorm.School_Number.unique()[:req_entries]
                df_denorm = df_denorm[df_denorm.School_Number.isin(school_list)]
                coll_status = np.nan
                if len(school_list) == 0:
                    coll_status = np.nan
                elif len(school_list) == req_entries:
                    coll_status = 'Succeeded'
                else:
                    coll_status = 'Partial'

                print(f'No Of Records collected: {len(school_list)}')
                status_df = pd.DataFrame({'District':[dist], 'Total_Records':[total_entries], 'Last_Collected_Page':[pg+1], 'Records_Collected': len(school_list), 'Status':[coll_status], 'Relevant_Date':[today.date()], 'Runtime':[today]})
                df_denorm.to_sql('FRC_GUJARAT_DIST_SCHOOL_WISE_FEE_STRUCTURE_YEARLY_DATA', if_exists='append', index=False, con = engine)
                query = f'UPDATE FRC_GUJARAT_DIST_SCHOOL_WISE_FEE_STRUCTURE_YEARLY_DATA_STATUS SET Total_Records = {status_df.Total_Records[0]}, Last_Collected_Page = {status_df.Last_Collected_Page[0]}, Records_Collected = {status_df.Records_Collected[0]}, Status = "{status_df.Status[0]}", Relevant_Date = "{status_df.Relevant_Date[0]}", Runtime = "{status_df.Runtime[0]}" WHERE District = "{status_df.District[0]}"'
                connection.execute(query)
            except:
                print('Failed for ---> ', dist)
                query = f'UPDATE FRC_GUJARAT_DIST_SCHOOL_WISE_FEE_STRUCTURE_YEARLY_DATA_STATUS SET Total_Records = Null, Last_Collected_Page = Null, Records_Collected = Null, Status = "Failed", Relevant_Date = {today.date()}, Runtime = {datetime.datetime.now(india_time)} WHERE District = "{dist}"'
                connection.execute(query)

        pw.stop()
        log.job_end_log(table_name,job_start_time,no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'", str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name, job_start_time, error_type, error_msg, no_of_ping)
if(__name__=='__main__'):
    run_program(run_by='manual')