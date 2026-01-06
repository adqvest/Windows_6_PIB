import datetime
import os
import re
import sys
import zipfile
import warnings
warnings.filterwarnings('ignore')
import pandas as pd
from bs4 import BeautifulSoup
from pytz import timezone
import requests
from requests_html import HTMLSession
from playwright.sync_api import sync_playwright
import time
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import ClickHouse_db
import JobLogNew as log

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir')
    engine = adqvest_db.db_conn()
    client = ClickHouse_db.db_conn()
    # ****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today = datetime.datetime.now(india_time)
    days = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'NSE_EQ_DAILY_DATA'
    if (py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if (run_by == 'Adqvest_Bot'):
            log.job_start_log_by_bot(table_name, py_file_name, job_start_time)
        else:
            log.job_start_log(table_name, py_file_name, job_start_time, scheduler)

        Latest_Date = pd.read_sql('select max(Relevant_Date) as Relevant_Date from NSE_EQ_DAILY_DATA', engine)
        Latest_Date = Latest_Date['Relevant_Date'][0] + datetime.timedelta(1)
        Day = datetime.timedelta(1)
        Start_Date = Latest_Date
        End_Date = yesterday.date()

        mkt_holidays = pd.read_sql('select Holiday, Relevant_Date from NSE_BSE_MARKET_HOLIDAYS_YEARLY_DATA', engine)
        mkt_holidays = mkt_holidays.drop_duplicates('Relevant_Date').reset_index(drop=True)

        final_df = pd.DataFrame()
        if Start_Date > End_Date:
            print('No Data Available')
        else:
            while (Start_Date <= End_Date):

                print(Start_Date)
                if mkt_holidays.Relevant_Date.isin([Start_Date]).any() == True:
                    holiday_idx = mkt_holidays[mkt_holidays.Relevant_Date.isin([Start_Date])]['Holiday'].index[0]
                    error_type = 'Market Holiday'
                    error_msg = 'Market Holiday on account of ' + mkt_holidays['Holiday'][holiday_idx]
                    print(error_msg)
                    Start_Date += days
                    log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

                elif ((Start_Date.weekday() == 5) | (Start_Date.weekday() == 6)):
                    print("NSE_EQ_DAILY_DATA")
                    print("Data not available for", Start_Date.strftime("%Y-%m-%d"))
                    print("Weekend , no data")
                    Start_Date += days

                else:
                    wd = r'C:/Users/Administrator/Junk/'
                    date = Start_Date.strftime("%d-%m-%Y")
                    print(date)
                    proxy_host = "proxy.crawlera.com"
                    proxy_port = "8011"
                    proxy_auth = "5c2fcd5a03ad47a8b87f3cc83450c4a7:"  # Make sure to include ':' at the end
                    proxies = {"https": "https://{}@{}:{}/".format(proxy_auth, proxy_host, proxy_port),
                               "http": "http://{}@{}:{}/".format(proxy_auth, proxy_host, proxy_port)}
                    proxies = {"https": "http://{}@{}:{}/".format(proxy_auth, proxy_host, proxy_port),
                               "http": "http://{}@{}:{}/".format(proxy_auth, proxy_host, proxy_port)}

                    url = "https://www1.nseindia.com/ArchieveSearch?h_filetype=eqbhav&date=" + date + "&section=EQ"
                    headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36'}
                    # payload = {'h_filetype': 'eqbhav',
                    #            'date': '18-11-2022',
                    #            'section': 'EQ'}
                    print(url)
                    session = HTMLSession()
                    r = session.get(url, headers=headers,proxies=proxies, verify=False)
                    print(r)
                    soup = BeautifulSoup(r.content, 'lxml')
                    href = soup.findAll('a', attrs={'target': 'new'})
                    if href != []:

                        with sync_playwright() as p:
                            browser = p.chromium.launch(headless=False, args = ['--disable-blink-features=AutomationControlled'])
                            context = browser.new_context(bypass_csp = True)
                            page = context.new_page()
                            page.goto(url, timeout=0)
                            time.sleep(5)
                            with page.expect_download() as download_info:
                                page.locator("a:has-text(\".csv.zip\")").click()

                            download = download_info.value
                            file_name = download.suggested_filename
                            destination_folder_path = wd
                            download.save_as(destination_folder_path + file_name)

                        z = zipfile.ZipFile(wd + file_name)
                        z.extractall(path=wd)
                        z.close()

                        file_name = str(z.filelist[0]).split("'")[1]
                        NSE_EQ_DATA = pd.read_csv(wd + file_name)
                        del (NSE_EQ_DATA['Unnamed: 13'])
                        NSE_EQ_DATA.columns = NSE_EQ_DATA.columns.str.title().str.replace(' ', '_')
                        NSE_EQ_DATA = NSE_EQ_DATA.rename(index=str, columns={'Tottrdqty': 'Total_Trd_Qty_No',
                                                                             'Tottrdval': 'Total_Trd_Val_INR',
                                                                             'Isin': 'ISIN'})
                        NSE_EQ_DATA['Relevant_Date'] = pd.to_datetime(NSE_EQ_DATA['Timestamp'])
                        del (NSE_EQ_DATA['Timestamp'])
                        NSE_EQ_DATA['Runtime'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
                        print("NSE_EQ_DATA")
                        print("Run Time : ", datetime.datetime.now(india_time))
                        print(NSE_EQ_DATA.head(4))
                        NSE_EQ_DATA.rename(columns={'Prevclose': 'Prev_Close', 'Totaltrades': 'Total_Trades_No'},inplace=True)
                        column_list = NSE_EQ_DATA.columns.tolist()
                        for column in column_list:
                            print(column)
                            try:
                                NSE_EQ_DATA[column] = NSE_EQ_DATA[column].apply(lambda x: x.lstrip())
                                NSE_EQ_DATA[column] = NSE_EQ_DATA[column].apply(lambda x: x.rstrip())
                            except:
                                continue
                        NSE_EQ_DATA.to_sql(name='NSE_EQ_DAILY_DATA', con=engine, if_exists='append', index=False)

                        print("NSE_EQ_DATA Loaded Sucessfully")
                        print("No of Rows inserted :", len(NSE_EQ_DATA))
                        Start_Date = Start_Date + Day

                        os.remove(wd + file_name)
                    else:
                        Start_Date = Start_Date + Day

                    for files in os.listdir():
                        if files.endswith('.xlsx'):
                            os.remove(files)


        click_max_date = client.execute("select max(Relevant_Date) from AdqvestDB.NSE_EQ_DAILY_DATA")
        click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])
        query = 'select * from AdqvestDB.NSE_EQ_DAILY_DATA WHERE Relevant_Date > "' + click_max_date + '"'
        df = pd.read_sql(query, engine)
        client.execute("INSERT INTO NSE_EQ_DAILY_DATA VALUES", df.values.tolist())

        log.job_end_log(table_name, job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'", str(sys.exc_info()[0])).group(1))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        error_msg = str(sys.exc_info()[1]) + " line " + str(exc_tb.tb_lineno)
        print(error_type)
        print(error_msg)

        log.job_error_log(table_name, job_start_time, error_type, error_msg, no_of_ping)


if (__name__ == '__main__'):
    run_program(run_by='manual')
