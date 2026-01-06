import datetime    
from pytz import timezone
import calendar
from bs4 import BeautifulSoup
import sys
from playwright.async_api import async_playwright
import asyncio
import nest_asyncio
import re
import pandas as pd
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
from adqvest_robotstxt import Robots
robot = Robots(__file__)

def short_year(year):
    return str(year)[-2:]

async def data_collection():
    print('Inside data_collection')
    
    pw = await async_playwright().start()
    browser = await pw.chromium.launch(headless=False)
    context = await browser.new_context(ignore_https_errors=True, no_viewport=True)
    page = await context.new_page() 

    start_year = 2016
    end_year = 2024
    start_month = 1
    end_month = 12
    start_day = 1
    end_day = 31   

    formats = [
        'Primary-Price-list-w.e.f.-{day}th-{month1}{year1}.pdf','Primary-Price-list-w.e.f.-{day}st-{month1}{year1}.pdf', 
        'Primary-Price-List-w.e.f.-{day}.{month}.{year}.pdf','Primary-Price-list-w.e.f.-{day}st-{month}{year}.pdf',
        'Primary-Price-List-w.e.f.-{day}.0.{year}.pdf','Primary-Price-list-w.e.f.-{day}st-{month1}{year}.pdf',
        'Primary-Price-List-w.e.f.-{day}{month}{year}.pdf','Primary-Price-List-w.e.f.-{day}.{month}.{year1}.pdf', 
        'Primary-Price-List-w.e.f.-{day}-{month_full}-{year}.pdf','Primary-Price-list-w.e.f.-{day}st-{month}{year1}.pdf',
        'Primary-Price-List-w.e.f.-{day}th-{month_full}-{year}.pdf','Primary-Price-List-w.e.f.-{day}{month_full}{year}.pdf'
        'Primary-Price-List-w.e.f.-{day}th.{month_full}.{year}.pdf', 'Primary-Price-List-w.e.f.-{day}{month}{year}.pdf'
        'Primary-Price-List-w.e.f.-{day}th{month_full}{year}.pdf','Primary-Price-List-w.e.f.-{day}{month}{year1}.pdf'
        'Primary-Price-List-w.e.f.-{day}th-{month_full}-{year1}.pdf','Primary-Price-List-w.e.f.-{day}{month_full}{year1}.pdf'
        'Primary-Price-List-w.e.f.-{day}-{month_full}-{year1}.pdf'
    ]

    add_links = []
    for year in range(start_year, end_year + 1):
        for month in range(start_month, end_month + 1):
            for day in range(start_day, end_day + 1):
                month1 = calendar.month_abbr[month]
                month_full = calendar.month_name[month]
                try:
                    if day > calendar.monthrange(year, month)[1]:
                        continue
                except:
                    continue

                year1 = short_year(year)
                day_str = f"{day:02d}"
                month_str = f"{month:02d}"
                
                for format_str in formats:
                    pdf_link = f'https://d2z1l9uefzbzxd.cloudfront.net/wp-content/uploads/{year}/{month_str}/' + \
                               format_str.format(day=day_str, month1=month1, year1=year1, month=month_str, year=year, month_full=month_full)

                    await page.goto(pdf_link, timeout=150000)
                    # await page.wait_for_timeout(3000)
                    html = await page.content()
                    soup = BeautifulSoup(html, 'lxml')
                    
                    if 'This XML file does not appear' not in soup.text:
                        add_links.append(pdf_link)
                        print('$$$ PDF Exists for', pdf_link, '$$$')
                    else:
                        # print('!!! DOES NOT EXIST FOR', pdf_link, '!!!')
                        pass

    await browser.close() 
    return add_links
       
def run_program(run_by='Adqvest_Bot', py_file_name=None):    
    engine = adqvest_db.db_conn()

     #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today = datetime.datetime.now(india_time)

    # job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'BALCO_OLD_LINKS_DATE'
    if py_file_name is None:
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = 'TEST_WINDOWS_SERVER2_SCHEDULER'
    no_of_ping = 0

    try:
        if run_by == 'Adqvest_Bot':
            log.job_start_log_by_bot(table_name, py_file_name, job_start_time)
        else:
            log.job_start_log(table_name, py_file_name, job_start_time, scheduler)
            
        nest_asyncio.apply()
        loop = asyncio.get_event_loop()
        balco_old = loop.run_until_complete(data_collection())

        df = pd.DataFrame(balco_old, columns=['PDF_Link'])
        df['Runtime'] = today
        df['Source'] = 'BALCO'
        
        df.to_sql('BALCO_OLD_LINKS_DATE', index=False, con=engine, if_exists='append')
        print("Data uploaded to SQL") 

        log.job_end_log(table_name, job_start_time, no_of_ping) 

    except:
        error_type = str(re.search("'(.+?)'", str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name, job_start_time, error_type, error_msg, no_of_ping)

if __name__ == '__main__':
    run_program(run_by='manual')