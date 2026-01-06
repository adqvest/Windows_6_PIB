import warnings
warnings.filterwarnings('ignore')
import pandas as pd
import numpy as np
import datetime
from pytz import timezone
import os
import sys
import re
import requests
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
# from State_Function import state_rewrite ToDo:Uncomment
import adqvest_db
import pdftoexcel
import dbfunctions
import MySql_To_Clickhouse as sqlch
from bs4 import BeautifulSoup
import JobLogNew as log
from adqvest_robotstxt import Robots
robot = Robots(__file__)

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    engine = adqvest_db.db_conn()
    india_time = timezone('Asia/Kolkata')
    today = datetime.datetime.now(india_time)

    job_start_time = datetime.datetime.now(india_time)
    table_name = 'CEA_INSTALLED_CAPACITY_RES_BREAKUP_MONTHLY_DATA'
    if (py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if (run_by == 'Adqvest_Bot'):
            log.job_start_log_by_bot(table_name, py_file_name, job_start_time)
        else:
            log.job_start_log(table_name, py_file_name, job_start_time, scheduler)

        # max_rel_date = pd.read_sql('Select max(Relevant_Date) as max from CEA_INSTALLED_CAPACITY_RES_BREAKUP_MONTHLY_DATA', con = engine)['max'][0]
        # if max_rel_date == None:
        max_rel_date = datetime.date(2016, 12, 31)
        # else:
        #     pass
        if today.date() - max_rel_date >= datetime.timedelta(30):
            print('here')
            url = 'https://cea.nic.in/wp-admin/admin-ajax.php'
            start_date = max_rel_date + relativedelta(months=2, days=-1, day = 1)
            end_date  = today.date()
            print(start_date, end_date)
            path = r'C:\Users\Administrator\Junk'
            while start_date <= end_date:
                print(start_date)
                month = start_date.strftime('%m')
                yr = start_date.year
                payload = {'action': 'monthly_archive_report',
                           'selMonthYear': f'{str(yr)}-{month}',
                           'reportType': 'installed'}
                r = requests.post(url, data=payload, verify=False, timeout=60)
                print(r)

                soup = BeautifulSoup(r.content)
                for i in soup.findAll('a', {'target': '_blank'}, href=True):
                    if '.pdf' in i.get('href'):
                        print(i.get('href'))
                        link = i.get('href')
                        robot.add_link(link)

                        r2 = requests.get(link)
                        filename = f'CEA_Installed_Capacity_{str(yr)}_{month}.pdf'
                        with open(path+'\\'+filename, 'wb') as f:
                            f.write(r2.content)
                        filename = filename.replace('.pdf', '')
                        filepath = pdftoexcel.pdftoexcel(path, filename)

                        # dt_txt = '-'.join(i.replace('.xlsx', '').split('_')[-2:])
                        # date = parse(dt_txt).date() + relativedelta(months=1, days=-1, day=1)
                        sheets = pd.ExcelFile(filepath)
                        sheets = sheets.sheet_names
                        df = pd.DataFrame()
                        for sheet in sheets:
                            temp = pd.read_excel(filepath, sheet_name=sheet,header=None)
                            if temp.iloc[:, 0].str.lower().str.contains('small.+hydro.+power', na=False).any():
                                df = pd.concat([df, temp], ignore_index = True)
                            elif temp.iloc[:, 1].str.lower().str.contains('.+break.+up.+', na=False).any():
                                df = pd.concat([df, temp], ignore_index = True)
                        df = df.reset_index(drop=True)
                        try:
                            st_idx = df[df.iloc[:, 1].str.lower().str.contains('.+break.+up.+', na=False)].index[0] + 1
                        except:
                            st_idx = df[df.iloc[:, 0].str.lower().str.contains('small.+hydro.+power', na=False)].index[0] + 1
                        end_idx = st_idx + 3
                        df = df.iloc[st_idx:end_idx, :]
                        df = df.dropna(how = 'all', axis= 1)
                        df = df.fillna('')
                        df.iloc[0, :] = df.iloc[0, :] + '_' + df.iloc[1, :]
                        df.iloc[0, :] = df.iloc[0, :].str.strip('_').str.replace('/', '_').str.replace('.', '').str.replace('$', '').str.replace('#', '').str.replace('-', '_').str.replace(' ', '_').str.strip()
                        df.columns = df.iloc[0, :].values
                        df = df.reset_index(drop=True)

                        df = df.iloc[-1:, :]
                        df = df.reset_index(drop=True)
                        df['Relevant_Date'] = start_date
                        df['Runtime'] = today
                        print(len(df))
                        print(df)
                        df.to_sql('CEA_INSTALLED_CAPACITY_RES_BREAKUP_MONTHLY_DATA', if_exists='append', index=False, con=engine)

                        os.remove(filepath)
                        os.remove(path+'\\'+filename+'.pdf')
                start_date = start_date + relativedelta(months=2, days=-1, day=1)

            # sqlch.ch_truncate_and_insert('CEA_INSTALLED_CAPACITY_RES_BREAKUP_MONTHLY_DATA')
        log.job_end_log(table_name, job_start_time, no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'", str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name, job_start_time, error_type, error_msg, no_of_ping)
if (__name__ == '__main__'):
    run_program(run_by='manual')