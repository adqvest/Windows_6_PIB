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


def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    engine = adqvest_db.db_conn()
    india_time = timezone('Asia/Kolkata')
    today = datetime.datetime.now(india_time)

    job_start_time = datetime.datetime.now(india_time)
    table_name = 'CEA_STATE_N_TYPE_WISE_INSTALLED_CAPACITY_MONTHLY_RAW_DATA'
    if (py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if (run_by == 'Adqvest_Bot'):
            log.job_start_log_by_bot(table_name, py_file_name, job_start_time)
        else:
            log.job_start_log(table_name, py_file_name, job_start_time, scheduler)

        max_rel_date = pd.read_sql('Select max(Relevant_Date) as max from CEA_STATE_N_TYPE_WISE_INSTALLED_CAPACITY_MONTHLY_RAW_DATA', con = engine)['max'][0]
        print(max_rel_date)
        if today.date() - max_rel_date >= datetime.timedelta(30):
            url = 'https://cea.nic.in/wp-admin/admin-ajax.php'
            start_date = max_rel_date + relativedelta(months=2, days=-1, day = 1)
            end_date  = today.date()
            path = r'C:\Users\Administrator\Junk'
            # path = '/Users/pushkar/Adqvest/files_collection/cea/'
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

                        r2 = requests.get(link)
                        filename = f'CEA_Installed_Capacity_{str(yr)}_{month}.pdf'
                        with open(path+'\\'+filename, 'wb') as f:
                            f.write(r2.content)
                        filename = filename.replace('.pdf', '')
                        filepath = pdftoexcel.pdftoexcel(path, filename)

                        key = 'CEA_POWER_DATA/INSTALLED_CAPACITY/'
                        dbfunctions.to_s3bucket(filepath, key)

                        # dt_txt = '-'.join(i.replace('.xlsx', '').split('_')[-2:])
                        # date = parse(dt_txt).date() + relativedelta(months=1, days=-1, day=1)
                        sheets = pd.ExcelFile(filepath)
                        sheets = sheets.sheet_names
                        df = pd.DataFrame()
                        for sheet in sheets:
                            temp = pd.read_excel(filepath, sheet_name=sheet,header=None)
                            if temp.iloc[:, 0].str.lower().str.contains('state', na=False).any():
                                df = pd.concat([df, temp], ignore_index = True)
                        try:
                            df['Region'] = np.where(df.iloc[:, 0].str.lower().str.contains('located in', na=False), df.iloc[:, 0].str.split('\n'), np.nan)
                            df['Region'] = df.Region.apply(lambda x : x[1] if type(x) != float else x)

                        except:
                            df['Region'] = np.where(df.iloc[:, 0].str.lower().str.contains('located in', na=False), df.iloc[:, 0].str.split('LOCATED IN'), np.nan)
                            df['Region'] = df.Region.apply(lambda x : x[1] if type(x) != float else x)

                        df['Region'] = df.Region.ffill()
                        df = df.reset_index(drop=True)

                        drp_idx = df[(df.iloc[:, 0].str.lower().str.contains('including', na=False) | (df.iloc[:, 0].str.lower().str.contains('as on', na=False)))].index
                        df = df.drop(index=drp_idx)
                        df = df.reset_index(drop=True)
                        drp_idx = df[(df.iloc[:, 0].str.lower().str.contains('located in', na=False) | (df.iloc[:, 0].str.lower().str.contains('state', na=False)))].index

                        df = df.drop(index = drp_idx)
                        df = df.reset_index(drop=True)
                        try:
                            df.columns = ['State', 'Sector', 'Thermal_Coal_MW', 'Thermal_Lignite_MW', 'Thermal_Gas_MW', 'Thermal_Diesel_MW', 'Thermal_Total_MW', 'Nuclear_MW', 'Renewable_Hydro_MW', 'Renewable_RES_MNRE_MW', 'Renewable_Total_MW', 'Grand_Total_MW', 'Region']
                        except:
                            pass
                        try:
                            df.columns = ['State', 'Sector', 'Thermal_Coal_MW', 'Thermal_Lignite_MW', 'Thermal_Gas_MW', 'Thermal_Diesel_MW', 'Thermal_Total_MW', 'Nuclear_MW', 'Renewable_Hydro_MW', 'Renewable_RES_MNRE_MW', 'Grand_Total_MW', 'Region']
                        except:
                            pass
                        try:
                            df.columns = ['State', 'Sector', 'Thermal_Coal_MW', 'Thermal_Gas_MW', 'Thermal_Diesel_MW', 'Thermal_Total_MW', 'Nuclear_MW', 'Renewable_Hydro_MW', 'Renewable_RES_MNRE_MW', 'Grand_Total_MW', 'Region']
                        except:
                            pass
                        df = df.fillna('')
                        drp_idx = df[(df.iloc[:, 0] == '') & (df.iloc[:, 1] == '')].index
                        df = df.drop(index = drp_idx)
                        df = df.replace('', np.nan)
                        df['State'] = df.State.ffill()
                        df = df[~df.iloc[:, 0].str.lower().str.contains('total*', na=False)]
                        df = df[~df.iloc[:, 1].str.lower().str.contains('sub-total*', na=False)]
                        df = df[~df.iloc[:, 1].str.lower().str.contains('_', na=False)]
                        df['State'] = df['State'].str.replace('  ', ' ')
                        df['State'] = df['State'].str.replace('\n', ' ')
                        df['Region'] = df.Region.str.title().str.replace('*', '')
                        df['Region'] = df.Region.str.replace('*', '')
                        df['Region'] = df.Region.str.strip()
                        df = df.reset_index(drop=True)
                        x = df.copy()
                        df = x.copy()
                        # for i in range(len(df.State)):
                        #     state = state_rewrite.state(df['State'][i]).split('|')
                        #     if 'Centre' in df['State'][i]:
                        #         df['State'][i] = df['State'][i]
                        #         print(state[-1])
                        #     elif len(state) > 1:
                        #         df['State'][i] = state[-1]
                        #     else:
                        #         pass
                        df['State'] = np.where(~df.State.str.isupper(),df.State.str.title(), df.State)

                        df['Relevant_Date'] = start_date
                        df['Runtime'] = today
                        df = df[~df.Region.isna()]
                        df = df[df.State.str.lower() != 'revised']
                        print(len(df))
                        if len(df) > 119:
                            raise Exception(f'Increase in Data count for {date.strftime("%b %Y")}')
                        df.to_sql('CEA_STATE_N_TYPE_WISE_INSTALLED_CAPACITY_MONTHLY_RAW_DATA', if_exists='append', index=False, con=engine)
                        print('To SQL: ', len(df))

                        os.remove(filepath)
                        os.remove(path+'\\'+filename+'.pdf')
                start_date = start_date + relativedelta(months=2, days=-1, day=1)

            # sqlch.ch_truncate_and_insert('CEA_STATE_N_TYPE_WISE_INSTALLED_CAPACITY_MONTHLY_RAW_DATA')
        else:
            print('No New Data')
        log.job_end_log(table_name, job_start_time, no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'", str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name, job_start_time, error_type, error_msg, no_of_ping)

if (__name__ == '__main__'):
    run_program(run_by='manual')