import warnings
import sys
import pandas as pd
import numpy as np
warnings.filterwarnings('ignore')
import re
import datetime
from pytz import timezone
import os
# sys.path.insert(0, '/home/ubuntu/AdQvestDir/Adqvest_Function')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import ClickHouse_db
import JobLogNew as log


def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    client = ClickHouse_db.db_conn()
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = ''
    if (py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        # wd = '/home/ubuntu/AdQvestDir/codes/'
        wd = 'C:/Users/Administrator/AdQvestDir/codes/'
        os.chdir(wd)
        scheduler = os.listdir(wd)
        dict = {'Scheduler': [],
                'Filename': [],
                'Comments': []}
        for j in scheduler:
            if 'SCHEDULER_ALL_CODES' in j and 'INSIGHTS' not in j and 'PRESENTATION' not in j and 'ONE_TIME' not in j:
                print(wd + j)
                os.chdir(wd + j)
                file_list = os.listdir()

                for k in file_list:
                    if '.py' in k:
                        with open(k, encoding="utf8") as f:
                            print(k)
                            for index, line in enumerate(f):
                                if ('zenrows' in line):
                                    dict['Filename'].append(k)
                                    dict['Scheduler'].append(j)
                                    dict['Comments'].append("Line {}: {}".format(index, line.strip()))
                                    print("Line {}: {}".format(index, line.strip()))
                            f.close()

        df = pd.DataFrame(dict)
        df = df.drop_duplicates()
        print(df)
        df.to_csv('C:/Users/Administrator/Junk/zenrows.csv')
        log.job_end_log(table_name,job_start_time, no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)
if(__name__=='__main__'):
    run_program(run_by='manual')