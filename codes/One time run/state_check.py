import sys
import os
import pandas as pd
from pytz import timezone
import datetime
import re
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/State_Function')
import state_rewrite
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import JobLogNew as log
import adqvest_db


def run_program(run_by='Adqvest_Bot', py_file_name=None):
    global no_of_ping
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = ''
    no_of_ping = 0
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''


    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        query = "select Distinct State from MCA_COMPANIES_MASTER_MONTHLY_DATA where state is not null "
        df1 = pd.read_sql(query, con=engine)
        dic ={}
        for i in df1['State']:
            states =  state_rewrite.state((i.lower()))
            dic[i.lower()] = states.split('|')[-1].upper()    
        query = "select * from MCA_COMPANIES_MASTER_MONTHLY_DATA where state is not null "
        df = pd.read_sql(query, con=engine)       
        df.to_sql(name = "MCA_COMPANIES_MASTER_MONTHLY_CLEAN_DATA",if_exists="append",index = False,con = engine)
        print('Process Done', datetime.datetime.now())
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)


        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')