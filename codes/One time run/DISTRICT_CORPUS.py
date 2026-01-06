
import os
import pandas as pd
import sqlalchemy
import numpy as np
import datetime as datetime
import re
from pytz import timezone
import sys
import time
import warnings
warnings.filterwarnings('ignore')

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log



# In[2]:



def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    engine = adqvest_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'LIFE_INSURANCE_COUNCIL_NEW_BUSINESS_PERFORMANCE'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
        
        os.chdir('C:/Users/Administrator/AdQvestDir/codes/One time run')
        print('START')
        final = pd.read_csv('Corpus.csv')
        final_df =pd.DataFrame(columns=['Words', 'District', 'Count'])
        for a, b, c in final.values:
            words = a.split()
            for x in words:
                final_df.loc[len(final_df.index)] = [x, b, c] 
        
        final_df.to_csv('District_Corpus.csv')
        print('END')
        

        log.job_end_log(table_name,job_start_time,no_of_ping)
        driver.quit()
    except:
        print(a, b, c)
        try:
            driver.quit()
            os.remove(os.listdir()[0])
        except:
            pass
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        error_msg = str(sys.exc_info()[1]) + " line " + str(exc_tb.tb_lineno)
        print(error_type)
        print(error_msg)

        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
