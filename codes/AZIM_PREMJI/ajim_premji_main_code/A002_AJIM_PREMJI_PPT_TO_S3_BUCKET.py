#!/usr/bin/env python
# coding: utf-8
## induja

import datetime as dt
import pandas as pd
from pandas.io import sql
import calendar
import os
import sys
from time import sleep
import re
import datetime as datetime
from pytz import timezone
import numpy as np
import sys
import time
from itertools import islice
from os import listdir
from os.path import isfile, join
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import numpy as np
from dateutil.relativedelta import relativedelta
import adqvest_db
import JobLogNew as log
import datetime as datetime
from pytz import timezone
import adqvest_s3
import boto3

def run_program(run_by='Adqvest_Bot', py_file_name=None):

    os.chdir('C:/Users/Administrator/AdQvestDir')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'NA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)


        ACCESS_KEY_ID = adqvest_s3.s3_cred()[0]
        ACCESS_SECRET_KEY = adqvest_s3.s3_cred()[1]
        BUCKET_NAME = 'alt-data-report-4-thurro'

        s3 = boto3.resource(
            's3',
            aws_access_key_id=ACCESS_KEY_ID,
            aws_secret_access_key=ACCESS_SECRET_KEY,
            config=boto3.session.Config(signature_version='s3v4',region_name = 'ap-south-1')
        )


        path = "C:/Users/Administrator/AdQvestDir/codes/ajim_premji/ajim_premji_output_files"
        dir_list = os.listdir(path)

        # print(dir_list)
        input_folder = 'C:/Users/Administrator/AdQvestDir/codes/8_30_AM_WINDOWS_SERVER_LIGHT_HOUSE_PPT/ajim_premji_output_files'

        list_files = []

        files = [f for f in listdir(input_folder) if isfile(join(input_folder, f))]
        for filename in files:
            joined = os.path.join(filename)
            list_files.append(joined)

        #print(list_files)


        final_list =  [x for x in list_files if '_1.pdf' not in x]
        final_list =  [x for x in final_list if '_2.pdf' not in x]
        final_list =  [x for x in final_list if '_Old.pdf' not in x]
        final_list =  [x for x in final_list if '_old.pdf' not in x]
        final_list =  [x for x in final_list if '_Ext.pdf' not in x]
        final_list =  [x for x in final_list if '.csv' not in x]
        final_list =  [x for x in final_list if '.py' not in x]
        final_list =  [x for x in final_list if '.pdf' not in x]
        final_list =  [x for x in final_list if '.R' not in x]
        # final_list =  [x for x in final_list if '.xlsx' not in x]
        

       
        # print(final_list)
        final_list.sort()



        wd = 'C:/Users/Administrator/AdQvestDir/codes/8_30_AM_WINDOWS_SERVER_LIGHT_HOUSE_PPT/ajim_premji_output_files/'
        os.chdir(wd)

        # file upload s3
        for i in final_list:
            data =  open(wd + i, 'rb')
            s3.Bucket(BUCKET_NAME).put_object(Key='ajim_premji_PPT/'+i, Body=data,ContentType='application/x-mspowerpoint')
            print(i)
            data.close()
        print('All files Uploaded')

        log.job_end_log(table_name,job_start_time,no_of_ping)

    except:
        try:
            connection.close()
        except:
            pass
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])

        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
