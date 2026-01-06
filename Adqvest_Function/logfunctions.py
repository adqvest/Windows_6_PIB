#!/usr/bin/env python
# coding: utf-8

import JobLogNew as log
import re
import sys

def start_log(run_by,table_name,py_file_name,job_start_time,scheduler):
    if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
    else:
        log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

def error_log(table_name,job_start_time,no_of_ping):
    error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
    error_msg = str(sys.exc_info()[1])
    print(error_msg)
    log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)