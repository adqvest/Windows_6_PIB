#!/usr/bin/env python
# coding: utf-8

# In[47]:


# Code Version v0.1
# Createb By Shivam and Subrata
# Creation Date 19-07-2019
# Update1 : Name and Date :
# Update1 Reason:
# Update2 : Name and Date :
# Update2 Reason:
# Use : This is the generic error handler

import sqlalchemy
import datetime as datetime
from pytz import timezone
import os
import pandas as pd

# derived package
import Cleaner as cleaner
import adqvest_db
import numpy as np

# In[48]:



#**** Credential Directory ****
#os.chdir('/home/ubuntu/AdQvestDir')
#os.chdir('C:/Users/krang/Dropbox/Subrata/Python')
#os.chdir('D:/Adqvest_Office_work/R_Script')

#DB Connection

engine     = adqvest_db.db_conn()


#****   Date Time *****
india_time = timezone('Asia/Kolkata')
days       = datetime.timedelta(1)
today      = datetime.datetime.now(india_time)

# In[49]:
def jobs_entry(all_files, scheduler):
    files_df = pd.DataFrame()
    files_df['Python_File_Name'] = all_files
    files_df['Schedular_Start_Time'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
    files_df['Run_By'] = "Adqvest_Bot"
    files_df['Scheduler'] = scheduler
    files_df['Relevant_Date'] = today.date()
    files_df.to_sql("TRANSACTION_LOG_AND_ERROR_LOG_DAILY_DATA", con=engine, if_exists='append', index=False)


def job_start_log_by_bot(table_name, py_file_name, job_start_time):

    relevant_date  = job_start_time.strftime("%Y-%m-%d")
    #print(py_file_name)
    print('select max(Schedular_Start_Time) as Time from TRANSACTION_LOG_AND_ERROR_LOG_DAILY_DATA where Python_File_Name = "' + py_file_name + '"')
    max_sc_time = pd.read_sql('select max(Schedular_Start_Time) as Time from TRANSACTION_LOG_AND_ERROR_LOG_DAILY_DATA where Python_File_Name = "' + py_file_name + '"', con=engine)['Time'][0].strftime("%Y-%m-%d %H:%M:%S")
    query      = 'Update TRANSACTION_LOG_AND_ERROR_LOG_DAILY_DATA set Start_Time = "' + job_start_time.strftime("%Y-%m-%d %H:%M:%S") + '", Table_Name = "' + table_name + '", Relevant_Date="' + relevant_date + '" where Schedular_Start_Time = "' + max_sc_time + '" and Python_File_Name = "'+py_file_name+'"'
    #print(query)
    connection = engine.connect()
    connection.execute(query)
    connection.execute('commit')
    connection.close()


#******************************************************** log job start time **************************************************************************************

def job_start_log(table_name, py_file_name,  job_start_time, scheduler = ''):
    """To create job running status with Start_Time in LOG_TABLE

       Args:
       table_name     : pass the table name for which you want to update Start_Time
       job_start_time : start time of the program. should be datetime format.
       batch          : put the batch name. by default it is null string
       """

    relevant_date  = job_start_time.strftime("%Y-%m-%d")
    job_start_time = pd.to_datetime(job_start_time.strftime("%Y-%m-%d %H:%M:%S"))

    log_Table     = pd.DataFrame({'Relevant_Date' : relevant_date,'Table_Name' : [table_name], 'Python_File_Name' : [py_file_name], 'Start_Time' : job_start_time, 'Scheduler' : scheduler})

    log_Table.to_sql(name='TRANSACTION_LOG_AND_ERROR_LOG_DAILY_DATA',con=engine,if_exists='append',index=False)



# In[50]:




# ******************************************************** log job end time**************************************************************************************

def job_end_log(table_name, job_start_time, no_of_ping):
    """To update the End_Time and Execution_Time_Seconds in LOG_TABLE for given table_name and job_start_time

       Args:
       table_name     : pass the table name for which you want to update End_Time
       job_start_time : start time of the program. should be DateTime format.
       """

    relevant_date = job_start_time.strftime("%Y-%m-%d")
    job_end_time       = datetime.datetime.now(india_time)
    job_execution_time = (job_end_time - job_start_time).total_seconds()
    job_end_time       = '"'+job_end_time.strftime("%Y-%m-%d %H:%M:%S")+'"'
    job_start_time = job_start_time.strftime("%Y-%m-%d %H:%M:%S")

    query      = 'Update TRANSACTION_LOG_AND_ERROR_LOG_DAILY_DATA set No_Of_Ping=' + str(no_of_ping) + ', End_Time = ' + job_end_time + ', Execution_Time_Seconds = "' + str(job_execution_time) + '", Runtime = ' + job_end_time + ' where Start_Time = "' + job_start_time + '" and Table_Name = "'+table_name+'"'
    connection = engine.connect()
    connection.execute(query)
    connection.execute('commit')
    connection.close()



# In[52]:




## ******************************************************** log job error type and error message *********************************************************************

def job_error_log(table_name, job_start_time, error_type, error_msg, no_of_ping=-10,error_desc = ''):
    """To update Error Type & Error Message in LOG_TABLE

       Args:
       table_name    : pass the table name for which you want to update in LOG_TABLE
       job_start_time: start time of a job
       error_type    : contains the type of error
       error_msg     : contains the error message
       no_of_ping    : contains number of pings hit by program
       error_desc    : contains the error description. By default it is a null string.

       """


    try:

        relevant_date = job_start_time.strftime("%Y-%m-%d")
        job_end_log(table_name, job_start_time, no_of_ping)
        job_start_time = job_start_time.strftime("%Y-%m-%d %H:%M:%S")
        #******* Cleaning error message/string *******
        error_type = cleaner.full_clean(error_type)
        error_msg = cleaner.full_clean(error_msg)

        query = 'Update TRANSACTION_LOG_AND_ERROR_LOG_DAILY_DATA set No_Of_Ping=' + str(no_of_ping) + ', Error_Type = "'+ error_type +'", Error_Msg = "'+ error_msg +'", Error_Desc = "'+ error_desc +'" where Start_Time = "' + job_start_time + '" and Table_Name = "'+table_name+'"'

        connection = engine.connect()
        connection.execute(query)
        connection.execute('commit')
        connection.close()
    except:
        error_type = "Error_Handler"
        error_msg  = "Another exception occured while handling Exception"
        error_desc = "Error Handler did not able to log the error. Error is occured in Error Handler code. Please check Error Handler code"
        query      = 'Update TRANSACTION_LOG_AND_ERROR_LOG_DAILY_DATA set No_Of_Ping=' + str(no_of_ping) + ', Error_Type = "'+ error_type +'", Error_Msg = "'+ error_msg + '", Error_Desc = "'+ error_desc +'" where Start_Time = "' + job_start_time + '" and Table_Name = "'+table_name+'"'

        connection = engine.connect()
        connection.execute(query)
        connection.execute('commit')
        connection.close()

def check_data(table_name, total_rows,thresh = 0.5):

    """day : Either today or yesterday
       table_name : Table Name"""
    df = pd.read_sql("select count(*) as Count,Relevant_Date from "+table_name+" group by Relevant_Date order by Relevant_Date desc limit 30",engine)
    count = df["Count"].median() * thresh

    if(total_rows<=count):
        raise Exception("Number of rows too less")
    else:
        print("Data Quality good")
