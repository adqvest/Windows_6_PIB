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


# In[48]:



#**** Credential Directory ****
os.chdir('C:/Users/Administrator/AdQvestDir')
#os.chdir('C:/Users/krang/Dropbox/Subrata/Python')
#os.chdir('D:/Adqvest_Office_work/R_Script')

#DB Connection
properties = pd.read_csv('AdQvest_properties.txt',delim_whitespace=True)

host    = list(properties.loc[properties['Env'] == 'Host'].iloc[:,1])[0]
port    = list(properties.loc[properties['Env'] == 'port'].iloc[:,1])[0]
db_name = list(properties.loc[properties['Env'] == 'DBname'].iloc[:,1])[0]

con_string = 'mysql+pymysql://' + host + ':' + port + '/' + db_name + '?charset=utf8'
engine     = sqlalchemy.create_engine(con_string,encoding='utf-8')


#****   Date Time *****
india_time = timezone('Asia/Kolkata')
days        = datetime.timedelta(1)


# In[49]:



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

    log_Table.to_sql(name='LOG_TABLE_NEW',con=engine,if_exists='append',index=False)




# In[50]:




# ******************************************************** log job end time**************************************************************************************

def job_end_log(table_name, job_start_time):
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

    query      = 'Update LOG_TABLE_NEW set End_Time = ' + job_end_time + ', Execution_Time_Seconds = "' + str(job_execution_time) + '", Runtime = ' + job_end_time + ' where Start_Time = "' + job_start_time + '" and Table_Name = "'+table_name+'"'
    connection = engine.connect()
    connection.execute(query)
    connection.execute('commit')
    connection.close()



# In[52]:




## ******************************************************** log job error type and error message *********************************************************************

def job_error_log(table_name, job_start_time, error_type, error_msg, error_desc = ''):
    """To update Error Type & Error Message in LOG_TABLE

       Args:
       table_name    : pass the table name for which you want to update in LOG_TABLE
       job_start_time: start time of a job
       error_type    : contains the type of error
       error_msg     : contains the error message
       error_desc    : contains the error description. By default it is a null string.

       """
    relevant_date = job_start_time.strftime("%Y-%m-%d")
    job_end_log(table_name, job_start_time)
    job_start_time = job_start_time.strftime("%Y-%m-%d %H:%M:%S")

    try:
        #******* Cleaning error message/string *******
        error_type = cleaner.full_clean(error_type)
        error_msg = cleaner.full_clean(error_msg)

        query = 'Update LOG_TABLE_NEW set Error_Type = "'+ error_type +'", Error_Msg = "'+ error_msg +'", Error_Desc = "'+ error_desc +'" where Start_Time = "' + job_start_time + '" and Table_Name = "'+table_name+'"'
        connection = engine.connect()
        connection.execute(query)
        connection.execute('commit')
        connection.close()
    except:
        error_type = "Error_Handler"
        error_msg  = "Another exception occured while handling Exception"
        error_desc = "Error Handler did not able to log the error. Error is occured in Error Handler code. Please check Error Handler code"
        query      = 'Update LOG_TABLE_NEW set Error_Type = "'+ error_type +'", Error_Msg = "'+ error_msg + '", Error_Desc = "'+ error_desc +'" where Start_Time = "' + job_start_time + '" and Table_Name = "'+table_name+'"'

        connection = engine.connect()
        connection.execute(query)
        connection.execute('commit')
        connection.close()
