#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#*** All Packages
import sqlalchemy
import pandas as pd
import os
import calendar
import datetime as datetime
from pytz import timezone
import numpy as np
import sys
import time
#from subprocess import call
from subprocess import run
from subprocess import Popen
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log


# In[ ]:


#**** Credential Directory ****
engine     = adqvest_db.db_conn()

#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days


# In[ ]:



scheduler = '2_AM_WINDOWS_SERVER_2_SCHEDULER'
folder_path = "C:/Users/Administrator/AdQvestDir/codes/2_AM_WINDOWS_SERVER_CRAWLER_SCHEDULER_ALL_CODES/"
sys.path.insert(1, folder_path)
all_files = os.listdir(folder_path)
all_files_without_extension = [file for file in all_files if(file.lower().endswith('.py'))]
all_files_without_extension = [file.replace('.py','') for file in all_files_without_extension]
print(all_files_without_extension)
all_files_without_extension.sort()
log.jobs_entry(all_files_without_extension, scheduler)

all_files.sort()
for file in all_files:
    try:
        if(file.lower().endswith('.py')):
            try:
                new_file = file.replace('.py','')
                mod = __import__(new_file)
                mod.run_program(py_file_name=new_file)
            except:
                continue

        elif(file.lower().endswith('.r')):
            try:
                run(["sudo", "Rscript", folder_path + file])
            except:
                continue
    except:
        continue
