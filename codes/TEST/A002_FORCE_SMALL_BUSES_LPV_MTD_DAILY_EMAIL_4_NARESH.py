
import glob
import os
import smtplib
import re
import numpy as np
import sqlalchemy
import pandas as pd
from pandas.io import sql
from dateutil.relativedelta import relativedelta
# import calender
import datetime as datetime
from pytz import timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import timedelta
import sys
# sys.path.insert(0, '/home/ubuntu/AdQvestDir/Adqvest_Function')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
import ClickHouse_db




# In[ ]:


def run_program(run_by='Adqvest_Bot', py_file_name=None):
    global no_of_ping
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]


    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday_date = today.date() - days

    engine = adqvest_db.db_conn()

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = ''

    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)



        if today.weekday() == 1:
            to = 'justkatariya@gmail.com'
            cc = 'rahul.sekhar@adqvest.com, s.akhuli@adqvest.com, mrinalini@adqvest.com'
            # to = 'rahul.sekhar@adqvest.com'
            # cc = 'rahul.sekhar@adqvest.com'
        else:
            to = 'justkatariya@gmail.com'
            cc = 'rahul.sekhar@adqvest.com, s.akhuli@adqvest.com, mrinalini@adqvest.com'

            # to = 'rahul.sekhar@adqvest.com'
            # cc = 'rahul.sekhar@adqvest.com'




        msg = MIMEMultipart()
        msg['From'] = "Thurro Insights <thurro@adqvest.com>"

        msg['To'] = to
        msg['Cc'] = cc
        recipients = to.split(",") + cc.split(",")
        msg['Subject'] = "FORCE MOTORS Small Buses (LPV) MTD REGISTRATIONS"

        max_date=pd.read_sql('SELECT max(Relevant_Date) as Max FROM AdqvestDB.VAHAN_MAKER_VS_CATEGORY_INDIA_LEVEL_DAILY_DATA', engine)['Max'][0]

        from_date=max_date-timedelta(days=90)

        body =f""" Hello, here is the daily registrations data for Force Motors (LPV) as on {str(max_date)}

        """
        final_out = pd.read_sql(f"SELECT Relevant_Date as Date, Total as Total_Registrations FROM VAHAN_MAKER_VS_CATEGORY_INDIA_LEVEL_DAILY_DATA WHERE Maker='FORCE MOTORS LIMITED' and Vehicle_Category='LPV' and Relevant_Date >'{str(from_date)}' ORDER BY Relevant_Date DESC", engine)


        msg.attach(MIMEText(body, 'plain'))
        sel_time = (datetime.datetime.now(india_time) - datetime.timedelta(1)).strftime("%Y-%m-%d %H:%M:%S")
        sel_end_time = (datetime.datetime.now(india_time) - datetime.timedelta(hours = 2)).strftime("%Y-%m-%d %H:%M:%S")
        time_now = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))







        html1 = """        <html>
          <head></head>

          <body>
            {0}
          </body>
        </html>
        """.format(final_out.to_html())

        part1 = MIMEText(html1, 'html')
        msg.attach(part1)


        smtp = smtplib.SMTP('smtp.gmail.com:587')
        smtp.starttls()
        smtp.login("thurro@adqvest.com", "Adqvest326$")
        smtp.sendmail(msg['From'],recipients,msg.as_string())
        smtp.quit()

        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)


if(__name__=='__main__'):
    run_program(run_by='manual')
