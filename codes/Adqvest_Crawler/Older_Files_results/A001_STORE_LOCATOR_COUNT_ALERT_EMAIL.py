#Created By Santonu
import warnings
warnings.filterwarnings('ignore')
import re
import pandas as pd
import datetime as datetime
from pytz import timezone
import sys
import asyncio
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
from AdqvestEmailSender import adqvestemailsender

# working_dir=r"C:\Users\Santonu\Adqvest_Crawler"
working_dir=r"C:\Users\Administrator\AdQvestDir\codes\Adqvest_Crawler"

sys.path.insert(0, working_dir)
from StoreLocator_Table_Info import Table_Info
 

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    global no_of_ping
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]

    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

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

        to = 'santonu.debnath@thurro.com'
        cc = 'santonu.debnath@thurro.com'

        email_sender = adqvestemailsender()
        # Collect all email body parts
        body_parts = []
        # Subject
        subject = "STORES COUNT ALERT as of " + str(yesterday.date())
        
        #%%
        body1 = f"Hello Team,\n\nPlease find the attachment for the STORES COUNT.\n\nHere is the Stores having Weekly Count issue as of {yesterday.date()}."
        
        df_weekly=asyncio.run(Table_Info.get_data_info(weekly_info=True)) 
        df_monthly=asyncio.run(Table_Info.get_data_info(monthly_info=True))

        body_parts.append({'type': 'plain', 'content': body1})
        df_weekly.reset_index(drop=True,inplace=True)
        body_parts.append({'type': 'html', 'content': df_weekly.to_html()})
        #%%
        body2 = f"\n\nHere is the Stores having Monthly Count issue as of {yesterday.date()}."
        body_parts.append({'type': 'plain', 'content': body2})
    
        
        df_monthly.reset_index(drop=True,inplace=True)
        body_parts.append({'type': 'html', 'content': df_monthly.to_html()})
        #%%
        
        
        # Create and send email using Microsoft Graph API
        message = email_sender.create_email_message(to, cc, subject, body_parts)
        
        if email_sender.send_email(message):
            print("Email sent successfully via Microsoft Graph API")
        else:
            print("Failed to send email via Microsoft Graph API")    
            
        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')