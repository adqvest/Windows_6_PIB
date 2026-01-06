import re
import sys

import pandas as pd
import datetime as datetime
from pytz import timezone

import base64

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
from AdqvestEmailSender import adqvestemailsender

india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days

run_time = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    engine = adqvest_db.db_conn()
    india_time = timezone('Asia/Kolkata')

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = ''
    if (py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0
    
    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        if (today.date().day == 1):
            # Define multiple recipients
            to = 'r.poduri@qfc.qa,l.kgasago@qfc.qa'
            # to = 's.akhuli@thurro.com'
            cc = 'mrinalini@thurro.com,s.akhuli@thurro.com,pushkar.patil@thurro.com'
            # cc = 'pushkar.patil@thurro.com'
            print('Recipients Updated')
      
            # DIFC data
            query = 'select * from AdqvestDB.DIFC_LISTED_COMPANY_MONTHLY_NO_PIT_DATA WHERE Link IS NOT NULL;'
            df1 = pd.read_sql(query, engine)
            print('DF Generated')
            del df1['Relevant_Date']
            del df1['Runtime']
            del df1['Link']
            df1.to_excel('DIFC_LISTED_COMPANY_DETAILS.xlsx', index = False)
            
            # ADGM data
            query = 'select * from AdqvestDB.ADGM_FINANCIAL_SERVICES_FIRM_DETAILS_MONTHLY_DATA WHERE Relevant_Date = (SELECT MAX(Relevant_Date) FROM AdqvestDB.ADGM_FINANCIAL_SERVICES_FIRM_DETAILS_MONTHLY_DATA)'
            df2 = pd.read_sql(query, engine)
            print('DF Generated')
            del df2['Relevant_Date']
            del df2['Runtime']
            del df2['Link']
            df2.to_excel('ADGM_FINANCIAL_SERVICES_FIRM.xlsx', index = False)
            
            query = 'select * from AdqvestDB.NASDAQ_DUBAI_LISTED_SECURITIES_DAILY_NO_PIT_DATA'
            df3 = pd.read_sql(query, engine)
            print('DF Generated')
            del df3['Relevant_Date']
            del df3['Runtime']
            del df3['Act_Runtime']
            del df3['Security_Category']
            df3 = df3[['Security_Type', 'Security_Name']]
            df3.to_excel('NASDAQ_DUBAI_LISTED_SECURITIES.xlsx', index = False)

            
            # Send email
            email_sender = adqvestemailsender()
    
            body_parts=[]
    
            body ="""Hello , Please find the Excel Sheets Attached with this Email"""
            body_parts.append({'type': 'plain', 'content': body})
    
            subject = "DIFC, ADGM AND NASDAQ SECURITIES DATA as on " + str(today.date())
            
            message = email_sender.create_email_message(to, cc, subject, body_parts)
            
            message["message"]["attachments"] = []

            for file_name in ["DIFC_LISTED_COMPANY_DETAILS.xlsx", "ADGM_FINANCIAL_SERVICES_FIRM.xlsx", 'NASDAQ_DUBAI_LISTED_SECURITIES.xlsx']:
                with open(file_name, "rb") as f:
                    file_bytes = f.read()
                encoded = base64.b64encode(file_bytes).decode("utf-8")
                attachment = {
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "name": file_name,
                    "contentType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    "contentBytes": encoded
                }
                message["message"]["attachments"].append(attachment)
                    
            if email_sender.send_email(message):
                print("Email sent successfully via Microsoft Graph API")
            else:
                print("Failed to send email via Microsoft Graph API")
    
            print('REPORT SENT')

        log.job_end_log(table_name,job_start_time, no_of_ping)
    except Exception as e:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(e) + " line " + str(sys.exc_info()[2].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')