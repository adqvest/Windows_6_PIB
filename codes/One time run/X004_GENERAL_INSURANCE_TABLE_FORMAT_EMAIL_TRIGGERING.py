import warnings
warnings.filterwarnings('ignore')
import os
import smtplib
import re
import pandas as pd
from datetime import datetime
from dateutil import parser, relativedelta
import datetime as datetime
from pytz import timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.base import MIMEBase
from email import encoders
import sys
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday_date = today.date() - days

    engine = adqvest_db.db_conn()
    connection = engine.connect()
    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = ''
    scheduler = ''
    no_of_ping = 0

    if (py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        db_status = pd.read_sql("SELECT Description, max(Relevant_Date) as max FROM GENERIC_DATA_ALERT_AND_EMAIL_SENDER WHERE Name = 'GENERAL INSURANCE MONTHLY TABLE' and Description is null order BY Relevant_Date DESC;", con=engine)
        desc_status = db_status['Description'][0]
        max_db_date = db_status['max'][0]
        if desc_status == None:
            # file = pd.ExcelFile('/Users/pushkar/Adqvest/files_collection/GENERAL_INSURANCE.xlsx')
            file = pd.ExcelFile(r'C:\Users\Administrator\AdQvestDir\NIIF_PPT\GENERAL_INSURANCE.xlsx')
            sheet_names = file.sheet_names
            df = pd.read_excel(r'C:\Users\Administrator\AdQvestDir\NIIF_PPT\GENERAL_INSURANCE.xlsx', sheet_name=sheet_names[0])
            # df = pd.read_excel('/Users/pushkar/Adqvest/files_collection/GENERAL_INSURANCE.xlsx', sheet_name=sheet_names[0])
            # dates = df.columns.tolist()[-1]
            dates = df.columns.tolist()[-1]

            date = parser.parse(dates).date()
            if date > max_db_date:
                print(date)
                filepath = r'C:\Users\Administrator\AdQvestDir\NIIF_PPT\GENERAL_INSURANCE.xlsx'
                # filepath = '/Users/pushkar/Adqvest/files_collection/GENERAL_INSURANCE.xlsx'
                latest_month = date.strftime('%B')
                cc = 'pushkar.patil@adqvest.com'
                # to = 'pushkar.patil@adqvest.com'
                to = 's.akhuli@adqvest.com'

                msg = MIMEMultipart()
                msg['From'] = "adqvest.insights@adqvest.com"

                msg['To'] = to
                msg['Cc'] = cc
                recipients = to.split(",")

                msg['Subject'] = "GENERAL INSURANCE MONTHLY TABLE"
                body =""" Hello, here is the report for the month of """ + latest_month +"""."""

                msg.attach(MIMEText(body, 'plain'))
                sel_time = (datetime.datetime.now(india_time) - datetime.timedelta(1)).strftime("%Y-%m-%d %H:%M:%S")
                sel_end_time = (datetime.datetime.now(india_time) - datetime.timedelta(hours = 2)).strftime("%Y-%m-%d %H:%M:%S")
                time_now = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))

                file_data = open(filepath, 'rb')
                file_data = file_data.read()

                part = MIMEApplication(file_data, _subtype='xlsx')
                part.add_header('Content-Disposition', 'attachment', filename = 'GENERAL_INSURANCE.xlsx')
                msg.attach(part)
                smtp = smtplib.SMTP('smtp.gmail.com:587')
                smtp.starttls()
                smtp.login("adqvest.insights@adqvest.com", "adq@#$234newpass")
                smtp.sendmail(msg['From'],recipients,msg.as_string())
                smtp.quit()
                print(f'Email shared for {str(date)}')
                connection.execute(f'Update GENERIC_DATA_ALERT_AND_EMAIL_SENDER SET Description = "Done" WHERE Relevant_Date = "{str(date)})" and Description is Null')
                nxt_dt = date + relativedelta.relativedelta(months=1, days=-1, day=1)
                connection.execute(f'INSERT INTO GENERIC_DATA_ALERT_AND_EMAIL_SENDER VALUES ("GENERAL INSURANCE MONTHLY TABLE", null, null, "X004_GENERAL_INSURANCE_TABLE_FORMAT_EMAIL_TRIGGERING", "Monthly", "XLSX File", "{str(nxt_dt)}", "{str(today)}")')
            else:
                print(f'{str(max_db_date)} data not arrived')
        else:
            print('No file sent')
        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)
if(__name__=='__main__'):
    run_program(run_by='manual')



