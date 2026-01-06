#!/usr/bin/env python
# coding: utf-8

# In[1]:
#...
import re
import os
from pytz import timezone
import datetime as datetime
import pickle
import os.path
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from apiclient.http import MediaFileUpload
import calendar
import pandas as pd

import sys
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')

import adqvest_db
import JobLogNew as log


# In[3]:



def callback(request_id, response, exception):
    if exception:
        # Handle error
        print (exception)
    else:
        print ("Permission Id: %s" % response.get('id'))

def drive(name,files_list,parent_folder_id):
    file_addr ="C:/Users/Administrator/AdQvestDir/NIIF_PPT/"

    with open('C:/Users/Administrator/AdQvestDir/Adqvest_Function/token.pickle', 'rb') as token:
        creds = pickle.load(token)

    service = build('drive', 'v3', credentials=creds)

    file_metadata = {
        'name': name,
        'parents':[parent_folder_id],
        'mimeType': 'application/vnd.google-apps.folder'
    }
    file = service.files().create(body=file_metadata,
                                        fields='id').execute()
    print(file_metadata)

    all_files = service.files().list(q="name = '" + name +"'",
                pageSize=100, fields="nextPageToken, files(id, name)").execute()
    folder_id = all_files['files'][0]['id']

    all_files = service.files().list(q="'" + folder_id + "' in parents",
            pageSize=500, fields="nextPageToken, files(id, name)").execute()

    all_files = all_files['files']
    for file in all_files:
        service.files().delete(fileId=file['id']).execute()


    all_files = files_list

    for sys_file in all_files:


        file_metadata = {
            'name': sys_file,
            'parents': [folder_id],
            'viewersCanCopyContent':False
        }
        media = MediaFileUpload(file_addr + "/" + sys_file,
                                mimetype='application/vnd.openxmlformats-officedocument.presentationml.presentation',
                                resumable=True)
        print(media)
        print(file_addr + "/" + sys_file)
        file = service.files().create(body=file_metadata,
                                            media_body=media,
                                            fields='id').execute()

        print( 'File ID: %s' % file.get('id'))



# In[6]:

os.chdir('C:/Users/Administrator/AdQvestDir')
engine = adqvest_db.db_conn()
#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days

## job log details
job_start_time = datetime.datetime.now(india_time)
table_name     = 'REPORT_SHARING'

scheduler = ''
no_of_ping = 0





def run_program(run_by='Adqvest_Bot', py_file_name=None):
    global no_of_ping
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        with open('C:/Users/Administrator/AdQvestDir/Adqvest_Function/token.pickle', 'rb') as token:
            creds = pickle.load(token)

        service = build('drive', 'v3', credentials=creds)
        parent_folder = service.files().list(q="name ='NIIF PPT'",
                    pageSize=60, fields="nextPageToken, files(id, name)").execute()

        print(parent_folder)
        parent_folder_id = parent_folder['files'][0]['id']
        
        all_folders = service.files().list(q="'" + parent_folder_id + "' in parents",
        pageSize=1, fields="nextPageToken, files(id, name)").execute()

        for file in all_folders["files"]:
            service.files().delete(fileId=file['id']).execute()


        drive(name ="NIIF PPT",files_list = ["NIIF_PPT.pptx"],parent_folder_id = parent_folder_id)
       
        email_addrs = ['karthik@adqvest.com', 'mrinalini@adqvest.com', 's.akhuli@adqvest.com','kamalinee.mk@adqvest.com', 'pushkar.patil@adqvest.com','abdulmuizz.khalak@adqvest.com','tharanitharan.ramajayam@adqvest.com','induja.p@adqvest.com','s.heteshkumar@adqvest.com','santonu.debnath@adqvest.com','rahul.sekhar@adqvest.com','judejoe.vivegan@adqvest.com','nidhi.goel@adqvest.com']
        # email_addrs = ['mrinalini@adqvest.com', 's.akhuli@adqvest.com','santonu.debnath@adqvest.com']

        if((calendar.weekday(today.year, today.month, today.day) == 0) & ((datetime.datetime.now(india_time).hour >= 8) & (datetime.datetime.now(india_time).hour < 12)) ):

            # email_addrs = pd.read_excel("/home/ubuntu/AdQvestDir/codes/INPUT_FILES/CLIENTS_EMAIL_ADDRESS.xlsx")['Clients_Email_Address'].tolist()
            email_addrs = ['karthik@adqvest.com', 'mrinalini@adqvest.com', 's.akhuli@adqvest.com', 'kamalinee.mk@adqvest.com','pushkar.patil@adqvest.com','abdulmuizz.khalak@adqvest.com','tharanitharan.ramajayam@adqvest.com','induja.p@adqvest.com','s.heteshkumar@adqvest.com','santonu.debnath@adqvest.com','rahul.sekhar@adqvest.com','judejoe.vivegan@adqvest.com','nidhi.goel@adqvest.com']
            # email_addrs = ['mrinalini@adqvest.com', 's.akhuli@adqvest.com','santonu.debnath@adqvest.com']


        for email in email_addrs:
            batch = service.new_batch_http_request(callback=callback)
            if((email == 'karthik@adqvest.com')|(email == 'mrinalini@adqvest.com')):
                user_permission = {
                    'type': 'user',
                    'role': 'writer',
                    'emailAddress': email
                }
            else:
                user_permission = {
                    'type': 'user',
                    'role': 'reader',
                    'emailAddress': email
                }
            batch.add(service.permissions().create(
                    fileId=parent_folder_id,
                    body=user_permission,
                    fields='id',
            ))


            batch.execute()
            print("All Done")
        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
