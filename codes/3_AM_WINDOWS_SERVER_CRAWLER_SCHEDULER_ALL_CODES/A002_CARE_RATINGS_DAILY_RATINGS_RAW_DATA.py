#!/usr/bin/env python
# coding: utf-8

# In[4]:


import sqlalchemy
import pandas as pd
from pandas.io import sql

import xml.etree.ElementTree as ET
import os
import requests
import json
from bs4 import BeautifulSoup
import PyPDF2
import camelot
import re
import ast
import datetime as datetime
from pytz import timezone
import requests, zipfile, io
import csv
import numpy as np
import time
import zipfile
import sys
from sqlalchemy import text
# sys.path.insert(0, '/home/ubuntu/AdQvestDir/Adqvest_Function')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import warnings
warnings.filterwarnings('ignore')
import adqvest_db
import JobLogNew as log
from adqvest_robotstxt import Robots
robot = Robots(__file__)

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    # os.chdir('/home/ubuntu/AdQvestDir')
    os.chdir(r'C:\Users\Administrator\AdQvestDir\Junk_Care')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'CARE_RATINGS_DAILY_RATINGS_RAW_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        def link_com(x):
            x = x.split('/')[-1]
            nos = x.count('-')
            x = ''.join(re.split('-',x,nos-2)[:-1])
            x = x.replace('%20',' ')
            return x


        def clean(df):

            df = df[df.iloc[:,1] != 'F']
            df = df[df.iloc[:,1] != 'Instruments']
            df = df[df.iloc[:,1] != 'Instrument']
            df = df[df.iloc[:,1] != 'Instruments/Facilities']
            df = df[df.iloc[:,1] != 'Instrument/Facilities']
            df = df[df.iloc[:,1] != 'Instruments/facilities']
            df = df[df.iloc[:,1] != 'Issuer Rating']
            df = df[df.iloc[:,1] != '@Instruments']
            df = df[df.iloc[:,1] != '@Facilities']
            df = df[df.iloc[:,1] != 'acilities']
            df = df[~df.iloc[:,1].str.startswith('@Total')]
            df = df[~df.iloc[:,1].str.startswith('Tot')]
            df = df[~df.iloc[:,1].str.startswith('TOT')]
            df = df[~df.iloc[:,1].str.startswith('Sub-tot')]
            df = df[~df.iloc[:,1].str.startswith('Sub Tot')]
            df = df[~df.iloc[:,1].str.startswith('Facili')]
            df = df[~df.iloc[:,1].str.startswith('FACILI')]
            df = df[~df.iloc[:,1].str.startswith('Type of Rating')]
            df = df[~df.iloc[:,1].str.startswith('Sr. No.')]
            df = df[~df.iloc[:,1].str.startswith('Scheme Name')]

            return(df)

        #####
        def price(x):
            x =  re.findall(r'-?\d+\.?\d*',str(x).replace(',',''))
            if(len(x)==0):
                return(np.nan)
            else:
                return(x[0])

        def upload_data(CARE_RATINGS_DATA):
            column_list = CARE_RATINGS_DATA.columns.tolist()
            for column in column_list:
                #print(column)
                try:
                    CARE_RATINGS_DATA[column] = CARE_RATINGS_DATA[column].apply(lambda x: x.lstrip())
                    CARE_RATINGS_DATA[column] = CARE_RATINGS_DATA[column].apply(lambda x: x.rstrip())
                except:
                    continue
            #print('y')
            CARE_RATINGS_DATA['Amount_Description'] = np.where((CARE_RATINGS_DATA['Amount_Description']=='-'),'',CARE_RATINGS_DATA['Amount_Description'])
            CARE_RATINGS_DATA['Ratings'] = np.where((CARE_RATINGS_DATA['Ratings']=='-'),'',CARE_RATINGS_DATA['Ratings'])
            CARE_RATINGS_DATA['Remarks'] = np.where((CARE_RATINGS_DATA['Remarks']=='-'),'',CARE_RATINGS_DATA['Remarks'])


            #CARE_RATINGS_DATA = NULL_TO_BLANK.null_to_blank(table_name = "CARE_RATINGS_DAILY_RATINGS_RAW_DATA",df = CARE_RATINGS_DATA)
            CARE_RATINGS_DATA['Amount_Description'] = np.where((CARE_RATINGS_DATA['Amount_Description']=='nan'),'',CARE_RATINGS_DATA['Amount_Description'])
            CARE_RATINGS_DATA['Ratings'] = np.where((CARE_RATINGS_DATA['Ratings']=='nan'),'',CARE_RATINGS_DATA['Ratings'])
            CARE_RATINGS_DATA['Remarks'] = np.where((CARE_RATINGS_DATA['Remarks']=='nan'),'',CARE_RATINGS_DATA['Remarks'])
            CARE_RATINGS_DATA['Facilities'] = np.where((CARE_RATINGS_DATA['Facilities']=='nan'),'',CARE_RATINGS_DATA['Facilities'])

            CARE_RATINGS_DATA.to_sql(name='CARE_RATINGS_DAILY_RATINGS_RAW_DATA',con=engine,if_exists='append',index=False)

        count = 0

        ###################### Functions required for CARE RATINGS DATA

        # os.chdir('/home/ubuntu/care_ratings_data')
        os.chdir(r'C:\Users\Administrator\AdQvestDir\Junk_Care')

        links_df = pd.read_sql("select * from CARE_RATINGS_DAILY_DATA_CORPUS where Status_Ratings is null and Download_Status is not null ",engine)
        if(links_df.empty):
            print("No new data")
        else:
            links_df = links_df.sort_values(by = "Relevant_Date")
            print(len(links_df))

            for _,i in links_df.iterrows():

                print(i["Generated_File_Name"])
                try:
                    object = PyPDF2.PdfFileReader(i["Generated_File_Name"])
                    print(object)
                    PageObj = object.getPage(0)
                    Text = PageObj.extractText()
                    ResSearch = re.search('SEBI/HO/MIRSD/MIRSD4/CIR/2017/71', Text)
                except:
                    ResSearch = False

                #os.remove(file)
                if(ResSearch):
                    #os.remove(file)
                    df = pd.DataFrame(data=[[np.nan, np.nan, np.nan, np.nan, np.nan]] ,columns=['Facilities','Amount_Description', 'Amount_Cr', 'Ratings', 'Remarks'])
                    # df['Company'] = re.search('PR/(.+?)-', i["Links"]).group(1).replace("%20",' ')
                    print("Company_Name : ")
                    print(i["Company_Name"])
                    df['Company'] = i["Company_Name"]
                    df['Relevant_Date'] = i["Relevant_Date"]
                    df['File_Link'] = i["File_Link"]
                    #upload_data(df)
                    if "'" in i["Company_Name"]:
                        i["Company_Name"] = i["Company_Name"].translate ({ord(z): "\\'" for z in "'"})
                    check_query = """
                        SELECT * FROM CARE_RATINGS_DAILY_DATA_CORPUS
                        WHERE Generated_File_Name = :file_name
                        AND Relevant_Date = :rel_date
                        AND File_ID = :file_id
                    """
                    
                    result = connection.execute(
                        text(check_query),
                        {
                            "file_name": i["Generated_File_Name"],
                            "rel_date": i["Relevant_Date"].strftime('%Y-%m-%d'),
                            "file_id": i["File_ID"]
                        }
                    ).fetchall()
                    
                    print(f"Rows matched: {len(result)}")

                    connection = engine.connect()
                    update_query = """
                        UPDATE CARE_RATINGS_DAILY_DATA_CORPUS 
                        SET Is_Ratings_Table_Present = 'Yes',
                            Status_Ratings = 'Succeeded'
                        WHERE Generated_File_Name = :file_name
                        AND DATE(Relevant_Date) = :rel_date
                        AND File_ID = :file_id
                    """
                    
                    with connection.begin():  # Start a transaction
                        result = connection.execute(
                            text(update_query),
                            {
                                "file_name": i["Generated_File_Name"],
                                "rel_date": i["Relevant_Date"].strftime('%Y-%m-%d'),
                                "file_id": i["File_ID"]
                            }
                        )
                        print(f"Rows affected: {result.rowcount}")
                        # Commit is done automatically at the end of the block
                        print("Commit successful")
                    try:
                        os.remove(i["Generated_File_Name"])
                        print(f"Deleted file: {i['Generated_File_Name']}")
                    except Exception as e:
                        print(f"Could not delete file {i['Generated_File_Name']}: {e}")

                else:
                    try:
                        print(i["Generated_File_Name"])
                        condition_1 = False
                        try:
                            temp = camelot.read_pdf(i["Generated_File_Name"], line_scale=35)                            
                        except :
                            if "'" in i["Company_Name"]:
                                i["Company_Name"] = i["Company_Name"].translate ({ord(z): "\\'" for z in "'"})
                            print("Not Found",i["Generated_File_Name"])
                            query = "update CARE_RATINGS_DAILY_DATA_CORPUS set Is_Ratings_Table_Present = 'No',Status_Ratings = 'Failed',Comments_Ratings = 'File Not Accessible' where Relevant_Date='" + i['Relevant_Date'].strftime('%Y-%m-%d') + "' and Company_Name = '"+i["Company_Name"]+"'"
                            print(query)
                            connection.execute(query)
                            connection.execute('commit')

                            # raise Exception("Failed")
                            continue

                        try:
                            try:
                                del df
                            except:
                                pass
                            for table in temp:
                                try:
                                    if(table.df[0].str.lower().str.contains('instrument').any()|table.df[1].str.lower().str.contains('instrument').any()| table.df[0].str.lower().str.contains('facilities').any() | table.df[1].str.lower().str.contains('facilities').any()):
                                        df= table.df
                                        break
                                except:
                                    continue

                            df = df.apply(lambda x: x.str.replace('\n',''))
                            print(df)
                        except:
                            print('Got into Second except')
                            if "'" in i["Company_Name"]:
                                i["Company_Name"] = i["Company_Name"].translate ({ord(z): "\\'" for z in "'"})
                            query = "update CARE_RATINGS_DAILY_DATA_CORPUS set Is_Ratings_Table_Present = 'No',Status_Ratings = 'Failed' where Relevant_Date='" + i['Relevant_Date'].strftime('%Y-%m-%d') + "' and Company_Name = '"+i["Company_Name"]+"'"
                            print(query)
                            connection.execute(query)
                            connection.execute('commit')
                            continue

                        try:
                            put_col = {}
                            new_df = df.iloc[:2]
                            print(new_df)
                            for col in new_df.columns:
                                if(new_df[col].str.lower().str.contains('instrument').any() | new_df[col].str.lower().str.contains('facilities').any() | new_df[col].str.lower().str.contains('scheme name').any() | new_df[col].str.lower().str.contains('type of rating').any() | new_df[col].str.lower().str.contains('particular').any() | (new_df[col].str.lower()=='obligation').any() | (new_df[col].str.lower()=='obligation').any()):
                                    put_col[col] = 'Facilities'
                                if(new_df[col].str.lower().str.contains('amount').any() | new_df[col].str.lower().str.contains('principal outstanding').any() | new_df[col].str.lower().str.contains('current pool outstanding').any() | new_df[col].str.lower().str.contains('current principal outstanding').any() | new_df[col].str.lower().str.contains('volume').any() | new_df[col].str.lower().str.contains('ptc principal outstanding').any() | new_df[col].str.lower().str.contains('outstanding principal').any()):
                                    put_col[col] = 'Amount_Description'
                                if(new_df[col].str.lower().str.contains('rating1').any() | new_df[col].str.lower().str.contains('ratings1').any() | (new_df[col].str.lower()=='rating').any() | (new_df[col].str.lower()=='ratings').any() | (new_df[col].str.lower()=='rating*').any() | new_df[col].str.lower().str.contains('credit opinion').any() | (new_df[col].str.lower()=='rating[1]').any() | (new_df[col].str.lower()=='rating [1]').any()):
                                    put_col[col] = 'Ratings'
                                if(new_df[col].str.lower().str.contains('rating action').any() | new_df[col].str.lower().str.contains('remarks').any() | new_df[col].str.lower().str.contains('remark').any() | new_df[col].str.lower().str.contains('ratings action').any()):
                                    put_col[col] = 'Remarks'
                                if(new_df[col].str.lower().str.contains('trust').any() | new_df[col].str.lower().str.contains('transaction name').any()):
                                    put_col[col] = 'Company'
                                    condition_1 = True

                            if(list(put_col.values()).count('Amount_Description')>1):
                                index = [key for key, value in put_col.items() if(value=='Amount_Description')]
                                for j in index:
                                    del put_col[j]
                                for j in index:
                                    if(new_df[j].str.lower().str.contains('current').any() ):
                                        put_col[j]='Amount_Description'
                                        break
                                    elif(new_df[j].str.lower().str.contains('o/s').any()):
                                        put_col[j]='Amount_Description'
                                        break

                            df = df[list(put_col.keys())]
                            print(df)
                            for col in df.columns:
                                df[col] = np.where(df[col]=='', np.nan, df[col])

                            df.dropna(axis=0, inplace=True, how='all')
                            df = df.rename(columns=put_col)

                            df.reset_index(drop=True, inplace=True)
                            df.drop([0], axis=0, inplace=True)

                            for col in set(['Company','Amount_Description', 'Facilities', 'Ratings', 'Remarks']) - set(df.columns):
                                df[col] = np.nan

                            df['Amount_Cr'] = df['Amount_Description'].map(price)
                            df = df[['Company','Facilities','Amount_Description', 'Amount_Cr', 'Ratings', 'Remarks']]
                            try:
                                df = clean(df)
                            except:
                                pass

                            df['Relevant_Date'] = i["Relevant_Date"]
                            df['File_Link'] = i["File_Link"]
                            if(condition_1):
                                pass
                            else:
                                df['Company'] = i['Company_Name']
                            df['Remarks'] = df['Remarks'].ffill()
                            df["Amount_Cr"] = df["Amount_Cr"].astype(float)
                            df['Runtime'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
                            df.dropna(inplace = True)
                            if df.shape[0] != 0:
                                print('one uploaded')
                                upload_data(df)
                                print('UPLOAD Done')
                                if "'" in i["Company_Name"]:
                                    i["Company_Name"] = i["Company_Name"].translate ({ord(z): "\\'" for z in "'"})
    
                                check_query = """
                                    SELECT * FROM CARE_RATINGS_DAILY_DATA_CORPUS
                                    WHERE Generated_File_Name = :file_name
                                    AND Relevant_Date = :rel_date
                                    AND File_ID = :file_id
                                """
                                
                                result = connection.execute(
                                    text(check_query),
                                    {
                                        "file_name": i["Generated_File_Name"],
                                        "rel_date": i["Relevant_Date"].strftime('%Y-%m-%d'),
                                        "file_id": i["File_ID"]
                                    }
                                ).fetchall()
                                
                                print(f"Rows matched: {len(result)}")

                                connection = engine.connect()
                                update_query = """
                                    UPDATE CARE_RATINGS_DAILY_DATA_CORPUS 
                                    SET Is_Ratings_Table_Present = 'Yes',
                                        Status_Ratings = 'Succeeded'
                                    WHERE Generated_File_Name = :file_name
                                    AND DATE(Relevant_Date) = :rel_date
                                    AND File_ID = :file_id
                                """
                                
                                with connection.begin():  # Start a transaction
                                    result = connection.execute(
                                        text(update_query),
                                        {
                                            "file_name": i["Generated_File_Name"],
                                            "rel_date": i["Relevant_Date"].strftime('%Y-%m-%d'),
                                            "file_id": i["File_ID"]
                                        }
                                    )
                                    print(f"Rows affected: {result.rowcount}")
                                    # Commit is done automatically at the end of the block
                                    print("Commit successful")
    
                                print("Uploaded",i["File_Link"])
                                try:
                                    os.remove(i["Generated_File_Name"])
                                    print(f"Deleted file: {i['Generated_File_Name']}")
                                except Exception as e:
                                    print(f"Could not delete file {i['Generated_File_Name']}: {e}")
                            else:
                                raise Exception('No Data Available')
                        except:
                            print('Got into last except')
                            if "'" in i["Company_Name"]:
                                i["Company_Name"] = i["Company_Name"].translate ({ord(z): "\\'" for z in "'"})
                            query = "update CARE_RATINGS_DAILY_DATA_CORPUS set Is_Ratings_Table_Present='Yes',Status_Ratings = 'Failed' where Relevant_Date='" + i['Relevant_Date'].strftime('%Y-%m-%d') + "' and Company_Name = '"+i["Company_Name"]+"'"
                            print(query)
                            connection.execute(query)
                            connection.execute('commit')
                            continue

                        # os.remove(file)
                        # time.sleep(15) #uncomment

                    except:
                        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
                        print(error_msg)
                        if(error_msg == "Failed"):
                            continue
                        else:
                            raise Exception("Check code")

            print ("CARE_RATINGS_DATA_CAMELOT")

            print ("Run Time : ",datetime.datetime.now(india_time))

            print ("CARE_RATINGS_DATA_CAMELOT Loaded Sucessfully")


            #mail_sender.mail_call(engine)
        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        print('error')
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        #mail_sender.mail_call(engine)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
