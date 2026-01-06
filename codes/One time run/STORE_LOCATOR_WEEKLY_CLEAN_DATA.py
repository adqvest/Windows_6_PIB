import sqlalchemy
import pandas as pd
from pandas.io import sql
import calendar
import os
import requests
import json
from bs4 import BeautifulSoup
from time import sleep
import random
import re
import ast
import datetime as datetime
from pytz import timezone
import requests, zipfile, io
import csv
import numpy as np
import zipfile
import sys
import time
import tabula
from lxml import etree
from fuzzywuzzy import fuzz

import warnings
warnings.filterwarnings('ignore')

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log



def drop_duplicates(df):
    columns = df.columns.to_list()
    columns.remove('Runtime')
    df = df.drop_duplicates(subset=columns, keep='last')
    return df




def run_program(run_by='Adqvest_Bot', py_file_name=None):
    engine = adqvest_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'STORE_LOCATOR_WEEKLY_CLEAN_DATA_Temp1_Nidhi'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        def full_clean(text):
            """Clean the text by replacing unwanted characters ('#', '@', '$', '%', '^', '&', '.', '!', '*', ';', ':', "'", '"', '?', '-', '(', ')', '[', ']', '{', '}', '\', '~', '`', '/', '_', '+', '=', ',', '|', '<', '>')



               Args:
               text : pass a string

               Output:
               Return a clean string by removing all unwanted characters"""
            #https://stackoverflow.com/questions/4278313/python-how-to-cut-off-sequences-of-more-than-2-equal-characters-in-a-string?fbclid=IwAR397HRMq3BILIL5VIs3kFBPf_KF7Z0siH2FG3MubLYPkOXIdcbVAaGj_Is
            #https://stackoverflow.com/questions/4278313/python-how-to-cut-off-sequences-of-more-than-2-equal-characters-in-a-string?fbclid=IwAR397HRMq3BILIL5VIs3kFBPf_KF7Z0siH2FG3MubLYPkOXIdcbVAaGj_Is

            #text = text.lower() # convert text to lower case string or lower case text

            #text = re.sub(r'((www\.[S]+)|(https?://[\S]+))','URL',text)# replcae the website link to only "URL"

            text = re.sub(r'#',' ',text) # Removing hash(#) symbol .
            #text = re.sub(r'@',' ',text) # replace @ with space
            text = re.sub(r'\$',' ',text) # replace $ with space
            text = re.sub(r'%',' ',text) # replace % with space
            text = re.sub(r'\^',' ',text) # replace ^ with space
            text = re.sub(r'&',' ',text) # replace & with space

            text = re.sub(r'\.+', ' ', text) # Replacing one or more dot to single space

            text = re.sub(r'!+',' ',text) # Replacing single or more than one explanation symbol to single space
            text = re.sub(r'\*+',' ',text) # Replacing one or more star(*) symbol to single space
            text = re.sub(r';+',' ',text)   # Replacing  single semicolor(;)  or more semicolon(;) symbol to single space
            text = re.sub(r':+',' ',text)   # Replacing single colon(:) or more colon(:) sybol to single space
            text = re.sub(r"\'+",' ', text) # Replacing single quote or more single quote to single space

            text = re.sub(r'\"+',' ', text) # Replacing double quote or more double  quote to single space
            text = re.sub(r'\?+',' ', text) # Replacing single question mark or more question mark to single space
            text = re.sub(r'-+', '', text) # Replacing single hypen  symbol or more hypen symbol  to single space
            text = re.sub(r'[\(\)]+',' ', text) # Replacing left paranthesis or right paraenthesis to single space
            text = re.sub(r'[\[\]]+',' ', text) # replacing [ and ]
            text = re.sub(r'[\{\}]+',' ', text) #replace { and } with space
            text = re.sub(r'[\\]+',' ', text) # replace \ with space
            text = re.sub(r'[~`]+',' ', text) # replace ~ and ` with space
            text = re.sub(r'[/]+',' ', text) # replace / with space
            text = re.sub(r'[_+=,]+',' ', text) # replace _ + = and , with space
            text = re.sub(r'[|]+',' ', text) # replace | with space
            text = re.sub(r'[<>]+',' ', text) # replace < and > with space

            #text = ReplaceThreeOrMore(text) # call the function ReplaceThreeOrMore(text)
            text = re.sub(r'  +',' ',text).strip() # Replacing two or more white space to single white space
            text = text.strip()

            return text  # return the text

        def letters_digit_check(x):
            d = 0
            l = 0
            for c in x:
                if c.isdigit():
                    d = d+1
                elif c.isalpha():
                    l = l+1
                else:
                    pass
            if(l == 0):
                l = 1
            if(d/l > 2):
                return True
            else:
                return False

        def lambda_pincode(x):
            try:
                pin = re.findall(r'[0-9]{6}',x)[0]
                return pin
            except:
                try:
                    pin = re.findall(r'[0-9]{3} [0-9]{3}',x)[0]
                    pin = pin.replace(" ","")
                    return pin
                except:
                    return None


        def clean_address(df):
            global del_df


            del_list = []
            df = df[df["Address"].notnull()]
            df["Address"] = df["Address"].apply(lambda x: x.replace(",","").replace("-","").replace("/","").strip())
            df["Address"] = df["Address"].apply(lambda x: full_clean(x) )
            df["Pincode"] = df["Address"].apply(lambda x: lambda_pincode(x))
            df = df[df["Address"] != df["City"]]
            df["Fuzz_Score"] = np.nan
            df.reset_index(drop = True,inplace = True)
            for i,row in df.iterrows():
                if( (row["Address"].replace(" ","").isdigit()) | (letters_digit_check(row["Address"].replace(" ",""))) ):

                    del_list.append(i)
        #         else:
        #             pass

                if(("http" in row["Address"]) | ("https" in row["Address"]) | ("@" in row["Address"])):
                    #del_df = del_df.append(row)


                    del_list.append(i)
        #         else:
        #             pass
                #fuzz_score = fuzz.token_sort_ratio(row["Address"], row["City"])
                fuzz_score = fuzz.token_sort_ratio(row["Address"], row["State"])
                df.loc[i,"Fuzz_Score"] = fuzz_score
                #row["Fuzz_Score"] = fuzz_score

        #         if(fuzz_score > 80):
        #             print(fuzz_score)


        #             del_df = del_df.append(row)

        #             #df.drop([i],inplace = True)
        #             del_list.append(i)
        # #         else:
        # #             pass

            del_list = list(set(del_list))
            df.drop(del_list,inplace = True)
            df = df[df["Fuzz_Score"] < 45]
            df = df[df["Address"] != df["State"]]
            df.drop_duplicates(subset = ["Address","Relevant_Date"],inplace = True)
            df.rename(columns = {"Category_1":"Sub_Category_1"},inplace = True)
            df.rename(columns = {"Category_2":"Sub_Category_2"},inplace = True)
            df.rename(columns = {'Company_Name':'Brand'},inplace = True)
            df.drop(columns = ["District","Crawler","Comments","Fuzz_Score"],inplace = True)
            df["Comments"] = "Crawler Data"

            return df
        #del_df = pd.DataFrame()
        clean_list = ["Biba",
            "Lifestyle Stores",
            "Bajaj Allianz Life Insurance",
            "Hero",
            "Religare Health",
            "Shriram General Insurance",
            "Shriram Life Insurance",
            "Vespa",
            "Bharti AXA LI",
            "Future Generali General Insurance",
            "Hyundai",
            "Liberty",
            "Suzuki",
            "ICICI Securities Limited",
            "Magma HDI",
            "Reliance General Insurance",
            "Reliance Life",
            "IFFCO Tokio",
            "Metropolis Labs",
            "DHFL Pramerica Life Insurance",
            "House of Indya",
            "Jawa",
            "AND",
            "Global Desi",
            "Keventers",
            "KFC",
            "Sharekhan Limited",
            "Spencers",
            "Kotak Securities Limited",
            "Bajaj",
            "Bharti AXA GI",
            "ICICI Lombard",
            "Future Generali Life insurance"]


        for company in clean_list:
            print(company)
            # max_rel_date = pd.read_sql("select max(Relevant_Date) as Max from STORE_LOCATOR_WEEKLY_CLEAN_DATA_Temp1_Nidhi where Brand = '"+company+"'",engine)
            # max_rel_date = max_rel_date["Max"][0]
            # print(company,max_rel_date.strftime("%Y-%m-%d"))
            clean_df = pd.read_sql("select * from STORE_LOCATOR_WEEKLY_DATA where Brand = '"+company+"'",engine)
            clean_df = clean_address(clean_df)
            clean_df=drop_duplicates(clean_df)
            clean_df['Act_Runtime'] = datetime.datetime.now(india_time)
            clean_df.to_sql(name = "STORE_LOCATOR_WEEKLY_CLEAN_DATA_Temp1_Nidhi",index = False,con = engine,if_exists = 'append')
            print(clean_df.shape)
            print(clean_df['Relevant_Date'].unique())


        log.job_end_log(table_name,job_start_time,no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        error_msg = str(sys.exc_info()[1]) + " line " + str(exc_tb.tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
