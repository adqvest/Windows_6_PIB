# -*- coding: utf-8 -*-
"""
Created on Tue Jan  4 11:49:20 2022

@author: Abhishek Shankar
"""





import sqlalchemy
import os
import pandas as pd
import datetime as datetime
from pytz import timezone
import re
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfVectorizer

import nltk
from nltk.corpus import stopwords
import numpy as np
import calendar

from subprocess import run
from subprocess import Popen
from nltk import ngrams
import warnings
warnings.filterwarnings('ignore')
#sys.path.insert(0, '/home/ubuntu/AdQvestDir/Adqvest_Function')
#import adqvest_db
#import JobLogNew as log
import sqlalchemy
import pandas as pd
from pandas.io import sql
import os
from dateutil import parser
import requests
from bs4 import BeautifulSoup
import re
import datetime as datetime
from pytz import timezone
import sys
import warnings
import json
import time
warnings.filterwarnings('ignore')
#sys.path.insert(0, '/home/ubuntu/AdQvestDir/Adqvest_Function')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
#sys.path.insert(0, r'C:\Adqvest')
import numpy as np
import adqvest_db
import JobLogNew as log
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
import os
import datetime
import time
import re
from clickhouse_driver import Client
nltk.download('stopwords')
client = Client('ec2-3-109-104-45.ap-south-1.compute.amazonaws.com',
                user='default',
                password='@Dqu&TP@ssw0rd',
                database='AdqvestDB',
               port=9000)

import random
import adqvest_db

engine = adqvest_db.db_conn()
#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days


#DB Connection
#properties = pd.read_csv(r'C:\Adqvest\Amazon_AdQvest_properties.txt',delim_whitespace=True)
properties = pd.read_csv(r'C:\Users\Administrator\AdQvestDir\Amazon_AdQvest_properties.txt',delim_whitespace=True)
#properties = pd.read_csv('AdQvest_properties.txt',delim_whitespace=True)

host    = list(properties.loc[properties['Env'] == 'Host'].iloc[:,1])[0]
port    = list(properties.loc[properties['Env'] == 'port'].iloc[:,1])[0]
db_name = list(properties.loc[properties['Env'] == 'DBname'].iloc[:,1])[0]

con_string = 'mysql+pymysql://' + host + ':' + port + '/' + db_name + '?charset=utf8'
engine1     = sqlalchemy.create_engine(con_string,encoding='utf-8')

def cleaner(text):
    text = text.lower()
    text = text.replace(',',' ')
    text = text.replace('|',' ')
    text = re.sub(r'#',' ',text) # Removing hash(#) symbol .
    text = re.sub(r'@',' ',text) # replace @ with space
    text = re.sub(r'\$',' ',text) # replace $ with space
    text = re.sub(r'%',' ',text) # replace % with space
    text = re.sub(r'\^',' ',text) # replace ^ with space
    text = re.sub(r'&',' ',text) # replace & with space
    text = re.sub(r'\d+ml', ' ', text)
    text = re.sub(r'\((.*?)\)', ' ', text)
    text = re.sub(r'[-]', ' ', text)
    text = re.sub(r'  +', ' ', text).strip()
    text = re.sub(r'\d+ ml', ' ', text)
    text = re.sub(r'\d+', ' ', text)
    text = re.sub(r"'\w+", " ", text)
    text = re.sub(r'  +', ' ', text).strip()
    return text

def bigram_gen(x,n):
    return [" ".join(list(x)) for x in ngrams(x.split(), n)]


def run_program(run_by='Adqvest_Bot', py_file_name=None):
    #os.chdir('/home/ubuntu/AdQvestDir')
    engine = adqvest_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'THURRO_WORD_CLOUD'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0
    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        from nltk.corpus import stopwords
        stop = stopwords.words('english')
        custom_stopwords = ["mix","herbalife","tablets","breakfast","break fast","sampann","break fast","khaman","break","grams","kilograms","milligrams","mg","fast","pack","powder","pouch","bag","strip","units","pcs","tablet","oil","oils","free","bottle", "gms"]

        list_of_stopwords = stopwords.words('english')
        list_of_stopwords = list_of_stopwords + custom_stopwords
        list_of_stopwords = list(set(list_of_stopwords))
        q = 'select count(*),Relevant_Month,Category,Sub_Category_1,Sub_Category_2,Sub_Category_3,Sub_Category_4 from AdqvestDB.AMAZON_BEAUTY_CPS_SCORES group by Relevant_Month,Category,Sub_Category_1,Sub_Category_2,Sub_Category_3,Sub_Category_4'
        df,cols = client.execute(q,with_column_types=True)
        df = pd.DataFrame(df, columns=[tuple[0] for tuple in cols])
        print(df)

        all_brand = pd.read_sql("select distinct(All_Brand) as All_Brand from AdqvestDB.AMAZON_BRANDS_ABHISHEK where Category='Beauty'", con=engine1)
        all_brand['All_Brand'] = all_brand['All_Brand'].apply(cleaner)


        all_catgs = df.copy()

        #Delete later
        #all_catgs = all_catgs[all_catgs['Sub_Category_2']=='Hair Oils']
        for k,row in all_catgs.iterrows():
        #    print(row)

            query = """Select * from AdqvestDB.AMAZON_BEAUTY_CPS_SCORES where
                    Relevant_Month = '"""+row['Relevant_Month']+"""' and
                    Category = '"""+row['Category']+"""'and
                    Sub_Category_1 = '"""+row['Sub_Category_1']+"""' and
                    Sub_Category_2 = '"""+row['Sub_Category_2']+"""' and
                    Sub_Category_3 = '"""+row['Sub_Category_3']+"""' and
                    Sub_Category_4 = '"""+row['Sub_Category_4']+"""'"""

            df,cols = client.execute(query,with_column_types=True)
            df = pd.DataFrame(df, columns=[tuple[0] for tuple in cols])
            #print(df)

            catgs = [word for line in list(row.iloc[2:7]) for word in line.split()]
            catgs = [ ' '+ x.lower() + ' ' for x in catgs]
            list_of_stopwords = [ ' '+ x.lower() + ' ' for x in list_of_stopwords]
            amazon = df.copy()
            rep_brand = [ ' '+ x.lower() + ' ' for x in list(all_brand.iloc[:,0])]+['laxm','loreal','lorl']+list_of_stopwords+catgs
            amazon['Cleaned_Name'] = amazon['Pdt'].apply(cleaner)
            amazon['Cleaned_Name'] = amazon['Cleaned_Name'].str.lower().apply(lambda x: ' '.join([word for word in x.split() if word not in (stop)]))
            amazon['Cleaned_Name'] = amazon['Cleaned_Name'].str.replace('hair fall', 'hairfall')
            amazon['Cleaned_Name'] = amazon['Cleaned_Name'].str.replace('aloe vera', 'aloevera')
            amazon['Cleaned_Name'] = " " + amazon['Cleaned_Name'] + " "
            amazon['Cleaned_Name'] = amazon['Cleaned_Name'].apply(lambda x:re.sub(r'  +', ' ', x))
            amazon['Cleaned_Name'] = amazon['Cleaned_Name'].str.lower().replace(rep_brand ,' ',regex=True)
            amazon['Cleaned_Name'] = amazon.apply(lambda x: x['Cleaned_Name'].replace(x['Brand'].lower(),''), axis=1)
            #amazon['Cleaned_Name'] = amazon['Cleaned_Name'].replace(rep_brand ,' ',regex=True)
            #for b in list(all_brand.iloc[:,0]):
            #    amazon['Cleaned_Name'] = amazon['Cleaned_Name'].str.replace(' ' + b + ' ' ,'')

            amazon['Cleaned_Name'] = amazon['Cleaned_Name'].apply(lambda x:re.sub(r'  +', ' ', x).strip())

            amazon['Bi_Grams'] = amazon['Cleaned_Name'].apply(lambda x : bigram_gen(x,2))

            reqd = amazon[['Bi_Grams','Value','Relevant_Date','Cleaned_Name','Pdt']]
            reqd['Bi_Grams'] = reqd['Bi_Grams'].apply(lambda x : [x])
            #reqd['Words'] = reqd.explode(['Bi_Grams'],axis=1)
            reqd = (reqd['Bi_Grams'].apply(lambda x: pd.Series(x[0]))
                     .stack()
                     .reset_index(level=1, drop=True)
                     .to_frame('Bi_Grams')
                     .join(reqd[['Value','Relevant_Date','Cleaned_Name','Pdt']], how='left'))

            reqd['Category'] = row['Category']
            reqd['Sub_Category_1'] = row['Sub_Category_1']
            reqd['Sub_Category_2'] = row['Sub_Category_2']
            reqd['Sub_Category_3'] = row['Sub_Category_3']
            reqd['Sub_Category_4'] = row['Sub_Category_4']
            combination = list(row)[-5:]
            combination = [x for x in combination if x!=""]
            combination = "|".join(combination)
            reqd['Combination'] = combination
            #reqd['Relevant_Date'] = today.date()
            reqd['Runtime'] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
            client.execute("INSERT INTO AdqvestDB.AMAZON_BEAUTY_WORD_CLOUD VALUES", reqd.values.tolist())
            print("DONE")
        log.job_end_log(table_name,job_start_time, no_of_ping)


    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
