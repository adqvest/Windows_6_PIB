import sqlalchemy
import pandas as pd
from pandas.io import sql
import os
import math
import calendar
import re
import ast
import datetime as datetime
import dateutil.relativedelta
from pytz import timezone
import numpy as np
import sys
import time
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import cleancompanies
import JobLogNew as log
from adqvest_robotstxt import Robots
robot = Robots(__file__)
import warnings
warnings.filterwarnings('ignore')
import ClickHouse_db

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    client = ClickHouse_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'PHARMA_CLEAN'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
        def cleaner(text):          
            text = text.lower()
            text = text.strip()
            text = text.replace('m b agribiz private limited plotno 81-82 inudstrial area sector 3','m b agribiz')
            text = text.replace(',',' ')
            text = text.replace('m/s',' ').strip()
            text=re.sub(r'\(.*?\)',' ',text).strip()
            text = text.replace("int'l",'international').strip()
            
            text = text.replace('(uni',' ').strip()
            text = text.replace('unit-iii',' ').strip()
            text = text.replace('unit-ii',' ').strip()
            text = text.replace('unit-i',' ').strip()   
            text = text.replace('unti ii',' ').strip()
            
            text = text.replace('|',' ').replace('*','').replace(']','').replace('[','').replace("`",' ')
            text = re.sub(r'#',' ',text) # Removing hash(#) symbol .
            text = re.sub(r'@',' ',text) # replace @ with space
            text = re.sub(r'\$',' ',text) # replace $ with space
            text = re.sub(r'%',' ',text) # replace % with space
            text = re.sub(r'\:',' ',text)
            text = re.sub(r'\>',' ',text)
            text = re.sub(r'\<',' ',text)
            text = re.sub(r'\^',' ',text)
            #text = re.sub(r'&',' ',text) # replace & with space
            text = re.sub(r'\d+ml', ' ', text)
            text = re.sub(r'\((.*?)\)', ' ', text)
            text = re.sub(r'[-]', ' ', text)
            text = re.sub(r'  +', ' ', text).strip()
            text = re.sub(r'\d+ ml', ' ', text)
            text = re.sub(r'\d+', ' ', text)
            text = re.sub(r"'\w+", " ", text)
            text = re.sub(r'  +', ' ', text).strip()
            text = text.replace('-','').replace('(','').replace(')',' ').replace('\n',' ').replace('.',' ')
            text = text.replace("'",' ').replace(';','').replace('"','')
            text = text.replace('/','').replace('+','')
            text = re.sub(r'[?|$|.|!]',r'',text)  
            text = text.replace(' the',' ').replace('the ',' ').replace('&',' and ')
            text = text.replace(' indian',' ').replace(' india',' ')
            text = text.replace('exportts','exports')   
            text = text.replace('privated limited',' ltd').replace('private limteed',' ltd').replace('private limited',' ltd').replace('private limite',' ltd')
            text = text.replace(' p ltd',' ltd').replace('ltd.(uni',' ') 
           
            text = text.replace(' p limited',' ltd').replace('limited',' ltd').replace(' limited',' ltd').replace('limite',' ltd').replace('limit',' ltd').replace('ltdd',' ltd').replace(' lt',' ltd').replace(' ltdd',' ltd').replace(' ldt',' ltd').replace(' ld',' ltd')
            text = text.replace('bnak','bank')
            text = text.replace('private',' pvt').replace('private',' pvt').replace(' privat',' pvt').replace(' priva',' pvt').replace('prvt',' pvt')      
            text = text.replace('pvt li',' pvt')
            text = text.replace('p ltd',' pvt')
            #text = text.replace('company',' co')
            text = text.replace('corporation',' corp').replace('corpn',' corp')
            text = text.replace('cooperative',' coop')

            text = text.replace(' pvt',' ')
            text = text.replace('pvt',' ')
            text = text.replace(' ltd',' ')
            text = text.replace('p ltd',' ')
            text = text.replace(' inc',' ')
            text = text.replace('llp',' ')       
            text = text.replace('ldt',' ')
            text = text.replace('pvt ',' ')
            text = text.replace(' pv',' ')
            text = text.replace(' cto',' ')
            text = text.replace('units i & iii',' ').strip()
                                                                                           
            text = text.replace(' xii',' ').replace(' xi',' ').replace(' x',' ').replace(' ix',' ').replace(' viii',' ').replace(' vii',' ').replace(' vi',' ')
            text = text.replace(' iv',' ').replace(' iii',' ').replace(' ii',' ') 
            text = text.replace('unit i',' ').strip()
            text = text.replace('unit',' ').strip()
            text = text.replace('  ',' ')
            text = re.sub(r'  +',' ', text).strip()
            for i in range(len(text)):
                if text.endswith(' l'):
                    text = text.replace(text[-1], 'ltd')
            text = text.replace(' ltd',' ')        
            text = text.replace('s collection','2 s collection').strip()
            text = text.replace('exxports','exports').strip()
            text = text.replace('reedy','reddy').strip()
            text = text.replace('reedy','reddy').strip()
            text = text.replace('i t c ','itc').strip()
          
            text = re.sub(r'  +',' ', text).strip()
            
            return text
        os.chdir('C:/Users/Administrator/AdQvestDir')
        stopwords = pd.read_csv('stop_words.csv')
        stopwords = stopwords['Stopwords'].to_list()

        def remove_stopwords(x):
            temp=[i for i in x if i not in stopwords]      
            res=' '.join(temp)
            return res
        
        query = "select Company, Company_Clean from USFDA_PHARMA_MAPPING_ONE_TIME"
        raw_df = pd.read_sql(query, con=engine)
        raw_df

        cleaning = dict(zip(raw_df.Company, raw_df.Company_Clean))
        cleaning

        columns = client.execute("desc US_FDA_WEEKLY_MONTHLY_DATA_Temp")
        columns = pd.DataFrame(columns)[0]
        columns = columns.to_list()
        columns


        company = client.execute("select DIstinct Manufacturer_Legal_Name from US_FDA_WEEKLY_MONTHLY_DATA_Temp where Manufacturer_Legal_Name != ''")
        company = pd.DataFrame(company)
        company.shape

        # company_temp = client.execute("select DIstinct Manufacturer_Legal_Name from US_FDA_WEEKLY_MONTHLY_DATA_Temp_Rahul")
        # company_temp = pd.DataFrame(company_temp)
        # company_temp.shape


        # new_company = []
        # for x in company[0]:
        #     if x not in company_temp[0].to_list():
        #         new_company.append(x)
        issued_company = []
        for i in company[0]:
            i = i.replace("'", "''")
            try:
                df1 = client.execute(f"select *  from US_FDA_WEEKLY_MONTHLY_DATA_Temp where Manufacturer_Legal_Name = '{i}'")
                df1 = pd.DataFrame(df1)
                df1.columns = columns
                df1['Manufacturer_Legal_Name_Clean'] = [x.lower() for x in df1['Manufacturer_Legal_Name']]
                df1['Manufacturer_Legal_Name_Clean'] = [remove_stopwords(x.split()) for x in df1['Manufacturer_Legal_Name_Clean']]
                df1['Manufacturer_Legal_Name_Clean'] = [cleaner(x) for x in df1['Manufacturer_Legal_Name_Clean']]
                df1['Manufacturer_Legal_Name_Clean'] = [x.replace(" and "," & ").strip() for x in df1['Manufacturer_Legal_Name_Clean']]
                df1['Manufacturer_Legal_Name_Clean'] = [cleaning[x] for x in df1['Manufacturer_Legal_Name_Clean']]
                df1['Manufacturer_Legal_Name_Clean'] = [x.upper() for x in df1['Manufacturer_Legal_Name_Clean']]
                client.execute("INSERT INTO US_FDA_WEEKLY_MONTHLY_DATA_Temp_Rahul VALUES", df1.values.tolist(), types_check=True)
            except:
                print(i)
                issued_company.append(i)

        log.job_end_log(table_name,job_start_time, no_of_ping)

    except:
        try:
            connection.close()
        except:
            pass
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        #error_msg = str(sys.exc_info()[1])
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)

        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')