from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import datetime as datetime
import re
import os
import requests
import warnings
warnings.filterwarnings('ignore')
import sqlalchemy

from pytz import timezone
import time
import sys
import json

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')

import adqvest_db
import ClickHouse_db
import JobLogNew as log
from adqvest_robotstxt import Robots
robot = Robots(__file__)

def clean_txt(text):
    #text = text.title()
    text = text.upper()
    text = text.replace('  ',' ').replace(',','').replace('-','').replace('(','').replace(')','').replace('\n','').replace('.','')
    text = text.replace('LTD','').replace('LIMITED','')
    text = text.replace('THE ','').replace('&','AND')
    text = re.sub(r'  +',' ', text).strip()
    return text


def run_program(run_by='Adqvest_Bot', py_file_name=None):

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
    table_name = 'MCA_INDEX_OF_CHARGES_PIT_RANDOM_DATA, MCA_INDEX_OF_CHARGES_NO_PIT_RANDOM_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        max_date_db = pd.read_sql('SELECT MAX(Relevant_Date) as max_date FROM MCA_INDEX_OF_CHARGES_WEEKLY_STATUS', con=engine)['max_date'][0]
        df_mapping=pd.read_sql("Select * from GENERIC_DICTIONARY_TABLE where Input_Table ='MCA_INDEX_OF_CHARGES_PIT_RANDOM_DATA'", engine)
        cin_list=pd.read_sql("select Cin,Company_Name,Status from AdqvestDB.MCA_INDEX_OF_CHARGES_WEEKLY_STATUS where Status is null",engine)

        if (today.date()-max_date_db).days >=0:      
            
            status_df=pd.read_sql("select Cin,Company_Name,Status from AdqvestDB.MCA_INDEX_OF_CHARGES_WEEKLY_STATUS ",engine)
            if status_df['Status'].isnull().sum()==0:
               status_df['Status']=None
               connection = engine.connect()
               connection.execute("UPDATE AdqvestDB.MCA_INDEX_OF_CHARGES_WEEKLY_STATUS set Status=NULL")

            cin_list=pd.read_sql("select Cin,Company_Name,Status from AdqvestDB.MCA_INDEX_OF_CHARGES_WEEKLY_STATUS where Status is null",engine)
            cin_list1 = cin_list['Cin']
            cin_company = cin_list['Company_Name']

            session = requests.Session()
            cookies = '; '.join([i+'='+j for i,j in session.cookies.get_dict().items()])

            for cin,company in zip(cin_list1,cin_company):
                print(cin,company)
                limit=0
                while True:
                    try:
                        headers={'sec-ch-ua':'"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                                'Accept':'application/json, text/javascript, */*; q=0.01',
                                'Content-Type':'application/json',
                                'X-Requested-With':'XMLHttpRequest',
                                'sec-ch-ua-mobile':'?0',
                                'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                                'sec-ch-ua-platform':'"Windows"',
                                'host':'www.mca.gov.in',
                                'Cookie':cookies}


                        url='https://www.mca.gov.in/bin/MDSMasterDataServlet'
                        robot.add_link(url)
                        payload='{"ID":"'+cin+'","requestID":"cin"}'
                        r=requests.post(url, data=payload, headers=headers)
                        data=json.loads(r.content)
                        print(data['data'].keys())
                        if (data['error'] != ''):
                            connection.execute("Update AdqvestDB.MCA_INDEX_OF_CHARGES_WEEKLY_STATUS set Status='No Data',Runtime = '"+datetime.datetime.now(india_time).strftime("%Y-%m-%d %H:%M:%S")+"',Relevant_Date = '"+datetime.datetime.now(india_time).strftime("%Y-%m-%d")+"' where Cin='"+cin+"'")
                            break
                        elif ('indexChargesData' not in data['data'].keys()):
                            connection.execute("Update AdqvestDB.MCA_INDEX_OF_CHARGES_WEEKLY_STATUS set Status='No Charges',Runtime = '"+datetime.datetime.now(india_time).strftime("%Y-%m-%d %H:%M:%S")+"',Relevant_Date = '"+datetime.datetime.now(india_time).strftime("%Y-%m-%d")+"' where Cin='"+cin+"'")
                            break
                        else:
                            mca_df=pd.DataFrame(data['data']['indexChargesData'])
                            mca_df['Cin'] = cin
                            mca_df['Company_Name'] = company
                            mca_df['Runtime'] = datetime.datetime.now()
                            mca_df['dateOfCreation'] = [datetime.datetime.strptime(str(x),'%m/%d/%Y').date() for x in mca_df['dateOfCreation']]
                            mca_df['dateOfModification'] = [datetime.datetime.strptime(str(x),'%m/%d/%Y').date() if x != '' else x for x in mca_df['dateOfModification']]
                            mca_df['dateOfSatisfaction'] = [datetime.datetime.strptime(str(x),'%m/%d/%Y').date() if x != '' else x for x in mca_df['dateOfSatisfaction']]
                            mca_df['Address'] = mca_df['StreetAddress'].astype(str) + ' ' + mca_df['StreetAddress2'].astype(str) + ' ' + mca_df['StreetAddress3'].astype(str) + ' ' + mca_df['StreetAddress4'].astype(str) + ' ' + mca_df['Locality'].astype(str) + ' ' + mca_df['District'].astype(str) + ' ' + mca_df['District'].astype(str) + ' ' + mca_df['State'].astype(str) + ' ' + mca_df['PostalCode'].astype(str) + ' ' + mca_df['Country'].astype(str)
                            mca_df.drop(columns = ['StreetAddress','StreetAddress2', 'StreetAddress3', 'StreetAddress4', 'Country','Locality', 'State', 'District', 'City', 'PostalCode', 'registeredName','propertyIntUnRegdFlag', 'chargeHolderName', 'chargeStatus'], inplace = True)
                            mca_df.columns = ['Srn', 'Charge_Id', 'Date_Of_Creation', 'Date_Of_Modification', 'Date_Of_Satisfaction', 'Amount','chName', 'Cin','Company_Name', 'Runtime', 'Address']
                            mca_df.rename(columns={"chName":'Charge_Holder_Name'}, inplace=True)
                            mca_df['Charge_Holder_Clean_Name'] = mca_df['Charge_Holder_Name'].apply(lambda x:clean_txt(x))
                            mca_df_pit = pd.merge(mca_df, df_mapping, left_on='Company_Name', right_on='Input', how='left')

                            mca_df_pit=mca_df_pit[['Cin','Srn','Company_Name','Output','Charge_Id','Charge_Holder_Name','Charge_Holder_Clean_Name','Date_Of_Creation','Date_Of_Modification','Date_Of_Satisfaction','Amount','Address','Runtime_x']]
                            mca_df_pit.columns=['Cin','Srn','Company_Name','Company_Name_Clean','Charge_Id','Charge_Holder_Name','Charge_Holder_Clean_Name','Date_Of_Creation','Date_Of_Modification','Date_Of_Satisfaction','Amount','Address','Runtime']
                            mca_df_pit['Type_of_Company_Nature'] = pd.np.where(pd.isna(mca_df_pit['Company_Name_Clean']), 'Others', 'NBFC')
                            print(mca_df_pit)
                            mca_df_pit=mca_df_pit[['Cin', 'Srn', 'Company_Name', 'Company_Name_Clean', 'Charge_Id','Charge_Holder_Name', 'Charge_Holder_Clean_Name', 'Date_Of_Creation','Date_Of_Modification', 'Date_Of_Satisfaction', 'Type_of_Company_Nature', 'Amount', 'Address','Runtime']]
                            
                            mca_df_pit.to_sql('MCA_INDEX_OF_CHARGES_PIT_RANDOM_DATA',index=False, if_exists='append', con=engine)
                            connection.execute("Update AdqvestDB.MCA_INDEX_OF_CHARGES_WEEKLY_STATUS set Status='Yes',Runtime = '"+datetime.datetime.now(india_time).strftime("%Y-%m-%d %H:%M:%S")+"',Relevant_Date = '"+datetime.datetime.now(india_time).strftime("%Y-%m-%d")+"' where Cin='"+cin+"'")
                            connection.execute("Delete from AdqvestDB.MCA_INDEX_OF_CHARGES_NO_PIT_RANDOM_DATA  where Cin='"+cin+"'")
                            mca_df_pit.to_sql('MCA_INDEX_OF_CHARGES_NO_PIT_RANDOM_DATA',index=False, if_exists='append', con=engine)
                            df = pd.read_sql("select * from AdqvestDB.MCA_INDEX_OF_CHARGES_NO_PIT_RANDOM_DATA where Cin = '"+cin+"'",con=engine)
                            df['Date_Of_Creation'] = df['Date_Of_Creation'].apply(lambda x:str(x))
                            df['Date_Of_Modification'] = df['Date_Of_Modification'].apply(lambda x:str(x))
                            df['Date_Of_Satisfaction'] = df['Date_Of_Satisfaction'].apply(lambda x:str(x))

                            df=df[['Cin', 'Srn', 'Company_Name', 'Company_Name_Clean', 'Charge_Id','Charge_Holder_Name', 'Charge_Holder_Clean_Name', 'Date_Of_Creation','Date_Of_Modification', 'Date_Of_Satisfaction', 'Type_of_Company_Nature', 'Amount', 'Address','Runtime']]
                            
                            client.execute("INSERT INTO MCA_INDEX_OF_CHARGES_PIT_RANDOM_DATA VALUES", df.values.tolist())
                            print('collection done')
                            break

                    except:
                        limit=limit+1
                        if limit>5:
                            print('Error No Cin for data')
                            connection.execute("Update AdqvestDB.MCA_INDEX_OF_CHARGES_WEEKLY_STATUS set Status='No Cin',Runtime = '"+datetime.datetime.now(india_time).strftime("%Y-%m-%d %H:%M:%S")+"',Relevant_Date = '"+datetime.datetime.now(india_time).strftime("%Y-%m-%d")+"' where Cin='"+cin+"'")
                            break

        log.job_end_log(table_name, job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'", str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name, job_start_time, error_type, error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
