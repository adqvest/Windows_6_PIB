from genericpath import exists
import warnings
warnings.filterwarnings('ignore')
import re
import os
import ssl
import sys
import datetime
import requests
import numpy as np
import pandas as pd
from pytz import timezone
from json import JSONDecoder
import requests.packages.urllib3.util.ssl_
ssl._create_default_https_context = ssl._create_unverified_context
requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = 'ALL'
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
from adqvest_robotstxt import Robots
robot = Robots(__file__)
import JobLogNew as log
import ClickHouse_db
import adqvest_db

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/State_Function')
import state_rewrite
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/District_Function')
import district_rewrite

engine = adqvest_db.db_conn()
connection = engine.connect()
client = ClickHouse_db.db_conn()
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)

def drop_duplicates(df):
    columns = df.columns.to_list()
    columns.remove('Runtime')
    df = df.drop_duplicates(subset=columns, keep='last')
    return df

def Upload_Data(table_name, data,ty:str, db: list):
    query=f"select distinct Relevant_Date as Relevant_Date from {table_name} where State='{ty}';"
    # query=f"select distinct Relevant_Date as Relevant_Date from {table_name};"
    print(query)
    db_max_date = pd.read_sql(query,engine)   
    data['Relevant_Date'] = pd.to_datetime(data['Relevant_Date'], format='%Y-%m-%d')
    data=data.loc[data['State']==ty]
    data=data[data['Relevant_Date'].isin(db_max_date.Relevant_Date.tolist())==False]
    # print(data.info())
    if 'MySQL' in db:
        data.to_sql(table_name, con=engine, if_exists='append', index=False)
        # print(f'Done for --->{db_max_date,ty}')
        print(data.info())

def extract_json_objects(text, decoder=JSONDecoder()):
    pos = 0
    while True:
        match = text.find('{', pos)
        if match == -1:
            break
        try:
            result, index = decoder.raw_decode(text[match:])
            yield result
            pos = match + index
        except ValueError:
            pos = match + 1
            
def report_parameters_locator(report_name,year_id):
    cookies = {'JSESSIONID': '334H28XYe3Yu4qmkHgj5zwPkwBJRaAMuV2',
               'cookieWorked': 'yes',}
    
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Content-Type': 'text/plain; charset=UTF-8',
        # 'Cookie': 'JSESSIONID=C5F65078F897130653CDFA605C7F4BDD; cookieWorked=yes',
        'Origin': 'https://dashboard.udiseplus.gov.in',
        'Referer': 'https://dashboard.udiseplus.gov.in/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 Edg/107.0.1418.56',
        'sec-ch-ua': '"Microsoft Edge";v="107", "Chromium";v="107", "Not=A?Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }
    #################### GEtting Report ID #########################################################
    data = '5532'
    r = requests.post('https://dashboard.udiseplus.gov.in/BackEnd-master/api/report/getUserReport',cookies=cookies,headers=headers,data=data,)
    robot.add_link('https://dashboard.udiseplus.gov.in/BackEnd-master/api/report/getUserReport')
    
    demo_text = extract_json_objects(r.text, decoder=JSONDecoder())
    report_dict =[]
    for result in demo_text:
        report_dict.append(result)  
    report=[i for i in report_dict if re.findall(report_name.lower(),i['report_name'].lower())]
    report_id=report[0]['id']
    
    #################### GEtting years #########################################################
    d=r'{\"year\":\"'+str(year_id)+r'\",\"state\":\"national\",\"dist\":\"none\",\"block\":\"none\"}'
    data='{"mapId":"'+str(report_id)+'","dependencyValue":"'+d+'","isDependency":"Y","paramName":"civilian","paramValue":"","schemaName":"national","reportType":"T"}'
    
    r = requests.post('https://dashboard.udiseplus.gov.in/BackEnd-master/api/report/getTabularData', cookies=cookies,headers=headers,data=data,)
    robot.add_link('https://dashboard.udiseplus.gov.in/BackEnd-master/api/report/getTabularData')
    demo_text = extract_json_objects(r.text, decoder=JSONDecoder())
    
    year_dict =[]
    for result in demo_text:
        year_dict.append(result)
        year_dict= year_dict[0]['yearListMap']
    
    #################### GEtting States #########################################################
    data = f'{{"extensionCall":"GET_STATE","condition":" where year_id=\'{year_id}\' order by state_name"}}'
    r = requests.post('https://dashboard.udiseplus.gov.in/BackEnd-master/api/report/getMasterData',cookies=cookies,headers=headers,data=data,)
    robot.add_link('https://dashboard.udiseplus.gov.in/BackEnd-master/api/report/getMasterData')
    demo_text = extract_json_objects(r.text, decoder=JSONDecoder())
    
    state_dict =[]
    for result in demo_text:
        state_dict.append(result)
    state_dict= state_dict[0]['rowValue']
    return report_id,year_dict,state_dict 

def run_program(run_by = 'Adqvest_Bot', py_file_name = None):
    os.chdir(r'C:/Users/Administrator/AdQvestDir/')
     #****   Date Time *****
    job_start_time = datetime.datetime.now(india_time)
    table_name = "UDISE_STU_ADJ_NER_GEN_WISE_N_SCH_LVL_EDU_WISE_YEARLY_4012"
    scheduler = ''
    no_of_ping = 0

    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]

    try:
        if(run_by == 'Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
        
        working_table='UDISE_STU_ADJ_NER_GEN_WISE_N_SCH_LVL_EDU_WISE_YEARLY_4012'
        query=f"SELECT max(Academic_Year) as Academic_Year from {working_table} where Relevant_Date in (SELECT MAX(Relevant_Date) from {working_table})"
        max_timeperiod_db = pd.read_sql(query,engine)['Academic_Year'][0]
        print(max_timeperiod_db)
        df_st_alpha=pd.read_sql("select *  from INDIA_STATE_N_STATE_ALPHA_CODE_GST_CODE_MAPPING;", con=engine)
        
        report_parameters=report_parameters_locator('Retention Rate',22)
        report_id=report_parameters[0]
        year_dict=report_parameters[1]
        
        # collection_year_org=[i for i in year_dict if i['year'] > max_timeperiod_db]
        collection_year_org=[i for i in year_dict if i['year']]
        if collection_year_org:
            dates=[i['year']for i in collection_year_org]
            date={pd.to_datetime('20'+i.split("-")[1]+'-03'+'-31', format='%Y-%m-%d'):tp  for i,tp in zip(dates,year_dict)}
            
            for yr in collection_year_org:                
                data_list=[]
                report_parameters=report_parameters_locator('Retention Rate',yr['yearId'])
                state_dict=[]
                state_dict.append({'state_name': 'National', 'udise_state_code': 'national'})
                state_dict.append({'state_name': 'All India', 'udise_state_code': 'all'})
            
                for st in state_dict:
                    state_id=st['udise_state_code']
                    state_name=st['state_name']
                
                    academic_year=yr['year']
                    year_id=yr['yearId']
                    date='20'+academic_year.split("-")[1]+'-03'+'-31'
    
                    print(f'State:==={state_name}')
                    print(f'Collecting for :==={academic_year}')
    
                    cookies = { 'JSESSIONID': '112hTmGVNQ61U7s8wf4NCCRwERpPNyd6b9','cookieWorked': 'yes'}
                    
                    headers = { 'Accept': 'application/json, text/plain, */*', 'Accept-Language': 'en-US,en;q=0.9',
                        'Connection': 'keep-alive', 'Content-Type': 'text/plain; charset=UTF-8',
                        'Origin': 'https://dashboard.udiseplus.gov.in', 'Referer': 'https://dashboard.udiseplus.gov.in/',
                        'Sec-Fetch-Dest': 'empty', 'Sec-Fetch-Mode': 'cors', 'Sec-Fetch-Site': 'same-origin',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 Edg/107.0.1418.56',
                        'sec-ch-ua': '"Microsoft Edge";v="107", "Chromium";v="107", "Not=A?Brand";v="24"',
                        'sec-ch-ua-mobile': '?0', 'sec-ch-ua-platform': '"Windows"'}
                    
                    if state_id=='national':
                        d=r'{\"year\":\"'+str(year_id)+r'\",\"state\":\"'+str(state_id)+r'\",\"dist\":\"none\",\"block\":\"none\"}'
                    else:
                        d=r'{\"year\":\"'+str(year_id)+r'\",\"state\":\"'+str(state_id)+r'\",\"dist\":\"none\",\"block\":\"none\"}'

                    data='{"mapId":"'+str(report_id)+'","dependencyValue":"'+d+'","isDependency":"Y","paramName":"civilian","paramValue":"","schemaName":"national","reportType":"T"}'
                    r = requests.post('https://dashboard.udiseplus.gov.in/BackEnd-master/api/report/getTabularData',
                        cookies=cookies, headers=headers, data=data)
                    robot.add_link('https://dashboard.udiseplus.gov.in/BackEnd-master/api/report/getTabularData')
                    print(r)
                    demo_text = extract_json_objects(r.text, decoder=JSONDecoder())
                    jsons = []
                    for result in demo_text:
                        jsons.append(result)
                    
                    data_raw= jsons[0]['rowValue']
                    df = pd.DataFrame.from_dict(data_raw)
                    if len(df) != 0:
                        del_col=['state_cd']
                        df.drop(columns=[i for i in df.columns if i in del_col], inplace=True)
                        
                        df=df.rename(columns={  
                                                'rr5': 'Primary 1-5_Overall',
                                                'rr5_g': 'Primary 1-5_Girls',
                                                'rr5_b': 'Primary 1-5_Boys',
                                                'rr8': 'Elementary 1-8_Overall',
                                                'rr8_g': 'Elementary 1-8_Girls',
                                                'rr8_b': 'Elementary 1-8_Boys',
                                                'rr10': 'Secondary 1-10_Overall',
                                                'rr10_g': 'Secondary 1-10_Girls',
                                                'rr10_b': 'Secondary 1-10_Boys',
                                                'rr12': 'Higher Secondary 1-12_Overall',
                                                'rr12_g': 'Higher Secondary 1-12_Girls',
                                                'rr12_b': 'Higher Secondary 1-12_Boys',
                                                'state_name': 'State',
                                            })
                        
                        df = pd.melt(df, id_vars=['State'], var_name='Type', value_name='Retention_Rate')
                        df['Unit'] = '%'
                        df['Type_Clean']=df['Type'].apply(lambda x: x.split('1')[0].strip().title())
                        df['Class']=df['Type'].apply(lambda x: x.split(' ')[-1].split('_')[0])
                        df['Gender']=df['Type'].apply(lambda x: x.split('_')[-1])
                        
                        df['State_Clean']= df['State'].apply(lambda x:state_rewrite.state((x.lower())).split('|')[-1].strip().title())
                        
                        df=pd.merge(df, df_st_alpha[['State','State_Alpha_Code']],left_on='State_Clean',right_on='State',how='left')
                        df=df.rename(columns={'State_x':'State'})
                        df.drop(columns=['State_y'], inplace=True)

                        df['State_Alpha_Code']=np.where(df['State_Clean'].isin(['Andaman And Nicobar Islands']),'AN',df['State_Alpha_Code'])
                        df['State_Alpha_Code']=np.where(df['State_Clean'].isin(['Jammu And Kashmir']),'JK',df['State_Alpha_Code'])
                        df['State_Alpha_Code']=np.where(df['State_Clean'].isin(['The Dadra And Nagar Haveli And Daman And Diu']),'DH',df['State_Alpha_Code'])
                        
                        df['State']=np.where(df['State'].str.lower().str.contains('india'),'National',df['State'])
                        df['State_Clean']=np.where(df['State'].isin(['National']),'National',df['State_Clean'])
                        df['State_Alpha_Code']=np.where(df['State'].isin(['National']),np.nan,df['State_Alpha_Code'])
                        df = df[['State', 'State_Clean', 'State_Alpha_Code', 'Type', 'Type_Clean','Class', 'Gender', 'Retention_Rate', 'Unit']]

                        df['Academic_Year']=academic_year
                        df['Relevant_Date'] = pd.to_datetime(date, format='%Y-%m-%d')
                        df['Relevant_Date']=df['Relevant_Date'].dt.date
                        df['Runtime'] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
                        data_list.append(df)
                      
                final_df=pd.concat(data_list)  
                final_df=drop_duplicates(final_df)    
                final_df=final_df[~(final_df.State=='-')] 
                # final_df[final_df.Relevant_Date > datetime.date(2013,3,31)] 
                for j in final_df.State.unique().tolist():
                    Upload_Data('UDISE_STU_STATE_N_GEN_WISE_BY_EDU_LVL_RETENTION_RATE_YEARLY_4033',final_df,j,['MySQL'])
        else:
            print('No New Data')              
        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'", str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name, job_start_time, error_type, error_msg, no_of_ping)
if(__name__=='__main__'):
    run_program(run_by='manual')               