import re
import sys
import warnings
import datetime
import pandas as pd
import requests
from pytz import timezone
import numpy as np
warnings.filterwarnings('ignore')
import os
import json
from datetime import datetime as dt
import pytz

#%%
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
# sys.path.insert(0, '/home/ubuntu/AdQvestDir/Adqvest_Function')

import adqvest_db
import ClickHouse_db
import JobLogNew as log
import pytz
from adqvest_robotstxt import Robots
robot = Robots(__file__)
#%%
client = ClickHouse_db.db_conn()

india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days
#%%
def get_specfic_dict_key(di,l1): 
    multi_value_dict={}
    for k,v in di.items():
        if k in l1:
            multi_value_dict[k] =v   
    return multi_value_dict
def drop_duplicates(df):
    columns = df.columns.to_list()
    columns.remove('Runtime')
    df = df.drop_duplicates(subset=columns, keep='last')
    return df
def get_renamed_columns(df,col_dict):
    df.fillna('#', inplace=True)
    for k,v in col_dict.items():
        # print(k)
        try:
            ele=[item for item in df.columns if k.lower() in item.lower()]
            k1=df.columns.to_list().index(ele[0])
            df=df.rename(columns={f"{df.columns[k1]}":f"{col_dict[k]}"})
        except:
            pass
    
    df.reset_index(drop=True,inplace=True)
    df=df.replace('#',np.nan)
    return df

def ext_datetime(x):
    try:
        timestamp = datetime.datetime.fromtimestamp(x,pytz.timezone('Asia/Kolkata'))
        return[timestamp.time(),timestamp.date()]
    except Exception as e:
        print(e)
        return[np.nan,np.nan]

def Upload_Data_Mysql(table_name, data):
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    data.to_sql(table_name, con=engine, if_exists='append', index=False)
    print(data.info())

   
def update_ch_condition_based(table_name):
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    client = ClickHouse_db.db_conn()
    
    click_max_date = client.execute(f"select max(Relevant_Date) from {table_name}")
    click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])
    query2 =f"select * from KPCB_DAILY_DATA_4_UPLOAD_STAGING WHERE Relevant_Date > '" + click_max_date +"';" 

    df = pd.read_sql(query2,engine)
    client.execute(f"INSERT INTO {table_name} VALUES",df.values.tolist())
    print("Data uplodedin Ch")

#%%
def run_program(run_by = 'Adqvest_Bot', py_file_name = None):
    # os.chdir('/home/ubuntu/AdQvestDir')
    os.chdir('C:/Users/Administrator/AdQvestDir')
    engine = adqvest_db.db_conn()
    connection = engine.connect()


    job_start_time = datetime.datetime.now(india_time)
    table_name = "KPCB_DAILY_DATA"
    scheduler = ''
    no_of_ping = 0

    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]

    try :
        if(run_by == 'Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
#%%     
        engine.execute('Delete from KPCB_DAILY_DATA_4_UPLOAD_STAGING')
        url='https://onlinekspcb.com/public/#/landing/dashboard/table'
        robot.add_link(url)
        # db_max_date = pd.read_sql("select max(Relevant_Date) as Max from KPCB_DAILY_DATA",engine)["Max"][0]
        db_max_date =client.execute("select max(Relevant_Date) as Max from KPCB_DAILY_DATA")
        db_max_date = str([a_tuple[0] for a_tuple in db_max_date][0])
        db_max_date=pd.to_datetime(db_max_date, format='%Y-%m-%d').date()
        
        if (today.weekday() in [5]):
            time_out=60
            # INPUT_DATE=pd.to_datetime('2021-01-01',format='%Y-%m-%d').date().strftime('%Y/%m/%d')
            # INPUT_DATE=today.date().strftime('%Y/%m/%d')
            time_range = pd.date_range(str(pd.to_datetime(str(db_max_date+days),format='%Y-%m-%d')), str(pd.to_datetime(str(yesterday.date()),format='%Y-%m-%d')), freq='D')
            '''Getting Category Information'''
            headers = {
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Connection': 'keep-alive',
                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                'Origin': 'https://onlinekspcb.com',
                'Referer': 'https://onlinekspcb.com/public/',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin',
                'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36 Edg/120.0.0.0',
               }
            
            data = '{"userType":"SuperRegulator","userId":"userId_524"}'
            
            r = requests.post('https://onlinekspcb.com/glens/categoryList', headers=headers, data=data)
            robot.add_link('https://onlinekspcb.com/glens/categoryList')
            categoryDetails = json.loads(r.text)['bodyContent']
            if len(time_range)>0:
                for t in time_range:
                    query1=f"select distinct Category,Relevant_Date,Site_ID from KPCB_DAILY_DATA where Relevant_Date='{str(t.date())}';"
                    a,cols = client.execute(query1,with_column_types=True)
                    kpcb_df = pd.DataFrame(a, columns=[tuple[0] for tuple in cols])
                    INPUT_DATE=t.date().strftime('%Y/%m/%d')
                    print(INPUT_DATE)
                    # categoryDetails=[i for i in categoryDetails if i['category'].lower() not in [i.lower() for i in kpcb_df.Category.to_list()]]
                    
                    '''Getting Industry details of individual category'''
                    
                    for i in categoryDetails:
                        final_df=pd.DataFrame()
                        final_nc_df=pd.DataFrame()
                        
                        cat=i['category']
                        cat_id=i['categoryId']
                                        
                        data1 ='{"userType":"SuperRegulator","userId":"userId_524","categoryId":"'+cat_id+'","category":"'+cat+'"}'
                        r1 = requests.post('https://onlinekspcb.com/glens/CategoryDetails', headers=headers, data=data1)
                        robot.add_link('https://onlinekspcb.com/glens/CategoryDetails')
                        if ((r1.status_code!=200) | (len(r1.text)==0)):
                            continue

                        cat_industry_df = json.loads(r1.text)['bodyContent']
                        # cat_industry_df={i['siteId']:i['state'] for i in cat_industry_df if str(i['siteId'].replace('site_','')) not in [str(i) for i in kpcb_df.Site_ID.to_list()] and str(i['siteId'].replace('site_','')) not in [str(i) for i in kpcb_nc_df.Site_ID.to_list()]}
                        cat_industry_df={i['siteId']:i['state'] for i in cat_industry_df if str(i['siteId'].replace('site_','')) not in [str(i) for i in kpcb_df.Site_ID.to_list()]}


                        #Getting information about a parucular Industry in that category.
                        for site_id,state in cat_industry_df.items():                    
                            try:
                                print(f'Category--->{cat},---->Site {site_id}')
                                data2 = '{"userType":"SuperRegulator","userId":"userId_524","parameters":[],"siteId":"'+site_id+'","criteria":"15min","toDate":"'+INPUT_DATE+'","reportFormat":"graph","fromDate":"'+INPUT_DATE+'"}'
                                r2 = requests.post('https://onlinekspcb.com/glens/industryDetails', headers=headers, data=data2)
                                robot.add_link('https://onlinekspcb.com/glens/industryDetails')
                                if ((r2.status_code!=200) | (len(r2.text)==0)):
                                         continue
                                industry_parameters = json.loads(r2.text)['parameters']
                            

                                industry_parameters=[f'"{i}"' for i in industry_parameters]
                                industry_parameters=[industry_parameters[i:i+25] for i in range(0, len(industry_parameters), 25)]
                                
                                for ind_par_chunk in industry_parameters:
                                    j="["
                                    for index,i in enumerate(ind_par_chunk):
                                        if index+1!=len(ind_par_chunk):
                                            j=j+i+','
                                        elif index+1==len(ind_par_chunk):
                                            j=j+i+"]"
                                        
                                    data3 = '{"userType":"SuperRegulator","userId":"userId_524","parameters":'+str(j)+',"siteId":"'+site_id+'","criteria":"15min","toDate":"'+INPUT_DATE+'","reportFormat":"graph","fromDate":"'+INPUT_DATE+'"}'
                                    r3 = requests.post('https://onlinekspcb.com/glens/industry-graph', headers=headers, data=data3)
                                    robot.add_link('https://onlinekspcb.com/glens/industry-graph')
                                    if ((r3.status_code!=200) | (len(r3.text)==0)):
                                         continue

                                    industry_df1 = json.loads(r3.text)['info']
                                   

                                   
                                    industry_df1={i['label']:i['value'] for i in industry_df1}
                                    industry_df1=get_specfic_dict_key(industry_df1,['Industry Name', 'Address', 'District', 'Category'])
                                    industry_df1['Site_ID']=site_id
                                    industry_df1['State']=state
                                    industry_df1=pd.DataFrame.from_dict([industry_df1]) 
                                  
                                    
                                    #Getting all parameter information of that industry under that category
                                    parameter_df = json.loads(r3.text)['graphDetails']
                                    if len(parameter_df)==0:
                                        continue
                                    para_df=pd.DataFrame()
                                    for p in parameter_df:
                                        if 'invalidData' not in p.keys():
                                            continue
                                        df_p=pd.DataFrame()
                                        df_p['Actual_Value_time']=p['invalidData']
                                        df_p['Actual_Value_time']=np.where((df_p['Actual_Value_time'].isna()==False),df_p['Actual_Value_time'],'')
                                        df_p['Actual_Value']=df_p['Actual_Value_time'].apply(lambda x:x[1] if len(x)>0 else np.nan)
                                        df_p['Time']=df_p['Actual_Value_time'].apply(lambda x:x[0] if len(x)>0 else np.nan)
                                        df_p['Relevant_Date'] = df_p['Time'].apply(lambda x : ext_datetime(x)[1])
                                        df_p['Time'] = df_p['Time'].apply(lambda x : ext_datetime(int(x))[0])
                                        df_p['Time'] = df_p['Time'].apply(lambda x: x.strftime("%H:%M:%S"))
                                        
                                        df_p['Parameter']=p['parameter']
                                        df_p['Threshold_Value']=p['threshold']
                                        df_p['Unit']=p['unit']
                                        df_p.drop(['Actual_Value_time'], axis=1, inplace=True)
                                        para_df=pd.concat([para_df,df_p])
                                    
                                    if len(para_df)>0:
                                        for index, row in industry_df1.iterrows():
                                            para_df['Category']=row['Category']
                                            para_df['Company_Name']=row['Industry Name']
                                            para_df['Location']=row['District']
                                            para_df['Site_ID']=row['Site_ID'].replace('site_','')
                                            para_df['State']=row['State']
                                            para_df['Runtime'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
                                            para_df=drop_duplicates(para_df)
                                    else:
                                        nc_df=pd.DataFrame()
                                        nc_df['Category']=[cat.lower()]
                                        nc_df['Site_ID']=[site_id.replace('site_','')]
                                        nc_df['Relevant_Date']=t.date()
                                        nc_df['Runtime'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
                                        final_nc_df=pd.concat([final_nc_df,nc_df])

                                    final_df=pd.concat([final_df,para_df])
                            except:
                                nc_df=pd.DataFrame()
                                nc_df['Category']=[cat.lower()]
                                nc_df['Site_ID']=[site_id.replace('site_','')]
                                nc_df['Relevant_Date']=t.date()
                                nc_df['Runtime'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
                                final_nc_df=pd.concat([final_nc_df,nc_df])
                                continue
                        print(final_df)
                        Upload_Data_Mysql('KPCB_DAILY_DATA_4_UPLOAD_STAGING',final_df)



        update_ch_condition_based('KPCB_DAILY_DATA')

        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        try:
            connection.close()
        except:
            pass
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + ": line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)
if(__name__=='__main__'):
    run_program(run_by = 'manual')
