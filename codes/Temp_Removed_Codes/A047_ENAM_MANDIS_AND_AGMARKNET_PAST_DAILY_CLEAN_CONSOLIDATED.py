# -*- coding: utf-8 -*-
"""
Created on Wed Apr 27 14:40:52 2022

@author: Santonu
"""

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
import numpy as np
warnings.filterwarnings('ignore')
#%%
# sys.path.insert(0, '/home/ubuntu/AdQvestDir/Adqvest_Function')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
#sys.path.insert(0, r'C:\Adqvest')
import numpy as np
import adqvest_db
import JobLogNew as log
requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = 'ALL:@SECLEVEL=1'
import ClickHouse_db


#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days

engine = adqvest_db.db_conn()
connection = engine.connect()
client = ClickHouse_db.db_conn()
#%%
def drop_duplicates(df):
    columns = df.columns.to_list()
    columns.remove('Runtime')
    df = df.drop_duplicates(subset=columns, keep='last')
    return df

def find_common_items(list1, list2):
    set1 = set(list1)
    set2 = set(list2)
    common_items = set1.intersection(set2)
    print(common_items)
    
    return list(common_items) 

def Upload_Data(table_name,data,state,src,db: list):

   
    # query="""select max(Relevant_Date) as Max from """ + table_name + """ where Source='""" + src + """' and Tagged_State='""" + state + """';"""
    # db_max_date = pd.read_sql(query,engine)
    # if (db_max_date['Max'].isnull().all()):
    #    pass
    # else:
    #     db_max_date = db_max_date["Max"][0]
    #     data=data.loc[data['Relevant_Date'] > db_max_date]
    
    if 'MySQL' in db:
        engine.execute('Delete from ENAM_MANDIS_AGMARKNET_PAST_DAILY_DATA_4_UPLOAD')
        data.to_sql('ENAM_MANDIS_AGMARKNET_PAST_DAILY_DATA_4_UPLOAD', con=engine, if_exists='append', index=False)
        print("Data uploded in MySQL")
        print(data.info())

    if 'Click_house' in db:
        q1=f"select max(Relevant_Date) from AdqvestDB.{table_name} where Source ='{src}' and  State='{str(state)}'"
        ch_max_date = client.execute(q1)
        ch_max_date = str([a_tuple[0] for a_tuple in ch_max_date][0])
        query = 'select * from AdqvestDB.ENAM_MANDIS_AGMARKNET_PAST_DAILY_DATA_4_UPLOAD WHERE Relevant_Date > "' + ch_max_date + '" and Source="'+src+'" and Tagged_State="'+state+'";'
        df = pd.read_sql(query,engine)
        # df=df.loc[df['Relevant_Date'] > ch_max_date]
        df['Act_Runtime'] = df.Act_Runtime.replace(np.nan, 0)
        df['Act_Runtime'] = df.Act_Runtime.replace(0, None)
        client.execute("INSERT INTO AdqvestDB."+table_name+" VALUES",df.values.tolist())
        print("Data uploded in Click_house")

def get_date_range(start_date):

    start_date = datetime.date(start_date.year, start_date.month, start_date.day) + datetime.timedelta(1)
    end_date = datetime.date(yesterday.year, yesterday.month, yesterday.day)
    if start_date>end_date:
        time_range=[]
    else:
        time_range = pd.date_range(str(start_date), str(end_date), freq='D')
    return time_range

#%%
def run_program(run_by='Adqvest_Bot', py_file_name=None):
    # os.chdir('/home/ubuntu/AdQvestDir')
    os.chdir('C:/Users/Administrator/AdQvestDir')
    engine = adqvest_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'ENAM_MANDIS_AND_AGMARKNET_PAST_DAILY_DATA_CLEAN_CONSOLIDATED'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
        

        # engine.execute('Delete from ENAM_MANDIS_AGMARKNET_PAST_DAILY_DATA_4_UPLOAD')
        # q3='''select * from AGMARKNET_PAST_DAILY_DATA where Relevant_Date >= (select min(t1.Relevant_Date) as Relevant_Date from (select State,max(Relevant_Date) as Relevant_Date from AGMARKNET_PAST_DAILY_DATA
        # where State not in ('Arunachal Pradesh','Jharkhand','Mizoram') group by State) as t1);'''

        # df2 = pd.read_sql(q3,engine)
        # df2.to_sql('ENAM_MANDIS_AGMARKNET_PAST_DAILY_DATA_4_UPLOAD', con=engine, if_exists='append', index=False)

        table1 = "AGMARKNET_PAST_DAILY_DATA"
        table2 = "ENAM_MANDIS_TRADE_DAILY_DATA"

        new_table = "ENAM_MANDIS_AND_AGMARKNET_PAST_DAILY_DATA_CLEAN_CONSOLIDATED"
        lookup = pd.read_sql("Select * from AdqvestDB.DISTRICT_LOOKUP_TABLE_STATIC",con=engine)
        state_list=client.execute("Select distinct Tagged_State as State from AdqvestDB."+new_table)
        state_list =[a_tuple[0] for a_tuple in state_list]
        # state_list=state_list.State.tolist()

        
        
        
        empty_serch=[]

        com_look="Select Input,Output from GENERIC_DICTIONARY_TABLE where Input_Table in ('ENAM_MANDIS_AND_AGMARKNET_PAST_DAILY_DATA_CLEAN_CONSOLIDATED')"
        com_lookup=pd.read_sql(com_look,engine)
        com_lookup.columns=['Commodity','Commodity_Clean']
        com_lookup['Commodity']=com_lookup['Commodity'].apply(lambda x:x.title())

        un_match=['Paddy -Bpt(Tn)','Paddy- Nlr(Tn)','Paddy Co 51(Tn)','Paddy Rnr (Tn)','Paddy Ponni(Tn)','Paddy Ponni White(Tn)','Paddy Adt-37(Tn)','Paddy Adt-39(Tn)','Paddy Adt-45(Tn)','Paddy Adt-51(Tn)',
                  'Dry Chilly_10 Kg(Tn)','Paddy(75 Kg)','Paddy- Nlr Old(Tn)','Gingelly_80 Kg(Tn)']
        

#%%
        '''
        AGMARKNET
        '''
        for st in state_list:   
            max_date = client.execute(f"select max(Relevant_Date) as Max from AdqvestDB.{new_table} where Source = 'Non-ENAM' and  State='{str(st)}'")
            max_date = str([a_tuple[0] for a_tuple in max_date][0])
            
            if (max_date==''):
                max_date=pd.to_datetime('2018-01-01', format='%Y-%m-%d').date()
            else:
                max_date=pd.to_datetime(max_date, format='%Y-%m-%d').date()
                # max_date = max_date["Max"][0]

            date_org =client.execute(f"select distinct Relevant_Date as Relevant_Date from AdqvestDB.{table1} where Relevant_Date>'{str(max_date)}' and State='{str(st)}'")
            # date_org=date_org.iloc[:,0].tolist()
            date_org =[a_tuple[0] for a_tuple in date_org]
            date_org=[str(i) for i in date_org]

            all_dates=get_date_range(max_date)
            all_dates=[str(i.date()) for i in all_dates]
            all_dates=find_common_items(all_dates,date_org)
            all_dates=sorted(all_dates) 
            print(all_dates)
            if len(all_dates)>0:
                print('Non-ENAM')
                for date1 in all_dates:
                    print(f'------>{date1}')
                    print(f'------>{st}')
                    # date1=date1.date()
                    df = client.execute("Select * from AdqvestDB."+table1+" where Relevant_Date >='"+str(date1)+"' and State= '"+str(st)+"';")
                    df = pd.DataFrame(df, columns=[column[0] for column in client.execute(f"desc {table1}")])
                    
                    if df.empty == False:
                        df['Search_Term'] = df['Market'] + " " +df['State']
                        df1=df.copy()
                        df = pd.merge(df.applymap(lambda s: s.lower() if type(s) == str else s), lookup[['Tagged_District','LGD_State','Search_Term']].applymap(lambda s: s.lower() if type(s) == str else s).drop_duplicates(['Search_Term']), how='inner', on='Search_Term')
                        df['Tagged_State'] = df['LGD_State']
                        print(df)
                        df=df.rename(columns={"Market":"Apmcs","Product":"Commodity",
                                             "Minimum_Prices":"Price_In_Rs_Min_Price",
                                             "Maximum_Prices":"Price_In_Rs_Max_Price",
                                             "Modal_Prices":"Price_In_Rs_Modal_Price",
                                             "Arrivals":"Commodity_Arrivals",
                                             "Unit_of_Arrivals":"Unit",
                                             'Group':'Commodity_Group'
                                             })
                        

                        df = df[['State', 'Apmcs','Tagged_District','Tagged_State','Commodity_Group','Commodity','Variety','Price_In_Rs_Min_Price',
                                'Price_In_Rs_Modal_Price', 'Price_In_Rs_Max_Price','Commodity_Arrivals', 'Unit', 'Relevant_Date', 'Runtime']]
                                
                        df['Commodity_Traded'] = None
                        df['District'] = None
                        df['Source'] = "Non-ENAM"

                        

                        if df.empty==True:
                            empty_serch.append(df1)
                            continue
                        
                               
                        check = df.copy()
                        
                        df = df.where(pd.notnull(df), None)
                        
                        df = df[['State', 'District', 'Tagged_District', 'Tagged_State', 'Apmcs','Commodity_Group',
                            'Commodity', 'Variety', 'Price_In_Rs_Min_Price',
                            'Price_In_Rs_Modal_Price', 'Price_In_Rs_Max_Price',
                            'Commodity_Arrivals', 'Unit','Commodity_Traded','Source', 'Relevant_Date', 'Runtime']]
                            

                        df['State']=df['State'].apply(lambda x:x.title())
                       
                        df['Tagged_District']=df['Tagged_District'].apply(lambda x:x.title())
                        df['Tagged_State']=df['Tagged_State'].apply(lambda x:x.title())
                        df['Commodity_Group']=df['Commodity_Group'].apply(lambda x:x.title())
                        df['Commodity']=df['Commodity'].apply(lambda x:x.title())
                        df['Apmcs']=df['Apmcs'].apply(lambda x:x.title())
                        df['Unit']=df['Unit'].apply(lambda x:x.title())
                        df['Tagged_State']=df['Tagged_State'].apply(lambda x:x.strip())
                        df['Tagged_District']=df['Tagged_District'].apply(lambda x:x.strip())
                        df['Commodity']=df['Commodity'].apply(lambda x:x.strip())

                        df['Unit']=df['Unit'].replace({'Nos':'No.'})
                        df['Unit']=df['Unit'].replace({'No':'No.'})
                        df['Unit']=df['Unit'].replace({'Qui':'Quintal'})
                        df['Unit']=df['Unit'].replace({'Kilogram ':'Kg'})
                        df['Unit']=df['Unit'].replace({'tonnes':'Tonnes'})

                        
                        df['Commodity_Arrivals']=np.where((df["Unit"]=="Tonnes"),df['Commodity_Arrivals']*10,df['Commodity_Arrivals'])
                        df['Unit']=np.where((df["Unit"]=="Tonnes"),'Quintal',df['Unit'])

                        df['Price_In_Rs_Modal_Price']=np.where((df["Commodity"].isin(un_match)),df['Price_In_Rs_Modal_Price']/100,df['Price_In_Rs_Modal_Price'])
                        df['Price_In_Rs_Min_Price']=np.where((df["Commodity"].isin(un_match)),df['Price_In_Rs_Min_Price']/100,df['Price_In_Rs_Min_Price'])
                        df['Price_In_Rs_Max_Price']=np.where((df["Commodity"].isin(un_match)),df['Price_In_Rs_Max_Price']/100,df['Price_In_Rs_Max_Price'])



                        df=df[df['Commodity_Arrivals'] > 0]

                        df=pd.merge(df,com_lookup,on='Commodity',how='left')
                        df=drop_duplicates(df)

                        df['Act_Runtime'] = datetime.datetime.now(india_time)

                        Upload_Data(new_table,df,st,'Non-ENAM','MySQL')
                        Upload_Data(new_table,df,st,'Non-ENAM','Click_house')
                        print("Uploaded AGM",str(date1))

                        break
                        if check['Tagged_District'].isnull().any():
                            raise Exception("New Seacrh Term Has Come up Check Training Data")
                            log.job_end_log(table_name,job_start_time, no_of_ping)
                   

            else:
                print(f"Clean Data Present for State --->{st}")
#%%
        '''
        ENAM MANDIS
        '''
        print('ENAM MANDIS Started')
        for st in state_list:
            st=st.upper()
            max_date =client.execute(f"select max(Relevant_Date) as Max from AdqvestDB.{new_table} where Source = 'ENAM' and  State='{str(st.title())}'")
            max_date = str([a_tuple[0] for a_tuple in max_date][0])
            
            if (max_date==''):
                max_date=pd.to_datetime('2018-01-01', format='%Y-%m-%d').date()
            else:
                max_date=pd.to_datetime(max_date, format='%Y-%m-%d').date()
                # max_date = max_date["Max"][0]
                

            date_org = client.execute(f"select distinct Relevant_Date as Relevant_Date from AdqvestDB.{table2} where Relevant_Date>'{str(max_date)}' and State='{str(st)}'")
            date_org =[a_tuple[0] for a_tuple in date_org]
            date_org=[str(i) for i in date_org]
            

            all_dates=get_date_range(max_date)
            all_dates=[str(i.date()) for i in all_dates]
            all_dates=find_common_items(all_dates,date_org)
            all_dates=sorted(all_dates)
            print(all_dates)
            if len(all_dates)>0:
                print('ENAM')
                for date1 in all_dates:
                    print(f'------>{date1}')
                    print(f'------>{st}')
                    df = client.execute("Select * from AdqvestDB."+table2+" where Relevant_Date >= '"+str(date1)+"' and State= '"+str(st)+"';")
                    df = pd.DataFrame(df, columns=[column[0] for column in client.execute(f"desc {table2}")])
                    if df.empty == False:
                        df['Search_Term'] = df['Apmcs'] + " " +df['State']
                        df1=df.copy()
                        df = pd.merge(df.applymap(lambda s: s.lower() if type(s) == str else s), lookup[['Tagged_District','LGD_State','Search_Term']].applymap(lambda s: s.lower() if type(s) == str else s).drop_duplicates(['Search_Term']), how='inner', on='Search_Term')
                        df['Tagged_State'] = df['LGD_State']
                        df = df[['State', 'Apmcs','Tagged_District','Tagged_State', 'Commodity', 'Price_In_Rs_Min_Price',
                                'Price_In_Rs_Modal_Price', 'Price_In_Rs_Max_Price',
                                'Commodity_Arrivals', 'Commodity_Traded', 'Unit','Relevant_Date', 'Runtime']]
                                
                        check = df.copy()
                        df['Variety'] = None
                        df['District'] = None
                        df['Commodity_Group'] = 'ENAM'
                        df['Source'] = "ENAM"
                        
                        df = df.where(pd.notnull(df), None)
                        if df.empty==True:
                            empty_serch.append(df1)
                            continue


                        df = df[['State', 'District', 'Tagged_District', 'Tagged_State', 'Apmcs','Commodity_Group',
                             'Commodity', 'Variety', 'Price_In_Rs_Min_Price',
                             'Price_In_Rs_Modal_Price', 'Price_In_Rs_Max_Price',
                             'Commodity_Arrivals', 'Unit','Commodity_Traded','Source', 'Relevant_Date', 'Runtime']]
                             


                        df['State']=df['State'].apply(lambda x:x.title())
                        df['Tagged_District']=df['Tagged_District'].apply(lambda x:x.title())
                        df['Tagged_State']=df['Tagged_State'].apply(lambda x:x.title())
                        df['Commodity']=df['Commodity'].apply(lambda x:x.title())
                        df['Apmcs']=df['Apmcs'].apply(lambda x:x.title())
                        df['Unit']=df['Unit'].apply(lambda x:x.title())
                        df['Tagged_State']=df['Tagged_State'].apply(lambda x:x.strip())
                        df['Tagged_District']=df['Tagged_District'].apply(lambda x:x.strip())
                        df['Commodity']=df['Commodity'].apply(lambda x:x.strip())
                        print(len(df))

                        df['Unit']=df['Unit'].replace({'Nos':'No.'})
                        df['Unit']=df['Unit'].replace({'No':'No.'})
                        df['Unit']=df['Unit'].replace({'Qui':'Quintal'})
                        df['Unit']=df['Unit'].replace({'Kilogram ':'Kg'})

                        df['Price_In_Rs_Modal_Price']=np.where((df["Commodity"].isin(un_match)),df['Price_In_Rs_Modal_Price']/100,df['Price_In_Rs_Modal_Price'])
                        df['Price_In_Rs_Min_Price']=np.where((df["Commodity"].isin(un_match)),df['Price_In_Rs_Min_Price']/100,df['Price_In_Rs_Min_Price'])
                        df['Price_In_Rs_Max_Price']=np.where((df["Commodity"].isin(un_match)),df['Price_In_Rs_Max_Price']/100,df['Price_In_Rs_Max_Price'])




                        df=pd.merge(df,com_lookup,on='Commodity',how='left')
                        df=drop_duplicates(df)

                        df['Act_Runtime'] = datetime.datetime.now(india_time)
                        Upload_Data(new_table,df,st,'ENAM','MySQL')
                        Upload_Data(new_table,df,st,'ENAM','Click_house')
                        break

                        print("Uploaded ENAM",str(date1))
                        if check['Tagged_District'].isnull().any():
                            raise Exception("New Search Term Has Come up Check Training Data")

            else:
                print(f"Clean Data Present for State --->{st}")
        if len(empty_serch)>0:
            df4=pd.concat(empty_serch)
            df4.to_excel(f'AGMARK_ENAM_Empty_search_term_{today.date()}.xlsx')
        log.job_end_log(table_name,job_start_time, no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_type)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)


if(__name__=='__main__'):
    run_program(run_by='manual')

