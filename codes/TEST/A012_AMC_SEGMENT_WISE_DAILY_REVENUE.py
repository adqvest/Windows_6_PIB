#!/usr/bin/env python
# coding: utf-8

# In[97]:


#****************************** Developed By  : Subrata Akhuli *************
#****************************** Date          : 31-01-2020     *************
#****************************** Update Date   : 03-03-2020     *************
#****************************** Version       : v0.12          *************
import sqlalchemy
import pandas as pd
from pandas.io import sql
import os
import requests
from bs4 import BeautifulSoup
import re
import datetime as datetime
from pytz import timezone
import sys
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
import xml.etree.ElementTree as ET
import numpy as np
from statistics import mean
import csv
import calendar
import pdb
import calendar
import ClickHouse_db


def get_quarter(p_date):
    return (p_date.month - 1) // 3 + 1

def getEndQuarter(p_date):
    quarter = get_quarter(p_date)
    return datetime.date(p_date.year + 3 * quarter // 12, 3 * quarter % 12 + 1, 1) + datetime.timedelta(days=-1)

def applyCorrectionFactor(df,AMC_Name, engine):

    df['Quarter_Date'] = df['Relevant_Date'].apply(getEndQuarter)
    
    df_copy = df.copy()
    
    df = df[['Quarter_Date', 'Revenue_Cr']]
    
    df = df.groupby(['Quarter_Date'], as_index = False).sum()
    
    actual_df= pd.read_sql(f"Select Relevant_Date as Quarter_Date, Value as Act_Revenue from QUARTERLY_ACTUAL_REVENUE_STATIC WHERE Company_Name = '{AMC_Name}' order by Relevant_Date desc",con=engine)
    
    actual_df['Act_Revenue'] = actual_df['Act_Revenue'].replace(0,np.nan)
    
    final_qtr = pd.merge(df, actual_df,how = 'left', on = ['Quarter_Date'])
    
    final_qtr['Multiply_Factor'] = final_qtr['Act_Revenue']/final_qtr['Revenue_Cr']
    
    final_qtr['Multiply_Factor'] = final_qtr['Multiply_Factor'].backfill()
    
    final_qtr = final_qtr.sort_values('Quarter_Date').reset_index(drop = True)
    
    final_qtr['Correction_Factor'] = [mean(final_qtr['Multiply_Factor'][i-3:i+1].to_list())
                                      if i not in [0,1,2] else final_qtr['Multiply_Factor'][i]
                                      for i in range(len(final_qtr['Multiply_Factor']))]
    
    final_qtr['Correction_Factor'] = final_qtr['Correction_Factor'].ffill()
    
    final_qtr = final_qtr[['Correction_Factor','Quarter_Date']]
    
    df_copy = pd.merge(df_copy,final_qtr, how = 'left', on = 'Quarter_Date')

    df_copy.rename(columns = {'Revenue_Cr' : 'Raw_Revenue_Cr'}, inplace = True)
    
    if AMC_Name == 'UTI MF':
        df_copy['Correction_Factor'] = 1
    df_copy['Revenue_Cr'] = df_copy['Raw_Revenue_Cr']*df_copy['Correction_Factor']
    
    df_copy = df_copy[['AMC_Name', 'Segment', 'Scheme_Name', 'Scheme_Type', 'AUM_Cr', 'Mgmt_Fee', 'Revenue_Cr','Raw_Revenue_Cr','Correction_Factor', 'Comments', 'Relevant_Date', 'Runtime']]
    
    return df_copy


def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir')
    #os.chdir('C:/Users/krang/Dropbox/Subrata/Python')
    engine = adqvest_db.db_conn()
    connection = engine.connect()

    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days
    DB_yesterday = datetime.datetime.now(india_time) - datetime.timedelta(1)

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'AMC_SEGMENT_WISE_DAILY_REVENUE'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = '7_AM_INSIGHTS_SCHEDULER'
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)


        client = ClickHouse_db.db_conn()

        #------------------------------------------------ Main Programm ----------------------------------------------

        AMCs = ['HDFC AMC','Nippon MF', 'Bandhan MF', 'UTI MF','Aditya Birla Sun Life MF']
        #AMCs = ['HDFC AMC','Nippon MF', 'IDFC MF', 'UTI MF','ICICI Prudential MF','SBI MF','Aditya Birla Sun Life MF','DSP MF','Kotak Mahindra MF','L&T MF']
        # AMCs = ['Bandhan MF']
        for AMC_Name in AMCs:
            print(AMC_Name)
            query = "Select Common_Scheme_Name as Scheme_Name, Relevant_Date, sum(Daily_AUM) as AUM_Cr from AMC_DAILY_AUM_CLEAN_DATA Where AMC_Name = '" +AMC_Name+ "' and  Relevant_Date >= '2019-04-01'  and Common_Scheme_Name IS Not Null group By Common_Scheme_Name, Relevant_Date ORDER BY Relevant_Date"
            daily = pd.read_sql(query, con=engine)

            query = "Select Common_Scheme_Name as Scheme_Name, Relevant_Date, sum(Total_AAUM_Cr) as AUM_Cr from AMC_MONTHLY_AAUM_CLEAN_DATA Where AMC_Name = '" +AMC_Name+ "' and  Relevant_Date > '2019-03-01'  and Common_Scheme_Name IS Not Null group By Common_Scheme_Name, Relevant_Date  ORDER BY Relevant_Date "
            monthly = pd.read_sql(query, con=engine)

            # Monthly Interpoation
            monthly = (monthly.set_index('Relevant_Date').groupby(['Scheme_Name'])
                         .apply(lambda x: x.reindex(pd.date_range(min(monthly.Relevant_Date),
                                                                  max(monthly.Relevant_Date),
                                                                  freq='M'))).drop({'Scheme_Name'}, axis=1).reset_index('Scheme_Name'))
            monthly.reset_index(inplace=True)
            monthly.rename(columns={'index':'Relevant_Date'}, inplace=True)
            if AMC_Name == 'Nippon MF':
                pass
            else:
                monthly['AUM_Cr'] = monthly['AUM_Cr'].fillna(0)
            monthly['Relevant_Date'] = monthly['Relevant_Date']-15*days

            # merging monthly and daily data, deduping and interpolating
            all_data = pd.DataFrame()
            all_data = pd.concat([daily,monthly])
            all_data['Relevant_Date'] =  pd.to_datetime(all_data['Relevant_Date'], format='%Y-%m-%d')
            all_data.drop_duplicates(subset=['Relevant_Date', 'Scheme_Name'], inplace=True)


            filled_df = (all_data.set_index('Relevant_Date').groupby(['Scheme_Name'])
                         .apply(lambda d: d.reindex(pd.date_range(min(all_data.Relevant_Date),
                                                                  max(all_data.Relevant_Date),
                                                                  freq='D'))).drop({'Scheme_Name'}, axis=1).reset_index('Scheme_Name'))
            filled_df.reset_index(inplace=True)
            filled_df.rename(columns={'index':'Relevant_Date'}, inplace=True)

            filled_df['AUM_Cr'] = filled_df.groupby('Scheme_Name')['AUM_Cr'].apply(lambda x: x.interpolate(method='linear', limit_direction ='forward'))
            filled_df['AUM_Cr'] = filled_df.groupby('Scheme_Name')['AUM_Cr'].apply(lambda x: x.interpolate(method='nearest', limit_direction ='forward'))
            

            #filled_df = filled_df[filled_df['AUM_Cr'].notnull()]




            # Management fee interpolation
            query = "Select Common_Scheme_Name as Scheme_Name, Relevant_Date, Mgmt_Fee from AMC_MGMT_FEE_HALF_YEARLY_CLEAN_DATA where AMC_Name = '" +AMC_Name+ "' AND Relevant_Date >= '2018-12-31' and Common_Scheme_Name IS NOT NULL  and month(Relevant_Date) IN (3,6,9,12) AND day(Relevant_Date) in (30,31)  group By Common_Scheme_Name, Relevant_Date"
            mgmt_fee = pd.read_sql(query, con=engine)

            mgmt_fee = (mgmt_fee.set_index('Relevant_Date').groupby(['Scheme_Name'])
                         .apply(lambda x: x.reindex(pd.date_range(datetime.datetime(2018, 12, 31),#hard code required
                                                                  max(all_data.Relevant_Date),
                                                                  freq='D'))).drop({'Scheme_Name'}, axis=1).reset_index('Scheme_Name'))
            mgmt_fee.reset_index(inplace=True)
            mgmt_fee.rename(columns={'index':'Relevant_Date'}, inplace=True)

            print(AMC_Name)

            if(AMC_Name == 'NO') :
                #mgmt_fee['Month'] = mgmt_fee['Relevant_Date'].apply(lambda x:x.month)
                #mgmt_fee['Day'] = mgmt_fee['Relevant_Date'].apply(lambda x:x.day)
                #mgmt_fee['Relevant_Date'] = np.where(((mgmt_fee['Month'] == 12) & (mgmt_fee['Day'] == 31)), mgmt_fee['Relevant_Date']+90*days, mgmt_fee['Relevant_Date'])
                #mgmt_fee['Relevant_Date'] = np.where(((mgmt_fee['Month'] == 6)  & (mgmt_fee['Day'] == 30)), mgmt_fee['Relevant_Date']+90*days, mgmt_fee['Relevant_Date'])
                #mgmt_fee = mgmt_fee[['Scheme_Name','Relevant_Date','Mgmt_Fee']]

                #mgmt_fee['Mgmt_Fee'] = mgmt_fee.groupby('Scheme_Name')['Mgmt_Fee'].apply(lambda x: x.interpolate(method='linear', limit_direction ='forward'))
                mgmt_fee = mgmt_fee.groupby(mgmt_fee.Scheme_Name).apply(lambda g: g.bfill())
                mgmt_fee = mgmt_fee.groupby(mgmt_fee.Scheme_Name).apply(lambda g: g.ffill())
            else :
                mgmt_fee['Mgmt_Fee'] = mgmt_fee.groupby('Scheme_Name')['Mgmt_Fee'].apply(lambda x: x.interpolate(method='linear', limit_direction ='forward'))
                #mgmt_fee = mgmt_fee.groupby(mgmt_fee.Scheme_Name).apply(lambda g: g.ffill())
                mgmt_fee = mgmt_fee.groupby(mgmt_fee.Scheme_Name).apply(lambda g: g.bfill())




            #filled_df = filled_df[filled_df['Mgmt_Fee'].notnull()]
            filled_df = pd.merge(filled_df, mgmt_fee, on=['Scheme_Name','Relevant_Date'], how='left')

            filled_df = filled_df[filled_df['Relevant_Date'] >= pd.to_datetime(datetime.date(2019, 4, 1))] #hard code required
            filled_df['Revenue_Cr'] = filled_df['AUM_Cr']*filled_df['Mgmt_Fee']/365

            query = "Select Common_Scheme_Name as Scheme_Name, Scheme_Type from AMC_MONTHLY_AAUM_CLEAN_DATA where AMC_Name = '" +AMC_Name+ "' AND Relevant_Date >= '2018-12-31' and Common_Scheme_Name IS NOT NULL group By Common_Scheme_Name"
            scheme_type = pd.read_sql(query, con=engine)

            filled_df = pd.merge(filled_df, scheme_type, on=['Scheme_Name'], how='left')

            filled_df['AMC_Name'] = AMC_Name
            filled_df['Segment'] = 'Management Fees'
            filled_df['Comments'] = ''
            filled_df['Runtime'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
            filled_df['Scheme_Type']=filled_df['Scheme_Type'].fillna('Other')
            print(AMC_Name)
            query =  "Delete From AMC_SEGMENT_WISE_DAILY_REVENUE where Segment = 'Management Fees' and AMC_Name = '" + AMC_Name + "'"
            connection.execute(query)
            connection.execute('commit')

            filled_df['Scheme_Type']=filled_df['Scheme_Type'].fillna('Other')
            df1=filled_df[~(filled_df['Scheme_Type']=='')]
            df2=filled_df[filled_df['Scheme_Type']=='']
            df2['Scheme_Type']='Other'
            filled_df=pd.concat([df1,df2])

            
            filled_df = applyCorrectionFactor(filled_df, AMC_Name, engine)

            filled_df.to_sql(name='AMC_SEGMENT_WISE_DAILY_REVENUE', con=engine, if_exists='append',index=False)

            ## ClickHouse
            query = "Select * from AMC_SEGMENT_WISE_DAILY_REVENUE Where AMC_Name = '" +AMC_Name+ "'"
            filled_df = pd.read_sql(query, con=engine)
            query="ALTER TABLE AdqvestDB.AMC_SEGMENT_WISE_DAILY_REVENUE DELETE WHERE Segment = 'Management Fees' and AMC_Name = '" + AMC_Name + "'"
            client.execute(query)
            client.execute("INSERT INTO AMC_SEGMENT_WISE_DAILY_REVENUE VALUES", filled_df.values.tolist())



        connection.close()

        AMC_Name =  'UTI MF'

        #For PMS Discretionary and Non-Discretionary
        query = "Select max(Relevant_Date) as Relevant_Date from AdqvestDB.AMC_SEGMENT_WISE_DAILY_REVENUE where AMC_Name = '" + AMC_Name + "'"
        uti = pd.read_sql(query, con=engine)

        query = "Select Relevant_Date, sum(Total) as AUM_Cr from AdqvestDB.PMS_PORTFOLIO_MONTHLY_DATA_STOPPED where Manager = 'UTI ASSET MANAGEMENT COMPANY PVT LTD' and Types_Of_Services = 'Discretionary Services' and Relevant_Date >= '2019-03-01' group by Relevant_Date"
        pms_dis_non_dis = pd.read_sql(query, con=engine)

        pms_dis_non_dis = (pms_dis_non_dis.set_index('Relevant_Date').apply(lambda x: x.reindex(pd.date_range(min(pms_dis_non_dis.Relevant_Date), max(uti.Relevant_Date), freq='D'))))
        # Daily Interpoation
        pms_dis_non_dis['AUM_Cr'] = pms_dis_non_dis.apply(lambda x: x.interpolate(method='linear', limit_direction ='forward'))

        pms_dis_non_dis.reset_index(inplace=True)
        pms_dis_non_dis.rename(columns={'index':'Relevant_Date'}, inplace=True)

        pms_dis_non_dis['Scheme_Name'] = 'PMS Discretionary Services and Non-Discretionary Services'
        pms_dis_non_dis['Mgmt_Fee'] = 0.0000375 #hard code
        pms_dis_non_dis['Revenue_Cr'] = pms_dis_non_dis['AUM_Cr']*pms_dis_non_dis['Mgmt_Fee']/365
        pms_dis_non_dis['AMC_Name'] = AMC_Name
        pms_dis_non_dis['Scheme_Type'] = 'PMS'
        pms_dis_non_dis['Segment'] = 'Management Fees'
        pms_dis_non_dis['Comments'] = ''
        pms_dis_non_dis['Runtime'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
        pms_dis_non_dis = pms_dis_non_dis[pms_dis_non_dis['Relevant_Date'] >= '2019-04-01']

        #For PMS Advisory
        query = "Select Relevant_Date, Total as AUM_Cr from AdqvestDB.PMS_ADVISORY_MONTHLY_DATA_STOPPED where Manager = 'UTI ASSET MANAGEMENT COMPANY PVT LTD' and  Advisory_Business = 'Value of Asset for which Advisory Services are being given (Amount in crores)' and  Relevant_Date >= '2019-03-01' group by Relevant_Date"
        pms_advisory = pd.read_sql(query, con=engine)

        pms_advisory = (pms_advisory.set_index('Relevant_Date').apply(lambda x: x.reindex(pd.date_range(min(pms_advisory.Relevant_Date), max(uti.Relevant_Date), freq='D'))))
        # Daily Interpoation
        pms_advisory['AUM_Cr'] = pms_advisory.apply(lambda x: x.interpolate(method='linear', limit_direction ='forward'))

        pms_advisory.reset_index(inplace=True)
        pms_advisory.rename(columns={'index':'Relevant_Date'}, inplace=True)

        pms_advisory['Scheme_Name'] = 'PMS Advisory'
        pms_advisory['Mgmt_Fee'] = 0.006277 #hard code
        pms_advisory['Revenue_Cr'] = pms_advisory['AUM_Cr']*pms_advisory['Mgmt_Fee']/365
        pms_advisory['AMC_Name'] = AMC_Name
        pms_advisory['Scheme_Type'] = 'PMS'
        pms_advisory['Segment'] = 'Management Fees'
        pms_advisory['Comments'] = ''
        pms_advisory['Runtime'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
        pms_advisory = pms_advisory[pms_advisory['Relevant_Date'] >= '2019-04-01']

        both = pd.concat([pms_dis_non_dis,pms_advisory], axis=0)

        connection = engine.connect()
        query =  "Delete From AMC_SEGMENT_WISE_DAILY_REVENUE where Segment = 'Management Fees' and AMC_Name = '" + AMC_Name + "' and Scheme_Type = 'PMS'"
        connection.execute(query)
        connection.execute('commit')
        both['Scheme_Type']=both['Scheme_Type'].fillna('Other')

        df1=both[~(both['Scheme_Type']=='')]
        df2=both[both['Scheme_Type']=='']
        both['Scheme_Type']='Other'
        both=pd.concat([df1,df2])

        both.to_sql(name='AMC_SEGMENT_WISE_DAILY_REVENUE', con=engine, if_exists='append',index=False)
        connection.close()

        query = "Select * from AMC_SEGMENT_WISE_DAILY_REVENUE where Segment = 'Management Fees' and AMC_Name = '" +AMC_Name+ "' and Scheme_Type = 'PMS'"
        both = pd.read_sql(query, con=engine)

        #ClickHouse
        query="ALTER TABLE AdqvestDB.AMC_SEGMENT_WISE_DAILY_REVENUE DELETE WHERE Segment = 'Management Fees' and AMC_Name = '" + AMC_Name + "' and Scheme_Type = 'PMS'"
        client.execute(query)
        client.execute("INSERT INTO AMC_SEGMENT_WISE_DAILY_REVENUE VALUES", both.values.tolist())

        log.job_end_log(table_name,job_start_time,no_of_ping)

    except:
        try:
            connection.close()
        except:
            pass
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        error_msg = str(sys.exc_info()[1]) + " line " + str(exc_tb.tb_lineno)
        print(error_type+ " " +error_msg)

        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)


# In[ ]:


if(__name__=='__main__'):
    run_program(run_by='manual')
