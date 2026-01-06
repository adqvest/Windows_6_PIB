# -*- coding: utf-8 -*-
"""
Created on Mon Aug  7 22:49:53 2023

@author: Santonu
"""

import xlsxwriter
import pandas as pd 
import sqlalchemy
from clickhouse_driver import Client
from datetime import datetime as strptime
import numpy as np
import os
import re
import datetime as datetime
from pytz import timezone
import sys
import warnings
warnings.filterwarnings('ignore')
#%%
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')

#%%
import ClickHouse_db
import adqvest_db
import JobLogNew as log
from adqvest_robotstxt import Robots
from clickhouse_driver import Client

robot = Robots(__file__)

engine = adqvest_db.db_conn()
connection = engine.connect()
client = ClickHouse_db.db_conn()

#%%
# code you want to evaluate
#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days

def get_df(q1,col_list):
    result = client.execute(q1)
    df = pd.DataFrame(result, columns=col_list)
    return df

def convert_date_format(input_date,output_format='%B-%y',input_format='%Y-%m-%d'):
    input_datetime = strptime.strptime(str(input_date), input_format)
    output_date = input_datetime.strftime(output_format)

    return output_date

def row_modificator(df,l1,col_idx,row_del=False,keep_row=False,update_row=False):
  from collections import ChainMap
  keep_inx=[]
  print(type(l1[0]))
  if isinstance(l1[0],dict):
      l1=dict(ChainMap(*l1))
  else:
      l1=dict.fromkeys(l1,np.nan)
    
  for i in l1.keys():
    df = df.reset_index(drop=True)
    r=df[df.iloc[:,col_idx].str.lower().str.contains(i.lower())==True].index.to_list()
    if (keep_row==True):
        keep_inx.append(r[0])
    
    if row_del==True:
        df.drop(index=r,inplace=True)
        df[df.columns[col_idx]]=df[df.columns[col_idx]].str.strip()
    else:
        if (update_row==True):
            for j in r:
                   print(r)
                   df.iloc[j,col_idx]=l1[i]
                   df[df.columns[col_idx]]=df[df.columns[col_idx]].str.strip()
            
  if(keep_row==True):
        drop_list=[i for i in df.index if i not in keep_inx]
        df.drop(index=drop_list,inplace=True)
               
  df.reset_index(drop=True,inplace=True)    
  return df 

def format_data_frame(df_c,l1):
    df_c['Relevant_Date']=df_c['Relevant_Date'].apply(lambda x:convert_date_format(x))
    dt=df_c['Relevant_Date'].tolist()[0]
    
    df=df_c.T
    df.columns=['Curr','Prev']
    df['Var']=df.index
    df.index = range(len(df.index))
    df=row_modificator(df,l1,2,update_row=True)
    df = df[[df.columns[2],df.columns[1],df.columns[0]]]
           
    indx= pd.DataFrame([df.iloc[0,:]])
    df=df.iloc[1:,:]
    df['Prev']=np.where(df.iloc[:,0].str.lower().str.contains('cr'), df['Prev']/10000000,df['Prev'])              
    df['Prev']=np.where(df.iloc[:,0].str.lower().str.contains('million'),df['Prev']/1000000,df['Prev'])
    df['Prev']=df['Prev'].apply(lambda x:round(x,2))
    
    
    df['Curr']=np.where(df.iloc[:,0].str.lower().str.contains('cr'),df['Curr']/10000000,df['Curr'])
    df['Curr']=np.where(df.iloc[:,0].str.lower().str.contains('million'),df['Curr']/1000000,df['Curr'])                 
    df['Curr']=df['Curr'].apply(lambda x:round(x,2))
    
    
    df['Growth (%)']=np.where(df[df.columns[2]]!=dt,((df.iloc[:,2]-df.iloc[:,1])/df.iloc[:,1])*100,np.nan)
    df['Growth (%)']=df['Growth (%)'].apply(lambda x:round(x,2))
    
    new_record = pd.DataFrame([['',np.nan,np.nan,np.nan]], columns=df.columns)
    
    df = pd.concat([indx, df,new_record], ignore_index=True)
    df.iloc[0,-1]='Growth (%)'
    return df


def make_excel_file(df_list):
    l=len(df_list)
    writer = pd.ExcelWriter("NIIF_IHMCL_Monthly_Report.xlsx", engine="xlsxwriter")
    
    for i in range(0,len(df_list)):
        if i==0:
            clo_st=1
        if i>0:
            clo_st=1+df_list[i].shape[1]+2
        
        
        df_list[i].to_excel(writer, sheet_name="Sheet_", startrow=1,startcol=clo_st, header=False,index=False)
        workbook  = writer.book
        worksheet = writer.sheets['Sheet_']
        header_format = workbook.add_format({'bold': True})
        worksheet.set_column(clo_st, clo_st, 60, header_format)
        
    writer.close()
    print('All Done')
#%%


## job log details
job_start_time = datetime.datetime.now(india_time)
table_name = 'NIIF_IHMCL_REPORT'

no_of_ping = 0
def run_program(run_by='Adqvest_Bot', py_file_name=None):
    global no_of_ping
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
            
        #%%
        os.chdir( "C:/Users/Administrator/AdQvestDir/NIIF_HEAT_MAPS")
        missing_month=['July',"August"]
        
        query=f"select max(Relevant_Date) as RD from  IHMCL_ETC_TRANSACTION_REPORTS_MONTHLY_DATA_MAPPED_FINAL where Relevant_Date='2023-04-30'"
        query=f"select max(Relevant_Date) as RD from  IHMCL_ETC_TRANSACTION_REPORTS_MONTHLY_DATA_MAPPED_FINAL"

        db_max_date = pd.read_sql(query,engine)["RD"][0]
        
        
        
        
        plz_mum_curr="""select count(*) as total from mv_ihmcl_curr where Relevant_Date=(select max(Relevant_Date) from mv_ihmcl_curr);"""
        plz_mum_map="""select count(*) as total from mv_ihmcl_base_data where Relevant_Date=(select max(Relevant_Date) from mv_ihmcl_base_data);"""
        plz_mum_last="""Select count(*) as total from mv_ihmcl_base_data_old;"""

        ##############################################################Temporaray removed################################
        if db_max_date.strftime('%B')  in missing_month:
            plz_mum_last="""Select count(*) as total from mv_ihmcl_base_data_old;"""
        else:
            plz_mum_last="""Select count(*) as total from mv_ihmcl_curr WHERE Relevant_Date = date_sub(MONTH,12,(SELECT MAX(Relevant_Date) FROM mv_ihmcl_curr));"""
        #################################################################################################################
        plz_mum_curr=get_df(plz_mum_curr,['total']).total.tolist()[0]
        plz_mum_last=get_df(plz_mum_last,['total']).total.tolist()[0]
        plz_mum_map=get_df(plz_mum_map,['total']).total.tolist()[0]
        
        
        # Overall: 
        q1="""Select Relevant_Date,sum(Total_Txn_Amount) as overall_col,sum(Total_Trncnt) as overall_tranx,sum(Total_Txn_Amount)/sum(Total_Trncnt) as Val_per_trnx
        	  from mv_ihmcl_curr WHERE Relevant_Date = date_sub(MONTH,0,(SELECT MAX(Relevant_Date) FROM mv_ihmcl_curr))
        	  group by Relevant_Date ORDER BY Relevant_Date Asc;"""
              
        q2="""Select Relevant_Date,sum(Total_Txn_Amount) as overall_col,sum(Total_Trncnt) overall_tranx,sum(Total_Txn_Amount)/sum(Total_Trncnt) as Val_per_trnx
        	  from mv_ihmcl_curr WHERE Relevant_Date = date_sub(MONTH,12,(SELECT MAX(Relevant_Date) FROM mv_ihmcl_curr))
        	  group by Relevant_Date ORDER BY Relevant_Date Asc;"""
              
              
        q2="""Select Relevant_Date,sum(Total_Txn_Amount) as overall_col,sum(Total_Trncnt) as overall_tranx,sum(Total_Txn_Amount)/sum(Total_Trncnt) as Val_per_trnx
       	  from mv_ihmcl_base_data_old WHERE Relevant_Date = date_sub(MONTH,0,(SELECT MAX(Relevant_Date) FROM mv_ihmcl_base_data_old))
       	  group by Relevant_Date ORDER BY Relevant_Date Asc;"""
              
        df1=get_df(q1,['Relevant_Date','overall_col','overall_tranx','Val_per_trnx'])
        df2=get_df(q2,['Relevant_Date','overall_col','overall_tranx','Val_per_trnx'])

        # Overall Map Collection:
        q3="""Select Relevant_Date,sum(Total_Txn_Amount) as overall_map_col,sum(Total_Trncnt) as overall_map_tranx,sum(Total_Txn_Amount)/sum(Total_Trncnt) as overall_map_val_per_trnx from mv_ihmcl_base_data WHERE Relevant_Date =(SELECT MAX(Relevant_Date) FROM mv_ihmcl_base_data) group by Relevant_Date ORDER BY Relevant_Date Asc;"""

        q4="""Select T2.Relevant_Date as Relevant_Date,sum(T2.Total_Txn_Amount) as overall_map_col,sum(T2.Total_Trncnt) as overall_map_tranx,sum(T2.Total_Txn_Amount)/sum(T2.Total_Trncnt) as overall_map_val_per_trnx 
        	  from mv_ihmcl_base_data WHERE T2.Relevant_Date =(SELECT MAX(T2.Relevant_Date) FROM mv_ihmcl_base_data) group by T2.Relevant_Date ORDER BY T2.Relevant_Date Asc;"""

        df3=get_df(q3,['Relevant_Date','overall_map_col','overall_map_tranx','overall_map_val_per_trnx'])
        df4=get_df(q4,['Relevant_Date','overall_map_col','overall_map_tranx','overall_map_val_per_trnx'])


        df1=pd.merge(pd.concat([df1,df2]), pd.concat([df3,df4]), on='Relevant_Date', how='inner')
        print(df1['Relevant_Date'])
        date=convert_date_format(df1['Relevant_Date'].tolist()[0]).replace('-', " '")


        #Collection Overall
        df_o_c=df1[['Relevant_Date','overall_col','overall_map_col','Val_per_trnx','overall_map_val_per_trnx']]
        df_o_t=df1[['Relevant_Date','overall_tranx','overall_map_tranx','Val_per_trnx','overall_map_val_per_trnx']]


        l1=[{'Relevant_Date':"Growth Toll Collections Overall (% yoy), "+date},
            {"overall_col":f'Total Collections In All Toll Plazas : INR Cr ({plz_mum_last} Plazas), ({plz_mum_curr} Plazas)'},
            {"overall_map_col":f'Total Collections In Mapped Toll Plazas : INR Cr ({plz_mum_map} Plazas)'},
            {"Val_per_trnx":'Value Per Transaction : INR'},
            {"overall_map_val_per_trnx":'Value Per Transaction in Mapped : INR'}]
        df_o_c=format_data_frame(df_o_c,l1)

        l2=[{'Relevant_Date':"Growth Toll Traffic Overall (% yoy), "+date},
            {"overall_tranx":f'Total Transactions In All Toll Plazas : Million ({plz_mum_last} Plazas), ({plz_mum_curr} Plazas)'},
            {"overall_map_tranx":f'Total Transactions In Mapped Toll Plazas : Million ({plz_mum_map})'},
            {"Val_per_trnx":'Value Per Transaction : INR'},
            {"overall_map_val_per_trnx":'Value Per Transaction in Mapped : INR'}]
        df_o_t=format_data_frame(df_o_t,l2)

        #%%
        # Non Commercial:

        q5="""select Relevant_Date,Ncom_map_col,Ncom_map_tranx,Ncom_map_col/Ncom_map_tranx  as Ncom_map_Val_per_tranx
           	from (select sum(Car_Jeep_Van_Txn_Amount +Bus_2_axle_Txn_Amount+Bus_3_axle_Txn_Amount+Mini_Bus_Txn_Amount) as Ncom_map_col,
           	sum(Car_Jeep_Van_Trncnt +Bus_2_axle_Trncnt+Bus_3_axle_Trncnt+Mini_Bus_Trncnt) as Ncom_map_tranx,
           	Relevant_Date from mv_ihmcl_base_data  WHERE Relevant_Date =(SELECT MAX(Relevant_Date) FROM mv_ihmcl_base_data) group by Relevant_Date);"""

        q6="""select T2.Relevant_Date as Relevant_Date,Ncom_map_col,Ncom_map_tranx,Ncom_map_col/Ncom_map_tranx  as Ncom_map_Val_per_tranx
        	from (select sum(T2.Car_Jeep_Van_Txn_Amount +T2.Bus_2_axle_Txn_Amount+T2.Bus_3_axle_Txn_Amount+T2.Mini_Bus_Txn_Amount) as Ncom_map_col,
        	sum(T2.Car_Jeep_Van_Trncnt +T2.Bus_2_axle_Trncnt+T2.Bus_3_axle_Trncnt+T2.Mini_Bus_Trncnt) as Ncom_map_tranx,
        	T2.Relevant_Date from mv_ihmcl_base_data  WHERE T2.Relevant_Date =(SELECT MAX(T2.Relevant_Date) FROM mv_ihmcl_base_data) group by T2.Relevant_Date);"""


        df5=get_df(q5,['Relevant_Date','Ncom_map_col','Ncom_map_tranx','Ncom_map_Val_per_tranx'])
        df6=get_df(q6,['Relevant_Date','Ncom_map_col','Ncom_map_tranx','Ncom_map_Val_per_tranx'])

        q7="""select Relevant_Date,Ncom_overall_col,Ncom_overall_tranx,Ncom_overall_col/Ncom_overall_tranx  as Ncom_overall_Val_per_tranx
        	from (select sum(Car_Jeep_Van_Txn_Amount+Bus_2_axle_Txn_Amount+Bus_3_axle_Txn_Amount+Mini_Bus_Txn_Amount) as Ncom_overall_col,
        	sum(Car_Jeep_Van_Trncnt +Bus_2_axle_Trncnt+Bus_3_axle_Trncnt+Mini_Bus_Trncnt) as Ncom_overall_tranx,
        	Relevant_Date from mv_ihmcl_curr  
        	WHERE Relevant_Date = date_sub(MONTH,0,(SELECT MAX(Relevant_Date) FROM mv_ihmcl_curr))
        	group by Relevant_Date);"""
            
        q8="""select Relevant_Date,Ncom_overall_col,Ncom_overall_tranx,Ncom_overall_col/Ncom_overall_tranx  as Ncom_overall_Val_per_tranx
        	from (select sum(Car_Jeep_Van_Txn_Amount+Bus_2_axle_Txn_Amount+Bus_3_axle_Txn_Amount+Mini_Bus_Txn_Amount) as Ncom_overall_col,
        	sum(Car_Jeep_Van_Trncnt +Bus_2_axle_Trncnt+Bus_3_axle_Trncnt+Mini_Bus_Trncnt) as Ncom_overall_tranx,
        	Relevant_Date from mv_ihmcl_curr  
        	WHERE Relevant_Date = date_sub(MONTH,12,(SELECT MAX(Relevant_Date) FROM mv_ihmcl_curr))
        	group by Relevant_Date);"""
            
        
        q8="""select Relevant_Date,Ncom_overall_col,Ncom_overall_tranx,Ncom_overall_col/Ncom_overall_tranx  as Ncom_overall_Val_per_tranx
        	from (select sum(Car_Jeep_Van_Txn_Amount+Bus_2_axle_Txn_Amount+Bus_3_axle_Txn_Amount+Mini_Bus_Txn_Amount) as Ncom_overall_col,
        	sum(Car_Jeep_Van_Trncnt +Bus_2_axle_Trncnt+Bus_3_axle_Trncnt+Mini_Bus_Trncnt) as Ncom_overall_tranx,
        	Relevant_Date from mv_ihmcl_base_data_old  
        	WHERE Relevant_Date = date_sub(MONTH,0,(SELECT MAX(Relevant_Date) FROM mv_ihmcl_base_data_old))
        	group by Relevant_Date);"""
            
        df7=get_df(q7,['Relevant_Date','Ncom_overall_col','Ncom_overall_tranx','Ncom_overall_Val_per_tranx'])
        df8=get_df(q8,['Relevant_Date','Ncom_overall_col','Ncom_overall_tranx','Ncom_overall_Val_per_tranx'])


        df2=pd.merge(pd.concat([df5,df6]), pd.concat([df7,df8]), on='Relevant_Date', how='inner')

        df_non_com_c=df2[['Relevant_Date','Ncom_overall_col','Ncom_map_col','Ncom_overall_Val_per_tranx','Ncom_map_Val_per_tranx']]
        df_non_com_t=df2[['Relevant_Date','Ncom_overall_tranx','Ncom_map_tranx','Ncom_overall_Val_per_tranx','Ncom_map_Val_per_tranx']]


        l3=[{'Relevant_Date':"Growth in Toll Collection of Non-Commercial Vehicles (% yoy), "+date},
            {"Ncom_overall_col":f'Total Collections In All Toll Plazas : INR Cr ({plz_mum_last} Plazas), ({plz_mum_curr} Plazas)'},
            {"Ncom_map_col":f'Total Collections In Mapped Toll Plazas : INR Cr ({plz_mum_map} Plazas)'},
            {"Ncom_overall_Val_per_tranx":'Value Per Transaction : INR'},
            {"Ncom_map_Val_per_tranx":'Value Per Transaction in Mapped : INR'}]
        df_non_com_c=format_data_frame(df_non_com_c,l3)


        l4=[{'Relevant_Date':"Growth in Toll Traffic of Non-Commercial Vehicles  (% yoy), "+date},
            {"Ncom_overall_tranx":f'Total Transactions In All Toll Plazas : Million ({plz_mum_last} Plazas), ({plz_mum_curr} Plazas)'},
            {"Ncom_map_tranx":f'Total Transactions In Mapped Toll Plazas : Million ({plz_mum_map})'},
            {"Val_per_trnx":'Value Per Transaction : INR'},
            {"overall_map_val_per_trnx":'Value Per Transaction in Mapped : INR'}]

        df_non_com_t=format_data_frame(df_non_com_t,l4)

        #%%
        # Commercial:
        q9="""select Relevant_Date,com_map_col,com_map_tranx,com_map_col/com_map_tranx  as com_map_Val_per_tranx
        	from (select sum(Total_Txn_Amount-Car_Jeep_Van_Txn_Amount-Bus_2_axle_Txn_Amount-Bus_3_axle_Txn_Amount-Mini_Bus_Txn_Amount) as com_map_col,sum(Total_Trncnt-Car_Jeep_Van_Trncnt-Bus_2_axle_Trncnt-Bus_3_axle_Trncnt-Mini_Bus_Trncnt) as com_map_tranx,
        	Relevant_Date from mv_ihmcl_base_data  WHERE Relevant_Date =(SELECT MAX(Relevant_Date) FROM mv_ihmcl_base_data) group by Relevant_Date);"""

        q10="""select T2.Relevant_Date as Relevant_Date,com_map_col,com_map_tranx,com_map_col/com_map_tranx  as com_map_Val_per_tranx
        	from (select sum(T2.Total_Txn_Amount-T2.Car_Jeep_Van_Txn_Amount -T2.Bus_2_axle_Txn_Amount-T2.Bus_3_axle_Txn_Amount-T2.Mini_Bus_Txn_Amount) as com_map_col,
        	sum(T2.Total_Trncnt-T2.Car_Jeep_Van_Trncnt -T2.Bus_2_axle_Trncnt-T2.Bus_3_axle_Trncnt-T2.Mini_Bus_Trncnt) as com_map_tranx,
        	T2.Relevant_Date from mv_ihmcl_base_data  WHERE T2.Relevant_Date =(SELECT MAX(T2.Relevant_Date) FROM mv_ihmcl_base_data) group by T2.Relevant_Date);"""

        df9=get_df(q9,['Relevant_Date','com_map_col','com_map_tranx','com_map_Val_per_tranx'])
        df10=get_df(q10,['Relevant_Date','com_map_col','com_map_tranx','com_map_Val_per_tranx'])

        q11="""select Relevant_Date,com_overall_col,com_overall_tranx,com_overall_col/com_overall_tranx  as com_overall_Val_per_tranx
        	from (select sum(Total_Txn_Amount-Car_Jeep_Van_Txn_Amount-Bus_2_axle_Txn_Amount-Bus_3_axle_Txn_Amount-Mini_Bus_Txn_Amount) as com_overall_col,sum(Total_Trncnt-Car_Jeep_Van_Trncnt-Bus_2_axle_Trncnt-Bus_3_axle_Trncnt-Mini_Bus_Trncnt) as com_overall_tranx,
        	Relevant_Date from mv_ihmcl_curr  
        	WHERE Relevant_Date = date_sub(MONTH,0,(SELECT MAX(Relevant_Date) FROM mv_ihmcl_curr)) group by Relevant_Date);"""

        q12="""select Relevant_Date,com_overall_col,com_overall_tranx,com_overall_col/com_overall_tranx  as com_overall_Val_per_tranx
        	from (select sum(Total_Txn_Amount-Car_Jeep_Van_Txn_Amount-Bus_2_axle_Txn_Amount-Bus_3_axle_Txn_Amount-Mini_Bus_Txn_Amount) as com_overall_col,sum(Total_Trncnt-Car_Jeep_Van_Trncnt-Bus_2_axle_Trncnt-Bus_3_axle_Trncnt-Mini_Bus_Trncnt) as com_overall_tranx,
        	Relevant_Date from mv_ihmcl_curr  
        	WHERE Relevant_Date = date_sub(MONTH,12,(SELECT MAX(Relevant_Date) FROM mv_ihmcl_curr)) group by Relevant_Date);"""
            
        
        q12="""select Relevant_Date,com_overall_col,com_overall_tranx,com_overall_col/com_overall_tranx  as com_overall_Val_per_tranx
        	from (select sum(Total_Txn_Amount-Car_Jeep_Van_Txn_Amount-Bus_2_axle_Txn_Amount-Bus_3_axle_Txn_Amount-Mini_Bus_Txn_Amount) as com_overall_col,sum(Total_Trncnt-Car_Jeep_Van_Trncnt-Bus_2_axle_Trncnt-Bus_3_axle_Trncnt-Mini_Bus_Trncnt) as com_overall_tranx,
        	Relevant_Date from mv_ihmcl_base_data_old  
        	WHERE Relevant_Date = date_sub(MONTH,0,(SELECT MAX(Relevant_Date) FROM mv_ihmcl_base_data_old)) group by Relevant_Date);"""

        df11=get_df(q11,['Relevant_Date','com_overall_col','com_overall_tranx','com_overall_Val_per_tranx'])
        df12=get_df(q12,['Relevant_Date','com_overall_col','com_overall_tranx','com_overall_Val_per_tranx'])


        df3=pd.merge(pd.concat([df9,df10]), pd.concat([df11,df12]), on='Relevant_Date', how='inner')


        df_com_c=df3[['Relevant_Date','com_overall_col','com_map_col','com_overall_Val_per_tranx','com_map_Val_per_tranx']]
        df_com_t=df3[['Relevant_Date','com_overall_tranx','com_map_tranx','com_overall_Val_per_tranx','com_map_Val_per_tranx']]


        l3=[{'Relevant_Date':"Growth in Toll Collections of Commercial Vehicles (% yoy), "+date},
            {"com_overall_col":f'Total Collections In All Toll Plazas : INR Cr ({plz_mum_last} Plazas), ({plz_mum_curr} Plazas)'},
            {"com_map_col":f'Total Collections In Mapped Toll Plazas : INR Cr ({plz_mum_map} Plazas)'},
            {"com_overall_Val_per_tranx":'Value Per Transaction : INR'},
            {"com_map_Val_per_tranx":'Value Per Transaction in Mapped : INR'}]

        df_com_c=format_data_frame(df_com_c,l3)
        l4=[{'Relevant_Date':"Growth in Toll Traffic of Commercial Vehicles (% yoy), "+date},
            {"com_overall_tranx":f'Total Transactions In All Toll Plazas : Million ({plz_mum_last} Plazas), ({plz_mum_curr} Plazas)'},
            {"com_map_tranx":f'Total Transactions In Mapped Toll Plazas : Million ({plz_mum_map})'},
            {"com_overall_Val_per_tranx":'Value Per Transaction : INR'},
            {"com_map_Val_per_tranx":'Value Per Transaction in Mapped : INR'}]

        df_com_t=format_data_frame(df_com_t,l4)
        #%%

        # Consinerire:
        plz_mum_curr="""select count(*) as total from mv_ihmcl_curr where Relevant_Date=(select max(Relevant_Date) from mv_ihmcl_curr) and Type = 'Conc.';"""
        plz_mum_map="""select count(*) as total from mv_ihmcl_base_data where Relevant_Date=(select max(Relevant_Date) from mv_ihmcl_base_data) and Type = 'Conc.';"""
        plz_mum_last="""Select count(*) as total from mv_ihmcl_base_data_old where Type = 'Conc.';"""

        ########################Temporary Removed############################################################
        if db_max_date.strftime('%B')  in missing_month:
            plz_mum_last="""Select count(*) as total from mv_ihmcl_base_data_old where Type = 'Conc.';"""
        else:
            plz_mum_last="""Select count(*) as total from mv_ihmcl_curr WHERE Relevant_Date = date_sub(MONTH,12,(SELECT MAX(Relevant_Date) FROM mv_ihmcl_curr)) and Type = 'Conc.';"""
        ###################################################################################################################

        plz_mum_curr_c=get_df(plz_mum_curr,['total']).total.tolist()[0]
        plz_mum_last_c=get_df(plz_mum_last,['total']).total.tolist()[0]
        plz_mum_map_c=get_df(plz_mum_map,['total']).total.tolist()[0]

        # Overall Map Collection Consinerire:

        q13="""select Relevant_Date,overall_map_col,overall_map_tranx,overall_map_col/overall_map_tranx as overall_map_val_per_trnx from (Select Relevant_Date,sum(Total_Txn_Amount) as overall_map_col,sum(Total_Trncnt) as overall_map_tranx
        	from mv_ihmcl_base_data WHERE Relevant_Date =(SELECT MAX(Relevant_Date) FROM mv_ihmcl_base_data) and Type = 'Conc.' 
        	group by Relevant_Date ORDER BY Relevant_Date Asc);"""

        q14="""select T2.Relevant_Date as Relevant_Date,overall_map_col,overall_map_tranx,overall_map_col/overall_map_tranx as overall_map_val_per_trnx 
        	from (Select T2.Relevant_Date,sum(T2.Total_Txn_Amount) as overall_map_col,sum(T2.Total_Trncnt) as overall_map_tranx
        	from mv_ihmcl_base_data WHERE T2.Relevant_Date =(SELECT MAX(T2.Relevant_Date) FROM mv_ihmcl_base_data) and Type = 'Conc.' 
        	group by T2.Relevant_Date ORDER BY T2.Relevant_Date Asc);"""

        df13=get_df(q13,['Relevant_Date','overall_map_col','overall_map_tranx','overall_map_val_per_trnx'])
        df14=get_df(q14,['Relevant_Date','overall_map_col','overall_map_tranx','overall_map_val_per_trnx'])


        q15="""select Relevant_Date,overall_col,overall_tranx,overall_col/overall_tranx as Overall_Val_per_trnx 
        	from (Select Relevant_Date,sum(Total_Txn_Amount) as overall_col,sum(Total_Trncnt) as overall_tranx
        	from mv_ihmcl_curr 
        	WHERE Relevant_Date = date_sub(MONTH,0,(SELECT MAX(Relevant_Date) FROM mv_ihmcl_curr)) and Type = 'Conc.' 
        	group by Relevant_Date ORDER BY Relevant_Date Asc);"""
            
        q16="""select Relevant_Date,overall_col,overall_tranx,overall_col/overall_tranx as Overall_Val_per_trnx 
        	from (Select Relevant_Date,sum(Total_Txn_Amount) as overall_col,sum(Total_Trncnt) as overall_tranx
        	from mv_ihmcl_curr 
        	WHERE Relevant_Date = date_sub(MONTH,12,(SELECT MAX(Relevant_Date) FROM mv_ihmcl_curr)) and Type = 'Conc.' 
        	group by Relevant_Date ORDER BY Relevant_Date Asc);"""
            
        q16="""select Relevant_Date,overall_col,overall_tranx,overall_col/overall_tranx as Overall_Val_per_trnx 
         	from (Select Relevant_Date,sum(Total_Txn_Amount) as overall_col,sum(Total_Trncnt) as overall_tranx
         	from mv_ihmcl_base_data_old 
         	WHERE Relevant_Date = date_sub(MONTH,0,(SELECT MAX(Relevant_Date) FROM mv_ihmcl_base_data_old)) and Type = 'Conc.' 
         	group by Relevant_Date ORDER BY Relevant_Date Asc);"""
            
        df15=get_df(q15,['Relevant_Date','overall_col','overall_tranx','Overall_Val_per_trnx'])
        df16=get_df(q16,['Relevant_Date','overall_col','overall_tranx','Overall_Val_per_trnx'])



        df4=pd.merge(pd.concat([df13,df14]), pd.concat([df15,df16]), on='Relevant_Date', how='inner')


        df_o_conc_c=df4[['Relevant_Date','overall_col','overall_map_col','Overall_Val_per_trnx','overall_map_val_per_trnx']]
        df_o_conc_t=df4[['Relevant_Date','overall_tranx','overall_map_tranx','Overall_Val_per_trnx','overall_map_val_per_trnx']]


        l5=[{'Relevant_Date':"Growth in Toll Collections of Overall Conc Vehicles (% yoy), "+date},
            {"overall_col":f'Total Collections In All Toll Plazas : INR Cr ({plz_mum_last_c} Plazas), ({plz_mum_curr_c} Plazas)'},
            {"overall_map_col":f'Total Collections In Mapped Toll Plazas : INR Cr ({plz_mum_map_c} Plazas)'},
            {"Overall_Val_per_trnx":'Value Per Transaction : INR'},
            {"overall_map_val_per_trnx":'Value Per Transaction in Mapped : INR'}]

        df_o_conc_c=format_data_frame(df_o_conc_c,l5)


        l6=[{'Relevant_Date':"Growth in Toll Traffic of Overall Conc Vehicles (% yoy), "+date},
            {"overall_tranx":f'Total Transactions In All Toll Plazas : Million ({plz_mum_last_c} Plazas), ({plz_mum_curr_c} Plazas)'},
            {"overall_map_tranx":f'Total Transactions In Mapped Toll Plazas : Million ({plz_mum_map_c})'},
            {"Overall_Val_per_trnx":'Value Per Transaction : INR'},
            {"overall_map_val_per_trnx":'Value Per Transaction in Mapped : INR'}]

        df_o_conc_t=format_data_frame(df_o_conc_t,l6)
        #%%
        # Overall Map Collection Non-Consinerire:
        
        plz_mum_curr="""select count(*) as total from mv_ihmcl_curr where Relevant_Date=(select max(Relevant_Date) from mv_ihmcl_curr) and Type !='Conc.';"""
        plz_mum_map="""select count(*) as total from mv_ihmcl_base_data where Relevant_Date=(select max(Relevant_Date) from mv_ihmcl_base_data) and Type !='Conc.';"""
        plz_mum_last="""Select count(*) as total from mv_ihmcl_base_data_old where Type !='Conc.';"""

        ##########################################################################################################
        if db_max_date.strftime('%B')  in missing_month:
            plz_mum_last="""Select count(*) as total from mv_ihmcl_base_data_old where Type !='Conc.';"""
        else:
            plz_mum_last="""Select count(*) as total from mv_ihmcl_curr WHERE Relevant_Date = date_sub(MONTH,12,(SELECT MAX(Relevant_Date) FROM mv_ihmcl_curr)) and Type !='Conc.';"""
        # ###################################################################################################################################################
        plz_mum_curr_nc=get_df(plz_mum_curr,['total']).total.tolist()[0]
        plz_mum_last_nc=get_df(plz_mum_last,['total']).total.tolist()[0]
        plz_mum_map_nc=get_df(plz_mum_map,['total']).total.tolist()[0]

        q17="""select Relevant_Date,overall_map_col,overall_map_tranx,overall_map_col/overall_map_tranx as overall_map_val_per_trnx from (Select Relevant_Date,sum(Total_Txn_Amount) as overall_map_col,sum(Total_Trncnt) as overall_map_tranx
        	from mv_ihmcl_base_data WHERE Relevant_Date =(SELECT MAX(Relevant_Date) FROM mv_ihmcl_base_data) and Type != 'Conc.' 
        	group by Relevant_Date ORDER BY Relevant_Date Asc);"""

        q18="""select T2.Relevant_Date as Relevant_Date,overall_map_col,overall_map_tranx,overall_map_col/overall_map_tranx as overall_map_val_per_trnx 
        	from (Select T2.Relevant_Date,sum(T2.Total_Txn_Amount) as overall_map_col,sum(T2.Total_Trncnt) as overall_map_tranx
        	from mv_ihmcl_base_data WHERE T2.Relevant_Date =(SELECT MAX(T2.Relevant_Date) FROM mv_ihmcl_base_data) and Type != 'Conc.' 
        	group by T2.Relevant_Date ORDER BY T2.Relevant_Date Asc);"""

        df17=get_df(q17,['Relevant_Date','overall_map_col','overall_map_tranx','overall_map_val_per_trnx'])
        df18=get_df(q18,['Relevant_Date','overall_map_col','overall_map_tranx','overall_map_val_per_trnx'])


        q19="""select Relevant_Date,overall_col,overall_tranx,overall_col/overall_tranx as Overall_Val_per_trnx from (Select Relevant_Date,sum(Total_Txn_Amount) as overall_col,sum(Total_Trncnt) as overall_tranx
        	from mv_ihmcl_curr 
        	WHERE Relevant_Date = date_sub(MONTH,0,(SELECT MAX(Relevant_Date) FROM mv_ihmcl_curr)) 
        	and Type != 'Conc.' group by Relevant_Date ORDER BY Relevant_Date Asc);"""
            
        q20="""select Relevant_Date,overall_col,overall_tranx,overall_col/overall_tranx as Overall_Val_per_trnx from (Select Relevant_Date,sum(Total_Txn_Amount) as overall_col,sum(Total_Trncnt) as overall_tranx
        	from mv_ihmcl_curr 
        	WHERE Relevant_Date = date_sub(MONTH,12,(SELECT MAX(Relevant_Date) FROM mv_ihmcl_curr)) 
        	and Type != 'Conc.' group by Relevant_Date ORDER BY Relevant_Date Asc);"""
    
        q20="""select Relevant_Date,overall_col,overall_tranx,overall_col/overall_tranx as Overall_Val_per_trnx from (Select Relevant_Date,sum(Total_Txn_Amount) as overall_col,sum(Total_Trncnt) as overall_tranx
        	from mv_ihmcl_base_data_old 
        	WHERE Relevant_Date = date_sub(MONTH,0,(SELECT MAX(Relevant_Date) FROM mv_ihmcl_base_data_old)) 
        	and Type != 'Conc.' group by Relevant_Date ORDER BY Relevant_Date Asc);"""
            
        df19=get_df(q19,['Relevant_Date','overall_col','overall_tranx','Overall_Val_per_trnx'])
        df20=get_df(q20,['Relevant_Date','overall_col','overall_tranx','Overall_Val_per_trnx'])

        df5=pd.merge(pd.concat([df17,df18]), pd.concat([df19,df20]), on='Relevant_Date', how='inner')



        df_non_conc_c=df5[['Relevant_Date','overall_col','overall_map_col','Overall_Val_per_trnx','overall_map_val_per_trnx']]
        df_non_conc_t=df5[['Relevant_Date','overall_tranx','overall_map_tranx','Overall_Val_per_trnx','overall_map_val_per_trnx']]



        l7=[{'Relevant_Date':"Growth in Toll Collections of Overall Non Conc Vehicles (% yoy), "+date},
            {"overall_col":f'Total Collections In All Toll Plazas : INR Cr ({plz_mum_last_nc} Plazas), ({plz_mum_curr_nc} Plazas)'},
            {"overall_map_col":f'Total Collections In Mapped Toll Plazas : INR Cr ({plz_mum_map_nc} Plazas)'},
            {"Overall_Val_per_trnx":'Value Per Transaction : INR'},
            {"overall_map_val_per_trnx":'Value Per Transaction in Mapped : INR'}]

        df_non_conc_c=format_data_frame(df_non_conc_c,l7)


        l8=[{'Relevant_Date':"Growth in Toll Traffic of Overall Non Conc Vehicles (% yoy),"+date},
            {"overall_tranx":f'Total Transactions In All Toll Plazas : Million ({plz_mum_last_nc} Plazas), ({plz_mum_curr_nc} Plazas)'},
            {"overall_map_tranx":f'Total Transactions In Mapped Toll Plazas : Million ({plz_mum_map_nc})'},
            {"Overall_Val_per_trnx":'Value Per Transaction : INR'},
            {"overall_map_val_per_trnx":'Value Per Transaction in Mapped : INR'}]

        df_non_conc_t=format_data_frame(df_non_conc_t,l8)
        #%%
        # Map Collection Commercial-Consinerire:
        q21="""select Relevant_Date,com_conc_map_col,com_conc_map_tranx,com_conc_map_col/com_conc_map_tranx  as com_conc_map_Val_per_tranx
        	from (select sum(Total_Txn_Amount-Car_Jeep_Van_Txn_Amount-Bus_2_axle_Txn_Amount-Bus_3_axle_Txn_Amount-Mini_Bus_Txn_Amount) as com_conc_map_col,sum(Total_Trncnt-Car_Jeep_Van_Trncnt-Bus_2_axle_Trncnt-Bus_3_axle_Trncnt-Mini_Bus_Trncnt) as com_conc_map_tranx,
        	Relevant_Date from mv_ihmcl_base_data  WHERE Relevant_Date =(SELECT MAX(Relevant_Date) FROM mv_ihmcl_base_data) and  Type = 'Conc.' group by Relevant_Date);"""

        q22="""select T2.Relevant_Date as Relevant_Date,com_conc_map_col,com_conc_map_tranx,com_conc_map_col/com_conc_map_tranx  as com_map_Val_per_tranx
        	from (select sum(T2.Total_Txn_Amount-T2.Car_Jeep_Van_Txn_Amount -T2.Bus_2_axle_Txn_Amount-T2.Bus_3_axle_Txn_Amount-T2.Mini_Bus_Txn_Amount) as com_conc_map_col,
        	sum(T2.Total_Trncnt-T2.Car_Jeep_Van_Trncnt -T2.Bus_2_axle_Trncnt-T2.Bus_3_axle_Trncnt-T2.Mini_Bus_Trncnt) as com_conc_map_tranx,
        	T2.Relevant_Date from mv_ihmcl_base_data  WHERE T2.Relevant_Date =(SELECT MAX(T2.Relevant_Date) FROM mv_ihmcl_base_data) and Type = 'Conc.' group by T2.Relevant_Date);"""

        df21=get_df(q21,['Relevant_Date','com_conc_map_col','com_conc_map_tranx','com_conc_map_Val_per_tranx'])
        df22=get_df(q22,['Relevant_Date','com_conc_map_col','com_conc_map_tranx','com_conc_map_Val_per_tranx'])

        q23="""select Relevant_Date,com_conc_Overall_col,com_conc_Overall_tranx,com_conc_Overall_col/com_conc_Overall_tranx  as com_conc_Overall_Val_per_tranx from  
              (select sum(Total_Txn_Amount-Car_Jeep_Van_Txn_Amount-Bus_2_axle_Txn_Amount-Bus_3_axle_Txn_Amount-Mini_Bus_Txn_Amount) as com_conc_Overall_col,
              sum(Total_Trncnt-Car_Jeep_Van_Trncnt-Bus_2_axle_Trncnt-Bus_3_axle_Trncnt-Mini_Bus_Trncnt) as com_conc_Overall_tranx,
              Relevant_Date from mv_ihmcl_curr Where
              Relevant_Date = date_sub(MONTH,0,(SELECT MAX(Relevant_Date) FROM mv_ihmcl_curr))  and  Type = 'Conc.' group by Relevant_Date);"""
        	
            
        q24="""select Relevant_Date,com_conc_Overall_col,com_conc_Overall_tranx,com_conc_Overall_col/com_conc_Overall_tranx  as com_conc_Overall_Val_per_tranx from  
              (select sum(Total_Txn_Amount-Car_Jeep_Van_Txn_Amount-Bus_2_axle_Txn_Amount-Bus_3_axle_Txn_Amount-Mini_Bus_Txn_Amount) as com_conc_Overall_col,
              sum(Total_Trncnt-Car_Jeep_Van_Trncnt-Bus_2_axle_Trncnt-Bus_3_axle_Trncnt-Mini_Bus_Trncnt) as com_conc_Overall_tranx,
              Relevant_Date from mv_ihmcl_curr Where
              Relevant_Date = date_sub(MONTH,12,(SELECT MAX(Relevant_Date) FROM mv_ihmcl_curr))  and  Type = 'Conc.' group by Relevant_Date);"""
        
        
        q24="""select Relevant_Date,com_conc_Overall_col,com_conc_Overall_tranx,com_conc_Overall_col/com_conc_Overall_tranx  as com_conc_Overall_Val_per_tranx from  
              (select sum(Total_Txn_Amount-Car_Jeep_Van_Txn_Amount-Bus_2_axle_Txn_Amount-Bus_3_axle_Txn_Amount-Mini_Bus_Txn_Amount) as com_conc_Overall_col,
              sum(Total_Trncnt-Car_Jeep_Van_Trncnt-Bus_2_axle_Trncnt-Bus_3_axle_Trncnt-Mini_Bus_Trncnt) as com_conc_Overall_tranx,
              Relevant_Date from mv_ihmcl_base_data_old Where
              Relevant_Date = date_sub(MONTH,0,(SELECT MAX(Relevant_Date) FROM mv_ihmcl_base_data_old))  and  Type = 'Conc.' group by Relevant_Date);"""
        	

        df23=get_df(q23,['Relevant_Date','com_conc_Overall_col','com_conc_Overall_tranx','com_conc_Overall_Val_per_tranx'])
        df24=get_df(q24,['Relevant_Date','com_conc_Overall_col','com_conc_Overall_tranx','com_conc_Overall_Val_per_tranx'])



        df6=pd.merge(pd.concat([df21,df22]), pd.concat([df23,df24]), on='Relevant_Date', how='inner')

        df_com_conc_c=df6[['Relevant_Date','com_conc_Overall_col','com_conc_map_col','com_conc_Overall_Val_per_tranx','com_conc_map_Val_per_tranx']]
        df_com_conc_t=df6[['Relevant_Date','com_conc_Overall_tranx','com_conc_map_tranx','com_conc_Overall_Val_per_tranx','com_conc_map_Val_per_tranx']]



        l7=[{'Relevant_Date':"Growth in Toll Collections of Commercial Conc Vehicles (% yoy), "+date},
            {"com_conc_Overall_col":f'Total Collections In All Toll Plazas : INR Cr ({plz_mum_last_c} Plazas), ({plz_mum_curr_c} Plazas)'},
            {"com_conc_map_col":f'Total Collections In Mapped Toll Plazas : INR Cr ({plz_mum_map_c} Plazas)'},
            {"com_conc_Overall_Val_per_tranx":'Value Per Transaction : INR'},
            {"com_conc_map_Val_per_tranx":'Value Per Transaction in Mapped : INR'}]

        df_com_conc_c=format_data_frame(df_com_conc_c,l7)


        l8=[{'Relevant_Date':"Growth in Toll Traffic of Commercial Conc Vehicles (% yoy),"+date},
            {"com_conc_Overall_tranx":f'Total Transactions In All Toll Plazas : Million ({plz_mum_last_c} Plazas), ({plz_mum_curr_c} Plazas)'},
            {"com_conc_map_tranx":f'Total Transactions In Mapped Toll Plazas : Million ({plz_mum_map_c})'},
            {"com_conc_Overall_Val_per_tranx":'Value Per Transaction : INR'},
            {"com_conc_map_Val_per_tranx":'Value Per Transaction in Mapped : INR'}]

        df_com_conc_t=format_data_frame(df_com_conc_t,l8)

        #%%
        collection=pd.concat([df_o_c,df_non_com_c,df_com_c,df_o_conc_c,df_non_conc_c,df_com_conc_c])
        trans=pd.concat([df_o_t,df_non_com_t,df_com_t,df_o_conc_t,df_non_conc_t,df_com_conc_t]) 
        make_excel_file([collection,trans])
#%%
        log.job_end_log(table_name,job_start_time,no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])
        error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)

        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
