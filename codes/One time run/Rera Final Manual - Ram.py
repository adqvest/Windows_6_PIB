#!/usr/bin/env python
# coding: utf-8

# In[9]:


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
import numpy as np
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
#sys.path.insert(0, r'C:\Adqvest')
engine = adqvest_db.db_conn()
connection = engine.connect()
#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days

#%%
def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

        ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'HARYANA_RERA_WEEKLY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
            #%%
        a = pd.read_html("https://haryanarera.gov.in/admincontrol/registered_projects/1")
        r = requests.get("https://haryanarera.gov.in/admincontrol/registered_projects/1")
        soup = BeautifulSoup(r.text)
        col = []
        col1 = []
        for row in soup.findAll('table',{"id":"compliant_hearing"})[0].tbody.findAll('tr'):
            third_column = row.findAll('td')[8].contents
            project_id_column = row.findAll('td')[2].contents
            col.append(third_column)
            col1.append(project_id_column)

        #start_time = timeit.default_timer()
        india_time = timezone('Asia/Kolkata')
        today      = datetime.datetime.now(india_time)
        days       = datetime.timedelta(1)
        yesterday =  today - days



        codes=[]
        links=[]
        for i in col:
            try:
                links.append(i[1].get('href'))
            except:
                pass

        for i in col1:
            try:
                codes.append(i[1])
            except:
                pass

        col1 = []
        for row in soup.findAll('table',{"id":"compliant_hearing"})[0].tbody.findAll('tr'):
            third_column1 = row.findAll('td')[2].contents
            col1.append(third_column1)

        links1=[]
        codes1=[]
        for i in col1:
            try:
                links1.append(i[1].get('href'))
                codes1.append(i[1].text)
            except:
                pass




        temp = pd.DataFrame({"urls":links1})
        temp['URL'] = temp["urls"].str.replace("searchprojectDetail", "project_preview_open")
        temp['Project ID'] = codes1
        temp





        df1=pd.DataFrame()
        a[1]
        df1 = a[1]
        df2 = pd.DataFrame()
        df2 = pd.merge(df1,temp, on = 'Project ID')
        df2 = df2.drop(['Serial No.'], axis = 1)
        df2 = df2.drop(['Details of Project(Form A-H)'], axis = 1)
        df2 = df2.drop(['View Certificate'], axis = 1)
        df2 = df2.drop(['Monitoring Orders'], axis = 1)
        df2 = df2.drop(['View Quarterly Progress'], axis = 1)
        df2 = df2.drop(['View OC/CC/PCC'], axis = 1)
        df2 = df2.drop(['urls'], axis = 1)
        df2.columns = ["Registration_Certificate_Number", "Project_ID", "Project_Name", "Builder", "Project_Location",
                      "Project_District","Registered_With","URL"]




        list_of_links = df2['URL'].to_list()
        list_of_links



        not_available = []
        output_df = pd.DataFrame()
        for k in list_of_links:
            print(k)
            time.sleep(1)
            final_df = pd.DataFrame()
            merge_df = pd.DataFrame()
            table1 = pd.DataFrame()
            urlss =k
        #    try:
        #        requests.get(url)
        #    except:
        #        try:
        #            time.sleep(10)
        #            requests.get(url)
        #        except:
        #            print("NA")
        #            continue
            try:
                table = pd.read_html(urlss)
            except:
                not_available.append(k)
                continue
            table
            table1 = pd.concat(table)
            if table1.iloc[:,0].str.lower().str.contains("password").any():
                continue
            table1 = table1.iloc[:,0:3]
            table1 = table1[table1.columns[table1.isnull().mean() < 0.9]]
            table1.columns = ["1","2","3"]
            t = table1[table1['1']=="1. Name of the project"]
            t1 = table1[table1['1'].str.contains("2. Address of the site of the project",na=False)]
            t2 = table1[table1['1'].str.contains("cost",na=False, case=False)]
            t4 = table1[table1['1']=="I. LAYOUT PLAN"]
            t5 = table1[table1['1'].str.contains("LICEN",na=False)]
            t6 = table1[table1['1']=="III. DEMARCATION PLAN"]
            t7 = table1[table1['1'].str.contains("COMPLETION",na=False)]
            t8 = table1[table1['1'].str.contains("SERVICE PLAN ESTIMATE",na=False)]
            t9 = table1[table1['2'].str.contains("APARTMENT/SHOPS/OTHER",na=False)]
            final_df=pd.concat([t,t1,t2,t4,t5,t6,t7,t8])
            final_df.columns = ["1","2","3"]
            for i in table:
                if i.shape[1]==2 and i[0].str.contains('Plot Area').any():
                    t3=i
                    first_row = t3.head(1)
                    t3 = t3.iloc[1: , :]
                    t3[0] = pd.to_numeric(t3[0],errors='ignore')
                    t3[1] = pd.to_numeric(t3[1],errors='ignore')
                    t3 = pd.concat([t3,pd.DataFrame(t3.sum(axis=0),columns=['Grand Total']).T])
                    last_row = t3.iloc[-1:]
        #            final_table = pd.concat([first_row,last_row])
                    final_table = pd.DataFrame(list(last_row.iloc[0])).T
                    final_table.columns = list(first_row.iloc[0])
                    merge_df = pd.concat([merge_df,final_table],axis = 1)
                    merge_df.columns = ["Plot_Area_In_Sq_Meters", "Total_Plots_In_Project"]
                else:
                    pass

            for i in table:
                if i.shape[1]==7 and i[1].str.contains('Plot/ apartment type').any():
                    t10=i
                    first_row1 = t10.head(1)
                    t10 = t10.iloc[1: , :]
                    t10[0] = pd.to_numeric(t10[0],errors='ignore')
                    t10[1] = pd.to_numeric(t10[1],errors='ignore')
                    t10[2] = pd.to_numeric(t10[2],errors='ignore')
                    t10[3] = pd.to_numeric(t10[3],errors='ignore')
                    t10[4] = pd.to_numeric(t10[4],errors='ignore')
                    t10[5] = pd.to_numeric(t10[5],errors='ignore')
                    t10[6] = pd.to_numeric(t10[6],errors='ignore')
                    t10 = pd.concat([t10,pd.DataFrame(t10.sum(axis=0),columns=['Grand Total']).T])
                    last_row1 = t10.iloc[-1:]
                    final_table1 = pd.DataFrame(list(last_row1.iloc[0])).T
                    final_table1.columns = list(first_row1.iloc[0])
                    print(final_table1)
                    final_table1 =final_table1.drop(['Sr. No.'], axis = 1)
                    final_table1.columns = ["Plot_Or_Aparment_Type", "Carpet_Area_In_Sq_Meters", "Total_Plot_Or_Aparment_In_Project",
                                        "Booked_Or_Sold", "Yet_To_Be_Booked", "Towers_Under_Construction_For_Booked_Apartments"]
            #            final_table1 = pd.DataFrame(list(last_row.iloc[0]),columns=list(first_row.iloc[0]))#pd.concat([first_row,last_row])
                    merge_df = pd.concat([merge_df,final_table1],axis = 1)
                    #merge_df = merge_df.drop(['Sr. No.'], axis = 1)
                    #merge_df = merge_df.dropna(how='all')
                    #print(merge_df)
                    #merge_df = merge_df.loc[:,~merge_df.columns.duplicated()]
#                        merge_df.to_csv(r"C:\Users\Administrator\AdQvestDir\codes\One time run\CHECK.csv")

                else:
                    pass

            v1 = list(set(np.where(final_df["1"].str.contains("1. Name of the project"),final_df["3"],"NA")))
            v1.remove("NA")
            merge_df['Project_Name_PDF'] = v1
            v2 = list(set(np.where(final_df["1"].str.contains("2. Address of the site of the project"),final_df["3"],"NA")))
            v2.remove("NA")
            merge_df['Address_PDF'] = v2
            v3 = list(set(np.where(final_df["1"].str.contains("Cost of the land"),final_df["3"],"NA")))
            v3.remove("NA")
            merge_df['Cost_Of_Land_In_Lakhs'] = v3
            v4 = list(set(np.where(final_df["1"].str.contains("Estimated cost of construction of apartments"),final_df["3"],"NA")))
            v4.remove("NA")
            merge_df['Estimated_Cost_Of_Construction_Of_Apartments_In_Lakhs'] = v4
            v5 = list(set(np.where(final_df["1"].str.contains("Estimated cost of infrastructure"),final_df["3"],"NA")))
            v5.remove("NA")
            merge_df['Estimated_Cost_Of_Infrastructure_In_Lakhs'] = v5
            v6 = list(set(np.where(final_df["1"].str.contains("Other Costs"),final_df["3"],"NA")))
            v6.remove("NA")
            merge_df['Other_Costs_In_Lakhs'] = v6
            #try:
        #      cols1 = [x for x in list(merge_df.iloc[0]) if (("plot" in x.lower())|("plots"  in x.lower()))]
        #      cols2 = [x for x in list(merge_df.columns) if len(str(x)) > 4]
        #      merge_df.columns = cols1+cols2
        #      merge_df = merge_df.iloc[1:]
            merge_df['URL'] = k
          #    df2 = pd.concat([df2,merge_df],axis = 1)
            output_df = pd.concat([output_df,merge_df])
            #except:
            #    print("####NA####")

        output_df['Relevant_Date'] = today.date()
        output_df['Runtime'] =  pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
        output_df['Cost_Of_Land_In_Lakhs'] = output_df['Cost_Of_Land_In_Lakhs'].str.replace('Lakhs','')
        output_df['Estimated_Cost_Of_Construction_Of_Apartments_In_Lakhs'] = output_df['Estimated_Cost_Of_Construction_Of_Apartments_In_Lakhs'].str.replace('Lakhs','')
        output_df['Estimated_Cost_Of_Infrastructure_In_Lakhs'] = output_df['Estimated_Cost_Of_Infrastructure_In_Lakhs'].str.replace('Lakhs','')
        output_df['Other_Costs_In_Lakhs'] = output_df['Other_Costs_In_Lakhs'].str.replace('Lakhs','')

        output_df = output_df.reset_index(drop=True)

        final_required = pd. DataFrame()
        final_required = pd.merge(df2,output_df, on = "URL")
        final_required['Cost_Of_Land_In_Lakhs'] = pd.to_numeric(final_required['Cost_Of_Land_In_Lakhs'],errors='ignore')
        final_required['Estimated_Cost_Of_Construction_Of_Apartments_In_Lakhs'] = pd.to_numeric(final_required['Estimated_Cost_Of_Construction_Of_Apartments_In_Lakhs'],errors='ignore')
        final_required['Estimated_Cost_Of_Infrastructure_In_Lakhs'] = pd.to_numeric(final_required['Estimated_Cost_Of_Infrastructure_In_Lakhs'],errors='ignore')
        final_required['Other_Costs_In_Lakhs'] = pd.to_numeric(final_required['Other_Costs_In_Lakhs'],errors='ignore')



        final_required.to_sql(name = "HARYANA_RERA_WEEKLY_DATA",if_exists = 'append',index = False,con =  engine)


#%%
        log.job_end_log(table_name,job_start_time, no_of_ping)

#

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
