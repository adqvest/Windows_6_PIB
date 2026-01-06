# -*- coding: utf-8 -*-
"""
Created on Fri Nov 26 17:13:17 2021

@author: Abhishek Shankar
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Nov 25 16:49:23 2021

@author: Abhishek Shankar
"""
import sqlalchemy
import pandas as pd
from pandas.io import sql
import os
import re
import datetime as datetime
from pytz import timezone
import sys
import warnings
warnings.filterwarnings('ignore')
import numpy as np
import time
import re
import os
import igraph as ig
import timeit
import time
import itertools
from clickhouse_driver import Client
import unidecode
import os
import datetime
import unidecode
import timeit
#sys.path.insert(0, r'C:\Adqvest')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import requests
import JobLogNew as log
requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = 'ALL:@SECLEVEL=1'

engine = adqvest_db.db_conn()
#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days
from clickhouse_driver import Client
from dateutil.relativedelta import relativedelta
client = Client('ec2-3-109-104-45.ap-south-1.compute.amazonaws.com',
                user='default',
                password='@Dqu&TP@ssw0rd',
                database='AdqvestDB',
               port=9000)


# code you want to evaluate
#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days


def ldm(any_day):
    # this will never fail
    # get close to the end of the month for any day, and add 4 days 'over'
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
    # subtract the number of remaining 'overage' days to get last day of current month, or said programattically said, the previous day of the first of next month
    return next_month - datetime.timedelta(days=next_month.day)

def fdm(any_day):

    days = int(ldm(any_day).strftime("%d"))
    fd = ldm(any_day) - datetime.timedelta(days = days-1)

    return fd


engine = adqvest_db.db_conn()
connection = engine.connect()
#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days

## job log details
job_start_time = datetime.datetime.now(india_time)
table_name = 'AMAZON_BEAUTY_CPS'

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

        start_time = timeit.default_timer()


        max_date,cols = client.execute("Select max(Relevant_Date) as RD from AdqvestDB.AMAZON_BEAUTY_CLEAN",with_column_types=True)
        max_date = pd.DataFrame(max_date, columns=[tuple[0] for tuple in cols]).iloc[0,0]

        min_date = max_date + relativedelta(months=-24)

        start = fdm(min_date)
        finish = ldm(max_date)

        date_iters = []
        while start<=finish:

          a = (start,ldm(start))
          print(a)
          date_iters.append(a)
          start = ldm(start)+datetime.timedelta(1)


        #r = pd.DataFrame()

        start = min_date
        finish = max_date


        ranking_vector_final = pd.DataFrame()


        #catg_df = pd.DataFrame()

        query = """Select Category,Sub_Category_1,Sub_Category_2,Sub_Category_3,Sub_Category_4
                  from AdqvestDB.AMAZON_BEAUTY_CLEAN where Category = 'Beauty'
                  and Relevant_Date >='"""+str(max_date-datetime.timedelta(30))+"""' group by Category,Sub_Category_1,Sub_Category_2,Sub_Category_3,Sub_Category_4"""


        df_catg,cols = client.execute(query,with_column_types=True)
        df_catg = pd.DataFrame(df_catg, columns=[tuple[0] for tuple in cols])
#        df_catg = df_catg[df_catg['Sub_Category_3']=='Talcum Powders']
        #Remove Line Later
        q = "Select Category,Sub_Category_1,Sub_Category_2,Sub_Category_3,Sub_Category_4 from AdqvestDB.AMAZON_BEAUTY_CPS_SCORES group by Category,Sub_Category_1,Sub_Category_2,Sub_Category_3,Sub_Category_4 order by Category,Sub_Category_1,Sub_Category_2,Sub_Category_3,Sub_Category_4;"
        df_1,cols = client.execute(q,with_column_types=True)
        df_catg = pd.DataFrame(df_1, columns=[tuple[0] for tuple in cols])
        #df_catg = df_catg[[x for x in df_catg.columns if x.lower()!='marker']]

        for k,row in df_catg.iterrows():


        #    a = row.copy()
        #    a = pd.DataFrame(row.reset_index()).T
        #    a.columns = a.iloc[0]
        #    a = a.iloc[-1:].reset_index(drop=True)
        #
        #    catg_df = pd.concat([catg_df,a])
            if row['Sub_Category_3']=='Bubble Bath':
                continue
            i = 0
            r = pd.DataFrame()
            category = row[0]
            sub_category1 = row[1]
            sub_category2 = row[2]
            sub_category3 = row[3]
            sub_category4 = row[4]
            row = row[row!='']
            combo1 = "|".join(list(row))
            catg_analyze = row[-1]
            catg_head = row[-1:].index[0]
            print(catg_analyze," : ",catg_head)

            query3 = "Select * from AdqvestDB.AMAZON_BEAUTY_CLEAN where Category = '"+category+"' and Sub_Category_1 = '"+sub_category1+"' and  Sub_Category_2 = '"+sub_category2+"' and Sub_Category_3 = '"+sub_category3+"' and Sub_Category_4 = '"+sub_category4+"' and Relevant_Date >= '"+str(start)+"' and Relevant_Date <= '"+str(finish)+"';"
            df,cols = client.execute(query3,with_column_types=True)
            df = pd.DataFrame(df, columns=[tuple[0] for tuple in cols])
            #CUTICLES
            if df.empty:
              continue
            for vals in date_iters:
                date1 = vals[0]
                date2 = vals[1]
            #    query3 = "Select * from AdqvestDB.AMAZON_BEAUTY_CLEAN where Category = 'Beauty' and Sub_Category_1 = 'Hair Care & Styling' and  Sub_Category_2 = 'Hair Care' and Sub_Category_3 = 'Hair Oils' and Sub_Category_4 = '' and Relevant_Date >= '"+str(date1)+"' and Relevant_Date <= '"+str(date2)+"';"
            #    df,cols = client.execute(query3,with_column_types=True)
            #    df1 = pd.DataFrame(df, columns=[tuple[0] for tuple in cols])
            #    query3 = "Select * from AdqvestDB.AMAZON_BEAUTY_CLEAN where Category = 'Beauty' and Sub_Category_1 = 'Hair Care & Styling' and  Sub_Category_2 = 'Hair Care' and Sub_Category_3 = 'Hair Oils' and Sub_Category_4 = '' and Relevant_Date >= '"+str(date1)+"' and Relevant_Date <= '"+str(date2)+"';"
            #    df,cols = client.execute(query3,with_column_types=True)
            #    df = pd.DataFrame(df, columns=[tuple[0] for tuple in cols])
            #    print(df.dtypes)
                df1 = df[((df['Relevant_Date']>=date1) & (df['Relevant_Date']<=date2))]
                if df1.empty:
                  continue
                nrow = len(df1['Name'].unique())
                if nrow <= 2:
                  continue
                tp = str(date2)
                data = df1.copy()
                data['Name'] = data['Clean_Name']
                data['Brand'] = data['SKU'].astype(str) + " " + data['SKU_Units'].astype(str)
                data = data[[ "Name","Rank",catg_head,"Brand","Relevant_Date" ]]
                cols = ["Name","Brand",catg_head]#

                data['Relevant_Date'] = pd.to_datetime(data['Relevant_Date'])
                #data['Brand'] = data['Brand'].apply(lambda x : re.sub("[^0-9A-Za-z///' ]", "", x))
                #data['Brand'] = data['Brand'].str.replace("'","")
                data['Brand'] = data['Brand'].str.upper()

                data['Name'] = data['Name'].apply(lambda x : re.sub("[^0-9A-Za-z///' ]", "", x))
                data['Name'] = data['Name'].str.replace("'","")
                data['Name'] = data['Name'].str.upper()

                norm = data[cols].drop_duplicates()
                data = data[['Name','Rank',"Relevant_Date"]]

                norm.columns = ['Pdt']+cols[1:]

                data1 = data.copy()
                data1.columns = ["Name1", "Rank1","Relevant_Date"]

                data1 = data1.sort_values(by=['Rank1'])
                data = data.sort_values(by=['Rank'])

                pair_data = data.merge(data1)
                pair_data['WEIGHT'] = np.where(pair_data['Rank1'] <= (pair_data['Rank']-1),1,0)
                pair_data = pair_data[pair_data['WEIGHT']==1]
                x = pair_data.groupby(['Name','Name1'])['WEIGHT'].sum().reset_index()
                pair = x
                pair.columns = ["from", "to", "weight"]

                #Building the graph
                G=ig.Graph.TupleList(pair.itertuples(index=False), directed=True, weights=True)
                word=[v['name'] for v in G.vs]
                ranking1 = G.personalized_pagerank(damping=0.85,weights='weight')
                ranking_vec=pd.DataFrame([x*100 for x in ranking1])
                ranking_vec['word']=word
                ranking_vec.columns = [tp,'Pdt']
                print(sum(ranking_vec.iloc[:,0]))
                ranking_vec = norm.merge(ranking_vec)

                #Final
                ranking_vec = ranking_vec.groupby(["Pdt",'Brand',catg_head,tp])["Pdt"].count().reset_index(name='Count')
                ranking_vec[tp] = ranking_vec[tp]/ranking_vec['Count']
                ranking_vec = ranking_vec[[x for x in ranking_vec.columns if x.lower()!='count']]
                ranking_vec=ranking_vec.sort_values(tp,ascending=False)

                if i != 0:
            #      r = r.merge(ranking_vec, left_index=True, right_index=False)
                  ranking_vec = ranking_vec.drop_duplicates('Pdt')
                  r = pd.merge(r,ranking_vec,how='outer')

            #      r = pd.concat([r,ranking_vec.iloc[:,-1]],axis=1,join='inner')
                else:
                  ranking_vec = ranking_vec.drop_duplicates('Pdt')
                  r = pd.concat([r,ranking_vec])
                i+=1

            if r.empty:
                continue
            r.fillna(0,inplace=True)
            #r = r.drop_duplicates(["Pdt"])

            r['Category'] = category
            r['Sub_Category_1'] = sub_category1
            r['Sub_Category_2'] = sub_category2
            r['Sub_Category_3'] = sub_category3
            r['Sub_Category_4'] = sub_category4



            from dateutil import parser
            df = r.copy()
            dates = []
            others = []
            for vals in df.columns:
              try:
                dates.append(parser.parse(vals).date())
              except:
                others.append(vals)


            dates.sort()
            dates = [str(x) for x in dates]
            df = df[others+dates]

            new_cols = []
            date_str = []
            for vals in df.columns:
              try:
                new_cols.append(parser.parse(vals).date())
                date_str.append(parser.parse(vals).date())

              except:
                new_cols.append(vals)

            df.columns = new_cols

            main1 = pd.melt(df, id_vars = ['Pdt', 'Brand', 'Category', 'Sub_Category_1', 'Sub_Category_2','Sub_Category_3', 'Sub_Category_4'] , value_vars = date_str)
            main1.columns = ['Pdt', 'Brand', 'Category', 'Sub_Category_1', 'Sub_Category_2','Sub_Category_3', 'Sub_Category_4']+['Relevant_Date','Value']
            main1['Relevant_Month'] = main1['Relevant_Date'].apply(lambda x : x.strftime("%B-%Y"))
            main1['Combination'] = combo1#main1['Category']+"|"+main1['Sub_Category_1']+"|"+main1['Sub_Category_2']+"|"+main1['Sub_Category_3']+"|"+main1['Sub_Category_4']
            main1['Type'] = 'SKU'
            main1 = main1[['Pdt', 'Brand','Type', 'Category', 'Sub_Category_1', 'Sub_Category_2','Sub_Category_3', 'Sub_Category_4','Value','Relevant_Month','Combination','Relevant_Date']]
            main1 = main1.rename(columns={'Brand': 'SKU'})
            print(main1.groupby(['Relevant_Month'])['Value'].sum())
            main1['Runtime'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))

            client.execute("INSERT INTO AdqvestDB.AMAZON_BEAUTY_CPS_SCORES_SKU VALUES", main1.values.tolist())
            del r
            del df
            del main1

            #print("DATA INSERTED")
        #    client.execute("INSERT INTO VAHAN_MAKER_VS_CATEGORY_RTO_LEVEL_DATA VALUES", df.values.tolist())
            #r = r.apply(pd.to_numeric,errors='coerce')
#            ranking_vector_final = pd.concat([ranking_vector_final,r])
            #brand = r.groupby(['Brand','Sub_Category_3']).sum().reset_index()

        elapsed = timeit.default_timer() - start_time
        print("TIME ELAPSED : ",elapsed)
        log.job_end_log(table_name,job_start_time,no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])

        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
