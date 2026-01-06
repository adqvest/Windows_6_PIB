# -*- coding: utf-8 -*-
"""
Created on Tue Jul 26 13:17:47 2022

@author: Abdulmuizz
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
import igraph as ig
import timeit
import itertools
import unidecode
from nltk.corpus import stopwords
from nltk.util import ngrams
from clickhouse_driver import Client
from dateutil.relativedelta import relativedelta
from dateutil import parser


india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days

### Amazon DB Connection

#DB Connection
properties = pd.read_csv(r"C:\Users\Administrator\AdQvestDir\Amazon_AdQvest_properties.txt",delim_whitespace=True)
#properties = pd.read_csv('AdQvest_properties.txt',delim_whitespace=True)

host    = list(properties.loc[properties['Env'] == 'Host'].iloc[:,1])[0]
port    = list(properties.loc[properties['Env'] == 'port'].iloc[:,1])[0]
db_name = list(properties.loc[properties['Env'] == 'DBname'].iloc[:,1])[0]

con_string = 'mysql+pymysql://' + host + ':' + port + '/' + db_name + '?charset=utf8'
engine1     = sqlalchemy.create_engine(con_string,encoding='utf-8')
connection  = engine1.connect()

#**** Credential Directory ****
#os.chdir('/home/ubuntu/AdQvestDir')
#os.chdir('E:/Adqvest files')
os.chdir(r'C:\Users\Administrator\AdQvestDir')
#os.chdir('D:/Adqvest_Office_work/R_Script')

#ClickHouse DB Connection
properties = pd.read_csv('Adqvest_ClickHouse_properties.txt',delim_whitespace=True)

host            = list(properties.loc[properties['Env'] == 'Host'].iloc[:,1])[0]
port            = list(properties.loc[properties['Env'] == 'Port'].iloc[:,1])[0]
db_name         = list(properties.loc[properties['Env'] == 'DB_Name'].iloc[:,1])[0]
user_name       = list(properties.loc[properties['Env'] == 'User_Name'].iloc[:,1])[0]
password_string = list(properties.loc[properties['Env'] == 'Password_String'].iloc[:,1])[0]

client = Client(host, user=user_name, password=password_string, database=db_name, port=port)


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

def strip_character(dataCol):
    r = re.compile(r'[^a-zA-Z !@#$%&*_+-=|\:";<>,./()[\]{}\']')
    return r.sub('', dataCol)


# table = 'AMAZON_GROCERY_AND_GOURMET_FOODS'
amazon_category = 'Beauty'
corpus = False
word_cloud = True
customer_pref_score = False

if customer_pref_score:

    max_date,cols = client.execute("Select max(Relevant_Date) as RD from AdqvestDB.AMAZON_BEAUTY_CLEAN_Temp_Abdul where Category = '"+amazon_category.title()+"'",with_column_types=True)
    max_date = pd.DataFrame(max_date, columns=[tuple[0] for tuple in cols]).iloc[0,0]

    min_date = max_date + relativedelta(months=-48)

    start = fdm(min_date)
    finish = ldm(max_date)

    date_iters = []
    while start<=finish:

        a = (start,ldm(start))
        print(a)
        date_iters.append(a)
        start = ldm(start)+datetime.timedelta(1)

    start = min_date
    finish = max_date


    query = """Select Category,Sub_Category_1,Sub_Category_2,Sub_Category_3,Sub_Category_4
                          from AdqvestDB.AMAZON_BEAUTY_CLEAN_Temp_Abdul where Category = '"""+amazon_category.title()+"""'
                          and Relevant_Date >='"""+str(min_date)+"""' group by Category,Sub_Category_1,Sub_Category_2,Sub_Category_3,Sub_Category_4"""

    df_catg,cols = client.execute(query,with_column_types=True)
    df_catg = pd.DataFrame(df_catg, columns=[tuple[0] for tuple in cols])



    for k,row in df_catg.iterrows():

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
        combo1 = "|".join(row)
        catg_analyze = row[-1]
        catg_head = row[-1:].index[0]
        print(catg_analyze," : ",catg_head)

        # added 29 june 2022
        query3 = "Select * from AdqvestDB.AMAZON_BEAUTY_CLEAN_Temp_Abdul where Category = '"+category+"' and Sub_Category_1 = '"+sub_category1+"' and  Sub_Category_2 = '"+sub_category2+"' and Sub_Category_3 = '"+sub_category3+"' and Sub_Category_4 = '"+sub_category4+"' and Relevant_Date >= '"+str(start)+"' and Relevant_Date <= '"+str(finish)+"';"


        df,cols = client.execute(query3,with_column_types=True)
        df = pd.DataFrame(df, columns=[tuple[0] for tuple in cols])
        #CUTICLES

        if df.empty:
            continue
        for vals in date_iters:
            date1 = vals[0]
            date2 = vals[1]
            df1 = df[((df['Relevant_Date']>=date1) & (df['Relevant_Date']<=date2))]
            if df1.empty:
                continue
            nrow = len(df1['Name'].unique())
            if nrow <= 2:
                continue
            if len(df1) > 100000:
                continue
            tp = str(date2)
            data = df1.copy()
            data['Name'] = data['Clean_Name']
            data['SKU'] = data['SKU'].astype(str) + " " + data['SKU_Units'].astype(str)
            data['Pack'] = data['QTY'].astype(str) + " " + data['QTY_Units'].astype(str)
            data = data[[ "Name","Rank",catg_head,"Brand",'SKU','Pack',"Relevant_Date" ]]

            # top 30 products
            data = data[data['Rank'] <= 30]


            cols = ["Name","Brand",'SKU','Pack',catg_head]#

            data['Relevant_Date'] = pd.to_datetime(data['Relevant_Date'])
            data['Brand'] = data['Brand'].apply(lambda x : re.sub("[^0-9A-Za-z///' ]", "", x))
            data['Brand'] = data['Brand'].str.replace("'","")
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
            ranking_vec = ranking_vec.groupby(["Pdt",'Brand','SKU','Pack',catg_head,tp])["Pdt"].count().reset_index(name='Count')
            ranking_vec[tp] = ranking_vec[tp]/ranking_vec['Count']
            ranking_vec = ranking_vec[[x for x in ranking_vec.columns if x.lower()!='count']]
            ranking_vec=ranking_vec.sort_values(tp,ascending=False)

            if i != 0:

                ranking_vec = ranking_vec.drop_duplicates('Pdt')
                r = pd.merge(r,ranking_vec,how='outer')

            else:
                ranking_vec = ranking_vec.drop_duplicates('Pdt')
                r = pd.concat([r,ranking_vec])
            i+=1

        if r.empty:
            continue

        r.fillna(0,inplace=True)


        r['Category'] = category
        r['Sub_Category_1'] = sub_category1
        r['Sub_Category_2'] = sub_category2
        r['Sub_Category_3'] = sub_category3
        r['Sub_Category_4'] = sub_category4




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

        main1 = pd.melt(df, id_vars = ['Pdt', 'Brand','SKU','Pack', 'Category', 'Sub_Category_1', 'Sub_Category_2','Sub_Category_3', 'Sub_Category_4'] , value_vars = date_str)
        main1.columns = ['Pdt', 'Brand','SKU','Pack', 'Category', 'Sub_Category_1', 'Sub_Category_2','Sub_Category_3', 'Sub_Category_4']+['Relevant_Date','Value']
        main1['Relevant_Month'] = main1['Relevant_Date'].apply(lambda x : x.strftime("%B-%Y"))
        main1['Combination'] = combo1#main1['Category']+"|"+main1['Sub_Category_1']+"|"+main1['Sub_Category_2']+"|"+main1['Sub_Category_3']+"|"+main1['Sub_Category_4']
        main1 = main1[['Pdt', 'Brand','SKU','Pack', 'Category', 'Sub_Category_1', 'Sub_Category_2','Sub_Category_3', 'Sub_Category_4','Value','Relevant_Month','Combination','Relevant_Date']]
        print(main1.groupby(['Relevant_Month'])['Value'].sum())
        main1['Runtime'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
        main1['SKU'] = np.where(main1['SKU'].str.contains("None"),"Not Classified",main1['SKU'])
        main1['Pack'] = np.where(main1['Pack'].str.contains("None"),"Not Classified",main1['Pack'])
        client.execute("INSERT INTO AdqvestDB.AMAZON_BEAUTY_CPS_SCORE_Temp_Abdul VALUES", main1.values.tolist())
        del r
        del df
        del main1




if word_cloud:
    custom_stopwords = ["mix","herbalife","tablets","breakfast","break fast","sampann","break fast","khaman","break","grams","kilograms","milligrams","mg","fast","pack","powder","pouch","bag","strip","units","pcs","tablet","oil","oils","free","bottle", "gms","set","buy","get","free","count"]

    list_of_stopwords = stopwords.words('english')
    list_of_stopwords = list_of_stopwords + custom_stopwords
    list_of_stopwords = list(set(list_of_stopwords))
    q = 'select count(*),Relevant_Month,Category,Sub_Category_1,Sub_Category_2,Sub_Category_3,Sub_Category_4 from AdqvestDB.AMAZON_BEAUTY_CPS_SCORE_Temp_Abdul group by Relevant_Month,Category,Sub_Category_1,Sub_Category_2,Sub_Category_3,Sub_Category_4'
    df,cols = client.execute(q,with_column_types=True)
    df = pd.DataFrame(df, columns=[tuple[0] for tuple in cols])
    print(df)

    all_brand = pd.read_sql("select distinct(All_Brand) as All_Brand from AdqvestDB.AMAZON_BRANDS_ABHISHEK where Category='"+amazon_category.title()+"'", con=engine1)
    all_brand['All_Brand'] = all_brand['All_Brand'].apply(cleaner)


    all_catgs = df.copy()

    #Delete later
    #all_catgs = all_catgs[all_catgs['Sub_Category_2']=='Hair Oils']
    for k,row in all_catgs.iterrows():

        query = """Select * from AdqvestDB.AMAZON_BEAUTY_CPS_SCORE_Temp_Abdul where
                Relevant_Month = '"""+row['Relevant_Month']+"""' and
                Category = '"""+row['Category']+"""'and
                Sub_Category_1 = '"""+row['Sub_Category_1']+"""' and
                Sub_Category_2 = '"""+row['Sub_Category_2']+"""' and
                Sub_Category_3 = '"""+row['Sub_Category_3']+"""' and
                Sub_Category_4 = '"""+row['Sub_Category_4']+"""'"""

        df,cols = client.execute(query,with_column_types=True)
        df = pd.DataFrame(df, columns=[tuple[0] for tuple in cols])
        print(df)
        combination = df['Combination'][0]
        combo2 = row[2:]
        combo2 = combo2[combo2 != '']

        catgs = [word for line in list(combo2) for word in line.split()]
        catgs = [ ' '+ x.lower() + ' ' for x in catgs]
        list_of_stopwords = [ ' '+ x.lower() + ' ' for x in list_of_stopwords]
        amazon = df.copy()
        rep_brand = [ ' '+ x.lower() + ' ' for x in list(all_brand.iloc[:,0])]+['laxm','loreal','lorl']+list_of_stopwords+catgs
        amazon['Cleaned_Name'] = amazon['Pdt'].apply(cleaner)
        amazon['Cleaned_Name'] = amazon['Cleaned_Name'].str.replace('hair fall', 'hairfall')
        amazon['Cleaned_Name'] = amazon['Cleaned_Name'].str.replace('aloe vera', 'aloevera')
        amazon['Cleaned_Name'] = " " + amazon['Cleaned_Name'] + " "
        amazon['Cleaned_Name'] = amazon['Cleaned_Name'].apply(lambda x:re.sub(r'  +', ' ', x))
        amazon['Cleaned_Name'] = amazon['Cleaned_Name'].str.lower().replace(rep_brand ,' ',regex=True)
        amazon['Cleaned_Name'] = amazon.apply(lambda x: x['Cleaned_Name'].replace(x['Brand'].lower(),''), axis=1)

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
        reqd['Combination'] = combination
        # reqd['Relevant_Date'] = today.date()
        reqd['Runtime'] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
        reqd = reqd[['Bi_Grams', 'Value', 'Relevant_Date', 'Cleaned_Name', 'Pdt', 'Category', 'Sub_Category_1', 'Sub_Category_2', 'Sub_Category_3', 'Sub_Category_4', 'Combination','Runtime']]
        client.execute("INSERT INTO AdqvestDB.AMAZON_BEAUTY_WORD_CLOUD_REVISED VALUES", reqd.values.tolist())
