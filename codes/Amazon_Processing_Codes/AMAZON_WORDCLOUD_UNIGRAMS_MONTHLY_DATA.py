# -*- coding: utf-8 -*-
"""
Created on Tue Nov 22 14:48:45 2022

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
from nltk import word_tokenize
from gensim.utils import lemmatize
from collections import defaultdict
# import spacy
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import ClickHouse_db
translationTable = str.maketrans("éàèùâêîôûç", "eaeuaeiouc")
# nlp = spacy.load('en_core_web_lg',  disable=["parser", "ner"])


india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days


engine = adqvest_db.db_conn()

#os.chdir('C:/Adqvest')
os.chdir('C:/Users/Administrator/AdQvestDir')
#DB Connection
#conndetail = pd.read_csv('C:/Adqvest/Amazon_AdQvest_properties.txt',delim_whitespace=True)
conndetail = pd.read_csv('Amazon_AdQvest_properties.txt',delim_whitespace=True)
#os.chdir('C:/Users/Administrator/AdQvestDir')  # 'C:/Users/Administrator/Documents'
#os.environ["http_proxy"] = "http://localhost:12345"

hostdet = conndetail.loc[conndetail['Env'] == 'Host']
port = conndetail.loc[conndetail['Env'] == 'port']
DBname = conndetail.loc[conndetail['Env'] == 'DBname']
host = list(hostdet.iloc[:,1])
port = list(port.iloc[:,1])
dbname = list(DBname.iloc[:,1])
Connectionstring = 'mysql+pymysql://' + host[0] + ':' + port[0] + '/' + dbname[0]
engine_amz = sqlalchemy.create_engine(Connectionstring)

client = ClickHouse_db.db_conn()

#%%
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

def ngram_gen(x,n):
    return [" ".join(list(x)) for x in ngrams(x.split(), n)]

def strip_character(dataCol):
    r = re.compile(r'[^a-zA-Z !@#$%&*_+-=|\:";<>,./()[\]{}\']')
    return r.sub('', dataCol)


def remove_small_char(sent):

    return ' '.join([x for x in sent.split() if len(x) > 2])


def lemma_gensim(sentence,combo):

    tokens = word_tokenize(sentence)

    catgs = [lemmatize(re.sub('[^\w]','',word).strip())[0].decode('utf-8').split('/')[0].lower() if lemmatize(re.sub('[^\w]','',word).strip()) != [] else word.strip().lower() for line in combo for word in word_tokenize(re.sub('[^\w ]','',line))]

    delete = [j for j in tokens if len(lemmatize(j)) > 0 if lemmatize(j)[0].decode('utf-8').split('/')[0].lower() in catgs]

    for i in delete:

        tokens.remove(i)

    sent = ' '.join(tokens)

    return sent

def lemma_spacy(sentence,combo):

    doc = nlp(sentence)

    catgs = [token.lemma_ for i in combo for token in nlp(i.lower())]

    #catgs = [lemmatize(re.sub('[^\w]','',word).strip())[0].decode('utf-8').split('/')[0].lower() if lemmatize(re.sub('[^\w]','',word).strip()) != [] else word.strip().lower() for line in combo for word in word_tokenize(re.sub('[^\w ]','',line))]

    delete = [j.text for j in doc if j.lemma_ in catgs]

    tokens = [i.text for i in doc]

    for i in delete:

        tokens.remove(i)

    sent = ' '.join(tokens)

    return sent

def getParentBrand(brands):
    d = defaultdict(list)
    for i in brands:
        j = i.split()[0]
        d[j].append(i)

    final_dict = defaultdict(list)

    all_brand = []

    for parent, children in zip(d.keys(),d.values()):

        if (len(children) > 1) and (len(parent) > 1) and (bool(re.search('\A\d+\Z', parent)) == False):


            sorted_children = sorted(list(set(children)), key=len)

            if parent in sorted_children:

                sorted_children.remove(parent)

                for i in sorted_children:

                    final_dict[parent].append(i)

            else:

                for child in sorted_children:

                    final_dict[child] = []

        else:

            all_brand.append(parent)

            all_brand = all_brand + children

    all_brand = list(set(all_brand + list(final_dict.keys())))

    return all_brand



def Clean_Brands(df):

    df.drop_duplicates('Brand_Name', inplace = True)

    df['Brand_Name'] = df['Brand_Name'].apply(lambda x: x.replace('&', 'and'))
    df['Brand_Name'] = df['Brand_Name'].apply(lambda x: x.translate(translationTable))

    df['Brand_Name'] = df['Brand_Name'].apply(lambda x: re.sub('[^\w ]+','',x))

    df['Brand_Name'] = df['Brand_Name'].apply(lambda x: re.sub('  +',' ',x).strip())

    df.drop_duplicates(subset = ['Brand_Name'], inplace = True)

    brands = df['Brand_Name'].str.lower().to_list()

    brands = list(set(brands))

    all_brands = getParentBrand(brands)

    return all_brands


def getBrands(amazon_category):

    if 'Beauty' in amazon_category:

        data = pd.read_sql('select distinct Brand_Name from PURPLLE_CATEGORY_WISE_BRANDS_DAILY_DATA union all select distinct Brand_Name from NYKAA_BRANDS_PRICING_DAILY_DATA', engine)
        data['Brand_Name'] = data['Brand_Name'].apply(lambda x: x.strip())


        data_amz = pd.read_sql("select All_Brand as Brand_Name from AMAZON_CATEGORY_WISE_ALL_BRANDS_ONE_TIME where Category = 'Beauty' union all select distinct Brands as Brand_Name from BRAND_CORPUS where Source = 'Flipkart'", engine_amz)
        data_amz2 = pd.read_sql("select distinct Brands as Brand_Name from BRAND_CORPUS where Source != 'Flipkart'",engine_amz)
        data_amz['Brand_Name'] = [x.strip() if len(x.strip().split()) <= 3 and len(x.strip()) > 1 else None for x in data_amz['Brand_Name']]

        data_amz.dropna(inplace = True)

        data = pd.concat([data, data_amz,data_amz2]).sort_values('Brand_Name')
        cleaned_brands = Clean_Brands(data)

        return cleaned_brands

#%%
def run_program():
    tables_and_catgs = [
        {
         'Base_Table' : 'AMAZON_CPS_SCORES_MONTHLY_DATA',
         'Target_Table' : 'AMAZON_WORDCLOUD_UNIGRAMS_MONTHLY_DATA',
         'amazon_category' : 'Beauty'
         }
        ]


    for table_catg in tables_and_catgs:

        global_base_table = table_catg['Base_Table']
        global_target_table = table_catg['Target_Table']
        global_amazon_category = table_catg['amazon_category'].title().replace('&','and').replace(' And ',' and ')

        #final_df = pd.DataFrame()

        custom_stopwords = ["mix","herbalife","tablets","breakfast","break fast","sampann","break fast","khaman","break","grams","kilograms","milligrams","mg","fast","pack","powder","pouch","bag","strip","units","pcs","tablet","oil","oils","free","bottle", "gms","set","buy","get","free","count","for",'g','gm','each','gram','mm']

        list_of_stopwords = stopwords.words('english')
        list_of_stopwords = list_of_stopwords + custom_stopwords
        list_of_stopwords = list(set(list_of_stopwords))

        all_brand = getBrands(global_amazon_category)

        all_catgs,cols = client.execute(f"Select distinct Required_Category from {global_base_table} where Main_Category = '{global_amazon_category}'",with_column_types=True)
        all_catgs = pd.DataFrame(all_catgs, columns=[tuple[0] for tuple in cols])

        for _,cats in all_catgs.iterrows():

            max_date_base,cols = client.execute(f"Select max(Relevant_Date) as Max from {global_base_table} where Required_Category = '{cats['Required_Category']}'",with_column_types=True)
            max_date_base = pd.DataFrame(max_date_base, columns=[tuple[0] for tuple in cols])['Max'][0]

            max_date_target,cols = client.execute(f"Select max(Relevant_Date) as Max from {global_target_table} where Required_Category = '{cats['Required_Category']}'",with_column_types=True)
            max_date_target = pd.DataFrame(max_date_target, columns=[tuple[0] for tuple in cols])['Max'][0]

            if max_date_base > max_date_target:

                # q = f'select count(*),Relevant_Month,Required_Category from AdqvestDB.{global_base_table} where month(Relevant_Date) in (select month(max(Relevant_Date)) from AdqvestDB.{global_base_table}) and year(Relevant_Date) in (select year(max(Relevant_Date)) from AdqvestDB.{global_base_table}) where Main_Category = "{global_amazon_category}" group by Relevant_Month,Required_Category'

                q = f"select count(*),Relevant_Date,Required_Category from AdqvestDB.{global_base_table}  where Relevant_Date > '{str(max_date_target)}' and Required_Category = '{cats['Required_Category']}' group by Relevant_Date,Required_Category order by Relevant_Date"

                df,cols = client.execute(q,with_column_types=True)
                df = pd.DataFrame(df, columns=[tuple[0] for tuple in cols])

                if df.empty:
                    continue


                # all_brand = pd.read_sql("select distinct(All_Brand) as All_Brand from AdqvestDB.AMAZON_CATEGORY_WISE_ALL_BRANDS_ONE_TIME where Category='"+global_amazon_category+"'", con=engine)
                # all_brand['All_Brand'] = all_brand['All_Brand'].apply(cleaner)

                for _,row in df.iterrows():


                    print(row['Relevant_Date'], '  ',row['Required_Category'])

                    query = f"""Select * from AdqvestDB.{global_base_table} where Relevant_Date = '{row['Relevant_Date']}' and Required_Category = '{row['Required_Category']}'"""

                    amazon,cols = client.execute(query,with_column_types=True)
                    amazon = pd.DataFrame(amazon, columns=[tuple[0] for tuple in cols])

                    if amazon.empty:
                        continue

                    combo2 = [global_amazon_category , row['Required_Category']]

                    catgs = [re.sub('[^\w]','',word).strip() for line in combo2 for word in line.split()]
                    catgs = [ ' '+ x.lower() + ' ' for x in catgs]
                    # list_of_stopwords = [ ' '+ x.lower() + ' ' for x in list_of_stopwords]
                    # amazon = df.copy()
                    rep_brand = [ ' '+ x.lower() + ' ' for x in all_brand]+[' laxm ',' loreal ',' lorl ']+catgs

                    amazon['Cleaned_Name'] = amazon['Pdt'].apply(cleaner)
                    amazon['Cleaned_Name'] = amazon['Cleaned_Name'].str.replace('hair fall', 'hairfall')
                    amazon['Cleaned_Name'] = amazon['Cleaned_Name'].str.replace('aloe vera', 'aloevera')
                    amazon['Cleaned_Name'] = " " + amazon['Cleaned_Name'] + " "
                    amazon['Cleaned_Name'] = amazon['Cleaned_Name'].apply(lambda x:re.sub(r'  +', ' ', x))
                    amazon['Cleaned_Name'] = amazon.apply(lambda x: x['Cleaned_Name'].replace(x['Child_Brand'].lower(),''), axis=1)
                    amazon['Cleaned_Name'] = amazon['Cleaned_Name'].apply(lambda x: re.sub("|".join(sorted(rep_brand, key = len, reverse = True)), ' ', x))
                    amazon['Cleaned_Name'] = amazon['Cleaned_Name'].apply(lambda x: ' '.join([word for word in word_tokenize(x) if word not in (list_of_stopwords)]))
                    # amazon['Cleaned_Name'] = amazon['Cleaned_Name'].str.lower().replace(rep_brand ,' ',regex=True)

                    limit = 0
                    while True:
                        try:
                            amazon['Cleaned_Name'] = amazon['Cleaned_Name'].apply(lambda x: lemma_gensim(x,combo2))
                            break
                        except:
                            limit += 1
                            if limit > 7:
                                raise Exception("Gensim Generator Error")


                    amazon['Cleaned_Name'] = amazon['Cleaned_Name'].apply(remove_small_char)

                    amazon['Cleaned_Name'] = amazon['Cleaned_Name'].apply(lambda x:re.sub(r'  +', ' ', x).strip())

                    n_grams = 1


                    if n_grams == 1:
                        amazon['Uni_Grams'] = amazon['Cleaned_Name'].apply(lambda x : ngram_gen(x,1))

                        reqd = amazon[['Uni_Grams','Value','Relevant_Date','Cleaned_Name','Pdt']]
                        reqd['Uni_Grams'] = reqd['Uni_Grams'].apply(lambda x : [x])
                        #reqd['Words'] = reqd.explode(['Uni_Grams'],axis=1)
                        reqd = (reqd['Uni_Grams'].apply(lambda x: pd.Series(x[0]))
                                  .stack()
                                  .reset_index(level=1, drop=True)
                                  .to_frame('Uni_Grams')
                                  .join(reqd[['Value','Relevant_Date','Cleaned_Name','Pdt']], how='left'))

                        reqd['Main_Category'] = global_amazon_category

                        reqd['Required_Category'] = row['Required_Category']
                        # reqd['Relevant_Date'] = today.date()
                        reqd['Runtime'] = reqd['Relevant_Date'].apply(lambda x: pd.to_datetime(x.strftime("%Y-%m-%d %H:%M:%S")))
                        reqd = reqd[['Uni_Grams', 'Value', 'Cleaned_Name', 'Pdt', 'Main_Category', 'Required_Category', 'Relevant_Date','Runtime']]
                        #final_df = pd.concat([final_df,reqd])
                        client.execute(f"INSERT INTO AdqvestDB.{global_target_table} VALUES", reqd.values.tolist())

                    elif n_grams == 2:
                        amazon['Bi_Grams'] = amazon['Cleaned_Name'].apply(lambda x : ngram_gen(x,2))

                        reqd = amazon[['Bi_Grams','Value','Relevant_Date','Cleaned_Name','Pdt']]
                        reqd['Bi_Grams'] = reqd['Bi_Grams'].apply(lambda x : [x])
                        #reqd['Words'] = reqd.explode(['Bi_Grams'],axis=1)
                        reqd = (reqd['Bi_Grams'].apply(lambda x: pd.Series(x[0]))
                                  .stack()
                                  .reset_index(level=1, drop=True)
                                  .to_frame('Bi_Grams')
                                  .join(reqd[['Value','Relevant_Date','Cleaned_Name','Pdt']], how='left'))

                        reqd['Main_Category'] = global_amazon_category

                        reqd['Combination'] = row['Required_Category']
                        # reqd['Relevant_Date'] = today.date()
                        reqd['Runtime'] = reqd['Relevant_Date'].apply(lambda x: pd.to_datetime(x.strftime("%Y-%m-%d %H:%M:%S")))
                        reqd = reqd[['Bi_Grams', 'Value', 'Cleaned_Name', 'Pdt', 'Main_Category', 'Required_Category', 'Relevant_Date','Runtime']]
                        #client.execute(f"INSERT INTO AdqvestDB.{global_target_table} VALUES", reqd.values.tolist())

run_program()
