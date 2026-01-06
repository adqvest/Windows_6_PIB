# -*- coding: utf-8 -*-
"""
Created on Fri Jan 28 12:00:54 2022

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
import JobLogNew as log

from nltk.util import ngrams
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.collocations import *


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

def strip_character(dataCol):
    r = re.compile(r'[^a-zA-Z !@#$%&*_+-=|\:";<>,./()[\]{}\']')
    return r.sub('', dataCol)

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    #os.chdir('/home/ubuntu/AdQvestDir')

    engine = adqvest_db.db_conn()
    connection = engine.connect()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'FMCG_CORPUS_BUILDER'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        #wd = "C:/Adqvest/Grocery_&_Gourmet/"
        wd = "C:/Users/Administrator/AdQvestDir/Spell_Check_Corpus/"
        category1 = "Grocery & Gourmet Foods"
        fname = category1.replace(" ","_")
        start_time = timeit.default_timer()

        max_date,cols = client.execute("Select max(Relevant_Date) from AdqvestDB.AMAZON_DATA_CLEAN where Category = '"+category1+"'",with_column_types=True)
        max_date = pd.DataFrame(max_date, columns=[tuple[0] for tuple in cols]).iloc[0,0]

        #min_date = max_date + relativedelta(months=-60)
        min_date,cols = client.execute("Select min(Relevant_Date) from AdqvestDB.AMAZON_DATA_CLEAN where Category = '"+category1+"'",with_column_types=True)
        min_date = pd.DataFrame(min_date, columns=[tuple[0] for tuple in cols]).iloc[0,0]
        start = fdm(min_date)
        finish = ldm(max_date)

        date_iters = []
        while start<=finish:

          a = start
          date_iters.append(a)
          start = start+datetime.timedelta(1)


        import nltk

        all_counters = []

        #ngram = 2
        #reverse = True
        ngram = 1
        reverse = False


        ngram_list = [1,2,2]
        reverse_list = [False,False,True]

        ngram_list = [2,2]
        reverse_list = [False,True]
        #ngram_list = [2]
        #reverse_list = [True]

        print(min(date_iters),"###",max(date_iters))
        for ngram,reverse in zip(ngram_list,reverse_list):
          for dates in date_iters:

              query3 = "Select * from AdqvestDB.AMAZON_DATA_CLEAN where Category = '"+category1+"' and Relevant_Date = '"+str(dates)+"';"
              df,cols = client.execute(query3,with_column_types=True)
              df = pd.DataFrame(df, columns=[tuple[0] for tuple in cols])

              if df.empty:
                  continue

              from collections import Counter

              output = df.copy()
              output = output[['Name']]
              pattern = re.compile(r'\s+')

              spl_chars = list(set(list(' '.join(output.Name))))
              spl_chars = [y for y in spl_chars if ((y.isalnum()==False) & (y!=' '))]

              for vals in spl_chars:

                output['Name'] = output['Name'].str.replace(vals,"")

              if ngram==2:
                output['Name'] = output['Name'].apply(lambda x : re.sub('[^a-zA-Z]+', ' ', x))
              else:
                output['Name'] = output['Name'].apply(lambda x : re.sub('[\W_]+', ' ', x))

              output['Name'] = output['Name'].apply(lambda x : re.sub(pattern, ' ', x))

              if ngram==2:
                if reverse:
                  output['Tokenized'] = output.Name.str.split().apply(lambda x: [ "|".join(pair[::-1]) for pair in nltk.bigrams(x)])
                else:
                  output['Tokenized'] = output.Name.str.split().apply(lambda x: [ "|".join(pair) for pair in nltk.bigrams(x)])

              else:

                output['Tokenized'] = output.Name.str.split().apply(lambda x: dict(Counter(x)))

              compiled = output['Tokenized'].to_list()
              result = Counter()
              for d in compiled:
                  result.update(d)


              all_counters.append(result)
              print("DONE : ",dates)


          alt = all_counters
          corpus = Counter()
          for d in alt:
              corpus.update(d)

          if ngram==2:
            if reverse:
              dict_corpus = dict(corpus)
              with open(wd+fname+"_Reverse_Pairs_Corpus.txt", 'w') as f:
                  for key, value in dict_corpus.items():
                      f.write('%s:%s\n' % (key, value))
            else:
              dict_corpus = dict(corpus)
              with open(wd+fname+"_Pairs_Corpus.txt", 'w') as f:
                  for key, value in dict_corpus.items():
                      f.write('%s:%s\n' % (key, value))
          else:
            dict_corpus = dict(corpus)
            with open(wd+fname+"_Corpus.txt", 'w') as f:
                for key, value in dict_corpus.items():
                    f.write('%s:%s\n' % (key, value))


        elapsed = timeit.default_timer() - start_time
        print("TIME ELAPSED : ",elapsed)

        with open(wd+fname+"_Corpus.txt") as f:
          d1 = dict(x.rstrip().split(":", 1) for x in f)
        d1 = dict((k,int(v)) for k,v in d1.items())


        with open(wd+fname+"_Pairs_Corpus.txt") as f:
          d2 = dict(x.rstrip().split(":", 1) for x in f)
        d2 = dict((k,int(v)) for k,v in d2.items())

        with open(wd+fname+"_Reverse_Pairs_Corpus.txt") as f:
          d3 = dict(x.rstrip().split(":", 1) for x in f)
        d3 = dict((k,int(v)) for k,v in d3.items())



        single = pd.DataFrame(d1.items(), columns=['Words', 'Value'])
        single['Value'] = single['Value'].astype(float)
        single = single.sort_values("Value",ascending=False)
        single['Cumsum'] = single['Value'].cumsum()
        single['Cumsum_Percent'] = (single['Cumsum']/sum(single['Value']))*100

        double = pd.DataFrame(d2.items(), columns=['Words', 'Value'])
        double['Value'] = double['Value'].astype(int)
        double = double.sort_values("Value",ascending=False)
        double['Cumsum'] = double['Value'].cumsum()
        double['Cumsum_Percent'] = (double['Cumsum']/sum(double['Value']))*100

        double_rev = pd.DataFrame(d3.items(), columns=['Words', 'Value'])
        double_rev['Value'] = double_rev['Value'].astype(float)
        double_rev = double_rev.sort_values("Value",ascending=False)
        double_rev['Cumsum'] = double_rev['Value'].cumsum()
        double_rev['Cumsum_Percent'] = (double_rev['Cumsum']/sum(double_rev['Value']))*100


        a = single[single['Value']>=25]#W
        b = double[double['Value'] >= 100]#2W
        c = double_rev[double_rev['Value'] >= 100]#2W

        a = single[single['Value']>=10]#W
        b = double[double['Value'] >= 95]#2W
        c = double_rev[double_rev['Value'] >= 95]#2W

        single =  dict(zip(a.Words, a.Value.astype(int)))
        double =  dict(zip(b.Words, b.Value.astype(int)))
        double_rev =  dict(zip(c.Words, c.Value.astype(int)))

        with open (wd+fname+'_1W.txt', 'w') as fp:
            for p in single.items():
                fp.write("%s\t%s\n" % p)
        with open (wd+fname+'_2W.txt', 'w') as fp:
            for p in double.items():
                fp.write("%s\t%s\n" % p)
        with open (wd+fname+'_2W_Rev.txt', 'w') as fp:
            for p in double_rev.items():
                fp.write("%s\t%s\n" % p)


        log.job_end_log(table_name,job_start_time, no_of_ping)


    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
