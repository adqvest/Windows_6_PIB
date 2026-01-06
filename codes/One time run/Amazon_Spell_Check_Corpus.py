# -*- coding: utf-8 -*-
"""
Created on Thu Dec 23 16:40:40 2021

@author: Abhishek Shankar
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Dec  2 09:57:34 2021

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

from nltk.util import ngrams
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.collocations import *

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

def strip_character(dataCol):
    r = re.compile(r'[^a-zA-Z !@#$%&*_+-=|\:";<>,./()[\]{}\']')
    return r.sub('', dataCol)


#%%
start_time = timeit.default_timer()

max_date,cols = client.execute("Select max(Relevant_Date) from AdqvestDB.AMAZON_DATA_CLEAN where Category = 'Beauty'",with_column_types=True)
max_date = pd.DataFrame(max_date, columns=[tuple[0] for tuple in cols]).iloc[0,0]

#min_date = max_date + relativedelta(months=-60)
min_date,cols = client.execute("Select min(Relevant_Date) from AdqvestDB.AMAZON_DATA_CLEAN where Category = 'Beauty'",with_column_types=True)
min_date = pd.DataFrame(min_date, columns=[tuple[0] for tuple in cols]).iloc[0,0]
start = fdm(min_date)
finish = ldm(max_date)

date_iters = []
while start<=finish:

  a = start
  date_iters.append(a)
  start = start+datetime.timedelta(1)


import nltk
import random


all_counters = []

#ngram = 2
#reverse = True
ngram = 1
reverse = False


ngram_list = [1,2,2]
reverse_list = [False,False,True]

ngram_list = [2]
reverse_list = [True]

print(min(date_iters),"###",max(date_iters))
for ngram,reverse in zip(ngram_list,reverse_list):
  for dates in date_iters:
      #print(dates)
      query3 = "Select * from AdqvestDB.AMAZON_DATA_CLEAN where Category = 'Beauty' and Relevant_Date = '"+str(dates)+"';"
      df,cols = client.execute(query3,with_column_types=True)
      df = pd.DataFrame(df, columns=[tuple[0] for tuple in cols])
      #print(df.dtypes)
      #bigrams = ngrams(df['Name'], 2)
      #pd.Series(bigrams).value_counts()
      if df.empty:
          continue
      #product_name = " ".join(df['Name'].to_list())
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
#        output['Name'] = output['Name'].apply()
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
      with open("C:/Users/Administrator/AdQvestDir/Spell_Check_Corpus/Beauty_Reverse_Pairs_Corpus.txt", 'w') as f:
          for key, value in dict_corpus.items():
              f.write('%s:%s\n' % (key, value))
    else:
      dict_corpus = dict(corpus)
      with open("C:/Users/Administrator/AdQvestDir/Spell_Check_Corpus/Beauty_Pairs_Corpus.txt", 'w') as f:
          for key, value in dict_corpus.items():
              f.write('%s:%s\n' % (key, value))
  else:
    dict_corpus = dict(corpus)
    with open("C:/Users/Administrator/AdQvestDir/Spell_Check_Corpus/Beauty_Corpus.txt", 'w') as f:
        for key, value in dict_corpus.items():
            f.write('%s:%s\n' % (key, value))


elapsed = timeit.default_timer() - start_time
print("TIME ELAPSED : ",elapsed)
