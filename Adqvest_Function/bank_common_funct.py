#!/usr/bin/env python

import pandas as pd
import camelot
import warnings
warnings.filterwarnings('ignore')
from dateutil import parser
import numpy as np
import sqlalchemy
from pandas.io import sql
from bs4 import BeautifulSoup
import calendar
import datetime as datetime
from pytz import timezone
import requests
import sys
import time
import os
import re
import sqlalchemy
import numpy as np
from selenium import webdriver
from pandas.core.common import flatten
# sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
# import JobLogNew as log
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
engine = adqvest_db.db_conn()


def row_col_index_locator(df,l1):
    import pandas as pd
    df.reset_index(drop=True,inplace=True)
    df.fillna('#', inplace=True)

    index2=[]
    dict1={}
    for j in l1:
        for i in range(df.shape[1]):
            if df.iloc[:,i].str.lower().str.contains(j.lower()).any()==True:
                print(f"Found--{j}")
                print(f"Column--{i}")
                index2.append(i)
                row_index=df[df.iloc[:, i].str.lower().str.contains(j.lower()) == True].index[0]
                print(f"Row--{row_index}")
                index2.append(row_index)
                break

    return index2

def get_desired_table(tables,search_str):
    import pandas as pd
    df_f=pd.DataFrame()
    for st in search_str:
        for i in range(tables.n):
            for col in tables[i].df.columns:
                if tables[i].df[col].str.lower().str.contains(st.lower()).any():
                    print(i)
                    tb=i
                    rcol=col
                    df=tables[i].df.copy()
                    df.dropna(axis=1,how='all',inplace=True)
                    df_f=pd.concat([df_f,df])
                    df_f.drop_duplicates(keep='first', inplace=True)
                    break
    return df_f

def row_modificator(df,l1,col_idx,row_del=False,keep_row=False,update_row=False):
  
    from collections import ChainMap
    keep_inx=[]
    print(type(l1[0]))
    if isinstance(l1[0],dict):
        l1=dict(ChainMap(*l1))
    else:
        l1=dict.fromkeys(l1,np.nan)

    for i in l1.keys():
        print(i)
        #     print(df)
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
#%%
def column_modificator(df,l1,update_col=False):
    from collections import ChainMap
    keep_inx=[]
    print(type(l1[0]))
    if isinstance(l1[0],dict):
        l1=dict(ChainMap(*l1))
    else:
        l1=dict.fromkeys(l1,np.nan)
        
    print(l1)      
    df.columns=df.columns.astype(str)
    df.reset_index(drop=True,inplace=True)
    df.fillna('#', inplace=True)
    
    index2=[]
    dict1={}
    for j in l1.keys():
        print(j)
        for i in range(df.shape[1]):
            if df.iloc[:,i].str.lower().str.contains(j.lower()).any()==True:
                print(f"found--{j}")
                print(f'Column_number-->{i}')
                dict1[i]=j
                index2.append(i)
                break
                    
    if (update_col==True):
        if (len(dict1)==0):
                 dict2=l1
                 print(dict1)
        else:
            dict2=dict1
            print(dict1)
            
        for k,v in dict2.items():
            print(k,v)
            if (len(dict1)==0):
                ele=[item for item in df.columns if k.lower() in item.lower()]
                k1=df.columns.to_list().index(ele[0])
                df=df.rename(columns={f"{df.columns[k1]}":f"{l1[k]}"})
                print(k1)
                df.reset_index(drop=True,inplace=True)
                df=df.replace('#',np.nan) 
            else:
                print(df.columns)
                
                df=df.rename(columns={f"{df.columns[k]}":f"{v}"})
                print('Problem')
#                 df.reset_index(drop=True,inplace=True)
#                 df=df.replace('#',np.nan) 
                # df[df.columns[k]]=df[df.columns[k]].str.strip()
        
    
    return df

def make_data_from_rows(df,str_search,split_str='   ',
                        extra_str=' ',str_replace=[]):
    
    req_row=row_col_index_locator(df,[str_search.lower()])
    my_var=df.iloc[req_row[1],req_row[0]]
    
    if len(str_replace)>0:
        from collections import ChainMap
        str_replace=dict(ChainMap(*str_replace))
        for k,v in str_replace.items():
            try: 
                my_var=my_var.replace(k,v)
                print(my_var)
            except:
                pass
    
    try:  
        my_var=my_var.split(extra_str)[0]
    except:
        pass
    
    my_var=my_var.split(split_str)
    my_var=[i.strip() for i in my_var if ":" not in i]
    my_var=[i.strip() for i in my_var if i!=""]
    my_val=df.iloc[req_row[1],req_row[0]+1]
    my_val=my_val.split()
    if len(my_val)!=len(my_var):
        print('Please Chaek Data manually')
    dict_1=dict(zip(my_var,my_val))
    print(dict_1)
    data=pd.DataFrame.from_dict([dict_1])
    data=data.T
    data['Var']=data.index
    data.index = range(len(data.index))
    data.columns=data.columns.astype(str)
    data=data.rename(columns={f"{df.columns[0]}":f"{'Value'}"})
    data=data[['Var','Value']]
    
    return data



