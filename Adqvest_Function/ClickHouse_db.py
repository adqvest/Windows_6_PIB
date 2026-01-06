#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import pandas as pd
from clickhouse_driver import Client

def db_conn():
    #**** Credential Directory ****
    os.chdir('C:/Users/Administrator/AdQvestDir/Adqvest_Function')
    #os.chdir('/home/ubuntu/AdQvestDir')
    #os.chdir('E:/Adqvest files')
    #os.chdir('C:/Users/sakhu/OneDrive/Desktop/Adqvest/002_Python')
    #os.chdir('D:/Adqvest_Office_work/R_Script')

    #ClickHouse DB Connection
    properties = pd.read_csv('Adqvest_ClickHouse_properties.txt',delim_whitespace=True)

    host            = list(properties.loc[properties['Env'] == 'Host'].iloc[:,1])[0]
    port            = list(properties.loc[properties['Env'] == 'Port'].iloc[:,1])[0]
    db_name         = list(properties.loc[properties['Env'] == 'DB_Name'].iloc[:,1])[0]
    user_name       = list(properties.loc[properties['Env'] == 'User_Name'].iloc[:,1])[0]
    password_string = list(properties.loc[properties['Env'] == 'Password_String'].iloc[:,1])[0]

    client = Client(host, user=user_name, password=password_string, database=db_name, port=port)

    return client
