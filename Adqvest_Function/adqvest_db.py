#!/usr/bin/env python
# coding: utf-8


import sqlalchemy
import os
import pandas as pd

def db_conn():
    #**** Credential Directory ****
    os.chdir('C:/Users/Administrator/AdQvestDir/Adqvest_Function')
    # os.chdir('E:/Adqvest files')
    #os.chdir('C:/Users/krang/Dropbox/Subrata/Python')
    #os.chdir('D:/Adqvest_Office_work/R_Script')

    #DB Connection
    properties = pd.read_csv('AdQvest_properties.txt',delim_whitespace=True)
    #properties = pd.read_csv('AdQvest_properties.txt',delim_whitespace=True)

    host    = list(properties.loc[properties['Env'] == 'Host'].iloc[:,1])[0]
    port    = list(properties.loc[properties['Env'] == 'port'].iloc[:,1])[0]
    db_name = list(properties.loc[properties['Env'] == 'DBname'].iloc[:,1])[0]

    con_string = 'mysql+pymysql://' + host + ':' + port + '/' + db_name + '?charset=utf8'
    engine     = sqlalchemy.create_engine(con_string)
    return engine
