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
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
engine = adqvest_db.db_conn()
from bank_common_funct import *
from bank_common_funct2 import *
def clean_dataframe(df):
    df=df.replace('',np.nan).replace('#',np.nan)
    df.dropna(axis=1,how='all',inplace=True)
    df.reset_index(drop=True,inplace=True)
    return df

def get_correct_links(links_capital):
    links_capital=links_capital[links_capital['Bank_Capital_Requirement_Operational_Risk_Status']!='COMPLETED']
    columns = links_capital.columns.to_list()
    columns.remove('Runtime')
    columns.remove('Last_Updated')
    unique = links_capital.drop_duplicates(subset=columns, keep='last')
    unique.reset_index(drop=True,inplace=True)
    return unique
def get_error_df(table_name,bank,e,date,df_error_final):
    df_error=pd.DataFrame(columns=['Table_name','Bank_Name','Error_message','Relevant_Date','Runtime'])
    df_error = df_error.append({
    'Table_name': table_name,
    'Bank_Name': bank,
    'Error_message': str(e),
    'Relevant_Date': date,
    'Runtime': pd.to_datetime('now')
    }, ignore_index=True)
    df_error_final=df_error_final.append(df_error)
    df_error_final.to_sql('BANK_ERROR_TABLE', index=False, if_exists='append', con=engine)
    return df_error_final
def update_basel_iii(bank_name,date):
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    connection.execute("update BANK_BASEL_III_QUARTERLY_LINKS set Bank_Capital_Requirement_Operational_Risk_Status = 'COMPLETED' where Relevant_Date = '"+date+"' and Bank= '"+bank_name+"'")
    

bank_name='BANK_OVERALL_NPA_QTLY_DATA'


# In[6]:


bank='STATE BANK OF INDIA'
max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
for a,values in links.iterrows():
    link=values['Links']
    date=values['Relevant_Date']
    bank=values['Bank']
    bank_type=values['Bank_Type']
link=links_capital['Links'][0]
print(link)
bank_type = links_capital['Bank_Type'][0]
# bank = links_capital['Links'][0]
date = links_capital['Relevant_Date'][0]
tables = extract_tables_from_pdf(link,'stream',10,200,40)
df=data_collection(tables,bank_type,bank,date,'camelot',1)


# In[8]:


bank='HDFC BANK'
max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
link=links_capital['Links'][0]
link=link.replace(' ','%20')
print(link)
bank_type = links_capital['Bank_Type'][0]
# bank = links_capital['Links'][0]
date = links_capital['Relevant_Date'][0]
tables = extract_tables_from_pdf(link,'stream',10,200,40)
df=data_collection(tables,bank_type,bank,date)


# In[9]:


df


# In[14]:


bank='AXIS BANK'
max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
search_str=['doubtful 1']
link=links_capital['Links'][0]
bank_type = links_capital['Bank_Type'][0]
# bank = links_capital['Links'][0]
date = links_capital['Relevant_Date'][0]
tables = extract_tables_from_pdf(link,'stream',10,200,40)
df=data_collection(tables,bank_type,bank,date)


# In[88]:


def data_collection_central(tables,bank_type,bank,date,type_read,loop):
    try:
        if type_read=='camelot':
            df = get_desired_table(tables, ['substandard'])
        else:
            df=tables
        col = row_col_index_locator(df, ['doubtful'])[0]
        strt_index = row_col_index_locator(df, ['substandard'])[1]
        end_index = row_col_index_locator(df, ['movement'])[1]
        df=df.iloc[strt_index:end_index,:]
        df=df[df[1]!='']
        df.reset_index(drop=True,inplace=True)
        df[col][len(df)-2]='gross advance'
        df[col][len(df)-1]='net advance'
        df=df.replace('', '').replace('',np.nan)
        df.dropna(axis=1,how='all',inplace=True)
        df = df.T
        df.reset_index(drop=True, inplace=True)
        df.columns = df.iloc[0, :]
        df = df.iloc[1:]
        df.reset_index(drop=True, inplace=True)
        df.columns=['Sub_Standard_In_Mn','Doubtful_1_In_Mn','Doubtful_2_In_Mn','Doubtful_3_In_Mn','Loss_In_Mn','Net_NPA_In_Mn','Net_NPA_To_NET_Advances_Pct','Gross_NPA_To_Gross_Advances_Pct']
        try:
            df = df.replace('', np.nan).astype('float')
        except:
            df.columns=df.iloc[0]
            df = df.drop(df.index[0])
            df = df.replace('', np.nan).astype('float')
        df.fillna(0, inplace=True)
        df['Gross_NPA_Mn'] = round(df['Sub_Standard_In_Mn'] + df['Doubtful_1_In_Mn'] + df['Doubtful_2_In_Mn'] + df['Doubtful_3_In_Mn'] + df['Loss_In_Mn'], 4)
        df['Provisions_In_Mn'] = round(df['Gross_NPA_Mn'] - df['Net_NPA_In_Mn'], 4)
        today = pd.Timestamp.now()

        df['Bank_Type'] = bank_type
        df['Bank'] = bank
        df['Relevant_Date'] = date
        df['Runtime'] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
        return df
    except:
        if loop<2:
            print('Falied with camelot trying with ilovepdf')
            path,sheets=read_data_using_ilovepdf(link,bank)
            heading='substandard'
            end_text='net advances'
            df=search(heading,end_text,path,sheets)
            df=data_collection_central(df,bank_type,bank,date,'ilovepdf',2)
        else:
            raise Exception("Something went wrong either camelot or illovepdf worked check manually")
    return df 


# In[99]:


bank='CENTRAL BANK OF INDIA'
max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
link=links_capital['Links'][0]
link='https://www.centralbankofindia.co.in/sites/default/files/Basel-Disclosure/BASEL%20III%20Disclosures%20as%20on%2031.12.2022.pdf'
print(link)
bank_type = links_capital['Bank_Type'][0]
date = links_capital['Relevant_Date'][0]
tables = extract_tables_from_pdf(link,True,'stream',10,300,70)
df=data_collection_central(tables,bank_type,bank,date,'camelot',1)


# In[31]:


def data_collection(tables,search_str,bank_type,bank,date,type_read,loop):
    try:
        if type_read=='camelot':
            if bank=='AU SMALL FINANCE BANK':
                df = get_desired_table(tables, ['doubtful1', 'doubtful2'])
            else:
                df = get_desired_table(tables, ['doubtful 1', 'doubtful 2'])
        else:
            df=tables
            df=df.reset_index(drop=True,inplace=False)
        col = row_col_index_locator(df, ['doubtful'])[0]
        print(df)
        if bank=='CITY UNION BANK':
            row_dict = [
                {'Sub-standard': 'Sub_Standard_In_Mn'},
                {'Doubtful 1': 'Doubtful_1_In_Mn'},
                {'Doubtful 2': 'Doubtful_2_In_Mn'},
                {'Doubtful 3': 'Doubtful_3_In_Mn'},
                {'loss': 'Loss_In_Mn'},
                {'npa total': 'Net_NPA_In_Mn'},
                {'net advances': 'Net_NPA_To_NET_Advances_Pct'},
                {'gross advances': 'Gross_NPA_To_Gross_Advances_Pct'}
            ]
        elif bank=='AU SMALL FINANCE BANK':
            row_dict = [
                {'Substandard': 'Sub_Standard_In_Mn'},
                {'Doubtful1': 'Doubtful_1_In_Mn'},
                {'Doubtful2': 'Doubtful_2_In_Mn'},
                {'Doubtful3': 'Doubtful_3_In_Mn'},
                {'loss': 'Loss_In_Mn'},
                {'net npa': 'Net_NPA_In_Mn'},
                {'net advances': 'Net_NPA_To_NET_Advances_Pct'},
                {'gross advances': 'Gross_NPA_To_Gross_Advances_Pct'}
            ]
        else:
            row_dict = [
                {'Substandard': 'Sub_Standard_In_Mn'},
                {'Doubtful 1': 'Doubtful_1_In_Mn'},
                {'Doubtful 2': 'Doubtful_2_In_Mn'},
                {'Doubtful 3': 'Doubtful_3_In_Mn'},
                {'loss': 'Loss_In_Mn'},
                {'net npa': 'Net_NPA_In_Mn'},
                {'net advances': 'Net_NPA_To_NET_Advances_Pct'},
                {'gross advances': 'Gross_NPA_To_Gross_Advances_Pct'}
            ]

        df = row_modificator(df, row_dict, col, row_del=False, keep_row=True, update_row=True)
        df=df.replace('#','')
        df=df.replace('', '').replace('',np.nan)
        df.dropna(axis=1,how='all',inplace=True)
        df.reset_index(drop=True,inplace=True)
        if bank=='CITY UNION BANK':
            for col in range(1,len(df.columns)-1):
                for i in range(len(df)):
                    if pd.isna(df.iloc[i, col]):
                        df.iloc[i, col] = df.iloc[i, col + 1]
                        df.iloc[i, col + 1] = np.nan
            df.dropna(axis=1,how='all',inplace=True)
            df.reset_index(drop=True,inplace=True)
        if bank=='HDFC BANK':
            df=df.iloc[:,:2]
        df = df.T
        df.reset_index(drop=True, inplace=True)
        df.columns = df.iloc[0, :]
        df = df.iloc[1:]
        df.reset_index(drop=True, inplace=True)
        try:
            df = df.replace('', np.nan).astype('float')
        except:
            df.columns=df.iloc[0]
            df = df.drop(df.index[0])
            df = df.replace('', np.nan).astype('float')
        df.fillna(0, inplace=True)
        df['Gross_NPA_Mn'] = round(df['Sub_Standard_In_Mn'] + df['Doubtful_1_In_Mn'] + df['Doubtful_2_In_Mn'] + df['Doubtful_3_In_Mn'] + df['Loss_In_Mn'], 4)
        df['Provisions_In_Mn'] = round(df['Gross_NPA_Mn'] - df['Net_NPA_In_Mn'], 4)
        today = pd.Timestamp.now()

        df['Bank_Type'] = bank_type
        df['Bank'] = bank
        df['Relevant_Date'] = date
        df['Runtime'] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
    except:
        if loop<2:
            print('Falied with camelot trying with ilovepdf')
            path,sheets=read_data_using_ilovepdf(link,bank)
            heading='doubtful'
            end_text='net advances'
            df=search(heading,end_text,path,sheets)
            df=data_collection(df,heading,bank_type,bank,date,'ilovepdf',2)
        else:
            raise Exception("Something went wrong either camelot or illovepdf worked check manually")
    return df 


# In[33]:


bank='FEDERAL BANK'
max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
link='https://www.federalbank.co.in/documents/10180/327310020/LCR_NSFR_Disclosure_June+2023+new.pdf/47033ddb-f237-92f1-3a4a-62c9d05c071a?t=1689329173839'
link=links_capital['Links'][0]
print(link)
link=link.replace(' ','%20')
bank_type = 'private'
date = '2023-03-31'
search_str=['doubtful 1']
try:
    tables = extract_tables_from_pdf(link,False,'stream',10,100,40)
    df=data_collection(tables,search_str,bank_type,bank,date,'camelot',1)
except:
    path,sheets=read_data_using_ilovepdf(link,bank)
    heading='interest rate risk'
    end_text='total'
    df=search(heading,end_text,path,sheets)
    df=data_collection(df,search_str,bank_type,bank,date,'ilovepdf',1)


# In[27]:


bank='INDUSIND BANK'
max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
link=links_capital['Links'][0]
link=link.replace(' ','%20')
bank_type = 'private'
date = '2023-03-31'
print(link)
search_str=['foreign exchange risk']
tables = extract_tables_from_pdf(link,False,'stream',10,100,40)
df=data_collection(tables,search_str,bank_type,bank,date,'camelot',1)


# In[29]:


bank='IDBI BANK'
max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
link='https://www.idbibank.in/pdf/basel/Pillar-III-December-2022.pdf'
link=link.replace(' ','%20')
bank_type = 'private'
date = '2023-03-31'
print(link)
search_str=['interest rate risk']
tables = extract_tables_from_pdf(link,False,'lattice',10,100,40)
df=data_collection(tables,search_str,bank_type,bank,date,'camelot',1)


# In[30]:


df


# In[28]:


bank='RBL BANK'
max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
link=links_capital['Links'][0]
link=link.replace(' ','%20')
bank_type = 'private'
date = '2023-03-31'
print(link)
search_str=['foreign exchange risk']
tables = extract_tables_from_pdf(link,False,'stream',10,100,40)
df=data_collection(tables,search_str,bank_type,bank,date,'camelot',1)


# In[24]:


bank='BANK OF BARODA'
max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
link='https://www.bankofbaroda.in/-/media/Project/BOB/CountryWebsites/India/shareholders-corner2/2023/23-06/basel-iii-disclosures-mar-2023-final-31-10.pdf'
link=link.replace(' ','%20')
bank_type = 'private'
date = '2023-03-31'
print(link)
search_str=['foreign exchange risk']
tables = extract_tables_from_pdf(link,False,'stream',10,100,40)
df=data_collection(tables,search_str,bank_type,bank,date,'camelot',1)


# In[17]:


bank='AU SMALL FINANCE BANK'
# max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
search_str=['securitised portfolio']
link='https://www.aubank.in/Basel-ll-Pillar-lll-Disclosures-31-mar23-Basel-ll-Pillar-lll-Disclosures-31-mar23.pdf'
bank_type = 'private'
date = '2023-03-31'
tables = extract_tables_from_pdf(link,True,'stream',10,200,70)
search_str=['doubtful1']
df=data_collection(tables,search_str,bank_type,bank,date,'camelot',1)


# In[95]:


bank='CITY UNION BANK'
max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
search_str=['doubtful 1']
link=links_capital['Links'][0]
print(link)
bank_type = links_capital['Bank_Type'][0]
date = links_capital['Relevant_Date'][0]
tables = extract_tables_from_pdf(link,False,'stream',10,100,40)
df=data_collection(tables,bank_type,bank,date,'camelot',1)


# In[100]:


def data_collection_icici(tables,bank_type,bank,date,type_read,loop):
    try:
        if type_read=='camelot':
            df = get_desired_table(tables, ['doubtful'])
        else:
            df=tables
        col = row_col_index_locator(df, ['doubtful'])[0]
        try:
            row_dict = [
                {'Substandard': 'Sub_Standard_In_Mn'},
                {'Doubtful 1': 'Doubtful_1_In_Mn'},
                {'Doubtful 2': 'Doubtful_2_In_Mn'},
                {'Doubtful 3': 'Doubtful_3_In_Mn'},
                {'loss': 'Loss_In_Mn'},
            ]
            df2 = row_modificator(df, row_dict, col, row_del=False, keep_row=True, update_row=True)
        except:
            row_dict = [
                {'Substandard': 'Sub_Standard_In_Mn'},
                {'Doubtful 11': 'Doubtful_1_In_Mn'},
                {'Doubtful 21': 'Doubtful_2_In_Mn'},
                {'Doubtful 31': 'Doubtful_3_In_Mn'},
                {'loss': 'Loss_In_Mn'},
            ]
            df2 = row_modificator(df, row_dict, col, row_del=False, keep_row=True, update_row=True)
        df2=df2.replace('',np.nan)
        df2.dropna(axis=1,how='all',inplace=True)
        df2.reset_index(drop=True,inplace=True)
        net_npa_col=row_col_index_locator(df, ['net npl'])[0]
        net_npa_row=row_col_index_locator(df, ['total'])[1]
        ratio_row=row_col_index_locator(df, ['npl ratio'])[1]
        gross_col=row_col_index_locator(df, ['gross npl'])[0]
        df1=pd.DataFrame({0:['Net_NPA_In_Mn','Net_NPA_To_NET_Advances_Pct','Gross_NPA_To_Gross_Advances_Pct'],
                         1:[df[net_npa_col][net_npa_row],df[net_npa_col][ratio_row],df[gross_col][ratio_row]]})
        df = pd.concat([df1, df2.iloc[:,:2]], ignore_index=True)
        df = df.T
        df.reset_index(drop=True, inplace=True)
        df.columns = df.iloc[0, :]
        df = df.iloc[1:]
        df.reset_index(drop=True, inplace=True)
        df.columns=['Sub_Standard_In_Mn','Doubtful_1_In_Mn','Doubtful_2_In_Mn','Doubtful_3_In_Mn','Loss_In_Mn','Net_NPA_In_Mn','Net_NPA_To_NET_Advances_Pct','Gross_NPA_To_Gross_Advances_Pct']
        try:
            df = df.replace('', np.nan).astype('float')
        except:
            df.columns=df.iloc[0]
            df = df.drop(df.index[0])
            df = df.replace('', np.nan).astype('float')
        df.fillna(0, inplace=True)
        df['Gross_NPA_Mn'] = round(df['Sub_Standard_In_Mn'] + df['Doubtful_1_In_Mn'] + df['Doubtful_2_In_Mn'] + df['Doubtful_3_In_Mn'] + df['Loss_In_Mn'], 4)
        df['Provisions_In_Mn'] = round(df['Gross_NPA_Mn'] - df['Net_NPA_In_Mn'], 4)
        today = pd.Timestamp.now()

        df['Bank_Type'] = bank_type
        df['Bank'] = bank
        df['Relevant_Date'] = date
        df['Runtime'] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
    except:
        if loop<2:
            print('Falied with camelot trying with ilovepdf')
            path,sheets=read_data_using_ilovepdf(link,bank)
            heading='doubtful'
            end_text='npl ratio'
            df=search(heading,end_text,path,sheets)
            df=data_collection(df,bank_type,bank,date,'ilovepdf',2)
        else:
            raise Exception("Something went wrong either camelot or illovepdf worked check manually")
    return df


# In[ ]:


bank='ICICI BANK'
max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
search_str=['doubtful 1']
link=links_capital['Links'][0]
print(link)
bank_type = links_capital['Bank_Type'][0]
# bank = links_capital['Links'][0]
date = links_capital['Relevant_Date'][0]
tables = extract_tables_from_pdf(link,False,'lattice',20,200,70)
df=data_collection_icici(tables,bank_type,bank,date,'camelot',1)


# In[109]:


def data_collect_overseas(tables,bank_type,bank,date,type_read,loop):
    try:
        if type_read=='camelot':
            df = get_desired_table(tables, ['doubtful'])
        else:
            df=tables
        col = row_col_index_locator(df, ['doubtful'])[0]
        row_dict = [
                {'Substandard': 'Sub_Standard_In_Mn'},
                {'D1': 'Doubtful_1_In_Mn'},
                {'D2': 'Doubtful_2_In_Mn'},
                {'D3': 'Doubtful_3_In_Mn'},
                {'loss': 'Loss_In_Mn'},
                {'net npa': 'Net_NPA_In_Mn'},
                {'net advances': 'Net_NPA_To_NET_Advances_Pct'},
                {'gross advances': 'Gross_NPA_To_Gross_Advances_Pct'}
            ]
        df = row_modificator(df, row_dict, col, row_del=False, keep_row=True, update_row=True)
        df=df.replace('#','')
        df=df.replace('', '').replace('',np.nan)
        df.dropna(axis=1,how='all',inplace=True)
        df.reset_index(drop=True,inplace=True)
        df = df.T
        df.reset_index(drop=True, inplace=True)
        df.columns = df.iloc[0, :]
        df = df.iloc[1:]
        df.reset_index(drop=True, inplace=True)
        try:
            df = df.replace('', np.nan).astype('float')
        except:
            df.columns=df.iloc[0]
            df = df.drop(df.index[0])
            df = df.replace('', np.nan).astype('float')
        df.fillna(0, inplace=True)
        df['Gross_NPA_Mn'] = round(df['Sub_Standard_In_Mn'] + df['Doubtful_1_In_Mn'] + df['Doubtful_2_In_Mn'] + df['Doubtful_3_In_Mn'] + df['Loss_In_Mn'], 4)
        df['Provisions_In_Mn'] = round(df['Gross_NPA_Mn'] - df['Net_NPA_In_Mn'], 4)
        today = pd.Timestamp.now()

        df['Bank_Type'] = bank_type
        df['Bank'] = bank
        df['Relevant_Date'] = date
        df['Runtime'] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
    except:
        if loop<2:
            print('Falied with camelot trying with ilovepdf')
            path,sheets=read_data_using_ilovepdf(link,bank)
            heading='doubtful'
            end_text='net npa'
            df=search(heading,end_text,path,sheets)
            df=data_collection(df,bank_type,bank,date,'ilovepdf',2)
        else:
            raise Exception("Something went wrong either camelot or illovepdf worked check manually")
    return df


# In[110]:


bank='INDIAN OVERSEAS BANK'
max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
search_str=['doubtful 1']
link=links_capital['Links'][0]
print(link)
bank_type = links_capital['Bank_Type'][0]
date = links_capital['Relevant_Date'][0]
tables = extract_tables_from_pdf(link,False,'stream',10,100,40)
df=data_collect_overseas(tables,bank_type,bank,date,'camelot',1)


# In[ ]:


def data_collection_central(tables,bank_type,bank,date):
    df = get_desired_table(tables, ['substandard'])
    col = row_col_index_locator(df, ['doubtful'])[0]
    strt_index = row_col_index_locator(df, ['substandard'])[1]
    end_index = row_col_index_locator(df, ['movement'])[1]
    df=df.iloc[strt_index:end_index,:]
    df=df[df[1]!='']
    df.reset_index(drop=True,inplace=True)
    df[col][len(df)-2]='gross advance'
    df[col][len(df)-1]='net advance'
    df=df.replace('', '').replace('',np.nan)
    df.dropna(axis=1,how='all',inplace=True)
    if bank=='HDFC BANK':
        df=df.iloc[:,:2]
    df = df.T
    df.reset_index(drop=True, inplace=True)
    df.columns = df.iloc[0, :]
    df = df.iloc[1:]
    df.reset_index(drop=True, inplace=True)
    df.columns=['Sub_Standard_In_Mn','Doubtful_1_In_Mn','Doubtful_2_In_Mn','Doubtful_3_In_Mn','Loss_In_Mn','Net_NPA_In_Mn','Net_NPA_To_NET_Advances_Pct','Gross_NPA_To_Gross_Advances_Pct']
    try:
        df = df.replace('', np.nan).astype('float')
    except:
        df.columns=df.iloc[0]
        df = df.drop(df.index[0])
        df = df.replace('', np.nan).astype('float')
    df.fillna(0, inplace=True)
    df['Gross_NPA_Mn'] = round(df['Sub_Standard_In_Mn'] + df['Doubtful_1_In_Mn'] + df['Doubtful_2_In_Mn'] + df['Doubtful_3_In_Mn'] + df['Loss_In_Mn'], 4)
    df['Provisions_In_Mn'] = round(df['Gross_NPA_Mn'] - df['Net_NPA_In_Mn'], 4)
    today = pd.Timestamp.now()

    df['Bank_Type'] = bank_type
    df['Bank'] = bank
    df['Relevant_Date'] = date
    df['Runtime'] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
    return df


# In[ ]:


bank='CANARA BANK'
max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
search_str=['doubtful 1']
link=links_capital['Links'][0]
link=link.replace(' ','%20')
link='https://canarabank.com/media/8896/Basel%20III%20Disclosures-02-06-2023.pdf'
print(link)
bank_type = links_capital['Bank_Type'][0]
# bank = links_capital['Links'][0]
date = links_capital['Relevant_Date'][0]
tables = extract_tables_from_pdf(link,False,'lattice',10,100,70)
df=data_collection_canara(tables,bank_type,bank,date,'camelot',1)


# In[ ]:


def data_collection_kotak(tables,bank_type,bank,date,type_read,loop):
    try:
        if type_read=='camelot':
            df = get_desired_table(tables, ['doubtful'])
        else:
            df=tables
        col = row_col_index_locator(df, ['doubtful'])[0]
        try:
            row_dict = [
                {'Substandard': 'Sub_Standard_In_Mn'},
                {'Doubtful 1': 'Doubtful_1_In_Mn'},
                {'Doubtful 2': 'Doubtful_2_In_Mn'},
                {'Doubtful 3': 'Doubtful_3_In_Mn'},
                {'loss': 'Loss_In_Mn'},
            ]
            df2 = row_modificator(df, row_dict, col, row_del=False, keep_row=True, update_row=True)
        except:
            row_dict = [
                {'Substandard': 'Sub_Standard_In_Mn'},
                {'Doubtful 11': 'Doubtful_1_In_Mn'},
                {'Doubtful 21': 'Doubtful_2_In_Mn'},
                {'Doubtful 31': 'Doubtful_3_In_Mn'},
                {'loss': 'Loss_In_Mn'},
            ]
            df2 = row_modificator(df, row_dict, col, row_del=False, keep_row=True, update_row=True)
        df2=df2.replace('',np.nan)
        df2.dropna(axis=1,how='all',inplace=True)
        df2.reset_index(drop=True,inplace=True)
        net_npa_col=row_col_index_locator(df, ['net npa'])[0]
        net_npa_row=row_col_index_locator(df, ['total'])[1]
        ratio_row=row_col_index_locator(df, ['npa ratio'])[1]
        gross_col=row_col_index_locator(df, ['gross npa'])[0]
        df1=pd.DataFrame({0:['Net_NPA_In_Mn','Net_NPA_To_NET_Advances_Pct','Gross_NPA_To_Gross_Advances_Pct'],
                         1:[df[net_npa_col][net_npa_row],df[net_npa_col][ratio_row],df[gross_col][ratio_row]]})
        df = pd.concat([df1, df2.iloc[:,:2]], ignore_index=True)
        df = df.T
        df.reset_index(drop=True, inplace=True)
        df.columns = df.iloc[0, :]
        df = df.iloc[1:]
        df.reset_index(drop=True, inplace=True)
        df.columns=['Sub_Standard_In_Mn','Doubtful_1_In_Mn','Doubtful_2_In_Mn','Doubtful_3_In_Mn','Loss_In_Mn','Net_NPA_In_Mn','Net_NPA_To_NET_Advances_Pct','Gross_NPA_To_Gross_Advances_Pct']
        try:
            df = df.replace('', np.nan).astype('float')
        except:
            df.columns=df.iloc[0]
            df = df.drop(df.index[0])
            df = df.replace('', np.nan).astype('float')
        df.fillna(0, inplace=True)
        df['Gross_NPA_Mn'] = round(df['Sub_Standard_In_Mn'] + df['Doubtful_1_In_Mn'] + df['Doubtful_2_In_Mn'] + df['Doubtful_3_In_Mn'] + df['Loss_In_Mn'], 4)
        df['Provisions_In_Mn'] = round(df['Gross_NPA_Mn'] - df['Net_NPA_In_Mn'], 4)
        today = pd.Timestamp.now()

        df['Bank_Type'] = bank_type
        df['Bank'] = bank
        df['Relevant_Date'] = date
        df['Runtime'] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
    except:
        if loop<2:
            print('Falied with camelot trying with ilovepdf')
            path,sheets=read_data_using_ilovepdf(link,bank)
            heading='doubtful'
            end_text='npl ratio'
            df=search(heading,end_text,path,sheets)
            df=data_collection(df,bank_type,bank,date,'ilovepdf',2)
        else:
            raise Exception("Something went wrong either camelot or illovepdf worked check manually")
    return df


# In[ ]:


bank='KOTAK MAHINDRA BANK'
max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
search_str=['doubtful 1']
link=links_capital['Links'][0]
link=link.replace(' ','%20')
link='https://www.kotak.com/content/dam/Kotak/investor-relation/Financial-Result/Regulatory-Disclosure/FY-2023/Basel-3-disclosures-for-period-ending-30th-June-2022-new.pdf'
print(link)
bank_type = links_capital['Bank_Type'][0]
# bank = links_capital['Links'][0]
date = links_capital['Relevant_Date'][0]
tables = extract_tables_from_pdf(link,False,'lattice',10,100,70)
df=data_collection_kotak(tables,bank_type,bank,date,'camelot',1)


# In[19]:


bank='UJJIVAN SMALL BANK'
max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
link='https://www.ujjivansfb.in/sites/default/files/2023-06/Pillar-III-Disclosure-March-2023-final-VI.pdf'
# link=links_capital['Links'][51]
link=link.replace(' ','%20')
bank_type = 'private'
date = '2023-03-31'
print(link)
search_str=['foreign exchange risk']
tables = extract_tables_from_pdf(link,False,'stream',10,100,40)
# df=data_collection_common(tables,search_str,bank_type,bank,date,'camelot',1)


# In[25]:


bank='INDUSIND BANK'
max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
link=links_capital['Links'][0]
link=link.replace(' ','%20')
bank_type = 'private'
date = '2023-03-31'
print(link)
search_str=['foreign exchange risk']
tables = extract_tables_from_pdf(link,False,'stream',10,100,40)
# df=data_collection_common(tables,search_str,bank_type,bank,date,'camelot',1)


# In[26]:


search_str=['doubtful 1']
df=get_desired_table(tables,search_str)
df


# In[21]:


type_read='camelot'
if type_read=='camelot':
    if bank=='AU SMALL FINANCE BANK':
        df = get_desired_table(tables, ['doubtful1', 'doubtful2'])
    else:
else:
    df=tables
col = row_col_index_locator(df, ['doubtful'])[0]
print(df)
if bank=='CITY UNION BANK':
    row_dict = [
        {'Sub-standard': 'Sub_Standard_In_Mn'},
        {'Doubtful 1': 'Doubtful_1_In_Mn'},
        {'Doubtful 2': 'Doubtful_2_In_Mn'},
        {'Doubtful 3': 'Doubtful_3_In_Mn'},
        {'loss': 'Loss_In_Mn'},
        {'npa total': 'Net_NPA_In_Mn'},
        {'net advances': 'Net_NPA_To_NET_Advances_Pct'},
        {'gross advances': 'Gross_NPA_To_Gross_Advances_Pct'}
    ]
elif bank=='AU SMALL FINANCE BANK':
    row_dict = [
        {'Substandard': 'Sub_Standard_In_Mn'},
        {'Doubtful1': 'Doubtful_1_In_Mn'},
        {'Doubtful2': 'Doubtful_2_In_Mn'},
        {'Doubtful3': 'Doubtful_3_In_Mn'},
        {'loss': 'Loss_In_Mn'},
        {'net npa': 'Net_NPA_In_Mn'},
        {'net advances': 'Net_NPA_To_NET_Advances_Pct'},
        {'gross advances': 'Gross_NPA_To_Gross_Advances_Pct'}
    ]
else:
    row_dict = [
        {'Substandard': 'Sub_Standard_In_Mn'},
        {'Doubtful 1': 'Doubtful_1_In_Mn'},
        {'Doubtful 2': 'Doubtful_2_In_Mn'},
        {'Doubtful 3': 'Doubtful_3_In_Mn'},
        {'loss': 'Loss_In_Mn'},
        {'net npa': 'Net_NPA_In_Mn'},
        {'net advances': 'Net_NPA_To_NET_Advances_Pct'},
        {'gross advances': 'Gross_NPA_To_Gross_Advances_Pct'}
    ]

df = row_modificator(df, row_dict, col, row_del=False, keep_row=True, update_row=True)
df=df.replace('#','')
df=df.replace('', '').replace('',np.nan)
df.dropna(axis=1,how='all',inplace=True)
df.reset_index(drop=True,inplace=True)
if bank=='CITY UNION BANK':
    for col in range(1,len(df.columns)-1):
        for i in range(len(df)):
            if pd.isna(df.iloc[i, col]):
                df.iloc[i, col] = df.iloc[i, col + 1]
                df.iloc[i, col + 1] = np.nan
    df.dropna(axis=1,how='all',inplace=True)
    df.reset_index(drop=True,inplace=True)
if bank=='HDFC BANK':
    df=df.iloc[:,:2]
df = df.T
df.reset_index(drop=True, inplace=True)
df.columns = df.iloc[0, :]
df = df.iloc[1:]
df.reset_index(drop=True, inplace=True)
try:
    df = df.replace('', np.nan).astype('float')
except:
    df.columns=df.iloc[0]
    df = df.drop(df.index[0])
    df = df.replace('', np.nan).astype('float')
df.fillna(0, inplace=True)
df['Gross_NPA_Mn'] = round(df['Sub_Standard_In_Mn'] + df['Doubtful_1_In_Mn'] + df['Doubtful_2_In_Mn'] + df['Doubtful_3_In_Mn'] + df['Loss_In_Mn'], 4)
df['Provisions_In_Mn'] = round(df['Gross_NPA_Mn'] - df['Net_NPA_In_Mn'], 4)
today = pd.Timestamp.now()

df['Bank_Type'] = bank_type
df['Bank'] = bank
df['Relevant_Date'] = date
df['Runtime'] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))


# In[13]:


df


# In[143]:


import requests
import PyPDF2
import re
from io import BytesIO

def extract_values_from_pdf(pdf_url):
    response = requests.get(pdf_url)
    pdf_file = response.content

    reader = PyPDF2.PdfReader(BytesIO(pdf_file))
    num_pages = len(reader.pages)

    for page_number in range(num_pages):
        page = reader.pages[page_number]
        text = page.extract_text()

        # Search for the desired values using regular expressions
#         gross_npas_match = re.search(r"Total NPAs \(Gross\) (\d+\.\d+)", text)
        net_npas_match = re.search(r"The amount of net NPAs is Rs\. (\d+\.\d+)", text)
        gross_npa_ratio_match = re.search(r"Gross NPAs to Gross Advances: (\d+\.\d+)", text)
        net_npa_ratio_match = re.search(r"Net NPAs to Net Advances: (\d+\.\d+)", text)

        if net_npas_match and gross_npa_ratio_match and net_npa_ratio_match:
#             gross_npas = float(gross_npas_match.group(1))
            net_npas = float(net_npas_match.group(1))
            gross_npa_ratio = float(gross_npa_ratio_match.group(1))
            net_npa_ratio = float(net_npa_ratio_match.group(1))

            return net_npas, gross_npa_ratio, net_npa_ratio

    # Return None if the values were not found
    return None


# In[126]:


bank='UNION BANK OF INDIA'
max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
search_str=['doubtful 1']
link=links_capital['Links'][0]
link=link.replace(' ','%20')
print(link)
bank_type = links_capital['Bank_Type'][0]
date = links_capital['Relevant_Date'][0]
# tables = extract_tables_from_pdf(link,False,'lattice',20,200,40)
tables=camelot.read_pdf(link,pages='1-end',strip_text=[',','-','\n','%'])
# df=data_collect_overseas(tables,bank_type,bank,date,'camelot',1)


# In[127]:


df = get_desired_table(tables, ['doubtful'])


# In[128]:


df


# In[144]:


net_npas, gross_npa_ratio, net_npa_ratio=extract_values_from_pdf(link)


# In[170]:


def text_clean(text):
    text=text.lower()
    return text


# In[174]:


url='https://www.unionbankofindia.co.in/pdf/Disclosure-Sept.%202022.pdf'
print(url)
response = requests.get(url)
# response = requests.get(pdf_url)
pdf_file = BytesIO(response.content)
reader = PyPDF2.PdfReader(pdf_file)
num_pages = len(reader.pages)
for pages in  range(1,num_pages):
    page = reader.pages[pages]
    text = page.extract_text()
    text=text_clean(text)
    print(text)
    net_npas_match = re.search("amount of net", text)
    gross_npa_ratio_match = re.search("gross advances", text)
    net_npa_ratio_match = re.search("net advances", text)
    if net_npas_match:
        print(pages)
        lines1 = text.split('\n')
        for i,line in enumerate(lines1):
            if 'amount of net' in line.lower():
                net_amnt=lines1[i+1]
                pattern = r'^\d+\.\d+$'
                match = re.match(pattern, net_amnt)
                if match:
                    
                
                break
    if gross_npa_ratio_match:
        print(pages)
        lines2 = text.split('\n')
    if net_npa_ratio_match:
        print(pages)
        lines3 = text.split('\n')


# In[175]:


lines2


# In[157]:



tables = camelot.read_pdf(link, pages='all')

# Search for the desired values in the DataFrame
for table in tables:
    df = table.df
    for index, row in df.iterrows():
        if 'Total NPAs (Gross)' in row.values:
            gross_npas = float(row[row.str.contains('Total NPAs \(Gross\)')].values[0].split()[-1].replace(',', ''))
        if 'The amount of net NPAs is Rs.' in row.values:
            net_npas = float(row[row.str.contains('The amount of net NPAs is Rs\.')].values[0].split()[-1].replace(',', ''))
        if 'Gross NPAs to Gross Advances' in row.values:
            gross_npa_ratio = float(row[row.str.contains('Gross NPAs to Gross Advances')].values[0].split()[-1].replace('%', ''))
        if 'Net NPAs to Net Advances' in row.values:
            net_npa_ratio = float(row[row.str.contains('Net NPAs to Net Advances')].values[0].split()[-1].replace('%', ''))


    # Return the extracted values
print( gross_npas, net_npas, gross_npa_ratio, net_npa_ratio)


# In[161]:


tables[17].df

