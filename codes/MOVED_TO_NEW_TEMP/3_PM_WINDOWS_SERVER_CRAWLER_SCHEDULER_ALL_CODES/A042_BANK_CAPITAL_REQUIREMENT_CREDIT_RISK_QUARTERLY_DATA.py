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
    df=df.replace('',np.nan).replace('#',np.nan).replace('•',np.nan)
    df.dropna(axis=1,how='all',inplace=True)
    df.reset_index(drop=True,inplace=True)
    return df
def get_correct_links(links_capital):
    links_capital=links_capital[links_capital['Bank_Capital_Requirement_Credit_Risk_Status']!='COMPLETED']
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
    connection.execute("update BANK_BASEL_III_QUARTERLY_LINKS set Bank_Capital_Requirement_Credit_Risk_Status = 'COMPLETED' where Relevant_Date = '"+date+"' and Bank= '"+bank_name+"'")
    


def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    engine = adqvest_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'BANK_CAPITAL_REQUIREMENT_CREDIT_RISK_QUARTERLY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    ## S3 Credentials
    # ACCESS_KEY_ID = adqvest_s3.s3_cred()[0]
    # ACCESS_SECRET_KEY = adqvest_s3.s3_cred()[1]
    # BUCKET_NAME = 'adqvests3bucket'


    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
        bank_name='BANK_CAPITAL_REQUIREMENT_CREDIT_RISK_QUARTERLY_DATA'
        catch_error_df=pd.DataFrame(columns=['Table_name','Bank_Name','Error_message','Relevant_Date','Runtime'])
        sql="Delete from BANK_ERROR_TABLE where Table_Name = '" +bank_name+"'"
        engine.execute(sql)
        def delete_collected_data(bank_name,bank,date):
            sql="Delete from BANK_ERROR_TABLE where Table_Name = '" +bank_name+"' and Bank_Name = '" +bank+"' and Relevant_Date='" +date+"'"
            engine.execute(sql)


        def data_collection_au(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                col=row_col_index_locator(df,['credit risk'])[0]

                row_dict=[{'nonsecuritised portfolio':'Portfolios subject to standardized approach'},
                          {'for securitised portfolio':'Securitisation exposures'}]
                df=row_modificator(df,row_dict,col,row_del=False,keep_row=True,update_row=True)
                df.reset_index(drop=True,inplace=True)
                df=df.iloc[:,col:]
                df.columns=['Particulars','Amounts_In_Million']
                df['Amounts_In_Million']=pd.to_numeric(df['Amounts_In_Million'],errors='coerce')

                df['Bank']=bank
                df['Relevant_Date']=date
                df['Runtime']=pd.to_datetime('now')
                df=df[['Bank','Particulars','Amounts_In_Million','Relevant_Date','Runtime']]
                df['Bank_Type']=bank_type
                df.reset_index(drop=True,inplace=True)
                df=clean_table(df)
                df_final=df['Relevant_Date']>max_date
                df_final.to_sql("BANK_CAPITAL_REQUIREMENT_CREDIT_RISK_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print(len(df_final))
                print('Done for BANK---> ',bank)
                print('Done for DATE---> ',date)
            except:
                if loop<2:
                    print('Falied with camelot trying with ilovepdf')
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='credit risk'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_au(df,search_str,link,max_date,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return df


        bank='AU SMALL FINANCE BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['securitised portfolio']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                max_date,lnk=get_table_max_date_and_links(table_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'stream',10,200,70)
                        df=data_collection_au(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_au('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    print(e)


        def data_collection(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                col=row_col_index_locator(df,['portfolios subject'])[0]
                row=row_col_index_locator(df,['portfolios subject'])[1]
                df1=df.iloc[:row+1,:]
                row_dict=[{'portfolios subject':'Portfolios subject to standardized approach'}]
                df=row_modificator(df1,row_dict,col,row_del=False,keep_row=True,update_row=True)
                df=clean_dataframe(df)
                df.columns=['Particulars','Amounts_In_Million']
                df['Amounts_In_Million'] = df['Amounts_In_Million'].apply(lambda x: re.sub(r'[^0-9.]', '', str(x)))
                df['Amounts_In_Million']=pd.to_numeric(df['Amounts_In_Million'],errors='coerce')
                df=df.append({'Particulars':'Securitisation exposures','Amounts_In_Million':np.nan},ignore_index=True)
                df.reset_index(drop=True,inplace=True)
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Runtime']=pd.to_datetime('now')
                df=df[['Bank','Particulars','Amounts_In_Million','Relevant_Date','Runtime']]
                df['Bank_Type']=bank_type
                df.reset_index(drop=True,inplace=True)
                df=clean_table(df)
            except:
                if loop<2:
                    print('Falied with camelot trying with ilovepdf')
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='credit risk'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection(df,heading,link,max_date,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return df


        # In[12]:


        bank='STATE BANK OF INDIA'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['portfolios subject']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                max_date,lnk=get_table_max_date_and_links(table_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'stream',25,200,40)
                        df=data_collection(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        # In[15]:


        bank='CENTRAL BANK OF INDIA'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['portfolios subject']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                max_date,lnk=get_table_max_date_and_links(table_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'lattice',15,100,40)
                        df=data_collection(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        # In[70]:


        def data_collection_hdfc_axis_overseas(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                if bank=='INDIAN OVERSEAS BANK':
                    row1=row_col_index_locator(df,['securitisation exposure'])[1]
                    col=row_col_index_locator(df,['portfolio subject'])[0]
                    row=row_col_index_locator(df,['portfolio subject'])[1]
                else:
                    row1=row_col_index_locator(df,['securitisation exposure'])[1]
                    col=row_col_index_locator(df,['portfolios subject'])[0]
                    row=row_col_index_locator(df,['portfolios subject'])[1]
                df1=df.iloc[row:row1+1,col:]
                df1=df1.replace('',np.nan)
                df1.dropna(axis=1,how='all',inplace=True)
                df1.reset_index(drop=True,inplace=True)
                df=df1
                try:
                    col=row_col_index_locator(df,['total'])[0]
                    df.drop(df.index[col],axis=1,inplace=True)
                except:
                    print(df)

                try:
                    df.columns=['Particulars','Amounts_In_Million']
                    df['Amounts_In_Million']=df['Amounts_In_Million'].str.split(' ').apply(lambda x: x[0])
                except:
                    df=df.iloc[:,:len(df.columns)-1]
                    df.columns=['Particulars','Amounts_In_Million']
                    df['Amounts_In_Million']=df['Amounts_In_Million'].str.split(' ').apply(lambda x: x[0])
                df['Amounts_In_Million'] = df['Amounts_In_Million'].apply(lambda x: re.sub(r'[^0-9.]', '', str(x)))
                df['Amounts_In_Million']=pd.to_numeric(df['Amounts_In_Million'],errors='coerce')

                df['Bank']=bank
                df['Relevant_Date']=date
                df['Runtime']=pd.to_datetime('now')
                df=df[['Bank','Particulars','Amounts_In_Million','Relevant_Date','Runtime']]
                df['Bank_Type']=bank_type
                df.reset_index(drop=True,inplace=True)
                df=clean_table(df)
                df_final=df[df['Relevant_Date']>max_date]
                df_final.to_sql("BANK_CAPITAL_REQUIREMENT_CREDIT_RISK_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print(df_final)
                print('uploaded')
            except:
                if loop<2:
                    print('Falied with camelot trying with ilovepdf')
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    if bank=='INDIAN OVERSEAS BANK':
                        heading='portfolio subject'
                    else:
                        heading='portfolios subject'
                    end_text='securisation exposure'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_hdfc_axis_overseas(df,heading,link,max_date,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return df


        # In[72]:


        bank='HDFC BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['portfolios subject']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                max_date,lnk=get_table_max_date_and_links(table_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'stream',25,200,40)
                        df=data_collection_hdfc_axis_overseas(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_hdfc_axis_overseas('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        # In[73]:


        bank='AXIS BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['portfolios subject']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                max_date,lnk=get_table_max_date_and_links(table_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'stream',10,100,40)
                        df=data_collection_hdfc_axis_overseas(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_hdfc_axis_overseas('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        # In[75]:


        bank='INDIAN OVERSEAS BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['portfolio subject']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                max_date,lnk=get_table_max_date_and_links(table_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'stream',10,100,40)
                        df=data_collection_hdfc_axis_overseas(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_hdfc_axis_overseas('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        # In[77]:


        def data_collection_icici(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                row1=row_col_index_locator(df,['securitisation exposure'])[1]
                col=row_col_index_locator(df,['portfolio subject'])[0]
                row=row_col_index_locator(df,['portfolio subject'])[1]
                df1=df.iloc[row:row1+1,:]
                df1=df1.replace('',np.nan)
                df1.dropna(axis=1,how='all',inplace=True)
                df1.reset_index(drop=True,inplace=True)
                df=df1
                df.columns=['Particulars','Amounts_In_Million']
                df['Amounts_In_Million']=pd.to_numeric(df['Amounts_In_Million'],errors='coerce')
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Runtime']=pd.to_datetime('now')
                df=df[['Bank','Particulars','Amounts_In_Million','Relevant_Date','Runtime']]
                df['Bank_Type']=bank_type
                df.reset_index(drop=True,inplace=True)
                df=clean_table(df)
            except:
                if loop<2:
                    print('Falied with camelot trying with ilovepdf')
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='portfolios subject'
                    end_text='securisation exposure'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_icici(df,heading,link,max_date,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return df


        # In[79]:


        bank='ICICI BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['securitisation exposure']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                max_date,lnk=get_table_max_date_and_links(table_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'lattice',20,200,70)
                        df=data_collection_icici(tables,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_icici('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        # In[80]:


        def data_collection_city_canara_union_baroda_indusind(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                if bank=='INDUSIND BANK':
                    col=row_col_index_locator(df,['portfolio subject'])[0]
                    row=row_col_index_locator(df,['portfolio subject'])[1]
                    df1=df.iloc[row:row+2,:]
                    row_dict=[{'portfolio subject':'Portfolios subject to standardized approach'},
                             {'securitisation exposures':'Securitisation exposures'}]
                    df=row_modificator(df1,row_dict,col,row_del=False,keep_row=True,update_row=True)
                else:
                    col=row_col_index_locator(df,['portfolios subject'])[0]
                    row=row_col_index_locator(df,['portfolios subject'])[1]
                    df1=df.iloc[row:row+2,:]
                    try:
                        row_dict=[{'portfolios subject':'Portfolios subject to standardized approach'},
                                 {'securitisation exposures':'Securitisation exposures'}]
                        df=row_modificator(df1,row_dict,col,row_del=False,keep_row=True,update_row=True)
                    except:
                        if bank=='IDBI BANK':
                            row_dict=[{'portfolios subject':'Portfolios subject to standardized approach'},
                             {'securitisation':'Securitisation exposures'}]
                            df=row_modificator(df1,row_dict,col,row_del=False,keep_row=True,update_row=True)
                        else:
                            row_dict=[{'portfolios subject':'Portfolios subject to standardized approach'},
                         {'securitization exposures':'Securitisation exposures'}]
                            df=row_modificator(df1,row_dict,col,row_del=False,keep_row=True,update_row=True)
                df=clean_dataframe(df)
                df.columns=['Particulars','Amounts_In_Million']
                df['Amounts_In_Million'] = df['Amounts_In_Million'].apply(lambda x: re.sub(r'[^0-9.]', '', str(x)))
                df['Amounts_In_Million']=pd.to_numeric(df['Amounts_In_Million'],errors='coerce')
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Runtime']=pd.to_datetime('now')
                df=df[['Bank','Particulars','Amounts_In_Million','Relevant_Date','Runtime']]
                df['Bank_Type']=bank_type
                df.reset_index(drop=True,inplace=True)
                df=clean_table(df)
            except:
                if loop<2:
                    print('Falied with camelot trying with ilovepdf')
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='portfolios subject'
                    end_text='securisation exposure'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_city_canara_union_baroda_indusind(df,heading,link,max_date,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return df


        # In[82]:


        bank='CITY UNION BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['portfolios subject']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                max_date,lnk=get_table_max_date_and_links(table_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'stream',10,100,40)
                        df=data_collection_city_canara_union_baroda_indusind(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_city_canara_union_baroda_indusind('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        # In[84]:


        bank='CANARA BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['portfolios subject']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                max_date,lnk=get_table_max_date_and_links(table_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'stream',10,100,40)
                        df=data_collection_city_canara_union_baroda_indusind(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_city_canara_union_baroda_indusind('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        # In[89]:


        bank='UNION BANK OF INDIA'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['portfolios subject']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                max_date,lnk=get_table_max_date_and_links(table_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'stream',10,100,40)
                        df=data_collection_city_canara_union_baroda_indusind(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_city_canara_union_baroda_indusind('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        # In[91]:


        bank='KOTAK MAHINDRA BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['portfolios subject to standardised']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                max_date,lnk=get_table_max_date_and_links(table_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'stream',10,100,40)
                        df=data_collection_city_canara_union_baroda_indusind(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_city_canara_union_baroda_indusind('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        # In[8]:


        bank='FEDERAL BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['portfolios subject to standardised']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                max_date,lnk=get_table_max_date_and_links(table_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'stream',10,100,40)
                        df=data_collection_federal(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_federal('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        # In[64]:


        bank='BANK OF BARODA'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['portfolios subject']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                max_date,lnk=get_table_max_date_and_links(table_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'stream',10,100,40)
                        df=data_collection_city_canara_union_baroda_indusind(tables,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_city_canara_union_baroda_indusind('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        bank='INDUSIND BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['portfolio subject']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                max_date,lnk=get_table_max_date_and_links(table_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'stream',10,100,40)
                        df=data_collection_city_canara_union_baroda_indusind(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_city_canara_union_baroda_indusind('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        # In[89]:


        bank='RBL BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['portfolios subject']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                max_date,lnk=get_table_max_date_and_links(table_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'stream',10,100,40)
                        df=data_collection(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        # In[2]:


        bank='IDBI BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['portfolios subject']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                max_date,lnk=get_table_max_date_and_links(table_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'stream',10,100,40)
                        df=data_collection_city_canara_union_baroda_indusind(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_city_canara_union_baroda_indusind('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        # In[16]:



        def data_collection_federal(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                    
                df.reset_index(drop=True,inplace=True)
                col=row_col_index_locator(df,['Capital requirements for credit risk'])[0]
                row=row_col_index_locator(df,['Capital requirements for credit risk'])[1]
                row1=row_col_index_locator(df,['Securitization exposures'])[1]
                df1=df.iloc[row:row1+1,:]
                row_dict=[{'portfolios subject':'Portfolios subject to standardized approach'},
                         {'securitization exposures':'Securitisation exposures'}]
                df=row_modificator(df1,row_dict,col,row_del=False,keep_row=True,update_row=True)
                df=clean_dataframe(df)
                df.columns=['Particulars','Amounts_In_Million']
                df['Amounts_In_Million'] = df['Amounts_In_Million'].apply(lambda x: re.sub(r'[^0-9.]', '', str(x)))
                df['Amounts_In_Million']=pd.to_numeric(df['Amounts_In_Million'],errors='coerce')
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Runtime']=pd.to_datetime('now')
                df=df[['Bank','Particulars','Amounts_In_Million','Relevant_Date','Runtime']]
                df['Bank_Type']=bank_type
                df.reset_index(drop=True,inplace=True)
                df=clean_table(df)
                df.to_sql("BANK_CAPITAL_REQUIREMENT_CREDIT_RISK_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
            except:
                if loop<2:
                    print('This might happen because camelot failed trying with ilovepdf')
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='portfolios subject'
                    end_text='securitization exposures'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_federal(df,heading,link,max_date,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return df


        # In[6]:


        def data_collection_bandhan(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                col=row_col_index_locator(df,['credit risk:'])[0]
                d1=[{'securitisation exposures':'Securitisation exposures'},
                    {'Standardised approac':'Portfolios subject to standardized approach'}]
                df=row_modificator(df,d1,col,row_del=False,keep_row=True,update_row=True)

                df.columns=['Particulars','Amounts_In_Million']
                df['Amounts_In_Million']=pd.to_numeric(df['Amounts_In_Million'],errors='coerce')*10
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Runtime']=pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
                df=df[['Bank','Particulars','Amounts_In_Million','Relevant_Date','Runtime']]
                df['Bank_Type']=bank_type
                df.reset_index(drop=True,inplace=True)
                df=clean_table(df)
            except:
                if loop<2:
                    print('This might happen because camelot failed trying with ilovepdf')
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='portfolios subject'
                    end_text='securitization exposures'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_bandhan(df,heading,link,max_date,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return df


        # In[8]:


        bank='BANDHAN BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['portfolios subject']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                max_date,lnk=get_table_max_date_and_links(table_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'stream',10,100,40)
                        df=data_collection_bandhan(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_bandhan('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        def data_collection_punjab(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
            
                    col=row_col_index_locator(df,['Standardized approach'])[0]
                    row=row_col_index_locator(df,['particulars'])[0]

                    df.columns=['Particulars','Amounts_In_Million']
                    df=row_modificator(df,['Particulars'],col,row_del=True,keep_row=False,update_row=False)
                    df['Bank']='PUNJAB NATIONAL BANK'
                    df['Relevant_Date']=date
                    df['Runtime']=pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
                    df=df[['Bank','Particulars','Amounts_In_Million','Relevant_Date','Runtime']]
                    df['Bank_Type']=bank_type
                    df.reset_index(drop=True,inplace=True)
                    df=clean_table(df)
            except:
                if loop<2:
                    print('This might happen because camelot failed trying with ilovepdf')
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='Standardized approach'
                    end_text='securitization exposures'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_bandhan(df,heading,link,max_date,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return df

            return df


        # In[14]:


        bank='PUNJAB NATIONAL BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['Standardized approach']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                max_date,lnk=get_table_max_date_and_links(table_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'stream',10,100,40)
                        df=data_collection_punjab(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_punjab('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        # In[15]:



        def data_collection_uco(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                col=row_col_index_locator(df,['credit risk'])[0]
                row=row_col_index_locator(df,['credit risk'])[1]

                d1=[{'Standardised approac':'Portfolios subject to standardized approach'}]
                df=row_modificator(df,d1,col,row_del=False,keep_row=True,update_row=True)

                df.columns=df.columns=['Particulars','Amounts_In_Million']
                df=df.append({'Particulars':'Securitisation Exposures','Amounts_In_Million':' '},ignore_index=True)
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Runtime']=datetime.datetime.now()
                df['Amounts_In_Million'][0]=round(pd.to_numeric(re.match(r'\d+\.\d{2}',df['Amounts_In_Million'][0])[0])*10,2)
                #df['Amounts_In_Million']=pd.to_numeric(df['Amounts_In_Million'])*10
                df=df[['Bank','Particulars','Amounts_In_Million','Relevant_Date','Runtime']]
                df.reset_index(drop=True,inplace=True)
                for j in df.columns:
                    if j!='Runtime':
                        df[j]=df[j].apply(lambda x:str(x))
                df['Bank_Type']=bank_type
                df=clean_table(df)
            except:
                if loop<2:
                    print('This might happen because camelot failed trying with ilovepdf')
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='Standardized approach'
                    end_text='securitization exposures'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_uco(df,heading,link,max_date,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return df


        # In[16]:


        bank='UCO BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['Standardized approach']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                max_date,lnk=get_table_max_date_and_links(table_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'stream',10,100,40)
                        df=data_collection_uco(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_uco('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        # In[20]:


        def data_collection_idfc(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                col=row_col_index_locator(df,['credit risk'])[0]

                row_dict=[{'Standardised approac':'Portfolios subject to standardized approach'}]

                df=row_modificator(df,row_dict,col,2,row_del=False,keep_row=True,update_row=True)

                df=df.append({0:'Securitization exposures',1:np.nan},ignore_index=True)

                df.reset_index(drop=True,inplace=True)
                df.columns=['Particulars','Amounts_In_Million']
                df['Amounts_In_Million']=pd.to_numeric(df['Amounts_In_Million'],errors='coerce')

                df['Bank']=bank
                df['Relevant_Date']=date
                df['Runtime']=pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
                df=df[['Bank','Particulars','Amounts_In_Million','Relevant_Date','Runtime']]
                df['Bank_Type']=bank_type
                df.reset_index(drop=True,inplace=True)
                df=clean_table(df)
            except:
                if loop<2:
                    print('This might happen because camelot failed trying with ilovepdf')
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='portfolios subject'
                    end_text='securitization exposures'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_idfc(df,heading,link,max_date,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return df


        # In[21]:


        bank='IDFC FIRST BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['portfolios subject']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                max_date,lnk=get_table_max_date_and_links(table_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'stream',10,100,40)
                        df=data_collection_idfc(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_idfc('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        # In[23]:


        def data_collection_maharashtra(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                col=row_col_index_locator(df,['credit risk'])[0]

                row_dict=[{'Standardized approac':'Portfolios subject to standardized approach'}]
                df=row_modificator(df,row_dict,col,2,row_del=False,keep_row=True,update_row=True)

                df=df.append({1:'Securitisation exposures',2:np.nan},ignore_index=True)
                df.reset_index(drop=True,inplace=True)
                df.columns=['Particulars','Amounts_In_Million']
                df['Amounts_In_Million']=pd.to_numeric(df['Amounts_In_Million'],errors='coerce')

                df['Bank']=bank
                df['Relevant_Date']=date
                df['Runtime']=pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
                df=df[['Bank','Particulars','Amounts_In_Million','Relevant_Date','Runtime']]
                df['Bank_Type']=bank_type
                df.reset_index(drop=True,inplace=True)
                df=clean_table(df)
            except:
                if loop<2:
                    print('This might happen because camelot failed trying with ilovepdf')
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='portfolios subject'
                    end_text='securitization exposures'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_maharashtra(df,heading,link,max_date,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return df


        # In[24]:


        bank='BANK OF MAHARASHTRA'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['portfolios.+subject']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                max_date,lnk=get_table_max_date_and_links(table_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'stream',10,100,40)
                        df=data_collection_maharashtra(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_maharashtra('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:

        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_type)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
