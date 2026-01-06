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
    table_name = 'BANK_CAPITAL_REQUIREMENT_OPERATIONAL_RISK_QUARTERLY_DATA'
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


        bank_name='BANK_CAPITAL_REQUIREMENT_OPERATIONAL_RISK_QUARTERLY_DATA'
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
                col=row_col_index_locator(df,['operational risk'])[0]
                df=row_modificator(df,['Amount'],col,row_del=True,keep_row=False,update_row=False)
                d1=[{'operational risk':'Operational Risk'}]
                df=row_modificator(df,d1,col,row_del=False,keep_row=True,update_row=True)
                df=df.iloc[:,col:]
                df.columns=['Particulars','Amounts_In_Million']
                df['Amounts_In_Million']=df['Amounts_In_Million'].replace('',0)
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Runtime']=pd.to_datetime('now')
                df=df[['Bank','Particulars','Amounts_In_Million','Relevant_Date','Runtime']]
                df['Bank_Type']=bank_type
                df.reset_index(drop=True,inplace=True)
                df=clean_table(df)
                df_final=df['Relevant_Date']>max_date
                df_final.to_sql("BANK_CAPITAL_REQUIREMENT_OPERATIONAL_RISK_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print(len(df_final))
                print('Done for BANK---> ',bank)
                print('Done for DATE---> ',date)
            except:
                if loop<2:
                    print('Falied with camelot trying with ilovepdf')
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='securitised portfolio'
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
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        bank='CENTRAL BANK OF INDIA'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['basic indicator']
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
                        tables = extract_tables_from_pdf(link,False,'lattice',10,200,40)
                        df=data_collection_common(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_common('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)

        bank='STATE BANK OF INDIA'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['basic indicator']
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
                        tables = extract_tables_from_pdf(link,False,'lattice',20,100,40)
                        df=data_collection_common(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_common('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)

        bank='UNION BANK OF INDIA'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['basic indicator']
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
                        tables = extract_tables_from_pdf(link,False,'lattice',20,100,40)
                        df=data_collection_common(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_common('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        bank='HDFC BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['basic indicator']
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
                        tables = extract_tables_from_pdf(link,False,'stream',15,200,40)
                        df=data_collection_common(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_common('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        bank='AXIS BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['basic indicator']
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
                        tables = extract_tables_from_pdf(link,False,'stream',10,100,40)
                        df=data_collection_common(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_common('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)

        def data_collection_ios_idbi(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                col=row_col_index_locator(df,['basic indicator'])[0]
                row=row_col_index_locator(df,['basic indicator'])[1]
                df=df.iloc[row+1:row+2,col:]
                df=clean_dataframe(df)
                df.columns=['Particulars','Amounts_In_Million']
                col=row_col_index_locator(df,['Operational Risk'])[0]
                df=row_modificator(df,['Amount'],col,row_del=True,keep_row=False,update_row=False)
                d1=[{'Operational Risk':'Basic indicator approach'}]
                df=row_modificator(df,d1,col,row_del=False,keep_row=True,update_row=True)
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Runtime']=pd.to_datetime('now')
                df=df[['Bank','Particulars','Amounts_In_Million','Relevant_Date','Runtime']]
                df['Amounts_In_Million'] = df['Amounts_In_Million'].apply(lambda x: re.sub(r'^.*?(\d+\.\d+).*$', r'\1', str(x)))
                df['Amounts_In_Million'] = df['Amounts_In_Million'].astype(float)
                df['Bank_Type']=bank_type
                df.reset_index(drop=True,inplace=True)
                df=clean_table(df)
                df_final=df['Relevant_Date']>max_date
                df_final.to_sql("BANK_CAPITAL_REQUIREMENT_OPERATIONAL_RISK_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print(len(df_final))
                print('Done for BANK---> ',bank)
                print('Done for DATE---> ',date)
            except:
                if loop < 2:
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='operational risk'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_ios_idbi(df,search_str,link,max_date,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return df 

        bank='INDIAN OVERSEAS BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['basic indicator approach']
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
                        df=data_collection_ios_idbi(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_ios_idbi('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)

        def data_collection_icici(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                df=make_data_from_rows(df,'Capital required for operational risk')
                df=clean_dataframe(df)
                df.columns=df.columns=['Particulars','Amounts_In_Million']
                df['Particulars']='Basic indicator approach'
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Runtime']=datetime.datetime.now()
                df=df[['Bank','Particulars','Amounts_In_Million','Relevant_Date','Runtime']]
                df.reset_index(drop=True,inplace=True)
                df['Amounts_In_Million'] = df['Amounts_In_Million'].apply(lambda x: re.sub(r'^.*?(\d+\.\d+).*$', r'\1', str(x)))
                df['Amounts_In_Million'] = df['Amounts_In_Million'].astype(float)
                for j in df.columns:
                    if j!='Runtime':
                        df[j]=df[j].apply(lambda x:str(x))
                df['Bank_Type']=bank_type
                df=clean_table(df)
                df_final=df['Relevant_Date']>max_date
                df_final.to_sql("BANK_CAPITAL_REQUIREMENT_OPERATIONAL_RISK_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print(len(df_final))
                print('Done for BANK---> ',bank)
                print('Done for DATE---> ',date)
            except:
                if loop < 2:
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='Capital required for operational risk'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_icici(df,search_str,link,max_date,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return df 

        bank='ICICI BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['equity position risk']
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
                        df=data_collection_icici(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_icici('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)

        bank='AU SMALL FINANCE BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['securitised portfolio']
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
                        tables = extract_tables_from_pdf(link,True,'stream',10,200,70)
                        df=data_collection_au(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_au('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)

        bank='CANARA BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['basic indicator']
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
                        tables = extract_tables_from_pdf(link,True,'stream',10,200,70)
                        df=data_collection_common(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_common('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)

        def data_collection_common(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                if bank=='HDFC BANK':
                    col=row_col_index_locator(df,['Basic indicator approach'])[0]
                    row=row_col_index_locator(df,['Basic indicator approach'])[1]
                    df=df.iloc[row:row+1,col:]
                    df=df.replace('',np.nan)
                    df.dropna(axis=1,how='all',inplace=True)
                    df.reset_index(drop=True,inplace=True)
                    df=df.iloc[:,:len(df.columns)-1]
                if bank=='CANARA BANK':
                    df.reset_index(drop=True,inplace=True)
                    col=row_col_index_locator(df,['basic indicator approach'])[0]
                    row=row_col_index_locator(df,['basic indicator approach'])[1]
                    df=df.iloc[row:row+1,col:]
                else:
                    df.reset_index(drop=True,inplace=True)
                    col=row_col_index_locator(df,['basic indicator'])[0]
                    row=row_col_index_locator(df,['basic indicator'])[1]
                    df=df.iloc[row:row+1,col:]
                df=clean_dataframe(df)
                df.columns=['Particulars','Amounts_In_Million']
                col=row_col_index_locator(df,['Basic indicator'])[0]
                df=row_modificator(df,['Amount'],col,row_del=True,keep_row=False,update_row=False)
                d1=[{'Basic indicator':'Basic indicator approach'}]
                df=row_modificator(df,d1,col,row_del=False,keep_row=True,update_row=True)
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Runtime']=pd.to_datetime('now')
                df=df[['Bank','Particulars','Amounts_In_Million','Relevant_Date','Runtime']]
                df['Amounts_In_Million'] = df['Amounts_In_Million'].apply(lambda x: re.sub(r'^.*?(\d+\.\d+).*$', r'\1', str(x)))
                df['Amounts_In_Million'] = df['Amounts_In_Million'].astype(float)
                df['Bank_Type']=bank_type
                df.reset_index(drop=True,inplace=True)
                df=clean_table(df)
                df_final=df['Relevant_Date']>max_date
                df_final.to_sql("BANK_CAPITAL_REQUIREMENT_OPERATIONAL_RISK_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print(len(df_final))
                print('Done for BANK---> ',bank)
                print('Done for DATE---> ',date)
            except:
                if loop < 2:
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='basic indicator'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_common(df,search_str,link,max_date,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return df 


        bank='BANK OF BARODA'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['foreign exchange risk']
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
                        df=data_collection_common(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_common('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)

        bank='RBL BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['capital requirements for operational risk']
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
                        df=data_collection_common(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_common('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        bank='INDUSIND BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['capital requirements for operational risk']
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
                        df=data_collection_common(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_common('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)

        bank='CITY UNION BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['capital requirements for operational risk']
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
                        df=data_collection_common(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_common('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)

        bank='IDBI BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['capital requirements for operational risk']
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
                        df=data_collection_common(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_common('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        bank='KOTAK MAHINDRA BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['market risk']
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
                        tables = extract_tables_from_pdf(link,False,'lattice',20,100,40)
                        df=data_collection_common(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_common('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)

        bank='FEDERAL BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['operational risk']
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
                        df=data_collection_common(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_common('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        def data_collection_idfc(tables,search_str,bank_type,bank,date,type_read,loop):

            df=get_desired_table(tables,['basic indicator'])

            col=row_col_index_locator(df,['basic indicator'])[0]
            df=row_modificator(df,['basic indicator'],col,row_del=False,keep_row=True,update_row=False)
            df.columns=['Particulars','Amounts_In_Million']
            df['Amounts_In_Million']=pd.to_numeric(df['Amounts_In_Million'])*10

            df['Bank']=bank
            df['Relevant_Date']=date
            df['Runtime']=pd.to_datetime('now')
            df=df[['Bank','Particulars','Amounts_In_Million','Relevant_Date','Runtime']]
            df['Bank_Type']=bank_type
            df.reset_index(drop=True,inplace=True)
            df=clean_table(df)

            return df


        bank='IDFC FIRST BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['basic indicator']
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
        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:

        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_type)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
