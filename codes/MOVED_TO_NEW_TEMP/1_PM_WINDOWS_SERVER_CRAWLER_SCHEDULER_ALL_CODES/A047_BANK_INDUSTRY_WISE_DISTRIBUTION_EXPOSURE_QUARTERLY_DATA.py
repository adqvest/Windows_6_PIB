
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
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
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
    return df_error_final
def update_basel_iii(bank_name,date):
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    connection.execute("update BANK_BASEL_III_QUARTERLY_LINKS set Bank_Industry_Wise_Distribution_Exposure_Status = 'COMPLETED' where Relevant_Date = '"+str(date)+"' and Bank= '"+bank_name+"'")
    # connection.execute("update BANK_BASEL_III_QUARTERLY_LINKS set Bank_Capital_Requirement_Credit_Risk_Status = 'COMPLETED' where Relevant_Date = '"+date+"' and Bank= '"+bank_name+"'")
    
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
    table_name = 'BANK_INDUSTRY_WISE_DISTRIBUTION_EXPOSURE_QUARTERLY_DATA'
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

        print('Running code for data collection')
        bank_name='BANK_INDUSTRY_WISE_DISTRIBUTION_EXPOSURE_QUARTERLY_DATA'
        catch_error_df=pd.DataFrame(columns=['Table_name','Bank_Name','Error_message','Relevant_Date','Runtime'])
        sql="Delete from BANK_ERROR_TABLE where Table_Name = '" +bank_name+"'"
        engine.execute(sql)
        def delete_collected_data(bank_name,bank,date):
            engine = adqvest_db.db_conn()
            sql="Delete from BANK_ERROR_TABLE where Table_Name = '" +bank_name+"' and Bank_Name = '" +bank+"' and Relevant_Date='" +date+"'"
            engine.execute(sql)

        def data_collection_au(tables,search_str,max_date,bank_type,bank,date,type_read,loop,bank_name,catch_error_df):
            engine = adqvest_db.db_conn()
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                str_index=row_col_index_locator(df,['trade'])[1]
                end_index=row_col_index_locator(df,['industries'])[1]
                df=column_values_clean(df)
                df=df.iloc[str_index:end_index+1,:]
                print(df)
                try:
                    df.columns=['Industry_Code','Industry','Fund_Based_In_Million','Non_Fund_Based_In_Million']
                except:
                    df=df.replace('',np.nan)
                    df=df.replace('#',np.nan)
                    df.dropna(axis=1,how='all',inplace=True)
                    df.columns=['Industry_Code','Industry','Fund_Based_In_Million','Non_Fund_Based_In_Million']
                df['Fund_Based_In_Million']=pd.to_numeric(df['Fund_Based_In_Million'])*0.1
                df['Non_Fund_Based_In_Million']=pd.to_numeric(df['Non_Fund_Based_In_Million'])*0.1
                df['Runtime']=datetime.datetime.now()
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Sub_Industry']=np.nan
                print(df)
                df=df[['Bank','Industry','Sub_Industry','Fund_Based_In_Million','Non_Fund_Based_In_Million','Relevant_Date','Runtime','Industry_Code']]
                df['Bank_Type']=bank_type
                df.reset_index(drop=True,inplace=True)
                df=clean_table(df)
                print('done df')
                print(df)
                engine = adqvest_db.db_conn()
                df=df[['Bank','Bank_Type','Industry','Sub_Industry','Fund_Based_In_Million','Non_Fund_Based_In_Million','Relevant_Date','Runtime']]
                df=df[df['Relevant_Date']>max_date]
                df.to_sql("BANK_INDUSTRY_WISE_DISTRIBUTION_EXPOSURE_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print(df)
                print('uploaded')
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='industry classification'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_au(df,heading,max_date,bank_type,bank,date,'ilovepdf',2,bank_name,catch_error_df)
            return df 


        bank='AU SMALL FINANCE BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        catch_error_df=pd.DataFrame(columns=['Table_name','Bank_Name','Error_message','Relevant_Date','Runtime'])
        links_capital=get_correct_links(links_capital)
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                search_str=['industry classification']
                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'stream',10,200,70)
                        if len(tables)>1:
                            df=data_collection_au(tables,search_str,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                            connection = engine.connect()
                            connection.execute("update BANK_BASEL_III_QUARTERLY_LINKS set Bank_Industry_Wise_Distribution_Exposure_Status='Completed' where Links = '"+link+"'")
                            delete_collected_data(bank_name,bank,max_date)
                        else:
                            print('AU BANK DID NOT HAVE ALL DATA')
                    except:
                        df=data_collection_au('',search_str,max_date,bank_type,bank,date,'ilovepdf',1,bank_name,catch_error_df)
                        delete_collected_data(bank_name,bank,max_date)
                except Exception as e:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    print('Error occured for bank------->',bank)

        def data_collection_hdfc(tables,search_str,max_date,bank_type,bank,date,type_read,loop,bank_name,catch_error_df):
            engine = adqvest_db.db_conn()
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                str_index=row_col_index_locator(df,['agriculture'])[1]
                end_index=row_col_index_locator(df,['other industries'])[1]
                df=df.iloc[str_index:end_index+1,:]
                df=df.replace('#',np.nan)
                df.dropna(axis=1,how='all',inplace=True)
                df['Industry_Code'] = np.nan
                df.columns=['Industry','Fund_Based_In_Million','Non_Fund_Based_In_Million','Industry_Code']
                df['Fund_Based_In_Million']=pd.to_numeric(df['Fund_Based_In_Million'])*0.1
                df['Non_Fund_Based_In_Million']=pd.to_numeric(df['Non_Fund_Based_In_Million'])*0.1
                df['Runtime']=datetime.datetime.now()
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Sub_Industry']=np.nan
                df=df[['Bank','Industry','Sub_Industry','Fund_Based_In_Million','Non_Fund_Based_In_Million','Relevant_Date','Runtime','Industry_Code']]
                print(df.columns)
                df['Bank_Type']=bank_type
                df.reset_index(drop=True,inplace=True)
                df=clean_table(df)
                print(df)
                df=df[df['Relevant_Date']>max_date]
                print(df)
                engine = adqvest_db.db_conn()
                df=df[['Bank','Bank_Type','Industry','Sub_Industry','Fund_Based_In_Million','Non_Fund_Based_In_Million','Relevant_Date','Runtime']]
                df=df[df['Relevant_Date']>max_date]
                df.to_sql("BANK_INDUSTRY_WISE_DISTRIBUTION_EXPOSURE_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print(df)
                print('uploaded')
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='agriculture'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    loop+=1
                    df=data_collection_au(df,heading,max_date,bank_type,bank,date,'ilovepdf',2,bank_name,catch_error_df)
            return df 


        bank='HDFC BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        catch_error_df=pd.DataFrame(columns=['Table_name','Bank_Name','Error_message','Relevant_Date','Runtime'])
        links_capital=get_correct_links(links_capital)
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                act_link=link
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                search_str=['agriculture']
                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'stream',10,200,70)
                        if len(tables)>1:
                            df=data_collection_hdfc(tables,search_str,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                            print('returned df')
                            connection = engine.connect()
                            connection.execute("update BANK_BASEL_III_QUARTERLY_LINKS set Bank_Industry_Wise_Distribution_Exposure_Status='Completed' where Links = '"+act_link+"'")
                            delete_collected_data(bank_name,bank,max_date)
                        else:
                            print('HDFC BANK DID NOT HAVE ALL DATA')
                    except:
                        df=data_collection_hdfc('',search_str,max_date,bank_type,bank,date,'ilovepdf',1,bank_name,catch_error_df)
                        delete_collected_data(bank_name,bank,max_date)
                except Exception as e:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    print('Error occured for bank------->',bank)

        def data_collection_sbi(tables,search_str,max_date,bank_type,bank,date,type_read,loop,bank_name,catch_error_df):
            engine = adqvest_db.db_conn()
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                str_index=row_col_index_locator(df,['industry'])[1]
                end_index=row_col_index_locator(df,['total'])[1]
                df=df.iloc[str_index:end_index,:]
                df=column_values_clean(df)
                code_code=row_col_index_locator(df,['code'])
                industry_col=row_col_index_locator(df,['coal'])
                fund_col=row_col_index_locator(df,['total'])
                non_fund_col=row_col_index_locator(df,['non'])
                df=df[[code_code[0],industry_col[0],fund_col[0],non_fund_col[0]]]
                df.reset_index(drop=True,inplace=True)
                code_code_2=row_col_index_locator(df,['coal'])
                df=df.iloc[code_code_2[1]:,:]
                print(df)
                df.columns=['Industry_Code','Industry','Fund_Based_In_Million','Non_Fund_Based_In_Million']
                df['Sub_Industry']=df.apply(lambda _:'',axis=1)
                df.reset_index(drop=True,inplace=True)
                for row in range(df.shape[0]):
                    if (re.match(r'\d+\.',str(df['Industry_Code'][row]))):
                        df['Sub_Industry'].iloc[row]=df.iloc[row,1]
                        df['Industry'].iloc[row]=np.nan
                df['Industry'].fillna(method='ffill',inplace=True)
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Runtime']=datetime.datetime.now()
                df=df[['Bank','Industry','Sub_Industry','Fund_Based_In_Million','Non_Fund_Based_In_Million',
                                        'Relevant_Date','Runtime','Industry_Code']]
                df.reset_index(drop=True,inplace=True)
                for j in df.columns:
                    if j!='Runtime':
                        df[j]=df[j].apply(lambda x:str(x))
                df['Bank_Type']=bank_type
                df=clean_table(df)
                print(df)
                df['Fund_Based_In_Million'] = df['Fund_Based_In_Million'].str.replace(',', '')
                df['Non_Fund_Based_In_Million'] = df['Non_Fund_Based_In_Million'].str.replace(',', '')
                print('here')
                df['Fund_Based_In_Million']=pd.to_numeric(df['Fund_Based_In_Million'])*0.1
                df['Non_Fund_Based_In_Million']=pd.to_numeric(df['Non_Fund_Based_In_Million'])*0.1
                print('done')
                engine = adqvest_db.db_conn()
                df['Relevant_Date']=pd.to_datetime(df['Relevant_Date']).dt.date
                df=df[['Bank','Bank_Type','Industry','Sub_Industry','Fund_Based_In_Million','Non_Fund_Based_In_Million','Relevant_Date','Runtime']]
                df=df[df['Relevant_Date']>max_date]
                df.to_sql("BANK_INDUSTRY_WISE_DISTRIBUTION_EXPOSURE_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print(df)
                print('uploaded')
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='coal'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_sbi(df,heading,max_date,bank_type,bank,date,'ilovepdf',2,bank_name,catch_error_df)
            return df 

        bank='STATE BANK OF INDIA'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        catch_error_df=pd.DataFrame(columns=['Table_name','Bank_Name','Error_message','Relevant_Date','Runtime'])
        links_capital=get_correct_links(links_capital)
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                act_link=link
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                search_str=['code']
                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'stream',10,200,70)
                        if len(tables)>1:
                            df=data_collection_sbi(tables,search_str,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                            print('returned df')
                            connection = engine.connect()
                            connection.execute("update BANK_BASEL_III_QUARTERLY_LINKS set Bank_Industry_Wise_Distribution_Exposure_Status='Completed' where Links = '"+act_link+"'")
                            delete_collected_data(bank_name,bank,max_date)
                        else:
                            print('HDFC BANK DID NOT HAVE ALL DATA')
                    except:
                        df=data_collection_sbi('',search_str,max_date,bank_type,bank,date,'ilovepdf',1,bank_name,catch_error_df)
                        delete_collected_data(bank_name,bank,max_date)
                except Exception as e:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    print('Error occured for bank------->',bank)


        def data_collection_icici_axis_kotak(tables,search_str,max_date,bank_type,bank,date,type_read,loop,bank_name,catch_error_df):
            engine = adqvest_db.db_conn()
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                df.reset_index(drop=True,inplace=True)
                print(df)
                if bank=='AXIS BANK':
                    str_index=row_col_index_locator(df,['banking and finance'])[1]
                    end_index=row_col_index_locator(df,['other industries'])[1]
                elif bank=='KOTAK MAHINDRA BANK':
                    str_index=row_col_index_locator(df,['bank'])[1]
                    end_index=row_col_index_locator(df,['total'])[1]
                    
                else:
                    str_index=row_col_index_locator(df,['retail finance'])[1]
                    end_index=row_col_index_locator(df,['grand total'])[1]
                df=df.iloc[str_index:end_index,:]
                try:
                    df.columns=['Industry','Fund_Based_In_Million','Non_Fund_Based_In_Million']
                except:
                    df=df.replace('',np.nan)
                    df.dropna(axis=1,how='all',inplace=True)
                    if bank=='KOTAK MAHINDRA BANK':
                        df=df.iloc[:,:-1]
                    df.columns=['Industry','Fund_Based_In_Million','Non_Fund_Based_In_Million']
                df['Industry_Code'] = np.nan
                df['Sub_Industry']=np.nan
                df['Runtime']=datetime.datetime.now()
                df['Bank']=bank
                df['Relevant_Date']=date
                df=df[['Bank','Industry','Sub_Industry','Fund_Based_In_Million','Non_Fund_Based_In_Million','Relevant_Date','Runtime','Industry_Code']]
                df['Bank_Type']=bank_type
                df['Industry'] = df['Industry'].str.replace('\d+', '', regex=True)
                df['Fund_Based_In_Million']=pd.to_numeric(df['Fund_Based_In_Million'])*0.1
                df['Non_Fund_Based_In_Million']=pd.to_numeric(df['Non_Fund_Based_In_Million'])*0.1
                df.reset_index(drop=True,inplace=True)
                df=clean_table(df)
                engine = adqvest_db.db_conn()
                df['Relevant_Date']=pd.to_datetime(df['Relevant_Date']).dt.date
                df=df[['Bank','Bank_Type','Industry','Sub_Industry','Fund_Based_In_Million','Non_Fund_Based_In_Million','Relevant_Date','Runtime']]
                df=df[df['Relevant_Date']>max_date]
                print(df)
                df.to_sql("BANK_INDUSTRY_WISE_DISTRIBUTION_EXPOSURE_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print('uploaded')
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='retail finance'
                    end_text='grand total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_icici_axis_kotak(df,heading,max_date,bank_type,bank,date,'ilovepdf',2,bank_name,catch_error_df)
            return df

        bank='ICICI BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        catch_error_df=pd.DataFrame(columns=['Table_name','Bank_Name','Error_message','Relevant_Date','Runtime'])
        links_capital=get_correct_links(links_capital)
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                act_link=link
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                search_str=['retail finance','mining','grand total']
                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'flavour',20,200,70)
                        if len(tables)>1:
                            df=data_collection_icici_axis_kotak(tables,search_str,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                            print('returned df')
                            connection = engine.connect()
                            connection.execute("update BANK_BASEL_III_QUARTERLY_LINKS set Bank_Industry_Wise_Distribution_Exposure_Status='Completed' where Links = '"+act_link+"'")
                            delete_collected_data(bank_name,bank,max_date)
                        else:
                            print('HDFC BANK DID NOT HAVE ALL DATA')
                    except:
                        df=data_collection_icici_axis_kotak('',search_str,max_date,bank_type,bank,date,'ilovepdf',1,bank_name,catch_error_df)
                        delete_collected_data(bank_name,bank,max_date)
                except Exception as e:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    print('Error occured for bank------->',bank)

        bank='AXIS BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        catch_error_df=pd.DataFrame(columns=['Table_name','Bank_Name','Error_message','Relevant_Date','Runtime'])
        links_capital=get_correct_links(links_capital)
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                act_link=link
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                search_str=['industry classification','banking book investments']
                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'stream',10,100,40)
                        if len(tables)>1:
                            df=data_collection_icici_axis_kotak(tables,search_str,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                            print('returned df')
                            connection = engine.connect()
                            connection.execute("update BANK_BASEL_III_QUARTERLY_LINKS set Bank_Industry_Wise_Distribution_Exposure_Status='Completed' where Links = '"+act_link+"'")
                            delete_collected_data(bank_name,bank,max_date)
                        else:
                            print('HDFC BANK DID NOT HAVE ALL DATA')
                    except:
                        df=data_collection_icici_axis_kotak('',search_str,max_date,bank_type,bank,date,'ilovepdf',1,bank_name,catch_error_df)
                        delete_collected_data(bank_name,bank,max_date)
                except Exception as e:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    print('Error occured for bank------->',bank)

        time.sleep(5)
        bank='KOTAK MAHINDRA BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        catch_error_df=pd.DataFrame(columns=['Table_name','Bank_Name','Error_message','Relevant_Date','Runtime'])
        links_capital=get_correct_links(links_capital)
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                act_link=link
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                search_str=['industry','fund based']
                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'lattice',20,100,40)
                        if len(tables)>1:
                            df=data_collection_icici_axis_kotak(tables,search_str,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                            print('returned df')
                            connection = engine.connect()
                            connection.execute("update BANK_BASEL_III_QUARTERLY_LINKS set Bank_Industry_Wise_Distribution_Exposure_Status='Completed' where Links = '"+act_link+"'")
                            delete_collected_data(bank_name,bank,max_date)
                        else:
                            print('HDFC BANK DID NOT HAVE ALL DATA')
                    except:
                        df=data_collection_icici_axis_kotak('',search_str,max_date,bank_type,bank,date,'ilovepdf',1,bank_name,catch_error_df)
                        delete_collected_data(bank_name,bank,max_date)
                except Exception as e:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    print('Error occured for bank------->',bank)

        def data_collection_central_federal_baroda(tables,search_str,max_date,bank_type,bank,date,type_read,loop,bank_name,catch_error_df):
            engine = adqvest_db.db_conn()
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                df.reset_index(drop=True,inplace=True)
                print(df)
                if bank=='FEDERAL BANK':
                    str_index=row_col_index_locator(df,['industry name'])[1]
                    end_index=row_col_index_locator(df,['total exposure'])[1]
                    col1=row_col_index_locator(df,['industry name'])[0]
                    try:
                        col2=row_col_index_locator(df,['exposure funded'])[0]
                        col3=row_col_index_locator(df,['exposure non-funded'])[0]
                    except:
                        col2=row_col_index_locator(df,['funded'])[0]
                        col3=row_col_index_locator(df,['nonfunded'])[0]
                    df=df.iloc[str_index+1:end_index+1,[col1,col2,col3]]
                elif bank=='BANK OF BARODA':
                    str_index=row_col_index_locator(df,['mining'])[1]
                    end_index=row_col_index_locator(df,['total exposure'])[1]
                    df=df.iloc[str_index-1:end_index,:]
                else:
                    str_index=row_col_index_locator(df,['mining and'])[1]
                    end_index=row_col_index_locator(df,['residuary other advances'])[1]
                    df=df.iloc[str_index-1:end_index,:]
                df=clean_dataframe(df)
                str_index=row_col_index_locator(df,['mining'])[1]
                col=row_col_index_locator(df,['mining'])[0]
                if bank=='CENTRAL BANK OF INDIA':
                    end_index=row_col_index_locator(df,['all industries'])[1]
                    df=df.iloc[str_index:end_index+1,col:len(df.columns)-1]
                elif bank=='BANK OF BARODA':
                    df=df.iloc[str_index:,col:len(df.columns)-1]
                if bank=='FEDERAL BANK':
                    df=df.iloc[str_index:,:]
                df.columns=['Industry','Fund_Based_In_Million','Non_Fund_Based_In_Million']
                
                df['Industry_Code'] = df['Industry'].str.extract(r'([A-Z]\.[a-zA-Z\d]+\.\s*\d+|[A-Z]\.[a-zA-Z\d]+|[A-Z]\.\s*\d+|[A-Z]\.)')
                pattern = r"\b\d+(\.\d+){1,2}\b"
                extracted_codes = df['Industry'].str.extractall(pattern)
                extracted_codes = extracted_codes.groupby(level=0).agg(lambda x: ','.join(x.astype(str)))
                df.loc[pd.isna(df['Industry_Code']), 'Industry_Code'] = extracted_codes
                df['Sub_Industry']=' '
                df=df.replace('#','').replace('-','')
                df['Fund_Based_In_Million'] = df['Fund_Based_In_Million'].apply(lambda x: re.sub(r'^.*?(\d+\.\d+).*$', r'\1', str(x)))
                df['Non_Fund_Based_In_Million'] = df['Non_Fund_Based_In_Million'].apply(lambda x: re.sub(r'^.*?(\d+\.\d+).*$', r'\1', str(x)))
                df['Fund_Based_In_Million']=round(pd.to_numeric(df['Fund_Based_In_Million'])*10,2)
                df['Non_Fund_Based_In_Million']=round(pd.to_numeric(df['Non_Fund_Based_In_Million'])*10,2)

                for row in range(df.shape[0]):
                    if (re.match(r'[A-Z]\.\d',df.iloc[row,0])):
                        df['Sub_Industry'].iloc[row]=df.iloc[row,0].partition('.')[2].strip()
                        df['Industry'].iloc[row]=np.nan
                    elif (re.match(r'[a-z]\.',df.iloc[row,0])):
                        df['Sub_Industry'].iloc[row]=df.iloc[row,0].partition('.')[2].strip()
                        df['Industry'].iloc[row]=np.nan
                    elif (re.match(r'[A-Z]\.[a-z]',df.iloc[row,0])):
                        df['Sub_Industry'].iloc[row]=df.iloc[row,0].partition('.')[2].strip()
                        df['Industry'].iloc[row]=np.nan
                    elif (re.match(r'an',df.iloc[row,0])):
                        df['Sub_Industry'].iloc[row]=df.iloc[row,0].partition('.')[2].strip()
                        df['Industry'].iloc[row]=np.nan
                
                df['Industry']=df['Industry'].str.replace('c\.\)','Chemicals and Chemical Products (Dyes Paints')
                df['Sub_Industry']=df['Sub_Industry'].str.replace('\d+', '')
                df['Industry'] = df['Industry'].str.replace(r'\(.*?\)', '', regex=True)
                df['Industry'] = df['Industry'].str.replace(r'([A-Z]\.[a-zA-Z\d]+\.\s*\d+|[A-Z]\.[a-zA-Z\d]+|[A-Z]\.\s*\d+|[A-Z]\.)', '')
                df['Runtime']=datetime.datetime.now()
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Industry'].fillna(method='ffill',inplace=True)
                df=df[['Bank','Industry','Sub_Industry','Fund_Based_In_Million','Non_Fund_Based_In_Million','Relevant_Date','Runtime','Industry_Code']]
                df.reset_index(drop=True,inplace=True)
                for j in df.columns:
                    if j!='Runtime':
                        df[j]=df[j].apply(lambda x:str(x))
                df['Bank_Type']=bank_type
                df=clean_table(df)
                engine = adqvest_db.db_conn()
                df['Relevant_Date']=pd.to_datetime(df['Relevant_Date']).dt.date
                df=df[['Bank','Bank_Type','Industry','Sub_Industry','Fund_Based_In_Million','Non_Fund_Based_In_Million','Relevant_Date','Runtime']]
                df=df[df['Relevant_Date']>max_date]
                print(df)
                df.to_sql("BANK_INDUSTRY_WISE_DISTRIBUTION_EXPOSURE_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print('uploaded')
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    if bank=='CENTRAL BANK OF INDIA':
                        heading='mining and quarrying'
                        end_text='total'
                        df=search(heading,end_text,path,sheets)
                    else:
                        heading='industry name'
                        end_text='total'
                        df=search(heading,end_text,path,sheets)
                    df=data_collection_central_federal_baroda(df,heading,max_date,bank_type,bank,date,'ilovepdf',2,bank_name,catch_error_df)
            return df 

        bank='CENTRAL BANK OF INDIA'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        catch_error_df=pd.DataFrame(columns=['Table_name','Bank_Name','Error_message','Relevant_Date','Runtime'])
        links_capital=get_correct_links(links_capital)
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                act_link=link
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                search_str=['mining and quarrying','fertilizer','energy','telecommunication','total']
                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'flavor',10,200,70)
                        if len(tables)>1:
                            df=data_collection_central_federal_baroda(tables,search_str,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                            print('returned df')
                            connection = engine.connect()
                            connection.execute("update BANK_BASEL_III_QUARTERLY_LINKS set Bank_Industry_Wise_Distribution_Exposure_Status='Completed' where Links = '"+act_link+"'")
                            delete_collected_data(bank_name,bank,max_date)
                        else:
                            print('HDFC BANK DID NOT HAVE ALL DATA')
                    except:
                        df=data_collection_central_federal_baroda('',search_str,max_date,bank_type,bank,date,'ilovepdf',1,bank_name,catch_error_df)
                        delete_collected_data(bank_name,bank,max_date)
                except Exception as e:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    print('Error occured for bank------->',bank)

        bank='BANK OF BARODA'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        catch_error_df=pd.DataFrame(columns=['Table_name','Bank_Name','Error_message','Relevant_Date','Runtime'])
        links_capital=get_correct_links(links_capital)
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                act_link=link
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                search_str=['industry']
                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'lattice',10,100,40)
                        if len(tables)>1:
                            df=data_collection_central_federal_baroda(tables,search_str,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                            print('returned df')
                            connection = engine.connect()
                            connection.execute("update BANK_BASEL_III_QUARTERLY_LINKS set Bank_Industry_Wise_Distribution_Exposure_Status='Completed' where Links = '"+act_link+"'")
                            delete_collected_data(bank_name,bank,max_date)
                        else:
                            print('HDFC BANK DID NOT HAVE ALL DATA')
                    except:
                        df=data_collection_central_federal_baroda('',search_str,max_date,bank_type,bank,date,'ilovepdf',1,bank_name,catch_error_df)
                        delete_collected_data(bank_name,bank,max_date)
                except Exception as e:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    print('Error occured for bank------->',bank)
        time.sleep(5)
        bank='FEDERAL BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        catch_error_df=pd.DataFrame(columns=['Table_name','Bank_Name','Error_message','Relevant_Date','Runtime'])
        links_capital=get_correct_links(links_capital)
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                act_link=link
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                search_str=['industry name','total exposure']
                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'stream',10,100,40)
                        if len(tables)>1:
                            df=data_collection_central_federal_baroda(tables,search_str,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                            print('returned df')
                            connection = engine.connect()
                            connection.execute("update BANK_BASEL_III_QUARTERLY_LINKS set Bank_Industry_Wise_Distribution_Exposure_Status='Completed' where Links = '"+act_link+"'")
                            delete_collected_data(bank_name,bank,max_date)
                        else:
                            print('HDFC BANK DID NOT HAVE ALL DATA')
                    except:
                        df=data_collection_central_federal_baroda('',search_str,max_date,bank_type,bank,date,'ilovepdf',1,bank_name,catch_error_df)
                        delete_collected_data(bank_name,bank,max_date)
                except Exception as e:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    print('Error occured for bank------->',bank)

        def data_collection_union(tables,search_str,max_date,bank_type,bank,date,type_read,loop,bank_name,catch_error_df):
            engine = adqvest_db.db_conn()
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                str_index=row_col_index_locator(df,['mining & quarrying'])[1]
                end_index=row_col_index_locator(df,['other residuary'])[1]
                col=row_col_index_locator(df,['A.1'])[0]
                df=df.iloc[str_index:end_index+1,col:]
                # df=column_values_clean(df)
                try:
                    df.columns=['Industry_Code','Industry','Fund_Based_In_Million','Non_Fund_Based_In_Million']
                except:
                    df=clean_dataframe(df)
                    df.columns=['Industry_Code','Industry','Fund_Based_In_Million','Non_Fund_Based_In_Million']
                df['Fund_Based_In_Million']=pd.to_numeric(df['Fund_Based_In_Million'])*0.1
                df['Non_Fund_Based_In_Million']=pd.to_numeric(df['Non_Fund_Based_In_Million'])*0.1
                df['Sub_Industry']=np.nan
                for row in range(df.shape[0]):
                    try:
                        if (re.match(r'[A-Z]\.\d',df.iloc[row,0])):
                            df['Sub_Industry'].iloc[row]=df.iloc[row,1].partition('.')[0].strip()
                            df['Industry'].iloc[row]=np.nan
                        elif (re.match(r'[a-z]\.',df.iloc[row,0])):
                            df['Sub_Industry'].iloc[row]=df.iloc[row,1].partition('.')[0].strip()
                            df['Industry'].iloc[row]=np.nan
                        elif (re.match(r'[A-Z]\.[A-Z]',df.iloc[row,0])):
                            df['Sub_Industry'].iloc[row]=df.iloc[row,1].partition('.')[0].strip()
                            df['Industry'].iloc[row]=np.nan
                    except:
                        continue

                df['Industry'].fillna(method='ffill',inplace=True)
                #industry_df['Industry']=industry_df['Industry'].apply(lambda x:str(x)[1:])
                df['Industry']=df['Industry'].str.replace('\.\d','').str.strip('.').str.strip()
                df['Sub_Industry']=df['Sub_Industry'].apply(lambda x:re.sub('\d\.\d*','',str(x)))
                df['Sub_Industry']=df['Sub_Industry'].apply(lambda x:re.sub('\d','',str(x)))
                df['Sub_Industry']=df['Sub_Industry'].str.replace('[A-Z]\.','')
                df['Runtime']=datetime.datetime.now()
                df['Bank']=bank
                df['Relevant_Date']=date
                df=df[['Bank','Industry','Sub_Industry','Fund_Based_In_Million','Non_Fund_Based_In_Million','Relevant_Date','Runtime','Industry_Code']]
                df['Bank_Type']=bank_type
                df.reset_index(drop=True,inplace=True)
                df=clean_table(df)
                engine = adqvest_db.db_conn()
                df['Relevant_Date']=pd.to_datetime(df['Relevant_Date']).dt.date
                df=df[['Bank','Bank_Type','Industry','Sub_Industry','Fund_Based_In_Million','Non_Fund_Based_In_Million','Relevant_Date','Runtime']]
                df=df[df['Relevant_Date']>max_date]
                print(df)
                df.to_sql("BANK_INDUSTRY_WISE_DISTRIBUTION_EXPOSURE_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print('uploaded')
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='industry name'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_union(df,heading,max_date,bank_type,bank,date,'ilovepdf',2,bank_name,catch_error_df)
            return df 
        time.sleep(5)
        bank='UNION BANK OF INDIA'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        catch_error_df=pd.DataFrame(columns=['Table_name','Bank_Name','Error_message','Relevant_Date','Runtime'])
        links_capital=get_correct_links(links_capital)
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                act_link=link
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                search_str=['industry name','other residuary advances ']
                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'lattice',15,100,40)
                        if len(tables)>1:
                            df=data_collection_union(tables,search_str,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                            print('returned df')
                            connection = engine.connect()
                            connection.execute("update BANK_BASEL_III_QUARTERLY_LINKS set Bank_Industry_Wise_Distribution_Exposure_Status='Completed' where Links = '"+act_link+"'")
                            delete_collected_data(bank_name,bank,max_date)
                        else:
                            print('HDFC BANK DID NOT HAVE ALL DATA')
                    except:
                        df=data_collection_union('',search_str,max_date,bank_type,bank,date,'ilovepdf',1,bank_name,catch_error_df)
                        delete_collected_data(bank_name,bank,max_date)
                except Exception as e:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    print('Error occured for bank------->',bank)

        def data_collection_iob(tables,search_str,max_date,bank_type,bank,date,type_read,loop,bank_name,catch_error_df):
            engine = adqvest_db.db_conn()
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                str_index=row_col_index_locator(df,['mining and quarrying'])[1]
                end_index=row_col_index_locator(df,['total loans and'])[1]
                df=df.iloc[str_index:end_index,:]
                df.columns=['Industry','Fund_Based_In_Million']
                df['Sub_Industry']=' '
                df['Fund_Based_In_Million']=pd.to_numeric(df['Fund_Based_In_Million'])*10
                for row in range(df.shape[0]):
                    if (df.iloc[row,0][:2].lower()=='of'):
                        df['Sub_Industry'].iloc[row]=df.iloc[row,0]
                        df['Industry'].iloc[row]=np.nan
                df['Industry_Code']=range(len(df))
                df['Runtime']=datetime.datetime.now()
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Non_Fund_Based_In_Million']=' '
                df['Industry'].fillna(method='ffill',inplace=True)
                df=df[['Bank','Industry','Sub_Industry','Fund_Based_In_Million','Non_Fund_Based_In_Million','Relevant_Date','Runtime','Industry_Code']]
                df.reset_index(drop=True,inplace=True)
                for j in df.columns:
                    if j!='Runtime':
                        df[j]=df[j].apply(lambda x:str(x))
                df['Bank_Type']=bank_type
                df=clean_table(df)
                engine = adqvest_db.db_conn()
                df['Relevant_Date']=pd.to_datetime(df['Relevant_Date']).dt.date
                df=df[['Bank','Bank_Type','Industry','Sub_Industry','Fund_Based_In_Million','Non_Fund_Based_In_Million','Relevant_Date','Runtime']]
                df=df[df['Relevant_Date']>max_date]
                print(df)
                df.to_sql("BANK_INDUSTRY_WISE_DISTRIBUTION_EXPOSURE_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print('uploaded')
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='industry name'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_iob(df,heading,max_date,bank_type,bank,date,'ilovepdf',2,bank_name,catch_error_df)
            return df
        time.sleep(5)
        bank='INDIAN OVERSEAS BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        catch_error_df=pd.DataFrame(columns=['Table_name','Bank_Name','Error_message','Relevant_Date','Runtime'])
        links_capital=get_correct_links(links_capital)
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                act_link=link
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                search_str=['industry name','residuary other advances']
                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'lattice',10,100,40)
                        if len(tables)>1:
                            df=data_collection_iob(tables,search_str,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                            print('returned df')
                            connection = engine.connect()
                            connection.execute("update BANK_BASEL_III_QUARTERLY_LINKS set Bank_Industry_Wise_Distribution_Exposure_Status='Completed' where Links = '"+act_link+"'")
                            delete_collected_data(bank_name,bank,max_date)
                        else:
                            print('HDFC BANK DID NOT HAVE ALL DATA')
                    except:
                        df=data_collection_iob('',search_str,max_date,bank_type,bank,date,'ilovepdf',1,bank_name,catch_error_df)
                        delete_collected_data(bank_name,bank,max_date)
                except Exception as e:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    print('Error occured for bank------->',bank)

        def data_collection_rbl(tables,search_str,max_date,bank_type,bank,date,type_read,loop):
            engine = adqvest_db.db_conn()
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                    df.reset_index(drop=True,inplace=True)

                l_col=['industry name','fund based','nonfund based']
                df.reset_index(drop=True,inplace=True)
                df_clean=clean_row_col(l_col,df)
                str_index=row_col_index_locator(df_clean,['mining and quarrying'])[1]
                end_index=row_col_index_locator(df_clean,['other residuary'])[1]
                df_clean=df_clean.iloc[str_index:end_index+1,:]

                row_dict=['#','industry','amount']
                df_clean=row_modificator(df_clean,row_dict,1,row_del=True,keep_row=False,update_row=True)
                df_clean=clean_dataframe(df_clean)
                df_clean.columns=['Industry','Fund_Based_In_Million','Non_Fund_Based_In_Million']
                df_clean=df_clean.replace(np.nan,'')
                df=df_clean.copy()
                df['Industry_Code'] = df['Industry'].str.extract(r'([A-Z]\.[a-zA-Z\d]+\.\s*\d+|[A-Z]\.[a-zA-Z\d]+|[A-Z]\.\s*\d+|[A-Z]\.)')
                df=df[df['Industry']!='Amount (Rs in Millions)']
                df['Sub_Industry']=' '
                df = df[df['Industry'].str.strip() != '']
                df=df[~df['Industry'].str.contains('amount', case=False)]
                df=df.drop(df[df['Industry'].str.lower().str.contains('all industries')].index)
                df.drop(df[df['Industry'].str.contains('distribution')].index,inplace=True)
                df['Sub_Industry']=np.nan
                for row in range(df.shape[0]):
                    try:
                        if (re.match(r'^[A-Z]\.\d',df.iloc[row,0])):
                            df['Sub_Industry'].iloc[row]=df.iloc[row,0].partition('.')[2].strip()
                            df['Industry'].iloc[row]=np.nan
                        if (re.match(r'^[A-Z]\.\d\.\d',df.iloc[row,0])):
                            df['Sub_Industry'].iloc[row]=df.iloc[row,0].partition('.')[2].strip()
                            df['Industry'].iloc[row]=np.nan
                        elif (re.match(r'^[A-Z]\.',df.iloc[row,0])):
                            df['Sub_Industry'].iloc[row]=np.nan
                        else:
                            df.drop(row,inplace=True)
                    except:
                        continue
                df['Industry'] = df['Industry'].str.replace('[A-Z]\.', '')
                df['Industry'] = df['Industry'].str.replace('+', '')
                df['Industry'] = df['Industry'].str.replace('\d', '')
                df['Sub_Industry'] = df['Sub_Industry'].str.replace('\d', '')
                df['Sub_Industry'] = df['Sub_Industry'].str.replace('\.', '')
                df['Industry']=df['Industry'].fillna(method='ffill')
                df.drop(df[df['Industry']==''].index,inplace=True)
                df.drop(df[df['Industry'].str.lower().str.contains('industry')].index,inplace=True)
                df['Fund_Based_In_Million']=df['Fund_Based_In_Million'].apply(lambda x:pd.to_numeric(x))
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Runtime']=datetime.datetime.now()
                df=df[['Bank','Industry','Sub_Industry','Fund_Based_In_Million','Non_Fund_Based_In_Million',
                                        'Relevant_Date','Runtime','Industry_Code']]
                df['Bank_Type']=bank_type
                df.reset_index(drop=True,inplace=True)
                for j in df.columns:
                    if j!='Runtime':
                        df[j]=df[j].apply(lambda x:str(x))

                df=clean_table(df)
                engine = adqvest_db.db_conn()
                df['Relevant_Date']=pd.to_datetime(df['Relevant_Date']).dt.date
                df=df[['Bank','Bank_Type','Industry','Sub_Industry','Fund_Based_In_Million','Non_Fund_Based_In_Million','Relevant_Date','Runtime']]
                df=df[df['Relevant_Date']>max_date]
                print(df)
                df.to_sql("BANK_INDUSTRY_WISE_DISTRIBUTION_EXPOSURE_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print('uploaded')
                
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='industry name'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_rbl(df,heading,max_date,bank_type,bank,date,'ilovepdf',2,bank_name,catch_error_df)
            return df
        time.sleep(5)
        bank='RBL BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        catch_error_df=pd.DataFrame(columns=['Table_name','Bank_Name','Error_message','Relevant_Date','Runtime'])
        links_capital=get_correct_links(links_capital)
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                act_link=link
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                search_str=['industry name']
                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'lattice',10,100,40)
                        if len(tables)>1:
                            df=data_collection_rbl(tables,search_str,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                            print('returned df')
                            connection = engine.connect()
                            connection.execute("update BANK_BASEL_III_QUARTERLY_LINKS set Bank_Industry_Wise_Distribution_Exposure_Status='Completed' where Links = '"+act_link+"'")
                            delete_collected_data(bank_name,bank,max_date)
                        else:
                            print('HDFC BANK DID NOT HAVE ALL DATA')
                    except:
                        df=data_collection_rbl('',search_str,max_date,bank_type,bank,date,'ilovepdf',1,bank_name,catch_error_df)
                        delete_collected_data(bank_name,bank,max_date)
                except Exception as e:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    print('Error occured for bank------->',bank)

        def data_collection_indusind(tables,search_str,max_date,bank_type,bank,date,type_read,loop,bank_name,catch_error_df):
            engine = adqvest_db.db_conn()
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                l_col=['industry name','fund based','nonfund based']
                df_clean=clean_row_col(l_col,df)
                df=df_clean.copy()
                df.columns=['Industry','Fund_Based_In_Million','Non_Fund_Based_In_Million']
                str_index=row_col_index_locator(df,['nbfc'])[1]
                end_index=row_col_index_locator(df,['residual assets'])[1]
                df=df.iloc[str_index:end_index+1]
                str_index_df2=row_col_index_locator(df,['petroleum'])[1]
                df['Sub_Industry']=np.nan
                df2=df.iloc[str_index_df2:]
                df1=df.iloc[:str_index_df2]
                list_val=['nbfcs','construction','real estate','power','cables','steel','textiles','telecom','pharmaceuticals','chemicals','fertilisers','paper']
                def find_exact_match(row):
                    for val in list_val:
                        if row['Industry'].lower() == val:
                            return ''
                    return row['Industry']

                df1['Sub_Industry'] = df1.apply(find_exact_match, axis=1)
                df1['Industry']=np.where(df1['Sub_Industry']=='',df1['Industry'],np.nan)
                df=df1.append(df2)
                df['Industry'].fillna(method='ffill',inplace=True)
                df['Sub_Industry']=df['Sub_Industry'].replace('',np.nan)
                df.drop(df[df['Sub_Industry']=='Industry Name'].index.values,inplace=True)
                df.drop(df[df['Industry']=='Industry Name'].index.values,inplace=True)
                df.drop(df[df['Industry']==''].index.values,inplace=True)
                df['Runtime']=datetime.datetime.now()
                df['Bank']=bank
                df['Relevant_Date']=date
                df=df[['Bank','Industry','Sub_Industry','Fund_Based_In_Million','Non_Fund_Based_In_Million','Relevant_Date','Runtime']]
                df.reset_index(drop=True,inplace=True)
                df['Fund_Based_In_Million']=df['Fund_Based_In_Million'].apply(lambda x:round(pd.to_numeric(x),2))
                df['Non_Fund_Based_In_Million']=df['Non_Fund_Based_In_Million'].apply(lambda x:round(pd.to_numeric(x),2))
                df['Sub_Industry']=df['Sub_Industry'].apply(lambda x:str(x))
                for row in range(df.shape[0]-1):
                    if ((df['Sub_Industry'].iloc[row]=='nan') and (df['Sub_Industry'].iloc[row+1]!='nan')):
                        for j in range(row+2,row+13):
                            if df['Sub_Industry'].iloc[j]=='nan':
                                df['Fund_Based_In_Million'].iloc[row]=df['Fund_Based_In_Million'].iloc[row+1:j].sum()
                                df['Non_Fund_Based_In_Million'].iloc[row]=df['Non_Fund_Based_In_Million'].iloc[row+1:j].sum()
                                row=j
                                break
                for j in df.columns:
                    if j!='Runtime':
                        df[j]=df[j].apply(lambda x:str(x))
                df['Bank_Type']=bank_type
                df['Industry_Code']=np.nan
                df=clean_table(df)
                engine = adqvest_db.db_conn()
                df['Relevant_Date']=pd.to_datetime(df['Relevant_Date']).dt.date
                df=df[['Bank','Bank_Type','Industry','Sub_Industry','Fund_Based_In_Million','Non_Fund_Based_In_Million','Relevant_Date','Runtime']]
                df=df[df['Relevant_Date']>max_date]
                print(df)
                df.to_sql("BANK_INDUSTRY_WISE_DISTRIBUTION_EXPOSURE_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print('uploaded')
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='industry name'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_indusind(df,heading,max_date,bank_type,bank,date,'ilovepdf',2,bank_name,catch_error_df)
            return df
        time.sleep(5)
        bank='INDUSIND BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        catch_error_df=pd.DataFrame(columns=['Table_name','Bank_Name','Error_message','Relevant_Date','Runtime'])
        links_capital=get_correct_links(links_capital)
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                act_link=link
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                search_str=['industry name']
                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'stream',10,100,40)
                        if len(tables)>1:
                            df=data_collection_indusind(tables,search_str,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                            print('returned df')
                            connection = engine.connect()
                            connection.execute("update BANK_BASEL_III_QUARTERLY_LINKS set Bank_Industry_Wise_Distribution_Exposure_Status='Completed' where Links = '"+act_link+"'")
                            delete_collected_data(bank_name,bank,max_date)
                        else:
                            print('HDFC BANK DID NOT HAVE ALL DATA')
                    except:
                        df=data_collection_indusind('',search_str,max_date,bank_type,bank,date,'ilovepdf',1,bank_name,catch_error_df)
                        delete_collected_data(bank_name,bank,max_date)
                except Exception as e:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    print('Error occured for bank------->',bank)

        def data_collection_idbi_city(tables,search_str,max_date,bank_type,bank,date,type_read,loop,bank_name,catch_error_df):
            engine = adqvest_db.db_conn()
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                    df.reset_index(drop=True,inplace=True)
                else:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                if bank=='CITY UNION BANK':
                    str_index=row_col_index_locator(df,['industry'])[1]
                    end_index=row_col_index_locator(df,['total gross'])[1]
                    col1=row_col_index_locator(df,['industry'])[0]
                    col2=row_col_index_locator(df,['funded exposure'])[0]
                    col3=row_col_index_locator(df,['nonfunded exposure'])[0]
                    df=df.iloc[str_index+1:end_index,[col1,col2,col3]]
                else:
                    str_index=row_col_index_locator(df,['industry name'])[1]
                    print(str_index)
                    try:
                        end_index=row_col_index_locator(df,['other industries'])[1]
                        col1=row_col_index_locator(df,['industry'])[0]
                        col2=row_col_index_locator(df,['fund based exposure'])[0]
                        col3=row_col_index_locator(df,['non fund based'])[0]
                        df=df.iloc[str_index+1:end_index+1,[col1,col2,col3]]
                    except:
                        col1=row_col_index_locator(df,['industry'])[0]
                        col2=row_col_index_locator(df,['fund based'])[0]
                        col3=row_col_index_locator(df,['non fund'])[0]
                        df=df.iloc[str_index+1:,[col1,col2,col3]]
                df.columns=['Industry','Fund_Based_In_Million','Non_Fund_Based_In_Million']
                df.drop(df[df['Industry']=='Industry'].index.values,inplace=True)
                df['Sub_Industry']=np.nan
                df['Industry_Code']=np.nan
                df['Runtime']=datetime.datetime.now()
                df['Bank']=bank
                df['Relevant_Date']=date
                df=df[['Bank','Industry','Sub_Industry','Fund_Based_In_Million','Non_Fund_Based_In_Million','Relevant_Date','Runtime','Industry_Code']]
                df['Bank_Type']=bank_type
                df.reset_index(drop=True,inplace=True)
                df=clean_table(df)
                engine = adqvest_db.db_conn()
                df['Relevant_Date']=pd.to_datetime(df['Relevant_Date']).dt.date
                df=df[['Bank','Bank_Type','Industry','Sub_Industry','Fund_Based_In_Million','Non_Fund_Based_In_Million','Relevant_Date','Runtime']]
                df=df[df['Relevant_Date']>max_date]
                df=df[df['Industry']!='']
                print(df)
                df.to_sql("BANK_INDUSTRY_WISE_DISTRIBUTION_EXPOSURE_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print('uploaded')        
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='industry'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_idbi_city(df,heading,max_date,bank_type,bank,date,'ilovepdf',2,bank_name,catch_error_df)
            return df
        time.sleep(5)
        bank='IDBI BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        catch_error_df=pd.DataFrame(columns=['Table_name','Bank_Name','Error_message','Relevant_Date','Runtime'])
        links_capital=get_correct_links(links_capital)
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                act_link=link
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                search_str=['industry','agriculture','education','housing']
                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'lattice',10,100,40)
                        if len(tables)>1:
                            df=data_collection_idbi_city(tables,search_str,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                            print('returned df')
                            connection = engine.connect()
                            connection.execute("update BANK_BASEL_III_QUARTERLY_LINKS set Bank_Industry_Wise_Distribution_Exposure_Status='Completed' where Links = '"+act_link+"'")
                            delete_collected_data(bank_name,bank,max_date)
                        else:
                            print('HDFC BANK DID NOT HAVE ALL DATA')
                    except:
                        df=data_collection_idbi_city('',search_str,max_date,bank_type,bank,date,'ilovepdf',1,bank_name,catch_error_df)
                        delete_collected_data(bank_name,bank,max_date)
                except Exception as e:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    print(e)
        time.sleep(5)
        bank='CITY UNION BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        catch_error_df=pd.DataFrame(columns=['Table_name','Bank_Name','Error_message','Relevant_Date','Runtime'])
        links_capital=get_correct_links(links_capital)
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                act_link=link
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                search_str=['mining and quarrying']
                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        # tables = extract_tables_from_pdf(link,False,'lattice',10,100,40)
                        if len(tables)>1:
                            df=data_collection_idbi_city(tables,search_str,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                            print('returned df')
                            connection = engine.connect()
                            connection.execute("update BANK_BASEL_III_QUARTERLY_LINKS set Bank_Industry_Wise_Distribution_Exposure_Status='Completed' where Links = '"+act_link+"'")
                            delete_collected_data(bank_name,bank,max_date)
                        else:
                            print('HDFC BANK DID NOT HAVE ALL DATA')
                    except:
                        df=data_collection_idbi_city('',search_str,max_date,bank_type,bank,date,'ilovepdf',1,bank_name,catch_error_df)
                        delete_collected_data(bank_name,bank,max_date)
                except Exception as e:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    print('Error occured for bank------->',bank)

        def data_collection_canara(tables,search_str,max_date,bank_type,bank,date,type_read,loop,bank_name,catch_error_df):
            engine = adqvest_db.db_conn()
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                    df.reset_index(drop=True,inplace=True)
                else:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                try:
                    l_col=['sl','industry','fb exposure','nfb exposure']
                    df_clean=clean_row_col(l_col,df)
                    str_index=row_col_index_locator(df_clean,['mining and quarrying'])[1]
                    col=row_col_index_locator(df_clean,['mining and quarrying'])[0]
                    end_index=row_col_index_locator(df_clean,['other industries'])[1]
                except:
                    l_col=['sl','industry','fund based exposure','non fund based exposure']
                    df_clean=clean_row_col(l_col,df)
                    str_index=row_col_index_locator(df_clean,['mining and quarrying'])[1]
                    col=row_col_index_locator(df_clean,['mining and quarrying'])[0]
                    end_index=row_col_index_locator(df_clean,['other industries'])[1]

                df_clean=df_clean.iloc[str_index:end_index+1,:]
                print(df_clean)
                df_clean=clean_dataframe(df_clean)
                row_dict=['#','industry','amount']
                df_clean=row_modificator(df_clean,row_dict,1,row_del=True,keep_row=False,update_row=True)
                print(df_clean)
                df_clean.columns=['Industry_Code','Industry','Fund_Based_In_Million','Non_Fund_Based_In_Million']
                df_clean=df_clean.replace(np.nan,'')
                df_clean['Industry'] = df_clean['Industry_Code'].str.cat(df_clean['Industry'], sep=" ")
                df=df_clean.copy()
                pattern = r"(\b\d+\.\d+\.\d+\b|\b\d+\.\d+\b|\b\d+\.\b|\b\d+\b)"
                df['Industry_Code'] = df['Industry'].str.extract(pattern)
                df=df[df['Industry']!='Amount (Rs in Millions)']
                df['Sub_Industry']=' '
                df = df[df['Industry'].str.strip() != '']
                df=df[~df['Industry'].str.contains('amount', case=False)]
                df['Fund_Based_In_Million']=round(pd.to_numeric(df['Fund_Based_In_Million'])*10,2)
                df['Non_Fund_Based_In_Million']=round(pd.to_numeric(df['Non_Fund_Based_In_Million'])*10,2)
                for row in range(df.shape[0]):
                    if (re.match(r'\d\.\d.\d',df.iloc[row,0])):
                        df['Sub_Industry'].iloc[row]=df.iloc[row,1].partition('.')[2].strip()
                        df['Industry'].iloc[row]=np.nan
                df['Industry'].fillna(method='ffill',inplace=True)
                df['Industry'] = df['Industry'].str.replace(r'\(.*?\)', '', regex=True)
                df['Industry'] = df['Industry'].str.replace(pattern, '')
                df['Sub_Industry']=df['Sub_Industry'].str.replace(pattern, '')
                df['Runtime']=datetime.datetime.now()
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Industry'].fillna(method='ffill',inplace=True)
                df=df[['Bank','Industry','Sub_Industry','Fund_Based_In_Million','Non_Fund_Based_In_Million','Relevant_Date','Runtime','Industry_Code']]
                df.reset_index(drop=True,inplace=True)
                for j in df.columns:
                    if j!='Runtime':
                        df[j]=df[j].apply(lambda x:str(x))
                df['Bank_Type']=bank_type
                df=clean_table(df)
                engine = adqvest_db.db_conn()
                df['Relevant_Date']=pd.to_datetime(df['Relevant_Date']).dt.date
                df=df[['Bank','Bank_Type','Industry','Sub_Industry','Fund_Based_In_Million','Non_Fund_Based_In_Million','Relevant_Date','Runtime']]
                df=df[df['Relevant_Date']>max_date]
                df=df[df['Industry']!='']
                print(df)
                df.to_sql("BANK_INDUSTRY_WISE_DISTRIBUTION_EXPOSURE_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print('uploaded')
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='industry name'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_canara(df,heading,max_date,bank_type,bank,date,'ilovepdf',2,bank_name,catch_error_df)
            return df 
        time.sleep(5)
        bank='CANARA BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        catch_error_df=pd.DataFrame(columns=['Table_name','Bank_Name','Error_message','Relevant_Date','Runtime'])
        links_capital=get_correct_links(links_capital)
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                act_link=link
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                search_str=['industry']
                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'lattice',10,100,40)
                        if len(tables)>1:
                            df=data_collection_canara(tables,search_str,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                            print('returned df')
                            connection = engine.connect()
                            connection.execute("update BANK_BASEL_III_QUARTERLY_LINKS set Bank_Industry_Wise_Distribution_Exposure_Status='Completed' where Links = '"+act_link+"'")
                            delete_collected_data(bank_name,bank,max_date)
                        else:
                            print('HDFC BANK DID NOT HAVE ALL DATA')
                    except:
                        df=data_collection_canara('',search_str,max_date,bank_type,bank,date,'ilovepdf',1,bank_name,catch_error_df)
                        delete_collected_data(bank_name,bank,max_date)
                except Exception as e:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    print('Error occured for bank------->',bank)

        def data_collection_uco(tables,search_str,max_date,bank_type,bank,date,type_read,loop,bank_name,catch_error_df):
            engine = adqvest_db.db_conn()
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                col=row_col_index_locator(df,['Funded'])[0]
                row=row_col_index_locator(df,['Funded'])[1]
                df=df.iloc[row:,:]
                df.reset_index(drop=True,inplace=True)
                df=column_values_clean(df)
                df.columns=df.iloc[row_col_index_locator(df,['Funded'])[1],:]
                df=row_modificator(df,['Funded'],col,row_del=True)
                df.reset_index(drop=True,inplace=True)
                df.columns=['Industry','Fund_Based_In_Million','Non_Fund_Based_In_Million']
                df['Industry_Code'] = df['Industry'].str.extract(r'([A-Z]\.[a-zA-Z\d]+\.\s*\d+|[A-Z]\.[a-zA-Z\d]+|[A-Z]\.\s*\d+|[A-Z]\.|[A-Z]\.\s*\d+|\w\.)')
                df=df[df['Industry']!='Amount (Rs in Millions)']
                df['Sub_Industry']=' '
                df = df[df['Industry'].str.strip() != '']
                df=df[~df['Industry'].str.contains('amount', case=False)]
                df=df.drop(df[df['Industry'].str.lower().str.contains('all industries')].index)
                df.drop(df[df['Industry'].str.contains('distribution')].index,inplace=True)
                df['Sub_Industry']=np.nan
                for row in range(df.shape[0]):
                    try:
                        if (re.match(r'^[A-Z]\.\d',df.iloc[row,0])) or (re.match(r'^[a-z]\.\d',df.iloc[row,0])):
                            df['Sub_Industry'].iloc[row]=df.iloc[row,0].partition('.')[2].strip()
                            df['Industry'].iloc[row]=np.nan
                        if (re.match(r'^[A-Z]\.\d\.\d',df.iloc[row,0])) or (re.match(r'^[a-z]\.\d\.\d',df.iloc[row,0])):
                            df['Sub_Industry'].iloc[row]=df.iloc[row,0].partition('.')[2].strip()
                            df['Industry'].iloc[row]=np.nan
                        elif (re.match(r'^[A-Z]\.',df.iloc[row,0])) or (re.match(r'^[a-z]\.',df.iloc[row,0])):
                            df['Sub_Industry'].iloc[row]=np.nan
                        else:
                            df.drop(row,inplace=True)
                    except:
                        continue
                df['Industry'] = df['Industry'].str.replace('[A-Z]\.', '')
                df['Industry'] = df['Industry'].str.replace('[a-z]\.', '')
                df['Industry'] = df['Industry'].str.replace('+', '')
                df['Industry'] = df['Industry'].str.replace('\d', '')
                df['Sub_Industry'] = df['Sub_Industry'].str.replace('\d', '')
                df['Sub_Industry'] = df['Sub_Industry'].str.replace('\.', '')
                df['Industry']=df['Industry'].fillna(method='ffill')
                df.drop(df[df['Industry']==''].index,inplace=True)
                df.drop(df[df['Industry'].str.lower().str.contains('industry')].index,inplace=True)
                df['Fund_Based_In_Million']=df['Fund_Based_In_Million'].apply(lambda x:pd.to_numeric(x))
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Runtime']=datetime.datetime.now()
                df=df[['Bank','Industry','Sub_Industry','Fund_Based_In_Million','Non_Fund_Based_In_Million',
                                                'Relevant_Date','Runtime','Industry_Code']]
                df['Bank_Type']=bank_type
                df.reset_index(drop=True,inplace=True)
                for j in df.columns:
                    if j!='Runtime':
                        df[j]=df[j].apply(lambda x:str(x))

                df=clean_table(df)
                engine = adqvest_db.db_conn()
                df['Relevant_Date']=pd.to_datetime(df['Relevant_Date']).dt.date
                df=df[['Bank','Bank_Type','Industry','Sub_Industry','Fund_Based_In_Million','Non_Fund_Based_In_Million','Relevant_Date','Runtime']]
                df=df[df['Relevant_Date']>max_date]
                df=df[df['Industry']!='']
                df.to_sql("BANK_INDUSTRY_WISE_DISTRIBUTION_EXPOSURE_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print('uploaded')
                
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='industry name'
                    end_text='infrastructure'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_uco(df,heading,bank_type,bank,date,'ilovepdf',2,bank_name,catch_error_df)
            return df
        time.sleep(5)
        bank='UCO BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        catch_error_df=pd.DataFrame(columns=['Table_name','Bank_Name','Error_message','Relevant_Date','Runtime'])
        links_capital=get_correct_links(links_capital)
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                act_link=link
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                search_str=['industry','Other Industries','Infrastructure']
                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'lattice',10,100,40)
                        if len(tables)>1:
                            df=data_collection_uco(tables,search_str,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                            print('returned df')
                            connection = engine.connect()
                            connection.execute("update BANK_BASEL_III_QUARTERLY_LINKS set Bank_Industry_Wise_Distribution_Exposure_Status='Completed' where Links = '"+act_link+"'")
                            delete_collected_data(bank_name,bank,max_date)
                        else:
                            print('HDFC BANK DID NOT HAVE ALL DATA')
                    except:
                        df=data_collection_uco('',search_str,max_date,bank_type,bank,date,'ilovepdf',1,bank_name,catch_error_df)
                        delete_collected_data(bank_name,bank,max_date)
                except Exception as e:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    print('Error occured for bank------->',bank)

        def data_collection_punjab(tables,search_str,max_date,bank_type,bank,date,type_read,loop,bank_name,catch_error_df):
            engine = adqvest_db.db_conn()
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                col=row_col_index_locator(df,['Industry Name'])[0]
                row_end_fu=row_col_index_locator(df,['Total fund based'])[1]
                df=df.iloc[:,:col+3]
                df_fund=df.iloc[:row_end_fu,:]
                df_fund.columns=['Industry Name','Fund_Based_In_Million','Fund_Based_In_Million_Consolidated']
                df_fund=row_modificator(df_fund,['Industry Name'],col,row_del=True)

                row_start_nf=row_col_index_locator(df,['Total fund based'])[1]
                row_end_nf=row_col_index_locator(df,['Total nonfund based'])[1]
                df_nf=df.iloc[row_start_nf:row_end_nf,:]
                row_nf=row_col_index_locator(df_nf,['Mining and Quarrying'])[1]
                df_nf=df_nf.iloc[row_nf-1:,:]
                col_end=row_col_index_locator(df,['Industry Name'])[0]
                df_nf=df_nf.iloc[:,:col_end+3]
                df_nf.columns=['Industry Name','Non_Fund_Based_In_Million','Non_Fund_Based_In_Million_Consolidated']
                df_nf=row_modificator(df_nf,['Industry Name'],col,row_del=True)

                df=pd.merge(df_fund, df_nf, on = "Industry Name", how = "inner")

                my_list=['A.','B.','C.','D.','E.','F.','G.','H.','I.','J.','K.','L.','T.'
                         'G.','H.','I.','J.','K.','L.','M.','N.','O.','P.','Q.','R.','S.',
                         {'A':[1,3]},{'B':[1,10]},{'C':[1,10]},{'D':[1,10]},{'E':[1,10]},
                         {'M':[1,5]},{'N':[1,10]},{'R':[1,10]}, {'F':[1,10]},{'I':[1,10]}]



                df=get_clean_rows(df,0,my_list, row_update=True,new_col='Sl_No')
                result_list1=['Mining and Quarrying','Food Processing',
                             'Beverages|Tobacco','Textiles',
                             'Leather and Leather products','Wood|Wood Products',
                             'Paper and Paper Products','Petroleum, Coal Products|Nuclear Fuels',
                             'Chemicals|Chemical Products','Other Industries',
                             'Rubber, Plastic|their Products','Glass|Glassware','Textiles',
                             'Cement|Cement Products','Basic Metal|Metal Products','All Engineering',
                             'Vehicles, Vehicle Parts|Transport Equipments','Gems|Jewellery','Construction',
                             'Infrastructure','Other Industries','Residuary Other Advances']

                df=row_filling(df,0,result_list1,new_col='Industry_Clean')
                d1=[{'Industry Name':'Sub_Industry'},
                    {'Industry_Clean':'Industry'}]
                df=column_modificator(df,d1,update_col=True)


                df['Bank']=bank
                df['Relevant_Date']=date
                df['Runtime']=datetime.datetime.now()
                df.reset_index(drop=True,inplace=True)
                df=df[['Bank','Industry','Sub_Industry','Sl_No','Fund_Based_In_Million','Non_Fund_Based_In_Million','Relevant_Date','Runtime']]
                for j in df.columns:
                    if j!='Runtime':
                        df[j]=df[j].apply(lambda x:str(x))

                df['Bank_Type']=bank_type
                df=clean_table(df)
                engine = adqvest_db.db_conn()
                df['Relevant_Date']=pd.to_datetime(df['Relevant_Date']).dt.date
                df=df[['Bank','Bank_Type','Industry','Sub_Industry','Fund_Based_In_Million','Non_Fund_Based_In_Million','Relevant_Date','Runtime']]
                df=df[df['Relevant_Date']>max_date]
                df=df[df['Industry']!='']
                df.to_sql("BANK_INDUSTRY_WISE_DISTRIBUTION_EXPOSURE_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print('uploaded')
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='industry name'
                    end_text='infrastructure'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_punjab(df,heading,bank_type,bank,date,'ilovepdf',2,bank_name,catch_error_df)
            return df
        time.sleep(5)
        bank='PUNJAB NATIONAL BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        catch_error_df=pd.DataFrame(columns=['Table_name','Bank_Name','Error_message','Relevant_Date','Runtime'])
        links_capital=get_correct_links(links_capital)
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                act_link=link
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                search_str=['Total fund based','industry']
                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'lattice',10,100,40)
                        if len(tables)>1:
                            df=data_collection_punjab(tables,search_str,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                            print('returned df')
                            connection = engine.connect()
                            connection.execute("update BANK_BASEL_III_QUARTERLY_LINKS set Bank_Industry_Wise_Distribution_Exposure_Status='Completed' where Links = '"+act_link+"'")
                            delete_collected_data(bank_name,bank,max_date)
                        else:
                            print('HDFC BANK DID NOT HAVE ALL DATA')
                    except:
                        df=data_collection_punjab('',search_str,max_date,bank_type,bank,date,'ilovepdf',1,bank_name,catch_error_df)
                        delete_collected_data(bank_name,bank,max_date)
                except Exception as e:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    print('Error occured for bank------->',bank)
        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:

        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_type)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')

