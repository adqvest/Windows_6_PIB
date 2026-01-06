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
    df=df.replace('',np.nan)
    df.dropna(axis=1,how='all',inplace=True)
    df.reset_index(drop=True,inplace=True)
    return df
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
def get_correct_links(links_capital):
    links_capital=links_capital[links_capital['Bank_Geographic_Distribution_Of_Exposure_Status']!='COMPLETED']
    columns = links_capital.columns.to_list()
    columns.remove('Runtime')
    columns.remove('Last_Updated')
    unique = links_capital.drop_duplicates(subset=columns, keep='last')
    unique.reset_index(drop=True,inplace=True)
    return unique
def update_basel_iii(bank_name,date):
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    connection.execute("update BANK_BASEL_III_QUARTERLY_LINKS set Bank_Geographic_Distribution_Of_Exposure_Status = 'COMPLETED' where Relevant_Date = '"+str(date)+"' and Bank= '"+bank_name+"'")


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
    table_name = 'BANK_GEOGRAPHIC_DISTRIBUTION_OF_EXPOSURE_QUARTERLY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        print('Running code for data collection')
        bank_name='BANK_GEOGRAPHIC_DISTRIBUTION_OF_EXPOSURE_QUARTERLY_DATA'
        catch_error_df=pd.DataFrame(columns=['Table_name','Bank_Name','Error_message','Relevant_Date','Runtime'])
        sql="Delete from BANK_ERROR_TABLE where Table_Name = '" +bank_name+"'"
        engine.execute(sql)
        def delete_collected_data(bank_name,bank,date):
            sql="Delete from BANK_ERROR_TABLE where Table_Name = '" +bank_name+"' and Bank_Name = '" +bank+"' and Relevant_Date='" +date+"'"
            engine.execute(sql)

        def data_collection_hdfc(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop,bank_name,catch_error_df):
            print('in function')
            try:
                try:
                    df=get_desired_table(tables,search_str)
                except:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                print(df)
                row_strt=row_col_index_locator(df,['domestic'])[1]
                row=row_col_index_locator(df,['total'])[1]
                col=row_col_index_locator(df,['domestic'])[0]
                df=df.iloc[row_strt:row,col:]
                df.columns = [str(i) for i in range(len(df.columns))]
                df=df[df.iloc[:,1]!='']
                df.reset_index(drop=True,inplace=True)
                df=df.iloc[:,:3]
                df.columns=['Exposure_Distribution','Fund_Based_In_Million','Non_Fund_Based_In_Million']
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Bank_Type']=bank_type
                df['Runtime']=datetime.datetime.now()
                print("final_df")
                df.to_sql("BANK_GEOGRAPHIC_DISTRIBUTION_OF_EXPOSURE_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print("uploded")
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='exposure'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_hdfc(df,heading,link,max_date,bank_type,bank,date,'ilovepdf',2,bank_name,catch_error_df)
            return df

        bank='HDFC BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['exposure distribution']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)

                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'stream',10,300,70)
                        df=data_collection_hdfc(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                    except:
                        df=data_collection_hdfc('',search_str,link,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                    update_basel_iii(bank,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        def data_collection_sbi_fed(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop,bank_name,catch_error_df):
            print('in function')
            try:
                try:
                    df=get_desired_table(tables,search_str)
                except:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                print(df)
                if bank=='STATE BANK OF INDIA':
                    row_strt=row_col_index_locator(df,['geographic distribution'])[1]
                    row=row_col_index_locator(df,['industry type'])[1]
                    col=row_col_index_locator(df,['geographic distribution'])[0]
                    df=df.iloc[row_strt+1:row,col:]
                else:
                    row_strt=row_col_index_locator(df,['overseas'])[1]
                    row=row_col_index_locator(df,['metal'])[1]
                    col=row_col_index_locator(df,['overseas'])[0]
                    df=df.iloc[row_strt:row,col:]
                df.columns = [str(i) for i in range(len(df.columns))]
                df=df[df.iloc[:,1]!='']
                df.reset_index(drop=True,inplace=True)
                df=df.iloc[:,:3]
                df.columns=['Exposure_Distribution','Fund_Based_In_Million','Non_Fund_Based_In_Million']
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Relevant_Date']=pd.to_datetime(df['Relevant_Date'], format='%Y-%m-%d')
                df['Runtime']=datetime.datetime.now()
                df=df[['Bank','Exposure_Distribution','Fund_Based_In_Million','Non_Fund_Based_In_Million','Relevant_Date','Runtime']]
                df['Bank_Type']=bank_type
                df.reset_index(inplace=True,drop=True)
                print(df)
                df=clean_table(df)
                df.to_sql("BANK_GEOGRAPHIC_DISTRIBUTION_OF_EXPOSURE_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print('Geographic DF uploaded')
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='overseas'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_sbi_fed(df,heading,link,max_date,bank_type,bank,date,'ilovepdf',2,bank_name,catch_error_df)
            return df

        bank='STATE BANK OF INDIA'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['overseas']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)

                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'lattice',10,300,70)
                        df=data_collection_sbi_fed(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                    except:
                        df=data_collection_sbi_fed('',search_str,link,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                    update_basel_iii(bank,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)

        bank='FEDERAL BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['overseas']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)

                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'lattice',10,300,70)
                        df=data_collection_sbi_fed(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                    except:
                        df=data_collection_sbi_fed('',search_str,link,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                    update_basel_iii(bank,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)

        def data_collection_iob(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop,bank_name,catch_error_df):
            print('in function')
            try:
                try:
                    df=get_desired_table(tables,search_str)
                except:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                row_strt=row_col_index_locator(df,['geographic distribution'])[1]
                row=row_col_index_locator(df,['industry type'])[1]
                col=row_col_index_locator(df,['geographic distribution'])[0]
                df=df.iloc[row_strt:row,col:]
                df.columns = [str(i) for i in range(len(df.columns))]
                df=df[df.iloc[:,1]!='']
                df.reset_index(drop=True,inplace=True)
                df.iloc[:,0]=['Fund_Based_In_Million','Non_Fund_Based_In_Million','Fund_Based_In_Million','Non_Fund_Based_In_Million']
                df = df.pivot(columns=df.columns[0], values=df.columns[1])
                df=df.ffill()
                df=df.iloc[[1,3],:]
                df['Exposure_Distribution']=['Domestic','Overseas']
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Relevant_Date']=pd.to_datetime(df['Relevant_Date'], format='%Y-%m-%d')
                df['Runtime']=datetime.datetime.now()
                df=df[['Bank','Exposure_Distribution','Fund_Based_In_Million','Non_Fund_Based_In_Million','Relevant_Date','Runtime']]
                df['Bank_Type']=bank_type
                df.reset_index(inplace=True,drop=True)
                print(df)
                df=clean_table(df)
                df.to_sql("BANK_GEOGRAPHIC_DISTRIBUTION_OF_EXPOSURE_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print('Geographic DF uploaded')
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='overseas'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_iob(df,heading,link,max_date,bank_type,bank,date,'ilovepdf',2,bank_name,catch_error_df)
            return df

        bank='INDIAN OVERSEAS BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['overseas']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)

                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'stream',10,300,70)
                        df=data_collection_iob(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                    except:
                        df=data_collection_iob('',search_str,link,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                    update_basel_iii(bank,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        def data_collection_bandhan(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop,bank_name,catch_error_df):
            print('in function')
            try:
                try:
                    df=get_desired_table(tables,search_str)
                except:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                print(df)
                row_strt=row_col_index_locator(df,['overseas'])[1]
                row=row_col_index_locator(df,['metal'])[1]
                col=row_col_index_locator(df,['overseas'])[0]
                df=df.iloc[row_strt:row,col:]
                df.columns = [str(i) for i in range(len(df.columns))]
                df=df[df.iloc[:,1]!='']
                df.reset_index(drop=True,inplace=True)
                df=df.iloc[:,:3]
                df.columns=['Exposure_Distribution','Fund_Based_In_Million','Non_Fund_Based_In_Million']
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Relevant_Date']=pd.to_datetime(df['Relevant_Date'], format='%Y-%m-%d')
                df['Runtime']=datetime.datetime.now()
                df=df[['Bank','Exposure_Distribution','Fund_Based_In_Million','Non_Fund_Based_In_Million','Relevant_Date','Runtime']]
                df['Bank_Type']=bank_type
                df.reset_index(inplace=True,drop=True)
                print(df)
                df=clean_table(df)
                df.to_sql("BANK_GEOGRAPHIC_DISTRIBUTION_OF_EXPOSURE_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print('Geographic DF uploaded')
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='overseas'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_bandhan(df,heading,link,max_date,bank_type,bank,date,'ilovepdf',2,bank_name,catch_error_df)
            return df

        bank='BANDHAN BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['day 1']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)

                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'lattice',10,300,70)
                        df=data_collection_bandhan(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                    except:
                        df=data_collection_bandhan('',search_str,link,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                    update_basel_iii(bank,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        def data_collection_axis_bom_indusind_pnb_union(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop,bank_name,catch_error_df):
            print('in function')
            try:
                try:
                    df=get_desired_table(tables,search_str)
                except:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                df=get_desired_table(tables,search_str)
                if bank=='BANK OF MAHARASHTRA':
                    row_strt=row_col_index_locator(df,['fund based'])[1]
                    col=row_col_index_locator(df,['fund based'])[0]
                    df=df.iloc[row_strt:,col:]
                else:
                    row_strt=row_col_index_locator(df,['fund based'])[1]
                    row=row_col_index_locator(df,['total'])[1]
                    col=row_col_index_locator(df,['fund based'])[0]
                    df=df.iloc[row_strt:row,col:]
                df.columns = [str(i) for i in range(len(df.columns))]
                df=df[df.iloc[:,1]!='']
                df.reset_index(drop=True,inplace=True)
                df=df.iloc[:,:3]
                df = df.T  # Transpose the DataFrame
                df.columns = df.iloc[0]  # Set the first row as column names
                df = df[1:]
                df['Exposure_Distribution']=['Domestic','Overseas']
                df.columns=['Fund_Based_In_Million','Non_Fund_Based_In_Million','Exposure_Distribution']
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Relevant_Date']=pd.to_datetime(df['Relevant_Date'], format='%Y-%m-%d')
                df['Runtime']=datetime.datetime.now()
                df=df[['Bank','Exposure_Distribution','Fund_Based_In_Million','Non_Fund_Based_In_Million','Relevant_Date','Runtime']]
                df['Bank_Type']=bank_type
                df.reset_index(inplace=True,drop=True)
                print(df)
                df=clean_table(df)
                df.to_sql("BANK_GEOGRAPHIC_DISTRIBUTION_OF_EXPOSURE_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print('Geographic DF uploaded')
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='overseas'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_axis_bom_indusind_pnb_union(df,heading,link,max_date,bank_type,bank,date,'ilovepdf',2,bank_name,catch_error_df)
            return df

        bank='UNION BANK OF INDIA'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['overseas']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)

                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'lattice',10,300,70)
                        df=data_collection_axis_bom_indusind_pnb_union(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                    except:
                        df=data_collection_axis_bom_indusind_pnb_union('',search_str,link,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                    update_basel_iii(bank,date)
                except Exception as e:
                    print(e)
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                # break

        bank='PUNJAB NATIONAL BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['overseas']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)

                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'lattice',10,300,70)
                        df=data_collection_axis_bom_indusind_pnb_union(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                    except:
                        df=data_collection_axis_bom_indusind_pnb_union('',search_str,link,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                    update_basel_iii(bank,date)
                except Exception as e:
                    print(e)
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)

        bank='INDUSIND BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['overseas']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)

                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'lattice',10,300,70)
                        df=data_collection_axis_bom_indusind_pnb_union(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                    except:
                        df=data_collection_axis_bom_indusind_pnb_union('',search_str,link,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                    update_basel_iii(bank,date)
                except Exception as e:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)

        bank='BANK OF MAHARASHTRA'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['domestic']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)

                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'lattice',10,300,70)
                        df=data_collection_axis_bom_indusind_pnb_union(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                    except:
                        df=data_collection_axis_bom_indusind_pnb_union('',search_str,link,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                    update_basel_iii(bank,date)
                except Exception as e:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)

        bank='AXIS BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['overseas']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)

                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'lattice',10,300,70)
                        df=data_collection_axis_bom_indusind_pnb_union(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                    except:
                        df=data_collection_axis_bom_indusind_pnb_union('',search_str,link,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                    update_basel_iii(bank,date)
                except Exception as e:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)


        def data_collection_baroda_icici_idbi_idfc_kotak(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop,bank_name,catch_error_df):
            print('in function')
            try:
                try:
                    df=get_desired_table(tables,search_str)
                except:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                row_strt=row_col_index_locator(df,['domestic'])[1]
                row=row_col_index_locator(df,['total'])[1]
                col=row_col_index_locator(df,['Domestic'])[0]
                df=df.iloc[row_strt:row,col:]
                df.reset_index(drop=True,inplace=True)
                df=df.loc[:, (df!= '').any(axis=0)]
                df=df.loc[:, (df!= '#').any(axis=0)]
                df=df.iloc[:,:3]
                df.columns=['Exposure_Distribution','Fund_Based_In_Million','Non_Fund_Based_In_Million']
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Relevant_Date']=pd.to_datetime(df['Relevant_Date'], format='%Y-%m-%d')
                df['Runtime']=datetime.datetime.now()
                df=df[['Bank','Exposure_Distribution','Fund_Based_In_Million','Non_Fund_Based_In_Million','Relevant_Date','Runtime']]
                df['Bank_Type']=bank_type
                df.reset_index(inplace=True,drop=True)
                print(df)
                df=clean_table(df)
                df.to_sql("BANK_GEOGRAPHIC_DISTRIBUTION_OF_EXPOSURE_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print('Geographic DF uploaded')
            except Exception as e:
                    if loop < 2:
                        catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                        catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                        path,sheets=read_data_using_ilovepdf(link,bank)
                        heading='overseas'
                        end_text='total'
                        df=search(heading,end_text,path,sheets)
                        df=data_collection_baroda_icici_idbi_idfc_kotak(df,heading,link,max_date,bank_type,bank,date,'ilovepdf',2,bank_name,catch_error_df)
            return df

        bank='KOTAK MAHINDRA BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['overseas']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)

                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'lattice',10,300,70)
                        df=data_collection_baroda_icici_idbi_idfc_kotak(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                    except:
                        df=data_collection_baroda_icici_idbi_idfc_kotak('',search_str,link,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                    update_basel_iii(bank,date)
                except Exception as e:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)

        bank='IDFC FIRST BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['overseas']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)

                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'lattice',10,300,70)
                        df=data_collection_baroda_icici_idbi_idfc_kotak(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                    except:
                        df=data_collection_baroda_icici_idbi_idfc_kotak('',search_str,link,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                    update_basel_iii(bank,date)
                except Exception as e:
                    print(e)
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)

        bank='IDBI BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['overseas']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)

                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'lattice',10,300,70)
                        df=data_collection_baroda_icici_idbi_idfc_kotak(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                    except:
                        df=data_collection_baroda_icici_idbi_idfc_kotak('',search_str,link,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                    update_basel_iii(bank,date)
                except Exception as e:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)

        bank='ICICI BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['overseas']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)

                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'lattice',10,300,70)
                        df=data_collection_baroda_icici_idbi_idfc_kotak(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                    except:
                        df=data_collection_baroda_icici_idbi_idfc_kotak('',search_str,link,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                    update_basel_iii(bank,date)
                except Exception as e:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)

        bank='BANK OF BARODA'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['overseas']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)

                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'stream',10,300,70)
                        df=data_collection_baroda_icici_idbi_idfc_kotak(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                    except:
                        df=data_collection_baroda_icici_idbi_idfc_kotak('',search_str,link,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                    update_basel_iii(bank,date)
                except Exception as e:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)

        def data_collection_canara(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop,bank_name,catch_error_df):
            print('in function')
            try:
                try:
                    df=get_desired_table(tables,search_str)
                except:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                row_strt=row_col_index_locator(df,['domestic operations'])[1]
                row=row_col_index_locator(df,['overseas operations'])[1]
                col=row_col_index_locator(df,['domestic operations'])[0]
                df=df.iloc[row_strt:row+1,col:]
                df.reset_index(drop=True,inplace=True)
                df=df.loc[:, (df!= '').any(axis=0)]
                df=df.iloc[:,:3]
                df.columns=['Exposure_Distribution','Fund_Based_In_Million','Non_Fund_Based_In_Million']
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Relevant_Date']=pd.to_datetime(df['Relevant_Date'], format='%Y-%m-%d')
                df['Runtime']=datetime.datetime.now()
                df=df[['Bank','Exposure_Distribution','Fund_Based_In_Million','Non_Fund_Based_In_Million','Relevant_Date','Runtime']]
                df['Bank_Type']=bank_type
                df.reset_index(inplace=True,drop=True)
                print(df)
                df=clean_table(df)
                df.to_sql("BANK_GEOGRAPHIC_DISTRIBUTION_OF_EXPOSURE_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print('Geographic DF uploaded')
            except Exception as e:
                    if loop < 2:
                        catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                        catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                        path,sheets=read_data_using_ilovepdf(link,bank)
                        heading='overseas'
                        end_text='total'
                        df=search(heading,end_text,path,sheets)
                        df=data_collection_canara(df,heading,link,max_date,bank_type,bank,date,'ilovepdf',2,bank_name,catch_error_df)
            return df

        bank='CANARA BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['overseas']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)

                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'lattice',10,300,70)
                        df=data_collection_canara(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                    except:
                        df=data_collection_canara('',search_str,link,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                    update_basel_iii(bank,date)
                except Exception as e:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)

        def data_collection_central(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop,bank_name,catch_error_df):
            print('in function')
            try:
                try:
                    df=get_desired_table(tables,search_str)
                except:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                row_strt=row_col_index_locator(df,['overseas'])[1]
                df=df.loc[[row_strt]]
                values=re.findall(r'\d+',geographic_df.iloc[0,1])
                df.columns=['Exposure_Distribution','Fund_Based_In_Million']
                df=df.append({'Exposure_Distribution':'Domestic','Fund_Based_In_Million':values[1]},ignore_index=True)
                df=df.append({'Exposure_Distribution':'Overseas','Fund_Based_In_Million':values[0]},ignore_index=True)
                df=df.iloc[1:]
                df['Non_Fund_Based_In_Million']=' '
                df.iloc[0,0]='Domestic'
                df.iloc[1,0]='Overseas'
                df.reset_index(drop=True,inplace=True)
                df['Runtime']=datetime.datetime.now()
                df['Bank']=bank
                df['Relevant_Date']=date
                df=df[['Bank','Exposure_Distribution','Fund_Based_In_Million','Non_Fund_Based_In_Million','Relevant_Date','Runtime']]
                df['Fund_Based_In_Million']=round(pd.to_numeric(df['Fund_Based_In_Million'])*10)
                df['Bank_Type']=bank_type
                df.reset_index(drop=True,inplace=True)
                df=clean_table(df)
                df.to_sql("BANK_GEOGRAPHIC_DISTRIBUTION_OF_EXPOSURE_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                #print(geographic_df)
                print('uploaded')
            except Exception as e:
                    if loop < 2:
                        catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                        catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                        path,sheets=read_data_using_ilovepdf(link,bank)
                        heading='overseas'
                        end_text='total'
                        df=search(heading,end_text,path,sheets)
                        df=data_collection_central(df,heading,link,max_date,bank_type,bank,date,'ilovepdf',2,bank_name,catch_error_df)
            return df

        bank='CENTRAL BANK OF INDIA'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['overseas']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)

                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'lattice',10,300,70)
                        df=data_collection_central(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                    except:
                        df=data_collection_central('',search_str,link,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                    update_basel_iii(bank,date)
                except Exception as e:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)

        def data_collection_rbl(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop,bank_name,catch_error_df):
            print('in function')
            try:
                try:
                    df=get_desired_table(tables,search_str)
                except:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                print(df)
                row_strt=row_col_index_locator(df,['domestic'])[1]
                row=row_col_index_locator(df,['industry name'])[1]
                col=row_col_index_locator(df,['domestic'])[0]
                df=df.iloc[row_strt:row,col-1:]
                print(df)
                df.columns = [str(i) for i in range(len(df.columns))]
                df=df[df.iloc[:,1]!='']
                df.reset_index(drop=True,inplace=True)
                df=df.iloc[:,:3]
                df = df.T  # Transpose the DataFrame
                df.columns = df.iloc[0]  # Set the first row as column names
                df = df[1:]
                print(df)
                try:
                    df.drop(columns=['Total'],inplace=True)
                except:
                    pass
                df.columns=['Exposure_Distribution','Fund_Based_In_Million','Non_Fund_Based_In_Million']
                df['Bank']=bank
                df['Relevant_Date']=date
                print(df)
                df['Relevant_Date']=pd.to_datetime(df['Relevant_Date'], format='%Y-%m-%d')
                df['Runtime']=datetime.datetime.now()
                df['Bank_Type']=bank_type
                df.reset_index(drop=True,inplace=True)
                df=clean_table(df)
                df.to_sql("BANK_GEOGRAPHIC_DISTRIBUTION_OF_EXPOSURE_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                #print(geographic_df)
                print('uploaded')
            except Exception as e:
                print(e)
                if loop < 2:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='overseas'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_rbl(df,heading,link,max_date,bank_type,bank,date,'ilovepdf',2,bank_name,catch_error_df)
            return df

        bank='RBL BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['fund based']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)

                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'lattice',10,300,70)
                        df=data_collection_rbl(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                    except:
                        df=data_collection_rbl('',search_str,link,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                    update_basel_iii(bank,date)
                except Exception as e:
                    print(e)
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)

        def data_collection_city(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop,bank_name,catch_error_df):
            print('in function')
            try:
                try:
                    df=get_desired_table(tables,search_str)
                except:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                row_strt=row_col_index_locator(df,['fund based'])[1]
                row=row_col_index_locator(df,['total'])[1]
                col=row_col_index_locator(df,['fund based'])[0]
                df=df.iloc[row_strt:row,col:]
                df.columns = [str(i) for i in range(len(df.columns))]
                df=df[df.iloc[:,1]!='']
                df.reset_index(drop=True,inplace=True)
                df=df.iloc[:,:3]
                df = df.T  # Transpose the DataFrame
                df.columns = df.iloc[0]  # Set the first row as column names
                df = df[1:]
                df=df.iloc[:,:2]
                print(df)
                df['Exposure_Distribution']=['Domestic','Overseas']
                df.columns=['Fund_Based_In_Million','Non_Fund_Based_In_Million','Exposure_Distribution']
                df['Bank']=bank
                df['Bank_Type']=bank_type
                df['Relevant_Date']=date
                df['Relevant_Date']=pd.to_datetime(df['Relevant_Date'], format='%Y-%m-%d')
                df['Runtime']=datetime.datetime.now()
                df.to_sql("BANK_GEOGRAPHIC_DISTRIBUTION_OF_EXPOSURE_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                #print(geographic_df)
                print('uploaded')
            except Exception as e:
                    if loop < 2:
                        catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                        catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                        path,sheets=read_data_using_ilovepdf(link,bank)
                        heading='overseas'
                        end_text='total'
                        df=search(heading,end_text,path,sheets)
                        df=data_collection_city(df,heading,link,max_date,bank_type,bank,date,'ilovepdf',2,bank_name,catch_error_df)
            return df

        bank='CITY UNION BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['overseas']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)

                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'lattice',10,300,70)
                        df=data_collection_city(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                    except:
                        df=data_collection_city('',search_str,link,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                    update_basel_iii(bank,date)
                except Exception as e:
                    print(e)
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_type)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
