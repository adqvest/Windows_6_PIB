
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
    links_capital=links_capital[links_capital['Bank_Residual_Contractual_Maturity_Breakdown']!='COMPLETED']
    columns = links_capital.columns.to_list()
    columns.remove('Runtime')
    columns.remove('Last_Updated')
    unique = links_capital.drop_duplicates(subset=columns, keep='last')
    unique.reset_index(drop=True,inplace=True)
    return unique

def update_basel_iii(bank_name,date):
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    connection.execute("update BANK_BASEL_III_QUARTERLY_LINKS set Bank_Residual_Contractual_Maturity_Breakdown = 'COMPLETED' where Relevant_Date = '"+str(date)+"' and Bank= '"+bank_name+"'")

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
    table_name = 'BANK_RESIDUAL_CONTRACTUAL_MATURITY_BREAKDOWN_QUARTERLY_DATA'
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
        bank_name='BANK_RESIDUAL_CONTRACTUAL_MATURITY_BREAKDOWN_QUARTERLY_DATA'
        catch_error_df=pd.DataFrame(columns=['Table_name','Bank_Name','Error_message','Relevant_Date','Runtime'])
        sql="Delete from BANK_ERROR_TABLE where Table_Name = '" +bank_name+"'"
        engine.execute(sql)
        def delete_collected_data(bank_name,bank,date):
            sql="Delete from BANK_ERROR_TABLE where Table_Name = '" +bank_name+"' and Bank_Name = '" +bank+"' and Relevant_Date='" +date+"'"
            engine.execute(sql)

        def data_collection_iob(tables,search_str,max_date,bank_type,bank,date,type_read,loop,bank_name,catch_error_df):
            try:
                print('In try block')
                engine = adqvest_db.db_conn()
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                row_st=row_col_index_locator(df,['day 1'])[1]
                row_ed=row_col_index_locator(df,['> 5 years'])[1]
                df=df.iloc[row_st:row_ed+1]
                df=clean_dataframe(df)
                df.reset_index(drop=True,inplace=True)
                col=row_col_index_locator(df,['day 1'])[0]
                values_clean=[
                            {'day 1':'1 Day'},
                            {'2 days':'2 to 7 Days'},
                            {'8 days':'8 to 14 Days'},
                            {'15 days':'15 to 30 Days'},
                            {'31 days':'31 Days & upto 2 Months'},
                            {'2 months –':'Over 2 Months & upto 3 Months'},
                            {'3 months –':'Over 3 Months & upto 6 Months'},
                            {'>6 months':'Over 6 Months & upto 1 Year'},
                            {'>1 year':'Over 1 Year & upto 3 Years'},
                            {'>3 years':'Over 3 Years & upto 5 Years'},
                            {'> 5 years':'Over 5 Years'}
                        ]
                df=row_modificator(df,values_clean,col,row_del=False,keep_row=True,update_row=True)
                print(df)
                df.reset_index(drop=True,inplace=True)
                df.columns=['Maturity_Buckets','Total_Amount_In_Mn']
                df['Total_Amount_In_Mn']=round(pd.to_numeric(df['Total_Amount_In_Mn'])*10,2)
                df['Cash_Balances_With_Rbi_And_Other_Banks']=' '
                df['Investments']=' '
                df['Advances']=' '
                df['Fixed_Assets']=' '
                df['Cash_And_Balances_With_Rbi']=' '
                df['Balances_With_Banks_And_Money_At_Call_And_Short_Notice']=' '
                df['Other_Assets']=' '
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Runtime']=datetime.datetime.now()
                df=clean_table(df)
                df['Relevant_Date']=pd.to_datetime(df['Relevant_Date']).dt.date
                df=df[df['Relevant_Date']>max_date]
                print(df)
                engine = adqvest_db.db_conn()
                df.to_sql("BANK_RESIDUAL_CONTRACTUAL_MATURITY_BREAKDOWN_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print('uploaded')
                    
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    print('In Except block')
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    print('End except block')
                    print(path, sheet)
                    heading='day 1'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_iob(df,heading,max_date,bank_type,bank,date,'ilovepdf',2,bank_name,catch_error_df)
            return df 

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
                search_str=['day 1']
                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    # try:
                    #     tables = extract_tables_from_pdf(link,False,'lattice',10,100,40)
                    #     if len(tables)>1:
                    #         df=data_collection_iob(tables,search_str,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                    #         print('returned df')
                    #         connection = engine.connect()
                    #         connection.execute("update BANK_BASEL_III_QUARTERLY_LINKS set Bank_Residual_Contractual_Maturity_Breakdown='Completed' where Relevant_Date = '"+str(date)+"' and Bank= '"+bank+"'")
                    #         delete_collected_data(bank_name,bank,max_date)
                    #     else:
                    #         print('HDFC BANK DID NOT HAVE ALL DATA')
                    # except:
                    df=data_collection_iob('',search_str,max_date,bank_type,bank,date,'ilovepdf',1,bank_name,catch_error_df)
                except Exception as e:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    print('Error occured for bank------->',bank)


        def data_collection_canara(tables,search_str,max_date,bank_type,bank,date,type_read,loop,bank_name,catch_error_df):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                    df.reset_index(drop=True,inplace=True)
                else:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
            
                print(df)
                row_st=row_col_index_locator(df,['day1'])[1]
                row_ed=row_col_index_locator(df,['over 5 years'])[1]
                df=df.iloc[row_st:row_ed+1]
                df=clean_dataframe(df)
                df.reset_index(drop=True,inplace=True)
                col=row_col_index_locator(df,['1 day'])[0]
                values_clean=[
                            {'day1':'1 Day'},
                            {'2 to 7 day':'2 to 7 Days'},
                            {'8 to 14 day':'8 to 14 Days'},
                            {'15 to 30 day':'15 to 30 Days'},
                            {'31 days':'31 Days & upto 2 Months'},
                            {'upto 3 months':'Over 2 Months & upto 3 Months'},
                            {'over 3 months':'Over 3 Months & upto 6 Months'},
                            {'over 6 months':'Over 6 Months & upto 1 Year'},
                            {'over 1 year':'Over 1 Year & upto 3 Years'},
                            {'over 3 year':'Over 3 Years & upto 5 Years'},
                            {'over 5 year':'Over 5 Years'}
                        ]
                df=row_modificator(df,values_clean,col,row_del=False,keep_row=True,update_row=True)
                df.columns=['Maturity_Buckets','Advances','Investments','Fixed_Assets']
                df['Cash_And_Balances_With_Rbi']=' '
                df['Balances_With_Banks_And_Money_At_Call_And_Short_Notice']=' '
                df['Other_Assets']=' '
                df['Cash_Balances_With_Rbi_And_Other_Banks']=' '
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Runtime']=datetime.datetime.now()
                df['Advances'] = df['Advances'].str.replace(r"\(.*\)","")
                df['Investments'] = df['Investments'].str.replace(r"\(.*\)","")
                df['Fixed_Assets'] = df['Fixed_Assets'].str.replace(r"\(.*\)","")
                for col in ['Advances','Investments','Fixed_Assets']:
                    df[col]=round(pd.to_numeric(df[col]),2)

                df['Total_Amount_In_Mn']=df['Investments']+df['Advances']+df['Fixed_Assets']

                df=df[['Bank','Maturity_Buckets','Cash_And_Balances_With_Rbi','Balances_With_Banks_And_Money_At_Call_And_Short_Notice','Cash_Balances_With_Rbi_And_Other_Banks','Investments','Advances',
                                    'Fixed_Assets','Other_Assets','Total_Amount_In_Mn','Relevant_Date','Runtime']]

                df['Bank_Type']=bank_type
                df=clean_table(df)
                df['Relevant_Date']=pd.to_datetime(df['Relevant_Date']).dt.date
                df=df[df['Relevant_Date']>max_date]
                print(df)
                engine = adqvest_db.db_conn()
                df.to_sql("BANK_RESIDUAL_CONTRACTUAL_MATURITY_BREAKDOWN_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print('uploaded')
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='1 day'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_canara(df,heading,max_date,bank_type,bank,date,'ilovepdf',2,bank_name,catch_error_df)
            return df

        bank='CANARA BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        catch_error_df=pd.DataFrame(columns=['Table_name','Bank_Name','Error_message','Relevant_Date','Runtime'])
        links_capital=get_correct_links(links_capital)
        if links_capital.empty:
            print('No new data for CANARA BANK')
        else:
            print('New data for CANARA BANK')
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                act_link=link
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                search_str=['1 day']
                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'lattice',20,100,40)
                        if len(tables)>1:
                            df=data_collection_canara(tables,search_str,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                            print('returned df')
                            update_basel_iii(bank,date)
                            delete_collected_data(bank_name,bank,max_date)
                        else:
                            print('CANARA BANK DID NOT HAVE ALL DATA')
                    except:
                        df=data_collection_canara('',search_str,max_date,bank_type,bank,date,'ilovepdf',1,bank_name,catch_error_df)
                except Exception as e:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    print('Error occured for bank------->',bank)


        def data_collection_baroda(tables,search_str,max_date,bank_type,bank,date,type_read,loop,bank_name,catch_error_df):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                row_st=row_col_index_locator(df,['time bucket'])[1]
                row_ed=row_col_index_locator(df,['over 5 y'])[1]
                df=df.iloc[row_st:row_ed+1]
                df=clean_dataframe(df)
                df.reset_index(drop=True,inplace=True)
                col=row_col_index_locator(df,['1 d'])[0]
                df=df.replace('#','')
                df=column_values_clean(df)
                values_clean=[
                            {'1 d':'1 Day'},
                            {'27 d':'2 to 7 Days'},
                            {'814 d':'8 to 14 Days'},
                            {'1530 d':'15 to 30 Days'},
                            {'31 d':'31 Days & upto 2 Months'},
                            {'23 m':'Over 2 Months & upto 3 Months'},
                            {'6 m':'Over 3 Months & upto 6 Months'},
                            {'12 m':'Over 6 Months & upto 1 Year'},
                            {'3 y':'Over 1 Year & upto 3 Years'},
                            {'5 y':'Over 3 Years & upto 5 Years'},
                            {'over 5 y':'Over 5 Years'}
                        ]
                df=row_modificator(df,values_clean,col,row_del=False,keep_row=True,update_row=True)
                df.reset_index(drop=True,inplace=True)
                df.iloc[4][0]='31 Days & upto 2 Months'
                df.iloc[7][0]='Over 6 Months & upto 1 Year'
                df.iloc[9][0]='Over 3 Years & upto 5 Years'
                df.iloc[10][0]='Over 5 Years'
                df=df.replace('#',np.nan)
                df['Cash_Balances_With_Rbi_And_Other_Banks']=np.nan

                df.columns=['Maturity_Buckets','Cash_And_Balances_With_Rbi','Balances_With_Banks_And_Money_At_Call_And_Short_Notice',
                                                        'Advances','Investments','Fixed_Assets','Other_Assets','Total_Amount_In_Mn','Cash_Balances_With_Rbi_And_Other_Banks']
                for i in range(len(df)):
                    for j in range(1,len(df.columns)):
                        df.iloc[i,j]=pd.to_numeric(df.iloc[i,j])*10
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Runtime']=datetime.datetime.now()
                df=df[['Bank','Maturity_Buckets','Cash_And_Balances_With_Rbi','Balances_With_Banks_And_Money_At_Call_And_Short_Notice',
                                            'Cash_Balances_With_Rbi_And_Other_Banks','Investments','Advances',
                                        'Fixed_Assets','Other_Assets','Total_Amount_In_Mn','Relevant_Date','Runtime']]
                df['Bank_Type']=bank_type
                df.reset_index(drop=True,inplace=True)
                for j in df.columns:
                    if j!='Runtime':
                        df[j]=df[j].apply(lambda x:str(x))


                df=clean_table(df)
                df['Relevant_Date']=pd.to_datetime(df['Relevant_Date']).dt.date
                df=df[df['Relevant_Date']>max_date]
                print(df)
                engine = adqvest_db.db_conn()
                df.to_sql("BANK_RESIDUAL_CONTRACTUAL_MATURITY_BREAKDOWN_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print('uploaded') 
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='time bucket'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_baroda(df,heading,max_date,bank_type,bank,date,'ilovepdf',2,bank_name,catch_error_df)
            return df 


        bank='BANK OF BARODA'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        catch_error_df=pd.DataFrame(columns=['Table_name','Bank_Name','Error_message','Relevant_Date','Runtime'])
        links_capital=get_correct_links(links_capital)
        if links_capital.empty:
            print('No new links for BANK OF BARODA')
        else:
            print('New Data for BANK OF BARODA')
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                act_link=link
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                search_str=['time bucket']
                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'lattice',10,100,40)
                        if len(tables)>1:
                            df=data_collection_baroda(tables,search_str,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                            print('returned df')
                            connection = engine.connect()
                            # connection.execute("update BANK_BASEL_III_QUARTERLY_LINKS set Bank_Residual_Contractual_Maturity_Breakdown='Completed' where Relevant_Date = '"+str(date)+"' and Bank= '"+bank+"'")
                            update_basel_iii(bank,date)
                            delete_collected_data(bank_name,bank,max_date)
                        else:
                            print('BANK OF BARODA DID NOT HAVE ALL DATA')
                    except:
                        df=data_collection_baroda('',search_str,max_date,bank_type,bank,date,'ilovepdf',1,bank_name,catch_error_df)
                        delete_collected_data(bank_name,bank,max_date)
                except Exception as e:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    print(e)


        def data_collection_pnb(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                    # for i in range(tables.n):
                    #     try:
                    #         if tables[i].df.iloc[0,0].lower()=='maturity pattern':
                    #             #print(i)
                    #             df=tables[i].df.copy()
                    #             #print(maturity_df)
                    #             if df.shape[0]<23:
                    #                 df=df.append(tables[i+1].df.copy())
                    #             break
                    #     except:
                    #         continue
                else:
                    df=tables

                df[0]=df[0].replace('',np.nan)
                df.dropna(inplace=True)
                df=df[1:-1]
                df = df[df.iloc[1,:].str.lower().str.contains('advance',na=False).index[0]+1:].reset_index(drop=True)
                df.reset_index(drop=True,inplace=True)
                df.iloc[0,0]='1 Day'
                df.iloc[1,0]='2 to 7 Days'
                df.iloc[2,0]='8 to 14 Days'
                df.iloc[3,0]='15 to 30 Days'
                df.columns=['Maturity_Buckets','Advances','Investments','Other_Assets']
                df['Cash_And_Balances_With_Rbi']=np.nan
                df['Balances_With_Banks_And_Money_At_Call_And_Short_Notice']=np.nan
                df['Fixed_Assets']=np.nan
                df['Cash_Balances_With_Rbi_And_Other_Banks']=np.nan
                df['Bank']='PUNJAB NATIONAL BANK'
                df['Relevant_Date']=date
                df['Runtime']=datetime.datetime.now()
                print(df)

                for col in ['Advances','Investments','Other_Assets']:
                    df[col]=round(pd.to_numeric(df[col]),2)
                print(df.head())
                df['Total_Amount_In_Mn']=df['Investments']+df['Advances']+df['Other_Assets']
                df=df[['Bank','Maturity_Buckets','Cash_And_Balances_With_Rbi','Balances_With_Banks_And_Money_At_Call_And_Short_Notice','Cash_Balances_With_Rbi_And_Other_Banks','Investments','Advances',
                                    'Fixed_Assets','Other_Assets','Total_Amount_In_Mn','Relevant_Date','Runtime']]
                df['Bank_Type']=bank_type
                print(df[['Maturity_Buckets', 'Investments','Advances']])
                df=clean_table(df)
                df.to_sql("BANK_RESIDUAL_CONTRACTUAL_MATURITY_BREAKDOWN_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                #print(maturity_df.shape[0])
                print('uploaded')
            except:
                if loop <2:
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='maturity pattern'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_pnb(df,heading,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return df


        bank='PUNJAB NATIONAL BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['maturity pattern']
        if links_capital.empty:
            print('No new data for PNB')
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
                        df=data_collection_pnb(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_pnb('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
        print('Done with PNB')

        def data_collection_au(tables,search_str,max_date,bank_type,bank,date,type_read,loop,bank_name,catch_error_df):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                row=row_col_index_locator(df,['maturity bucket'])[1]
                col=row_col_index_locator(df,['maturity bucket'])[0]
                df=df.iloc[row:-1,col:]
                df=df[df[col]!='Total']
                col_end=row_col_index_locator(df,['total'])[0]
                df=df.iloc[:,:col_end]
                df=clean_dataframe(df)
                col=row_col_index_locator(df,['maturity bucket'])[0]
                values_clean=[
                    {'day 1':'1 Day'},
                    {'27 days':'2 to 7 Days'},
                    {'814 days':'8 to 14 Days'},
                    {'1530 days':'15 to 30 Days'},
                    {'31 to 2 months':'31 Days & upto 2 Months'},
                    {'more than 2':'Over 2 Months & upto 3 Months'},
                    {'over 3 months':'Over 3 Months & upto 6 Months'},
                    {'over 6 months':'Over 6 Months & upto 1 Year'},
                    {'over 1 year':'Over 1 Year & upto 3 Years'},
                    {'over 3 years':'Over 3 Years & upto 5 Years'},
                    {'over 5 years':'Over 5 Years'}
                ]
                df=row_modificator(df,values_clean,col,row_del=False,keep_row=True,update_row=True)
                df.reset_index(drop=True,inplace=True)


                df.columns=['Maturity_Buckets','Cash_And_Balances_With_Rbi','Balances_With_Banks_And_Money_At_Call_And_Short_Notice','Investments','Advances',
                                                        'Fixed_Assets','Other_Assets']
                for col in ['Cash_And_Balances_With_Rbi','Balances_With_Banks_And_Money_At_Call_And_Short_Notice','Investments','Advances','Fixed_Assets','Other_Assets']:
                    df[col]=round(pd.to_numeric(df[col],errors='coerce')*0.1,2)

                df=df.fillna(0)
                df['Total_Amount_In_Mn']=df['Cash_And_Balances_With_Rbi']+df['Balances_With_Banks_And_Money_At_Call_And_Short_Notice']+df['Investments']+df['Advances']                                            +df['Fixed_Assets']+df['Other_Assets']
                df['Cash_Balances_With_Rbi_And_Other_Banks']=df['Cash_And_Balances_With_Rbi']+df['Balances_With_Banks_And_Money_At_Call_And_Short_Notice']
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Runtime']=datetime.datetime.now()
                df=df[['Bank','Maturity_Buckets','Cash_And_Balances_With_Rbi','Balances_With_Banks_And_Money_At_Call_And_Short_Notice','Cash_Balances_With_Rbi_And_Other_Banks','Investments','Advances',
                                                            'Fixed_Assets','Other_Assets','Total_Amount_In_Mn','Relevant_Date','Runtime']]
                df['Bank_Type']=bank_type
                df=clean_table(df)
                df['Relevant_Date']=pd.to_datetime(df['Relevant_Date']).dt.date
                df=df[df['Relevant_Date']>max_date]
                print(df)
                engine = adqvest_db.db_conn()
                df.to_sql("BANK_RESIDUAL_CONTRACTUAL_MATURITY_BREAKDOWN_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print('uploaded')
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='day 1'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_au(tables,heading,max_date,bank_type,bank,date,'ilovepdf',2,bank_name,catch_error_df)
            return df 


        bank='AU SMALL FINANCE BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        catch_error_df=pd.DataFrame(columns=['Table_name','Bank_Name','Error_message','Relevant_Date','Runtime'])
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)
                search_str=['securitised portfolio']
                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'lattice',20,200,40)
                        if len(tables)>1:
                            df=data_collection_au(tables,search_str,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                            connection = engine.connect()
                            # connection.execute("update BANK_BASEL_III_QUARTERLY_LINKS set Bank_Residual_Contractual_Maturity_Breakdown='Completed' where Links = '"+link+"'")
                            delete_collected_data(bank_name,bank,max_date)
                        else:
                            print('AU BANK DID NOT HAVE ALL DATA')
                    except:
                        df=data_collection_au('',search_str,max_date,bank_type,bank,date,'ilovepdf',1,bank_name,catch_error_df)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    print('Error occured for bank------->',bank)

        def data_collection_central(tables,search_str,max_date,bank_type,bank,date,type_read,loop,bank_name,catch_error_df):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                    df.reset_index(drop=True,inplace=True)

                row_st=row_col_index_locator(df,['day 1'])[1]
                row_ed=row_col_index_locator(df,['over 5 year'])[1]
                df=df.iloc[row_st:row_ed+1]
                df=clean_dataframe(df)
                print(df)
                col=row_col_index_locator(df,['day 1'])[0]
                values_clean=[
                            {'day 1':'1 Day'},
                            {'02 days':'2 to 7 Days'},
                            {'08 days':'8 to 14 Days'},
                            {'15 days':'15 to 30 Days'},
                            {'31':'31 Days & upto 2 Months'},
                            {'above 2 months':'Over 2 Months & upto 3 Months'},
                            {'above 3 months':'Over 3 Months & upto 6 Months'},
                            {'above 6 months':'Over 6 Months & upto 1 Year'},
                            {'above 1 year':'Over 1 Year & upto 3 Years'},
                            {'above 3 years':'Over 3 Years & upto 5 Years'},
                            {'over 5 years':'Over 5 Years'}
                        ]
                df=row_modificator(df,values_clean,col,row_del=False,keep_row=True,update_row=True)
                df.reset_index(drop=True,inplace=True)
                df.columns=['Maturity_Buckets','Total_Amount_In_Mn']
                df['Total_Amount_In_Mn']=round(pd.to_numeric(df['Total_Amount_In_Mn'])*10,2)
                df['Cash_Balances_With_Rbi_And_Other_Banks']=' '
                df['Investments']=' '
                df['Advances']=' '
                df['Fixed_Assets']=' '
                df['Cash_And_Balances_With_Rbi']=' '
                df['Balances_With_Banks_And_Money_At_Call_And_Short_Notice']=' '
                df['Other_Assets']=' '
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Runtime']=datetime.datetime.now()
                df=df[['Bank','Maturity_Buckets','Cash_And_Balances_With_Rbi','Balances_With_Banks_And_Money_At_Call_And_Short_Notice','Cash_Balances_With_Rbi_And_Other_Banks','Investments','Advances','Fixed_Assets','Other_Assets','Total_Amount_In_Mn','Relevant_Date','Runtime']]
                df['Bank_Type']=bank_type
                df=clean_table(df)
                df=df[df['Relevant_Date']>max_date]
                engine = adqvest_db.db_conn()
                df.to_sql("BANK_RESIDUAL_CONTRACTUAL_MATURITY_BREAKDOWN_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print(df)
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='day 1'
                    end_text='above 6 months'
                    print('path------>',path)
                    df=search(heading,end_text,path,sheets)
                    print(df)
                    df=data_collection_central(df,heading,max_date,bank_type,bank,date,'ilovepdf',2,bank_name,catch_error_df)
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
                search_str=['equity risk']
                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'lattice',10,200,40)
                        if len(tables)>1:
                            df=data_collection_central(tables,search_str,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                            print('returned df')
                            connection = engine.connect()
                            connection.execute("update BANK_BASEL_III_QUARTERLY_LINKS set Bank_Residual_Contractual_Maturity_Breakdown='Completed' where Relevant_Date = '"+str(date)+"' and Bank= '"+bank+"'")
                            delete_collected_data(bank_name,bank,max_date)
                        else:
                            print('CENTRAL BANK DID NOT HAVE ALL DATA')
                    except:
                        df=data_collection_central('',search_str,max_date,bank_type,bank,date,'ilovepdf',1,bank_name,catch_error_df)
                except Exception as e:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    print('Error occured for bank------->',bank)


        def data_collection_union(tables,search_str,max_date,bank_type,bank,date,type_read,loop,bank_name,catch_error_df):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,['next day','5yr'])
                else:
                    df=tables
                    df.reset_index(drop=True,inplace=True)

                col1=row_col_index_locator(df,['maturity pattern'])[0]
                col2=row_col_index_locator(df,['advances'])[0]
                col3=row_col_index_locator(df,['investments'])[0]
                row=row_col_index_locator(df,['next day'])[1]
                col=row_col_index_locator(df,['next day'])[0]
                df=df.iloc[row:,[col1,col2,col3]]
                df.iloc[0,col]='1 Day'
                df.iloc[1,col]='2 to 7 Days'
                df.iloc[2,col]='8 to 14 Days'
                df.iloc[3,col]='15 to 30 Days'
                df.iloc[4,col]='31 Days & upto 2 Months'
                df.iloc[5,col]='Over 2 Months & upto 3 Months'
                df.iloc[6,col]='Over 3 Months & upto 6 Months'
                df.iloc[7,col]='Over 6 Months & upto 1 Year'
                df.iloc[8,col]='Over 1 Year & upto 3 Years'
                df.iloc[9,col]='Over 3 Years & upto 5 Years'
                df.iloc[10,col]='Over 5 Years'
                df.columns=['Maturity_Buckets','Advances','Investments']
                df=df[df['Maturity_Buckets']!='Total']
                df['Cash_Balances_With_Rbi_And_Other_Banks']=np.nan
                df['Fixed_Assets']=np.nan
                df['Cash_And_Balances_With_Rbi']=np.nan
                df['Balances_With_Banks_And_Money_At_Call_And_Short_Notice']=np.nan
                df['Other_Assets']=np.nan
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Runtime']=datetime.datetime.now()
                for col in ['Advances','Investments']:
                    df[col]=round(pd.to_numeric(df[col]),2)

                df['Total_Amount_In_Mn']=df['Investments']+df['Advances']
                df=df[['Bank','Maturity_Buckets','Cash_And_Balances_With_Rbi','Balances_With_Banks_And_Money_At_Call_And_Short_Notice','Cash_Balances_With_Rbi_And_Other_Banks','Investments','Advances',
                                    'Fixed_Assets','Other_Assets','Total_Amount_In_Mn','Relevant_Date','Runtime']]

                df['Bank_Type']=bank_type
                df=clean_table(df)
                df['Relevant_Date']=pd.to_datetime(df['Relevant_Date']).dt.date
                df=df[df['Relevant_Date']>max_date]
                print(df)
                engine = adqvest_db.db_conn()
                df.to_sql("BANK_RESIDUAL_CONTRACTUAL_MATURITY_BREAKDOWN_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print('uploaded')    
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='next day'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_union(tables,heading,bank_type,bank,date,'ilovepdf',2,bank_name,catch_error_df)
            return df 

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
                search_str=['market risk']
                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'lattice',20,100,40)
                        if len(tables)>1:
                            df=data_collection_union(tables,search_str,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                            print('returned df')
                            connection = engine.connect()
                            connection.execute("update BANK_BASEL_III_QUARTERLY_LINKS set Bank_Residual_Contractual_Maturity_Breakdown='Completed' where Relevant_Date = '"+str(date)+"' and Bank= '"+bank+"'")
                            delete_collected_data(bank_name,bank,max_date)
                        else:
                            print('UNION BANK DID NOT HAVE ALL DATA')
                    except:
                        df=data_collection_union('',search_str,max_date,bank_type,bank,date,'ilovepdf',1,bank_name,catch_error_df)
                except Exception as e:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    print('Error occured for bank------->',bank)


        def data_collection_hdfc(tables,search_str,max_date,bank_type,bank,date,type_read,loop,bank_name,catch_error_df):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                row=row_col_index_locator(df,['bucket'])[1]
                col=row_col_index_locator(df,['bucket'])[0]
                df=df.iloc[row:-1,col:]
                df=df[df[col]!='Total']
                col_end=row_col_index_locator(df,['total'])[0]
                df=df.iloc[:,:col_end]
                df=clean_dataframe(df)
                col=row_col_index_locator(df,['bucket'])[0]
                values_clean=[
                    {'1 Day':'1 Day'},
                    {'2 to 7 days':'2 to 7 Days'},
                    {'8 to 14 days':'8 to 14 Days'},
                    {'15 to 30 days':'15 to 30 Days'},
                    {'31 days':'31 Days & upto 2 Months'},
                    {'2 months to':'Over 2 Months & upto 3 Months'},
                    {'3 months to':'Over 3 Months & upto 6 Months'},
                    {'months to 1':'Over 6 Months & upto 1 Year'},
                    {'over 1 year':'Over 1 Year & upto 3 Years'},
                    {'3 years and':'Over 3 Years & upto 5 Years'},
                    {'over 5 years':'Over 5 Years'}
                ]
                df=row_modificator(df,values_clean,col,row_del=False,keep_row=True,update_row=True)
                df.reset_index(drop=True,inplace=True)
                df.iloc[4][0]='31 Days & upto 2 Months'
                df.columns=['Maturity_Buckets','Cash_And_Balances_With_Rbi','Balances_With_Banks_And_Money_At_Call_And_Short_Notice','Investments','Advances',
                                                                'Fixed_Assets','Other_Assets']
                for col in ['Cash_And_Balances_With_Rbi','Balances_With_Banks_And_Money_At_Call_And_Short_Notice','Investments','Advances','Fixed_Assets','Other_Assets']:
                    df[col]=round(pd.to_numeric(df[col],errors='coerce')*0.1,2)

                df=df.fillna(0)
                df['Total_Amount_In_Mn']=df['Cash_And_Balances_With_Rbi']+df['Balances_With_Banks_And_Money_At_Call_And_Short_Notice']+df['Investments']+df['Advances']                                            +df['Fixed_Assets']+df['Other_Assets']
                df['Cash_Balances_With_Rbi_And_Other_Banks']=df['Cash_And_Balances_With_Rbi']+df['Balances_With_Banks_And_Money_At_Call_And_Short_Notice']
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Runtime']=datetime.datetime.now()
                df=df[['Bank','Maturity_Buckets','Cash_And_Balances_With_Rbi','Balances_With_Banks_And_Money_At_Call_And_Short_Notice','Cash_Balances_With_Rbi_And_Other_Banks','Investments','Advances',
                                                            'Fixed_Assets','Other_Assets','Total_Amount_In_Mn','Relevant_Date','Runtime']]
                df['Bank_Type']=bank_type
                df=clean_table(df)
                df['Relevant_Date']=pd.to_datetime(df['Relevant_Date']).dt.date
                df=df[df['Relevant_Date']>max_date]
                print(df)
                engine = adqvest_db.db_conn()
                df.to_sql("BANK_RESIDUAL_CONTRACTUAL_MATURITY_BREAKDOWN_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print('uploaded')
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='1 day'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_hdfc(df,heading,max_date,bank_type,bank,date,'ilovepdf',2,bank_name,catch_error_df)
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
                search_str=['1 day']
                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'stream',15,250,40)
                        if len(tables)>1:
                            df=data_collection_hdfc(tables,search_str,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                            print('returned df')
                            connection = engine.connect()
                            connection.execute("update BANK_BASEL_III_QUARTERLY_LINKS set Bank_Residual_Contractual_Maturity_Breakdown='Completed' where Relevant_Date = '"+str(date)+"' and Bank= '"+bank+"'")
                            delete_collected_data(bank_name,bank,max_date)
                        else:
                            print('HDFC BANK DID NOT HAVE ALL DATA')
                    except:
                        df=data_collection_hdfc('',search_str,max_date,bank_type,bank,date,'ilovepdf',1,bank_name,catch_error_df)
                except Exception as e:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    print('Error occured for bank------->',bank)

        def data_collection_city(tables,search_str,max_date,bank_type,bank,date,type_read,loop,bank_name,catch_error_df):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                row=row_col_index_locator(df,['period'])[1]
                col=row_col_index_locator(df,['period'])[0]
                df=df.iloc[row:-1,col:]
                df=df[df[col]!='Total']
                col_end=row_col_index_locator(df,['total'])[0]
                df=df.iloc[:,:col_end]
                df=clean_dataframe(df)
                col=row_col_index_locator(df,['1 day'])[0]
                values_clean=[
                    {'1 Day':'1 Day'},
                    {'2 to 7 days':'2 to 7 Days'},
                    {'8 to 14 days':'8 to 14 Days'},
                    {'15 to 30 days':'15 to 30 Days'},
                    {'31 days':'31 Days & upto 2 Months'},
                    {'2 months to':'Over 2 Months & upto 3 Months'},
                    {'3 months to':'Over 3 Months & upto 6 Months'},
                    {'months to 1':'Over 6 Months & upto 1 Year'},
                    {'over 1 year':'Over 1 Year & upto 3 Years'},
                    {'3 years and':'Over 3 Years & upto 5 Years'},
                    {'over 5 years':'Over 5 Years'}
                ]
                df=df[df[col]!='#']
                df=df.iloc[1:,:]
                df.columns=['Maturity_Buckets','Cash_Balances_With_Rbi_And_Other_Banks','Advances','Investments','Fixed_Assets']
                df['Cash_And_Balances_With_Rbi']=np.nan
                df['Balances_With_Banks_And_Money_At_Call_And_Short_Notice']=np.nan
                df['Other_Assets']=np.nan
                df['Advances']=df['Advances'].apply(lambda x:re.match(r'\d+\.\d+',x.replace(' ',''))[0])
                df['Investments']=df['Investments'].apply(lambda x:re.match(r'\d+\.\d+',x.replace(' ',''))[0])
                df['Fixed_Assets']=df['Fixed_Assets'].apply(lambda x:re.match(r'\d+\.\d+',x.replace(' ',''))[0])
                df['Advances']=pd.to_numeric(df['Advances'])*10
                df['Investments']=pd.to_numeric(df['Investments'])*10
                df['Fixed_Assets']=pd.to_numeric(df['Fixed_Assets'])*10
                df['Cash_Balances_With_Rbi_And_Other_Banks']=pd.to_numeric(df['Cash_Balances_With_Rbi_And_Other_Banks'])*10
                df=df.round(2)
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Runtime']=datetime.datetime.now()

                for col in ['Advances','Investments','Fixed_Assets','Cash_Balances_With_Rbi_And_Other_Banks']:
                    df[col]=round(pd.to_numeric(df[col]),2)

                df['Total_Amount_In_Mn']=df['Investments']+df['Advances']+df['Fixed_Assets']+df['Cash_Balances_With_Rbi_And_Other_Banks']
                df=df[['Bank','Maturity_Buckets','Cash_And_Balances_With_Rbi','Balances_With_Banks_And_Money_At_Call_And_Short_Notice','Cash_Balances_With_Rbi_And_Other_Banks','Investments','Advances',
                                    'Fixed_Assets','Other_Assets','Total_Amount_In_Mn','Relevant_Date','Runtime']]

                df['Bank_Type']=bank_type
                df['Relevant_Date']=pd.to_datetime(df['Relevant_Date']).dt.date
                df=df[df['Relevant_Date']>max_date]
                print(df)
                engine = adqvest_db.db_conn()
                df.to_sql("BANK_RESIDUAL_CONTRACTUAL_MATURITY_BREAKDOWN_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print('uploaded')
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='1 day'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_city(tables,heading,max_date,bank_type,bank,date,'ilovepdf',2,bank_name,catch_error_df)
            return df 


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
                search_str=['1 day']
                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'stream',10,100,40)
                        if len(tables)>1:
                            df=data_collection_city(tables,search_str,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                            print('returned df')
                            connection = engine.connect()
                            connection.execute("update BANK_BASEL_III_QUARTERLY_LINKS set Bank_Residual_Contractual_Maturity_Breakdown='Completed' where Relevant_Date = '"+str(date)+"' and Bank= '"+bank+"'")
                            delete_collected_data(bank_name,bank,max_date)
                        else:
                            print('CITY UNION BANK DID NOT HAVE ALL DATA')
                    except:
                        df=data_collection_city('',search_str,max_date,bank_type,bank,date,'ilovepdf',1,bank_name,catch_error_df)
                except Exception as e:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    print('Error occured for bank------->',bank)


        def data_collection_axis_icici(tables,search_str,max_date,bank_type,bank,date,type_read,loop,bank_name,catch_error_df):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                row=row_col_index_locator(df,['maturity bucket'])[1]
                col=row_col_index_locator(df,['maturity bucket'])[0]
                df=df.iloc[row:-1,col:]
                df=df[df[col]!='Total']
                df=clean_dataframe(df)
                if bank=='ICICI BANK':
                    value='day 1'
                else:
                    value='1 day'
                col=row_col_index_locator(df,[value])[0]
                values_clean=[
                    {value:'1 Day'},
                    {'2 to 7 days':'2 to 7 Days'},
                    {'8 to 14 days':'8 to 14 Days'},
                    {'15 to 30 days':'15 to 30 Days'},
                    {'31 days':'31 Days & upto 2 Months'},
                    {'2 months to':'Over 2 Months & upto 3 Months'},
                    {'3 months to':'Over 3 Months & upto 6 Months'},
                    {'months to 1':'Over 6 Months & upto 1 Year'},
                    {'over 1 year':'Over 1 Year & upto 3 Years'},
                    {'3 years and':'Over 3 Years & upto 5 Years'},
                    {'over 5 years':'Over 5 Years'}
                ]
                df=df[df[col]!='#']
                df=df.replace('#',np.nan)
                df=df.iloc[1:,:]
                df.columns=['Maturity_Buckets','Cash_And_Balances_With_Rbi','Balances_With_Banks_And_Money_At_Call_And_Short_Notice','Cash_Balances_With_Rbi_And_Other_Banks','Investments','Advances','Fixed_Assets','Other_Assets']
                df['Bank']=bank
                for col in ['Advances','Investments','Other_Assets','Balances_With_Banks_And_Money_At_Call_And_Short_Notice','Fixed_Assets','Cash_And_Balances_With_Rbi']:
                    df[col]=round(pd.to_numeric(df[col]),2)

                df['Total_Amount_In_Mn']=df['Investments']+df['Advances']+df['Other_Assets']+df['Balances_With_Banks_And_Money_At_Call_And_Short_Notice']+df['Fixed_Assets']+df['Cash_And_Balances_With_Rbi']
                df['Cash_Balances_With_Rbi_And_Other_Banks']=df['Balances_With_Banks_And_Money_At_Call_And_Short_Notice']+df['Cash_And_Balances_With_Rbi']
                df['Relevant_Date']=date
                df['Runtime']=datetime.datetime.now()
                df=df[['Bank','Maturity_Buckets','Cash_And_Balances_With_Rbi','Balances_With_Banks_And_Money_At_Call_And_Short_Notice','Cash_Balances_With_Rbi_And_Other_Banks','Investments','Advances',
                                    'Fixed_Assets','Other_Assets','Total_Amount_In_Mn','Relevant_Date','Runtime']]
                df['Bank_Type']=bank_type
                df=clean_table(df)
                df['Relevant_Date']=pd.to_datetime(df['Relevant_Date']).dt.date
                df=df[df['Relevant_Date']>max_date]
                print(df)
                engine = adqvest_db.db_conn()
                df.to_sql("BANK_RESIDUAL_CONTRACTUAL_MATURITY_BREAKDOWN_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print('uploaded')
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='1 day'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_axis_icici(tables,heading,max_date,bank_type,bank,date,'ilovepdf',2,bank_name,catch_error_df)
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
                search_str=['day 1']
                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'lattice',20,200,70)
                        if len(tables)>1:
                            df=data_collection_axis_icici(tables,search_str,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                            print('returned df')
                            connection = engine.connect()
                            connection.execute("update BANK_BASEL_III_QUARTERLY_LINKS set Bank_Residual_Contractual_Maturity_Breakdown='Completed' where Relevant_Date = '"+str(date)+"' and Bank= '"+bank+"'")
                            delete_collected_data(bank_name,bank,max_date)
                        else:
                            print('HDFC BANK DID NOT HAVE ALL DATA')
                    except:
                        df=data_collection_axis_icici('',search_str,max_date,bank_type,bank,date,'ilovepdf',1,bank_name,catch_error_df)
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
                search_str=['bucket']
                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'lattice',10,100,40)
                        if len(tables)>1:
                            df=data_collection_axis_icici(tables,search_str,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                            print('returned df')
                            connection = engine.connect()
                            connection.execute("update BANK_BASEL_III_QUARTERLY_LINKS set Bank_Residual_Contractual_Maturity_Breakdown='Completed' where Relevant_Date = '"+str(date)+"' and Bank= '"+bank+"'")
                            delete_collected_data(bank_name,bank,max_date)
                        else:
                            print('HDFC BANK DID NOT HAVE ALL DATA')
                    except:
                        df=data_collection_axis_icici('',search_str,max_date,bank_type,bank,date,'ilovepdf',1,bank_name,catch_error_df)
                except Exception as e:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    print('Error occured for bank------->',bank)

        


        def data_collection_kotak(tables,search_str,max_date,bank_type,bank,date,type_read,loop,bank_name,catch_error_df):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                row_st=row_col_index_locator(df,['1 day'])[1]
                row_ed=row_col_index_locator(df,['over 5 years'])[1]
                df=df.iloc[row_st:row_ed+1]
                df=clean_dataframe(df)
                df.reset_index(drop=True,inplace=True)
                col=row_col_index_locator(df,['1 day'])[0]
                values_clean=[
                            {'1 day':'1 Day'},
                            {'2 to 7 days':'2 to 7 Days'},
                            {'8 to 14 days':'8 to 14 Days'},
                            {'15 to 30 days':'15 to 30 Days'},
                            {'31 days':'31 Days & upto 2 Months'},
                            {'over 2 months':'Over 2 Months & upto 3 Months'},
                            {'over 3 months':'Over 3 Months & upto 6 Months'},
                            {'over 6 months':'Over 6 Months & upto 1 Year'},
                            {'over 1 year':'Over 1 Year & upto 3 Years'},
                            {'over 3 year':'Over 3 Years & upto 5 Years'},
                            {'over 5 year':'Over 5 Years'}
                        ]
                df=row_modificator(df,values_clean,col,row_del=False,keep_row=True,update_row=True)
                df.reset_index(drop=True,inplace=True)
                df.iloc[4][0]='31 Days & upto 2 Months'
                df=df.replace('#',np.nan)
                df.columns=['Maturity_Buckets','Cash_And_Balances_With_Rbi','Balances_With_Banks_And_Money_At_Call_And_Short_Notice','Investments','Advances','Fixed_Assets','Other_Assets']
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Runtime']=datetime.datetime.now()
                for col in ['Advances','Investments','Other_Assets','Balances_With_Banks_And_Money_At_Call_And_Short_Notice','Fixed_Assets','Cash_And_Balances_With_Rbi']:
                    df[col]=round(pd.to_numeric(df[col]),2)

                df['Total_Amount_In_Mn']=df['Investments']+df['Advances']+df['Other_Assets']+df['Balances_With_Banks_And_Money_At_Call_And_Short_Notice']+df['Fixed_Assets']+df['Cash_And_Balances_With_Rbi']
                df['Cash_Balances_With_Rbi_And_Other_Banks']=df['Balances_With_Banks_And_Money_At_Call_And_Short_Notice']+df['Cash_And_Balances_With_Rbi']

                df=df[['Bank','Maturity_Buckets','Cash_And_Balances_With_Rbi','Balances_With_Banks_And_Money_At_Call_And_Short_Notice','Cash_Balances_With_Rbi_And_Other_Banks','Investments','Advances',
                                    'Fixed_Assets','Other_Assets','Total_Amount_In_Mn','Relevant_Date','Runtime']]

                df['Bank_Type']=bank_type
                df=clean_table(df)
                df['Relevant_Date']=pd.to_datetime(df['Relevant_Date']).dt.date
                df=df[df['Relevant_Date']>max_date]
                print(df)
                engine = adqvest_db.db_conn()
                df.to_sql("BANK_RESIDUAL_CONTRACTUAL_MATURITY_BREAKDOWN_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print('uploaded')
                    
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='1 day'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_kotak(df,heading,max_date,bank_type,bank,date,'ilovepdf',2,bank_name,catch_error_df)
            return df


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
                search_str=['1 day']
                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'lattice',20,100,40)
                        if len(tables)>1:
                            df=data_collection_kotak(tables,search_str,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                            print('returned df')
                            connection = engine.connect()
                            connection.execute("update BANK_BASEL_III_QUARTERLY_LINKS set Bank_Residual_Contractual_Maturity_Breakdown='Completed' where Relevant_Date = '"+str(date)+"' and Bank= '"+bank+"'")
                            delete_collected_data(bank_name,bank,max_date)
                        else:
                            print('HDFC BANK DID NOT HAVE ALL DATA')
                    except:
                        df=data_collection_kotak('',search_str,max_date,bank_type,bank,date,'ilovepdf',1,bank_name,catch_error_df)
                except Exception as e:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    print('Error occured for bank------->',bank)

        def data_collection_federal(tables,search_str,max_date,bank_type,bank,date,type_read,loop,bank_name,catch_error_df):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                row=row_col_index_locator(df,['day 1'])[1]
                col=row_col_index_locator(df,['day 1'])[0]
                df=df.iloc[row:-1,col:]
                df=df[df[col]!='Total']
                df=clean_dataframe(df)
                if bank=='ICICI BANK':
                    value='day 1'
                else:
                    value='1 day'
                col=row_col_index_locator(df,['day 1'])[0]
                values_clean=[
                    {value:'1 Day'},
                    {'7 days':'2 to 7 Days'},
                    {'14 days':'8 to 14 Days'},
                    {'30 days':'15 to 30 Days'},
                    {'31 days':'31 Days & upto 2 Months'},
                    {'over 2 months':'Over 2 Months & upto 3 Months'},
                    {'over 3 months':'Over 3 Months & upto 6 Months'},
                    {'over 6 month':'Over 6 Months & upto 1 Year'},
                    {'over 1 year':'Over 1 Year & upto 3 Years'},
                    {'over 3 year':'Over 3 Years & upto 5 Years'},
                    {'over 5 year':'Over 5 Years & upto 7 Years'},
                    {'over 7 year':'Over 7 Years & upto 10 Years'},
                    {'over 10 year':'Over 10 Years & upto 15 Years'},
                    {'over 15 year':'Over 15 Years'}
                ]
                df=row_modificator(df,values_clean,col,row_del=False,keep_row=True,update_row=True)
                df=df[df[col]!='#']
                df=df.replace('#',np.nan)
                df.columns=['Maturity_Buckets','Cash_And_Balances_With_Rbi','Balances_With_Banks_And_Money_At_Call_And_Short_Notice','Cash_Balances_With_Rbi_And_Other_Banks','Investments','Advances','Fixed_Assets','Other_Assets','Total_Amount_In_Mn']

                for i in range(len(df)):
                    for j in range(1,len(df.columns)):
                        df.iloc[i,j]=pd.to_numeric(df.iloc[i,j])*10
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Runtime']=datetime.datetime.now()
                df=df[['Bank','Maturity_Buckets','Cash_And_Balances_With_Rbi','Balances_With_Banks_And_Money_At_Call_And_Short_Notice',
                                         'Cash_Balances_With_Rbi_And_Other_Banks','Investments','Advances',
                                        'Fixed_Assets','Other_Assets','Total_Amount_In_Mn','Relevant_Date','Runtime']]
                df.reset_index(drop=True,inplace=True)
                for j in df.columns:
                    if j!='Runtime':
                        df[j]=df[j].apply(lambda x:str(x))
                df['Bank_Type']=bank_type
                df['Relevant_Date']=pd.to_datetime(df['Relevant_Date']).dt.date
                df=df[df['Relevant_Date']>max_date]
                print(df)
                engine = adqvest_db.db_conn()
                df.to_sql("BANK_RESIDUAL_CONTRACTUAL_MATURITY_BREAKDOWN_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print('uploaded')
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='day 1'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_federal(df,heading,max_date,bank_type,bank,date,'ilovepdf',2,bank_name,catch_error_df)
            return df 


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
                search_str=['day 1']
                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'lattice',10,100,40)
                        if len(tables)>1:
                            df=data_collection_federal(tables,search_str,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                            print('returned df')
                            connection = engine.connect()
                            connection.execute("update BANK_BASEL_III_QUARTERLY_LINKS set Bank_Residual_Contractual_Maturity_Breakdown='Completed' where Relevant_Date = '"+str(date)+"' and Bank= '"+bank+"'")
                            delete_collected_data(bank_name,bank,max_date)
                        else:
                            print('HDFC BANK DID NOT HAVE ALL DATA')
                    except:
                        df=data_collection_federal('',search_str,max_date,bank_type,bank,date,'ilovepdf',1,bank_name,catch_error_df)
                        delete_collected_data(bank_name,bank,max_date)
                except Exception as e:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    print('Error occured for bank------->',bank)

        def data_collection_rbl(tables,search_str,max_date,bank_type,bank,date,type_read,loop,bank_name,catch_error_df):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                row=row_col_index_locator(df,['1 day'])[1]
                col=row_col_index_locator(df,['1 day'])[0]
                df=df.iloc[row:-1,col:]
                df=df[df[col]!='Total']
                df=clean_dataframe(df)
                col=row_col_index_locator(df,['1 day'])[0]
                values_clean=[
                    {'1 day':'1 Day'},
                    {'7 days':'2 to 7 Days'},
                    {'14 days':'8 to 14 Days'},
                    {'30 days':'15 to 30 Days'},
                    {'31 days':'31 Days & upto 2 Months'},
                    {'2 to 3 month':'Over 2 Months & upto 3 Months'},
                    {'3 to 6 months':'Over 3 Months & upto 6 Months'},
                    {'6 to 12 month':'Over 6 Months & upto 1 Year'},
                    {'1 to 3 year':'Over 1 Year & upto 3 Years'},
                    {'3 to 5 year':'Over 3 Years & upto 5 Years'},
                    {'5 to 7 year':'Over 5 Years & upto 7 Years'},
                    {'7 to 10 year':'Over 7 Years & upto 10 Years'},
                    {'10 to 15 year':'Over 10 Years & upto 15 Years'},
                    {'over 15 year':'Over 15 Years'}
                ]
                df=row_modificator(df,values_clean,col,row_del=False,keep_row=True,update_row=True)
                df=df[df[col]!='#']
                df=df.replace('#',np.nan)
                df.iloc[4][0]='31 Days & upto 2 Months'
                df.columns=['Maturity_Buckets','Cash_Balances_With_Rbi_And_Other_Banks','Investments','Advances','Other_Assets']
                df['Cash_And_Balances_With_Rbi']=np.nan
                df['Balances_With_Banks_And_Money_At_Call_And_Short_Notice']=np.nan
                df['Total_Amount_In_Mn']=np.nan
                df['Fixed_Assets']=np.nan
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Runtime']=datetime.datetime.now()
                for col in ['Cash_Balances_With_Rbi_And_Other_Banks','Advances','Investments','Other_Assets']:
                    df[col]=round(pd.to_numeric(df[col]),2)
                df['Total_Amount_In_Mn']=df['Investments']+df['Advances']+df['Other_Assets']+df['Cash_Balances_With_Rbi_And_Other_Banks']
                df=df[['Bank','Maturity_Buckets','Cash_And_Balances_With_Rbi','Balances_With_Banks_And_Money_At_Call_And_Short_Notice','Cash_Balances_With_Rbi_And_Other_Banks','Investments','Advances',
                                    'Fixed_Assets','Other_Assets','Total_Amount_In_Mn','Relevant_Date','Runtime']]
                df['Bank_Type']=bank_type
                df.reset_index(drop=True,inplace=True)
                df['Relevant_Date']=pd.to_datetime(df['Relevant_Date']).dt.date
                df=df[df['Relevant_Date']>max_date]
                print(df)
                engine = adqvest_db.db_conn()
                df.to_sql("BANK_RESIDUAL_CONTRACTUAL_MATURITY_BREAKDOWN_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print('uploaded')
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='1 day'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_rbl(df,heading,max_date,bank_type,bank,date,'ilovepdf',2,bank_name,catch_error_df)
            return df 


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
                search_str=['1 day']
                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'stream',10,100,40)
                        if len(tables)>1:
                            df=data_collection_rbl(tables,search_str,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                            print('returned df')
                            connection = engine.connect()
                            connection.execute("update BANK_BASEL_III_QUARTERLY_LINKS set Bank_Residual_Contractual_Maturity_Breakdown='Completed' where Relevant_Date = '"+str(date)+"' and Bank= '"+bank+"'")
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
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                
                row=row_col_index_locator(df,['time bucket'])[1]
                col=row_col_index_locator(df,['time bucket'])[0]
                df=df.iloc[row:-1,col:]
                df=df[df[col]!='Total']
                df=clean_dataframe(df)
                col=row_col_index_locator(df,['day 1'])[0]
                df[0] = df[0].replace({'\n': ' ', r'\s+': ' '}, regex=True)
                print(df)
                values_clean=[
                    {'day 1':'1 Day'},
                    {'2 to 7 days':'2 to 7 Days'},
                    {'8 to 14 days':'8 to 14 Days'},
                    {'15 to 30 days':'15 to 30 Days'},
                    {'31 days':'31 Days & upto 2 Months'},
                    {'2 months to':'Over 2 Months & upto 3 Months'},
                    {'3 months to':'Over 3 Months & upto 6 Months'},
                    {'over 6 months':'Over 6 Months & upto 1 Year'},
                    {'over 1 year':'Over 1 Year & upto 3 Years'},
                    {'over 3 year':'Over 3 Years & upto 5 Years'},
                    {'over 5 year':'Over 5 Years & upto 7 Years'},
                    {'over 7 year':'Over 7 Years & upto 10 Years'},
                    {'over 10 year':'Over 10 Years & upto 15 Years'},
                    {'above 15 years':'Over 15 Years'}
                ]
                df=row_modificator(df,values_clean,col,row_del=False,keep_row=True,update_row=True)
                df=df[df[col]!='#']
                df=df.replace('#',np.nan)
                df = df.dropna(axis=1, how='all')
                df=df.replace('-',np.nan)
                df.iloc[:,1:] = df.iloc[:,1:].astype(str).apply(lambda x: pd.to_numeric(x.str.replace(',', ''), errors='coerce').fillna(0))
                print(df)
                df.columns=['Maturity_Buckets','Cash_And_Balances_With_Rbi','Balances_With_Banks_And_Money_At_Call_And_Short_Notice','Cash_Balances_With_Rbi_And_Other_Banks','Investments','Advances','Fixed_Assets','Other_Assets','Total_Amount_In_Mn']
                print('here')
                for i in range(len(df)):
                    for j in range(1,len(df.columns)):
                        df.iloc[i,j]=pd.to_numeric(df.iloc[i,j])
                
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Runtime']=datetime.datetime.now()
                maturity_df=df[['Bank','Maturity_Buckets','Cash_And_Balances_With_Rbi','Balances_With_Banks_And_Money_At_Call_And_Short_Notice',
                                            'Cash_Balances_With_Rbi_And_Other_Banks','Investments','Advances',
                                        'Fixed_Assets','Other_Assets','Total_Amount_In_Mn','Relevant_Date','Runtime']]
                df.reset_index(drop=True,inplace=True)
                for j in df.columns:
                    if j!='Runtime':
                        df[j]=df[j].apply(lambda x:str(x))
                df['Bank_Type']=bank_type
                df=clean_table(df)
                df['Relevant_Date']=pd.to_datetime(df['Relevant_Date']).dt.date
                df=df[df['Relevant_Date']>max_date]
                print(df)
                engine = adqvest_db.db_conn()
                df.to_sql("BANK_RESIDUAL_CONTRACTUAL_MATURITY_BREAKDOWN_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print('uploaded')
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='day 1'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_indusind(df,heading,max_date,bank_type,bank,date,'ilovepdf',2,bank_name,catch_error_df)
            return df

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
                search_str=['1 day']
                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'stream',18,150,40)
                        if len(tables)>1:
                            df=data_collection_indusind(tables,search_str,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                            print('returned df')
                            connection = engine.connect()
                            connection.execute("update BANK_BASEL_III_QUARTERLY_LINKS set Bank_Residual_Contractual_Maturity_Breakdown='Completed' where Relevant_Date = '"+str(date)+"' and Bank= '"+bank+"'")
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

        def data_collection_idbi(tables,search_str,max_date,bank_type,bank,date,type_read,loop,bank_name,catch_error_df):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                row=row_col_index_locator(df,['day 1'])[1]
                col=row_col_index_locator(df,['day 1'])[0]
                df=df.iloc[row:-1,col:]
                df=df[df[col]!='Total']
                df=clean_dataframe(df)
                col=row_col_index_locator(df,['day 1'])[0]
                df[col] = df[col].replace({'\n': ' ', r'\s+': ' '}, regex=True)
                values_clean=[
                    {'day 1':'1 Day'},
                    {'7 days':'2 to 7 Days'},
                    {'14 days':'8 to 14 Days'},
                    {'30 days':'15 to 30 Days'},
                    {'31 days':'31 Days & upto 2 Months'},
                    {'over 2 month':'Over 2 Months & upto 3 Months'},
                    {'over 3 month':'Over 3 Months & upto 6 Months'},
                    {'over 6 months':'Over 6 Months & upto 1 Year'},
                    {'over 1 year':'Over 1 Year & upto 3 Years'},
                    {'over 3 year':'Over 3 Years & upto 5 Years'},
                    {'over 5 yrs':'Over 5 Years'},
                ]
                df=row_modificator(df,values_clean,col,row_del=False,keep_row=True,update_row=True)
                df=df.iloc[:,:-1]
                df.columns=['Maturity_Buckets','Cash_Balances_With_Rbi_And_Other_Banks','Investments','Advances','Fixed_Assets']
                df['Cash_And_Balances_With_Rbi']=np.nan
                df['Balances_With_Banks_And_Money_At_Call_And_Short_Notice']=np.nan
                df['Other_Assets']=np.nan
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Runtime']=datetime.datetime.now()
                for col in ['Advances','Investments','Cash_Balances_With_Rbi_And_Other_Banks','Fixed_Assets']:
                    df[col]=round(pd.to_numeric(df[col]),2)
                df['Total_Amount_In_Mn']=df['Investments']+df['Advances']+df['Fixed_Assets']+df['Cash_Balances_With_Rbi_And_Other_Banks']
                df=df[['Bank','Maturity_Buckets','Cash_And_Balances_With_Rbi','Balances_With_Banks_And_Money_At_Call_And_Short_Notice','Cash_Balances_With_Rbi_And_Other_Banks','Investments','Advances',
                                    'Fixed_Assets','Other_Assets','Total_Amount_In_Mn','Relevant_Date','Runtime']]
                try:
                    df.drop(df[df.iloc[:,4]=='Investments'].index.values,inplace=True)
                except:
                    pass
                df['Bank_Type']=bank_type
                df=clean_table(df)
                df['Relevant_Date']=pd.to_datetime(df['Relevant_Date']).dt.date
                df=df[df['Relevant_Date']>max_date]
                print(df)
                engine = adqvest_db.db_conn()
                df.to_sql("BANK_RESIDUAL_CONTRACTUAL_MATURITY_BREAKDOWN_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print('uploaded')
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='maturity bucket'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_idbi(df,heading,max_date,bank_type,bank,date,'ilovepdf',2,bank_name,catch_error_df)
            return df

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
                search_str=['maturity bucket']
                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'lattice',10,100,40)
                        if len(tables)>1:
                            df=data_collection_idbi(tables,search_str,max_date,bank_type,bank,date,'camelot',1,bank_name,catch_error_df)
                            print('returned df')
                            connection = engine.connect()
                            connection.execute("update BANK_BASEL_III_QUARTERLY_LINKS set Bank_Residual_Contractual_Maturity_Breakdown='Completed' where Relevant_Date = '"+str(date)+"' and Bank= '"+bank+"'")
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
        def data_collection_maharastra(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                df.columns=['Maturity_Buckets','Investments','Advances','Fixed_Assets']
                df=df.iloc[:-1]
                start_index=df[df['Maturity_Buckets'].str.lower().str.contains('1 day')].index[0]
                df=df.loc[start_index:]
                df.reset_index(drop=True,inplace=True)
                df['Cash_Balances_With_Rbi_And_Other_Banks']=np.nan
                df['Cash_And_Balances_With_Rbi']=np.nan
                df['Balances_With_Banks_And_Money_At_Call_And_Short_Notice']=np.nan
                df['Other_Assets']=np.nan
                df['Bank']=bank
                df['Relevant_Date']=date
                for col in ['Investments','Advances','Fixed_Assets']:
                    df[col]=round(pd.to_numeric(df[col]),2)
                df['Total_Amount_In_Mn']=df['Investments']+df['Advances']+df['Fixed_Assets']
                df['Runtime']=datetime.datetime.now()
                df=df[['Bank','Maturity_Buckets','Cash_And_Balances_With_Rbi','Balances_With_Banks_And_Money_At_Call_And_Short_Notice','Cash_Balances_With_Rbi_And_Other_Banks','Investments','Advances',
                                                    'Fixed_Assets','Other_Assets','Total_Amount_In_Mn','Relevant_Date','Runtime']]
                df['Bank_Type']=bank_type
                df.reset_index(drop=True,inplace=True)
                df=clean_table(df)
                df.to_sql("BANK_RESIDUAL_CONTRACTUAL_MATURITY_BREAKDOWN_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print(df)
                print('uploaded')
            except:
                if loop <2:
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='maturity pattern'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_maharastra(df,heading,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return df
        bank='BANK OF MAHARASHTRA'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['maturity pattern']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)

                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'lattice',10,200,70)
                        df=data_collection_maharastra(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_maharastra('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank,date)
                except Exception as e:
                    catch_error_df=get_error_df(bank_name,bank,e,date,catch_error_df)
        def data_collection_idfc(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                df.columns=df.iloc[0]
                df=df[1:len(df)-1]
                df=df.rename(columns={'Residual Tenor Bucket':'Maturity_Buckets','Cash & Balance with RBI':'Cash_And_Balances_With_Rbi',
                                                       'Balances with banks & Money at call and short notice':'Balances_With_Banks_And_Money_At_Call_And_Short_Notice',
                                                       'Loans & Advances#':'Advances','Fixed Assets':'Fixed_Assets','Other Assets':'Other_Assets'})
                for i in range(len(df)):
                    for j in range(1,len(df.columns)):
                        df.iloc[i,j]=pd.to_numeric(df.iloc[i,j])*10
                df['Bank']='IDFC FIRST BANK'
                df['Relevant_Date']=date
                df['Runtime']=datetime.datetime.now()
                df=df[['Bank','Maturity_Buckets','Cash_And_Balances_With_Rbi','Balances_With_Banks_And_Money_At_Call_And_Short_Notice','Investments','Advances',
                                        'Fixed_Assets','Other_Assets','Relevant_Date','Runtime']]
                df.reset_index(drop=True,inplace=True)
                df['Cash_And_Balances_With_Rbi']=df['Cash_And_Balances_With_Rbi'].apply(lambda x:str(x))
                df['Balances_With_Banks_And_Money_At_Call_And_Short_Notice']=df['Balances_With_Banks_And_Money_At_Call_And_Short_Notice'].apply(lambda x:str(x))
                df['Advances']=df['Advances'].apply(lambda x:str(x))
                df['Fixed_Assets']=df['Fixed_Assets'].apply(lambda x:str(x))
                df['Other_Assets']=df['Other_Assets'].apply(lambda x:str(x))
                df['Investments']=df['Investments'].apply(lambda x:str(x))
                df['Total_Amount_In_Mn']=df['Investments']+df['Advances']+df['Fixed_Assets']
                df['Bank_Type']=bank_type
                df=clean_table(df)
                df.to_sql("BANK_RESIDUAL_CONTRACTUAL_MATURITY_BREAKDOWN_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                #print(maturity_df)
                print('Uploaded')
            except:
                if loop <2:
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='tenor bucket'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_sbi(df,heading,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return df
        bank='IDFC FIRST BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['tenor bucket']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)

                max_date,lnk=get_table_max_date_and_links(bank_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'lattice',10,200,70)
                        df=data_collection_idfc(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_idfc('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank,date)
                except Exception as e:
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

