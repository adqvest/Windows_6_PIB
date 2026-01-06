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
import MySql_To_Clickhouse as MySql_CH


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
    links_capital=links_capital[links_capital['Bank_Capital_Requirement_Credit_Risk_Status']!='COMPLETED']
    columns = links_capital.columns.to_list()
    columns.remove('Runtime')
    columns.remove('Last_Updated')
    unique = links_capital.drop_duplicates(subset=columns, keep='last')
    unique.reset_index(drop=True,inplace=True)
    return unique
def update_basel_iii(bank,date):
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    # connection.execute("update BANK_BASEL_III_QUARTERLY_LINKS set BANK_OVERALL_NPA_QTLY_DATA = 'COMPLETED' where Relevant_Date = '"+str(date)+"' and Bank= '"+bank+"'")

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
    table_name = 'BANK_OVERALL_NPA_QTLY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)


        catch_error_df=pd.DataFrame(columns=['Table_name','Bank_Name','Error_message','Relevant_Date','Runtime'])
        sql="Delete from BANK_ERROR_TABLE where Table_Name = '" +table_name+"'"
        engine.execute(sql)
        def delete_collected_data(table_name,bank,date):
            sql="Delete from BANK_ERROR_TABLE where Table_Name = '" +table_name+"' and Bank_Name = '" +bank+"' and Relevant_Date='" +date+"'"
            engine.execute(sql)

        def data_collection_hdfc(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop,table_name,catch_error_df):
            try:
                try:
                    df=get_desired_table(tables,search_str)
                except:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                print(df)
                row_strt=row_col_index_locator(df,['gross advances'])[1]
                row=row_col_index_locator(df,['total'])[1]
                col=row_col_index_locator(df,['gross advances'])[0]
                df=df.iloc[row_strt:row,col:]
                df.columns = [str(i) for i in range(len(df.columns))]
                df=df[df.iloc[:,1]!='']
                df.reset_index(drop=True,inplace=True)
                df=df.iloc[:,:2]
                df = df.T  # Transpose the DataFrame
                df.columns = df.iloc[0]  # Set the first row as column names
                df = df[1:]  # Remove the first row since it's now the column names
                df.columns=['Gross_NPA_To_Gross_Advances_Pct','Net_NPA_To_NET_Advances_Pct','Gross_NPA_Mn','Provisions_In_Mn','Net_NPA_In_Mn','Sub_Standard_In_Mn','Doubtful_1_In_Mn','Doubtful_2_In_Mn','Doubtful_3_In_Mn','Loss_In_Mn']
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Bank_Type']=bank_type
                df['Runtime']=datetime.datetime.now()
                print("final_df")
                df.to_sql("BANK_OVERALL_NPA_QTLY_DATA", index=False, if_exists='append', con=engine)
                print("uploded")
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='gross advances'
                    end_text='doubtful 1'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_hdfc(df,heading,max_date,bank_type,bank,date,'ilovepdf',2,table_name,catch_error_df)
            return df

        bank='HDFC BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['gross npas to gross advances','doubtful 1']
        if links_capital.empty:
            print('No new data for HDFC')
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
                        tables = extract_tables_from_pdf(link,True,'stream',10,300,70)
                        df=data_collection_hdfc(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1,table_name,catch_error_df)
                    except:
                        df=data_collection_hdfc('',search_str,link,max_date,bank_type,bank,date,'camelot',1,table_name,catch_error_df)
                    update_basel_iii(bank,date)
                except Exception as e:
                    print(e)
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)

        def data_collection_sbi_fed_au_axis(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop,table_name,catch_error_df):
            try:
                try:
                    df=get_desired_table(tables,search_str)
                except:
                    df=tables
                    df.reset_index(drop=True,inplace=True)

                
                row_strt=row_col_index_locator(df,['substandard'])[1]
                row=row_col_index_locator(df,['net advances'])[1]
                col=row_col_index_locator(df,['gross advances'])[0]
                df=df.iloc[row_strt:row+1,col:]
                df=df.loc[:, (df != '').any(axis=0)]
                df.columns = [str(i) for i in range(len(df.columns))]
                df=df[df.iloc[:,1]!='']
                df.replace('#',np.nan,inplace=True)
                df.dropna(axis=0,how='all',inplace=True)

                
                df.reset_index(drop=True,inplace=True)
                df = df.T  # Transpose the DataFrame
                df.columns = df.iloc[0]  # Set the first row as column names
                df = df[1:]
               
                df.drop(columns=[i for i in df.columns if i in ['Advances','NPA ratios']],inplace=True)

                df.columns=['Sub_Standard_In_Mn','Doubtful_1_In_Mn','Doubtful_2_In_Mn','Doubtful_3_In_Mn','Loss_In_Mn','Net_NPA_In_Mn','Gross_NPA_To_Gross_Advances_Pct','Net_NPA_To_NET_Advances_Pct']
                print(type(df))
                df.dropna(axis=0,how='all',inplace=True)
                df.reset_index(drop = True, inplace = True)
                df['Gross_NPA_Mn']=round(float(df['Sub_Standard_In_Mn'])+float(df['Doubtful_1_In_Mn'])+float(df['Doubtful_2_In_Mn'])+float(df['Doubtful_3_In_Mn'])+float(df['Loss_In_Mn']),4)
                df['Provisions_In_Mn']=round(float(df['Gross_NPA_Mn'])-float(df['Net_NPA_In_Mn']),4)
                df['Bank']=bank
                df['Bank_Type']=bank_type
                df['Relevant_Date']=date
                df['Runtime']=datetime.datetime.now()
                df.to_sql("BANK_OVERALL_NPA_QTLY_DATA", index=False, if_exists='append', con=engine)
            except Exception as e:
                error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
                print(error_msg)
                if loop < 2:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='gross advances'
                    end_text='doubtful 1'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_sbi_fed_au_axis(df,heading,'',max_date,bank_type,bank,date,'ilovepdf',2,table_name,catch_error_df)
            return df

        bank='STATE BANK OF INDIA'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['gross npas to gross advances','doubtful 1']
        if links_capital.empty:
            print('No new data for SBI')
        else:
            print('New data for SBI')
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)

                max_date,lnk=get_table_max_date_and_links(table_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'lattice',10,300,70)
                        print('done with tables')
                        df=data_collection_sbi_fed_au_axis(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1,table_name,catch_error_df)
                    except:
                        df=data_collection_sbi_fed_au_axis('',search_str,link,max_date,bank_type,bank,date,'camelot',1,table_name,catch_error_df)
                    update_basel_iii(bank,date)
                except Exception as e:
                    print(e)
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)

        bank='FEDERAL BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['gross npas to gross advances','doubtful 1']
        if links_capital.empty:
            print('No new data for FEDERAL')
        else:
            print('New data for FEDERAL')
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)

                max_date,lnk=get_table_max_date_and_links(table_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'lattice',10,300,70)
                        print('done with tables')
                        df=data_collection_sbi_fed_au_axis(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1,table_name,catch_error_df)
                    except:
                        df=data_collection_sbi_fed_au_axis('',search_str,link,max_date,bank_type,bank,date,'camelot',1,table_name,catch_error_df)
                except Exception as e:
                    print(e)
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)

        bank='AU SMALL FINANCE BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['substandard']
        if links_capital.empty:
            print('No new data for AU')
        else:
            print('New data for AU')
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)

                max_date,lnk=get_table_max_date_and_links(table_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'lattice',10,300,70)
                        df=data_collection_sbi_fed_au_axis(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1,table_name,catch_error_df)
                    except:
                        df=data_collection_sbi_fed_au_axis('',search_str,link,max_date,bank_type,bank,date,'camelot',1,table_name,catch_error_df)
                    update_basel_iii(bank,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)

        bank='AXIS BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['substandard']
        if links_capital.empty:
            print('No new data for AXIS')
        else:
            print('New data for AXIS')
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)

                max_date,lnk=get_table_max_date_and_links(table_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'lattice',10,300,70)
                        df=data_collection_sbi_fed_au_axis(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1,table_name,catch_error_df)
                    except:
                        df=data_collection_sbi_fed_au_axis('',search_str,link,max_date,bank_type,bank,date,'camelot',1,table_name,catch_error_df)
                    update_basel_iii(bank,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        def data_collection_iob(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop,table_name,catch_error_df):
            try:
                try:
                    df=get_desired_table(tables,search_str)
                except:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                row_strt=row_col_index_locator(df,['substandard'])[1]
                row=row_col_index_locator(df,['net advances'])[1]
                col=row_col_index_locator(df,['gross advances'])[0]
                df=df.iloc[row_strt:row+1,col:]
                df=df.loc[:, (df != '').any(axis=0)]
                df.columns = [str(i) for i in range(len(df.columns))]
                df=df[df.iloc[:,1]!='']
                df.reset_index(drop=True,inplace=True)
                df = df.T  # Transpose the DataFrame
                df.columns = df.iloc[0]  # Set the first row as column names
                df = df[1:]
                df = df.drop(df.columns[1], axis=1)
                df.columns=['Sub_Standard_In_Mn','Doubtful_1_In_Mn','Doubtful_2_In_Mn','Doubtful_3_In_Mn','Loss_In_Mn','Net_NPA_In_Mn','Gross_NPA_To_Gross_Advances_Pct','Net_NPA_To_NET_Advances_Pct']
                df.reset_index(drop = True, inplace = True)
                df['Gross_NPA_Mn']=round(float(df['Sub_Standard_In_Mn'])+float(df['Doubtful_1_In_Mn'])+float(df['Doubtful_2_In_Mn'])+float(df['Doubtful_3_In_Mn'])+float(df['Loss_In_Mn']),4)
                df['Provisions_In_Mn']=round(float(df['Gross_NPA_Mn'])-float(df['Net_NPA_In_Mn']),4)
                df['Bank']=bank
                df['Bank_Type']=bank_type
                df['Relevant_Date']=date
                df['Runtime']=datetime.datetime.now()
                #print(df)
                df.to_sql("BANK_OVERALL_NPA_QTLY_DATA", index=False, if_exists='append', con=engine)
                
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='gross advances'
                    end_text='doubtful 1'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_iob(df,heading,max_date,bank_type,bank,date,'ilovepdf',2,table_name,catch_error_df)
            return df

        bank='INDIAN OVERSEAS BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['substandard']
        if links_capital.empty:
            print('No new data for IOB')
        else:
            print('New data for IOB')
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)

                max_date,lnk=get_table_max_date_and_links(table_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'stream',10,300,70)
                        df=data_collection_iob(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1,table_name,catch_error_df)
                    except:
                        df=data_collection_iob('',search_str,link,max_date,bank_type,bank,date,'camelot',1,table_name,catch_error_df)
                    update_basel_iii(bank,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        def data_collection_bandhan(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop,table_name,catch_error_df):
            
            try:
                try:
                    df=get_desired_table(tables,search_str)
                except:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                row_strt=row_col_index_locator(df,['substandard'])[1]
                row=row_col_index_locator(df,['net advances'])[1]
                col=row_col_index_locator(df,['gross advances'])[0]
                # df=df.iloc[row_strt:row+1,col:]

                ##Modified| Santonu May 29,2024|
                row_st=row_col_index_locator(df,['gross advances'])[1]
                row_end=row_col_index_locator(df,['Net NPA'])[1]
                
                col=row_col_index_locator(df,['As on December '])[0]
                df=df.iloc[row_st:row_end+1,[0,5]]
                #-----------------------------------------------------

                
                df=df.loc[:, (df != '').any(axis=0)]
                df.columns = [str(i) for i in range(len(df.columns))]
                df=df[df.iloc[:,1]!='']
                df.reset_index(drop=True,inplace=True)
                df = df.T  # Transpose the DataFrame
                df.columns = df.iloc[0]  # Set the first row as column names
                df = df[1:]


                ##Modified| Santonu May 29,2024|
                df.drop(columns=[i for i in df.columns if i in ['#']],inplace=True)
                df=df[['1.    Substandard','2.    Doubtful 1', '3.    Doubtful 2', '4.    Doubtful 3','5.    Loss','Net NPA','Gross Advances','Net Advances']]
                #----------------------------------------------------
                df.columns=['Sub_Standard_In_Mn','Doubtful_1_In_Mn','Doubtful_2_In_Mn','Doubtful_3_In_Mn','Loss_In_Mn','Net_NPA_In_Mn','Gross_NPA_To_Gross_Advances_Pct','Net_NPA_To_NET_Advances_Pct']
                df.reset_index(drop = True, inplace = True)
                df['Gross_NPA_Mn']=round(float(df['Sub_Standard_In_Mn'])+float(df['Doubtful_1_In_Mn'])+float(df['Doubtful_2_In_Mn'])+float(df['Doubtful_3_In_Mn'])+float(df['Loss_In_Mn']),4)
                df['Provisions_In_Mn']=round(float(df['Gross_NPA_Mn'])-float(df['Net_NPA_In_Mn']),4)
                df['Bank']=bank
                df['Bank_Type']=bank_type
                df['Relevant_Date']=date
                df['Runtime']=datetime.datetime.now()
                #print(df)
                df.to_sql("BANK_OVERALL_NPA_QTLY_DATA", index=False, if_exists='append', con=engine)
                
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='gross advances'
                    end_text='doubtful 1'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_bandhan(df,heading,max_date,bank_type,bank,date,'ilovepdf',2,table_name,catch_error_df)
            return df

        bank='BANDHAN BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['substandard']
        if links_capital.empty:
            print('No new data for BANDHAN')
        else:
            print('New data for BANDHAN')
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)

                max_date,lnk=get_table_max_date_and_links(table_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'lattice',10,300,70)
                        df=data_collection_bandhan(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1,table_name,catch_error_df)
                    except:
                        df=data_collection_bandhan('',search_str,link,max_date,bank_type,bank,date,'camelot',1,table_name,catch_error_df)
                    update_basel_iii(bank,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        def data_collection_baroda(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop,table_name,catch_error_df):
            
            try:
                try:
                    df=get_desired_table(tables,search_str)
                except:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                df=get_desired_table(tables,search_str)
                row_strt=row_col_index_locator(df,['substandard'])[1]
                row=row_col_index_locator(df,['loss'])[1]
                col=row_col_index_locator(df,['substandard'])[0]
                df1=df.iloc[row_strt:row+1,col:]
                df1.reset_index(drop=True,inplace=True)
                row_strt=row_col_index_locator(df,['amount of net npa'])[1]
                row=row_col_index_locator(df,['amount of net npa'])[1]
                col=row_col_index_locator(df,['amount of net npa'])[0]
                df2=df.iloc[row_strt:row+1,col:]
                df2.reset_index(drop=True,inplace=True)
                row_strt=row_col_index_locator(df,['gross advances'])[1]
                row=row_col_index_locator(df,['net advances'])[1]
                col=row_col_index_locator(df,['net advances'])[0]
                df3=df.iloc[row_strt:row+1,col:]
                df3.reset_index(drop=True,inplace=True)
                df1=df1.loc[:, (df1!= '').any(axis=0)]
                df2=df2.loc[:, (df2!= '').any(axis=0)]
                df3=df3.loc[:, (df3!= '#').any(axis=0)]
                df1.columns=[0,1]
                df2.columns=[0,1]
                df3.columns=[0,1]
                df = pd.concat([df1, df2 ,df3], axis=0, ignore_index=True)
                df = df.T  # Transpose the DataFrame
                df.columns = df.iloc[0]  # Set the first row as column names
                df = df[1:]
                df.columns=['Sub_Standard_In_Mn','Doubtful_1_In_Mn','Doubtful_2_In_Mn','Doubtful_3_In_Mn','Loss_In_Mn','Net_NPA_In_Mn','Gross_NPA_To_Gross_Advances_Pct','Net_NPA_To_NET_Advances_Pct']
                df.reset_index(drop = True, inplace = True)
                df['Gross_NPA_Mn']=round(float(df['Sub_Standard_In_Mn'])+float(df['Doubtful_1_In_Mn'])+float(df['Doubtful_2_In_Mn'])+float(df['Doubtful_3_In_Mn'])+float(df['Loss_In_Mn']),4)
                df['Provisions_In_Mn']=round(float(df['Gross_NPA_Mn'])-float(df['Net_NPA_In_Mn']),4)
                df['Bank']=bank
                df['Bank_Type']=bank_type
                df['Relevant_Date']=date
                df['Runtime']=datetime.datetime.now()
                df.to_sql("BANK_OVERALL_NPA_QTLY_DATA", index=False, if_exists='append', con=engine)
                
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='gross advances'
                    end_text='doubtful 1'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_baroda(df,heading,max_date,bank_type,bank,date,'ilovepdf',2,table_name,catch_error_df)
            return df

        bank='BANK OF BARODA'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['substandard','net advances']
        if links_capital.empty:
            print('No new data for BOB')
        else:
            print('New data for BOB')
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)

                max_date,lnk=get_table_max_date_and_links(table_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'stream',10,300,70)
                        df=data_collection_baroda(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1,table_name,catch_error_df)
                    except:
                        df=data_collection_baroda('',search_str,link,max_date,bank_type,bank,date,'camelot',1,table_name,catch_error_df)
                    update_basel_iii(bank,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        def data_collection_bom_central(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop,table_name,catch_error_df):
            
            try:
                try:
                    df=get_desired_table(tables,search_str)
                except:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                if bank=='CENTRAL BANK OF INDIA':
                    row_strt=row_col_index_locator(df,['substandard'])[1]
                    row=row_col_index_locator(df,['movement of npas'])[1]
                    col=row_col_index_locator(df,['substandard'])[0]
                    df=df.iloc[row_strt:row,col:]
                else:
                    row_strt=row_col_index_locator(df,['substandard'])[1]
                    row=row_col_index_locator(df,['net advances'])[1]
                    col=row_col_index_locator(df,['gross advances'])[0]
                    df=df.iloc[row_strt:row+1,col:]
                df=df.loc[:, (df != '').any(axis=0)]
                df.columns = [str(i) for i in range(len(df.columns))]
                df=df[df.iloc[:,1]!='']
                df=df[df.iloc[:,0]!='Total']
                df.reset_index(drop=True,inplace=True)
                df = df.T  # Transpose the DataFrame
                df.columns = df.iloc[0]  # Set the first row as column names
                df = df[1:]
                df.columns=['Sub_Standard_In_Mn','Doubtful_1_In_Mn','Doubtful_2_In_Mn','Doubtful_3_In_Mn','Loss_In_Mn','Net_NPA_In_Mn','Gross_NPA_To_Gross_Advances_Pct','Net_NPA_To_NET_Advances_Pct']
                df.reset_index(drop = True, inplace = True)
                df['Gross_NPA_Mn']=round(float(df['Sub_Standard_In_Mn'])+float(df['Doubtful_1_In_Mn'])+float(df['Doubtful_2_In_Mn'])+float(df['Doubtful_3_In_Mn'])+float(df['Loss_In_Mn']),4)
                df['Provisions_In_Mn']=round(float(df['Gross_NPA_Mn'])-float(df['Net_NPA_In_Mn']),4)
                df['Bank']=bank
                df['Bank_Type']=bank_type
                df['Relevant_Date']=date
                df['Runtime']=datetime.datetime.now()
                df.to_sql("BANK_OVERALL_NPA_QTLY_DATA", index=False, if_exists='append', con=engine)
                
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='gross advances'
                    end_text='doubtful 1'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_bom_central(df,heading,link,max_date,bank_type,bank,date,'ilovepdf',2,table_name,catch_error_df)
            return df

        bank='BANK OF MAHARASHTRA'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['substandard','net advances']
        if links_capital.empty:
            print('No new data for BOM')
        else:
            print('New data for BOM')
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)

                max_date,lnk=get_table_max_date_and_links(table_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'stream',10,300,70)
                        df=data_collection_bom_central(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1,table_name,catch_error_df)
                    except:
                        df=data_collection_bom_central('',search_str,link,max_date,bank_type,bank,date,'camelot',1,table_name,catch_error_df)
                    update_basel_iii(bank,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)

        bank='CENTRAL BANK OF INDIA'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['substandard','net advances']
        if links_capital.empty:
            print('No new data CBI')
        else:
            print('New data for CBI')
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)

                max_date,lnk=get_table_max_date_and_links(table_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'stream',10,200,70)
                        df=data_collection_bom_central(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1,table_name,catch_error_df)
                    except:
                        df=data_collection_bom_central('',search_str,link,max_date,bank_type,bank,date,'camelot',1,table_name,catch_error_df)
                    update_basel_iii(bank,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)

        def data_collection_canara(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop,table_name,catch_error_df):
            
            try:
                try:
                    df=get_desired_table(tables,search_str)
                except:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                row_strt=row_col_index_locator(df,['doubtful 1'])[1]
                row=row_col_index_locator(df,['doubtful 3'])[1]
                col=row_col_index_locator(df,['doubtful 1'])[0]
                df1=df.iloc[row_strt-1:row+2,col:]
                df1.reset_index(drop=True,inplace=True)
                row_strt=row_col_index_locator(df,['net npas'])[1]
                row=row_col_index_locator(df,['net npas'])[1]
                col=row_col_index_locator(df,['net npas'])[0]
                df2=df.iloc[row_strt:row+1,col:]
                df2.reset_index(drop=True,inplace=True)
                row_strt=row_col_index_locator(df,['gross advances'])[1]
                row=row_col_index_locator(df,['net advances'])[1]
                col=row_col_index_locator(df,['net advances'])[0]
                df3=df.iloc[row_strt:row+1,col:]
                df3.reset_index(drop=True,inplace=True)
                df1=df1.loc[:, (df1!= '').any(axis=0)]
                df2=df2.loc[:, (df2!= '').any(axis=0)]
                df3=df3.loc[:, (df3!= '').any(axis=0)]
                # df1.columns=[0,1]
                # df2.columns=[0,1]
                # df3.columns=[0,1]
                df = pd.concat([df1, df2 ,df3], axis=0, ignore_index=True)
                df = df.T  # Transpose the DataFrame
                df.columns = df.iloc[0]  # Set the first row as column names
                df = df[1:]
                df.columns=['Sub_Standard_In_Mn','Doubtful_1_In_Mn','Doubtful_2_In_Mn','Doubtful_3_In_Mn','Loss_In_Mn','Net_NPA_In_Mn','Gross_NPA_To_Gross_Advances_Pct','Net_NPA_To_NET_Advances_Pct']
                df.reset_index(drop = True, inplace = True)
                #Modification:Santonu|May 29,2024
                df['Gross_NPA_To_Gross_Advances_Pct']=df['Gross_NPA_To_Gross_Advances_Pct'].apply(lambda x:str(x).replace('%',''))
                df['Net_NPA_To_NET_Advances_Pct']=df['Net_NPA_To_NET_Advances_Pct'].apply(lambda x:str(x).replace('%',''))
                
                df['Sub_Standard_In_Mn']=df['Sub_Standard_In_Mn'].apply(lambda x:str(x).replace('.',''))
                df['Doubtful_1_In_Mn']=df['Doubtful_1_In_Mn'].apply(lambda x:str(x).replace('.',''))
                df['Doubtful_2_In_Mn']=df['Doubtful_2_In_Mn'].apply(lambda x:str(x).replace('.',''))
                df['Doubtful_3_In_Mn']=df['Doubtful_3_In_Mn'].apply(lambda x:str(x).replace('.',''))
                df['Loss_In_Mn']=df['Loss_In_Mn'].apply(lambda x:str(x).replace('.',''))
                df['Net_NPA_In_Mn']=df['Net_NPA_In_Mn'].apply(lambda x:str(x).replace('.',''))
                
                

                #---------------------------------------------------------------------
                
                df['Gross_NPA_Mn']=round(float(df['Sub_Standard_In_Mn'])+float(df['Doubtful_1_In_Mn'])+float(df['Doubtful_2_In_Mn'])+float(df['Doubtful_3_In_Mn'])+float(df['Loss_In_Mn']),4)
                df['Provisions_In_Mn']=round(float(df['Gross_NPA_Mn'])-float(df['Net_NPA_In_Mn']),4)
                df['Bank']=bank
                df['Bank_Type']=bank_type
                df['Relevant_Date']=date
                df['Runtime']=datetime.datetime.now()
                df.to_sql("BANK_OVERALL_NPA_QTLY_DATA", index=False, if_exists='append', con=engine)
                
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='gross advances'
                    end_text='doubtful 1'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_canara(df,heading,link,max_date,bank_type,bank,date,'ilovepdf',2,table_name,catch_error_df)
            return df

        bank='CANARA BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['substandard','net advances']
        if links_capital.empty:
            print('No new data for CANARA')
        else:
            print('New data for CANARA')
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)

                max_date,lnk=get_table_max_date_and_links(table_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'lattice',10,300,70)
                        df=data_collection_canara(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1,table_name,catch_error_df)
                    except:
                        df=data_collection_canara('',search_str,link,max_date,bank_type,bank,date,'camelot',1,table_name,catch_error_df)
                    update_basel_iii(bank,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        def data_collection_icici(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop,table_name,catch_error_df):
            
            try:
                try:
                    df=get_desired_table(tables,search_str)
                except:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                df1=df.iloc[1:7,:2]
                df1=df1.drop(2)
                df1=df1.T
                df1=df1.iloc[1:]
                df1.columns=['Sub_Standard_In_Mn','Doubtful_1_In_Mn','Doubtful_2_In_Mn','Doubtful_3_In_Mn','Loss_In_Mn']
                df1['Net_NPA_In_Mn']=df.iloc[7,2]
                df1.reset_index(drop=True,inplace=True)
                df2=df.iloc[[-1]]
                df2=df2.drop(0,axis=1)
                df2.reset_index(drop=True,inplace=True)
                df2.columns=['Gross_NPA_To_Gross_Advances_Pct','Net_NPA_To_NET_Advances_Pct']
                final_df=pd.concat([df1,df2],axis=1)
                final_df.iloc[:,:6]=round(final_df.iloc[:,:6].astype('float'),4)
                final_df.fillna(0,inplace=True)
                final_df['Gross_NPA_Mn']=round(final_df['Sub_Standard_In_Mn']+final_df['Doubtful_1_In_Mn']+final_df['Doubtful_2_In_Mn']+final_df['Doubtful_3_In_Mn']+final_df['Loss_In_Mn'],4)
                final_df['Provisions_In_Mn']=round(final_df['Gross_NPA_Mn']-final_df['Net_NPA_In_Mn'],4)
                final_df['Bank']=bank
                final_df['Relevant_Date']=date
                final_df['Bank_Type']=bank_type
                final_df['Runtime']=datetime.datetime.now()
                print(final_df)
                final_df.to_sql("BANK_OVERALL_NPA_QTLY_DATA", index=False, if_exists='append', con=engine)
                
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='gross advances'
                    end_text='doubtful 1'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_icici(df,heading,link,max_date,bank_type,bank,date,'ilovepdf',2,table_name,catch_error_df)
            return df

        bank='ICICI BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['substandard','net advances']
        if links_capital.empty:
            print('No new data for ICICI')
        else:
            print('New data for ICICI')
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)

                max_date,lnk=get_table_max_date_and_links(table_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'lattice',10,300,70)
                        df=data_collection_icici(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1,table_name,catch_error_df)
                    except:
                        df=data_collection_icici('',search_str,link,max_date,bank_type,bank,date,'camelot',1,table_name,catch_error_df)
                    update_basel_iii(bank,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        def data_collection_idbi(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop,table_name,catch_error_df):
            
            try:
                try:
                    df=get_desired_table(tables,search_str)
                except:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                row_strt=row_col_index_locator(df,['substandard'])[1]
                row=row_col_index_locator(df,['to net advances'])[1]
                col=row_col_index_locator(df,['to gross advances'])[0]
                row_del=row_col_index_locator(df,['provision'])[1]
                df=df.iloc[row_strt:row+1,col:]
                df=df.loc[:, (df != '').any(axis=0)]
                df=df.drop(row_del)
                print(df)
                df.columns = [str(i) for i in range(len(df.columns))]
                df=df[df.iloc[:,1]!='']
                df=df[df.iloc[:,0]!='Total']
                df.reset_index(drop=True,inplace=True)
                df = df.T  # Transpose the DataFrame
                df.columns = df.iloc[0]  # Set the first row as column names
                df = df[1:]
                df.columns=['Sub_Standard_In_Mn','Doubtful_1_In_Mn','Doubtful_2_In_Mn','Doubtful_3_In_Mn','Loss_In_Mn','Net_NPA_In_Mn','Gross_NPA_To_Gross_Advances_Pct','Net_NPA_To_NET_Advances_Pct']
                df.reset_index(drop = True, inplace = True)
                df['Gross_NPA_Mn']=round(float(df['Sub_Standard_In_Mn'])+float(df['Doubtful_1_In_Mn'])+float(df['Doubtful_2_In_Mn'])+float(df['Doubtful_3_In_Mn'])+float(df['Loss_In_Mn']),4)
                df['Provisions_In_Mn']=round(float(df['Gross_NPA_Mn'])-float(df['Net_NPA_In_Mn']),4)
                df['Bank']=bank
                df['Bank_Type']=bank_type
                df['Relevant_Date']=date
                df['Runtime']=datetime.datetime.now()
                df.to_sql("BANK_OVERALL_NPA_QTLY_DATA", index=False, if_exists='append', con=engine)
                
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='gross advances'
                    end_text='doubtful 1'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_idbi(df,heading,link,max_date,bank_type,bank,date,'ilovepdf',2,table_name,catch_error_df)
            return df

        bank='IDBI BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['substandard','net advances']
        if links_capital.empty:
            print('No new data for IDBI')
        else:
            print('New data for IDBI')
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)

                max_date,lnk=get_table_max_date_and_links(table_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'lattice',10,300,70)
                        df=data_collection_idbi(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1,table_name,catch_error_df)
                    except:
                        df=data_collection_idbi('',search_str,link,max_date,bank_type,bank,date,'camelot',1,table_name,catch_error_df)
                    update_basel_iii(bank,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        def data_collection_indusind(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop,table_name,catch_error_df):
            
            try:
                try:
                    df=get_desired_table(tables,search_str)
                except:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                row_strt=row_col_index_locator(df,['substandard'])[1]
                row=row_col_index_locator(df,['to net advances'])[1]
                col=row_col_index_locator(df,['to gross advances'])[0]
                df=df.iloc[row_strt:row+1,col:]
                df=df.loc[:, (df != '').any(axis=0)]
                rows_to_delete = df[0].isin(["Doubtful","Total","Particulars", "Gross Standard Restructured Assets*"])
                # Delete rows
                df = df[~rows_to_delete]
                df.replace("#", 0, inplace=True)
                print(df)
                df.columns = [str(i) for i in range(len(df.columns))]
                df=df[df.iloc[:,1]!='']
                df=df[df.iloc[:,0]!='Total']
                df.reset_index(drop=True,inplace=True)
                df = df.T  # Transpose the DataFrame
                df.columns = df.iloc[0]  # Set the first row as column names
                df = df[1:]
                df.columns=['Sub_Standard_In_Mn','Doubtful_1_In_Mn','Doubtful_2_In_Mn','Doubtful_3_In_Mn','Loss_In_Mn','Gross_NPA_Mn','Gross_NPA_To_Gross_Advances_Pct','Net_NPA_In_Mn','Net_NPA_To_NET_Advances_Pct']
                df.reset_index(drop = True, inplace = True)
                numeric_columns = ['Gross_NPA_Mn','Net_NPA_In_Mn']
                df[numeric_columns] = df[numeric_columns].apply(lambda x: x.astype(float))
                # Calculate 'Provisions_In_Mn'
                df['Provisions_In_Mn'] = df['Gross_NPA_Mn'] - df['Net_NPA_In_Mn']
                df.replace(0, np.nan, inplace=True)
                df['Bank']=bank
                df['Bank_Type']=bank_type
                df['Relevant_Date']=date
                df['Runtime']=datetime.datetime.now()
                df.to_sql("BANK_OVERALL_NPA_QTLY_DATA", index=False, if_exists='append', con=engine)
                
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='gross advances'
                    end_text='doubtful 1'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_indusind(df,heading,link,max_date,bank_type,bank,date,'ilovepdf',2,table_name,catch_error_df)
            return df


        bank='INDUSIND BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['substandard','net advances']
        if links_capital.empty:
            print('No new data for INDUSIND')
        else:
            print('New data for INDUSIND')
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)

                max_date,lnk=get_table_max_date_and_links(table_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'lattice',10,300,70)
                        df=data_collection_indusind(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1,table_name,catch_error_df)
                    except:
                        df=data_collection_indusind('',search_str,link,max_date,bank_type,bank,date,'camelot',1,table_name,catch_error_df)
                    update_basel_iii(bank,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        def data_collection_idfc(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop,table_name,catch_error_df):
            
            try:
                try:
                    df=get_desired_table(tables,search_str)
                except:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                row_strt=row_col_index_locator(df,['substandard'])[1]
                row=row_col_index_locator(df,['to net advances'])[1]
                col=row_col_index_locator(df,['to gross advances'])[0]

                df.columns = df.loc[0]
                df=df.iloc[row_strt:row+1,col:]
                df=df.loc[:, (df != '').any(axis=0)]

                rows_to_delete = df[df.columns[0]].isin(["Doubtful","Total","Particulars", "Gross Standard Restructured Assets*"])
                # Delete rows
                df = df[~rows_to_delete]
                df.replace("#", 0, inplace=True)

                df=df[df.iloc[:,1]!='']
                df=df[df.iloc[:,0]!='Total']
                df.reset_index(drop=True,inplace=True)

                melt_df = pd.melt(df, id_vars=df.columns[0], value_vars=df.columns[1:], var_name='Type', value_name='Value')

                melt_df = melt_df[~(melt_df['Type'] == 'Net NPA')]
                del melt_df['Type']
                melt_df.columns = [str(i) for i in range(len(melt_df.columns))]
                tmelt_df = melt_df.T
                tmelt_df.columns = tmelt_df.iloc[0]  # Set the first row as column names
                tmelt_df = tmelt_df[1:]

                tmelt_df.columns=['Sub_Standard_In_Mn','Doubtful_1_In_Mn','Doubtful_2_In_Mn','Doubtful_3_In_Mn','Loss_In_Mn','Gross_NPA_Mn','Gross_NPA_To_Gross_Advances_Pct','Net_NPA_In_Mn','Net_NPA_To_NET_Advances_Pct']
                tmelt_df.reset_index(drop = True, inplace = True)
                numeric_columns = ['Gross_NPA_Mn','Net_NPA_In_Mn']
                tmelt_df[numeric_columns] = tmelt_df[numeric_columns].apply(lambda x: x.astype(float))
                # Calculate 'Provisions_In_Mn'
                tmelt_df['Provisions_In_Mn'] = tmelt_df['Gross_NPA_Mn'] - tmelt_df['Net_NPA_In_Mn']
                tmelt_df.replace(0, np.nan, inplace=True)
                tmelt_df['Bank']=bank
                tmelt_df['Bank_Type']=bank_type
                tmelt_df['Relevant_Date']=date
                tmelt_df['Runtime']=datetime.datetime.now()
                tmelt_df.to_sql("BANK_OVERALL_NPA_QTLY_DATA", index=False, if_exists='append', con=engine)
                
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='gross advances'
                    end_text='doubtful 1'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_idfc(df,heading,link,max_date,bank_type,bank,date,'ilovepdf',2,table_name,catch_error_df)
            return df

        bank='IDFC FIRST BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['substandard','net advances']
        if links_capital.empty:
            print('No new data for IDFC')
        else:
            print('New data for IDFC')
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)

                max_date,lnk=get_table_max_date_and_links(table_name,bank)
                try:
                    tables = extract_tables_from_pdf(link,True,'lattice',10,300,70)
                    df=data_collection_idfc(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1,table_name,catch_error_df)
                    update_basel_iii(bank,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        def data_collection_kotak(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop,table_name,catch_error_df):
            
            try:
                try:
                    df=get_desired_table(tables,search_str)
                except:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                df=get_desired_table(tables,search_str)
                row_strt=row_col_index_locator(df,['substandard'])[1]
                row=row_col_index_locator(df,['npa ratio'])[1]
                col=row_col_index_locator(df,['substandard'])[0]
                df=df.iloc[row_strt:row+1,col:]
                df=df.loc[:, (df != '').any(axis=0)]
                df.columns = [str(i) for i in range(len(df.columns))]
                df=df[df.iloc[:,1]!='#']
                df.reset_index(drop=True,inplace=True)
                df = df[df.ne('').any(axis=1)]
                df1=df.iloc[:,:2]
                df1.columns=[0,1]
                row_tot=row_col_index_locator(df,['total'])[1]
                df2=df.iloc[row_tot:,[0,2]]
                df2.columns=[0,1]
                df=pd.concat([df1,df2],axis=0)
                df=df.T
                df.columns=df.iloc[0,:]
                df = df[1:]
                df.columns=['Sub_Standard_In_Mn','Doubtful_1_In_Mn','Doubtful_2_In_Mn','Doubtful_3_In_Mn','Loss_In_Mn','Gross_NPA_Mn','Gross_NPA_To_Gross_Advances_Pct','Net_NPA_In_Mn','Net_NPA_To_NET_Advances_Pct']
                df=df.astype('float')
                df['Provisions_In_Mn']=round(df['Gross_NPA_Mn']-df['Net_NPA_In_Mn'],4)
                df['Bank']=bank
                df['Bank_Type']=bank_type
                df['Relevant_Date']=date
                df['Runtime']=datetime.datetime.now()
                #print(df)
                df.to_sql("BANK_OVERALL_NPA_QTLY_DATA", index=False, if_exists='append', con=engine)
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='substandard'
                    end_text='doubtful 1'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_kotak(df,heading,link,max_date,bank_type,bank,date,'ilovepdf',2,table_name,catch_error_df)
            return df

        bank='KOTAK MAHINDRA BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['substandard','net advances']
        if links_capital.empty:
            print('No new data for KOTAK')
        else:
            print('New data for KOTAK')
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)

                max_date,lnk=get_table_max_date_and_links(table_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'lattice',10,300,70)
                        df=data_collection_kotak(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1,table_name,catch_error_df)
                    except:
                        df=data_collection_kotak('',search_str,link,max_date,bank_type,bank,date,'camelot',1,table_name,catch_error_df)
                    update_basel_iii(bank,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        def data_collection_rbl(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop,table_name,catch_error_df):
            
            try:
                try:
                    df=get_desired_table(tables,search_str)
                except:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                row_strt=row_col_index_locator(df,['gross npas'])[1]
                row=row_col_index_locator(df,['total'])[1]
                col=row_col_index_locator(df,['gross npas'])[0]
                df=df.iloc[row_strt:row+1,col:]
                row_strt=row_col_index_locator(df,['Sub-standard'])[1]
                row=row_col_index_locator(df,['loss'])[1]
                df1=df.iloc[row_strt:row+1,col:]
                row_strt=row_col_index_locator(df,['gross npas'])[1]
                row=row_col_index_locator(df,['net npas'])[1]
                df2=df.iloc[row_strt:row+1,col:]
                row_strt=row_col_index_locator(df,['npa account'])[1]
                row=row_col_index_locator(df,['classification of'])[1]
                df3=df.iloc[row_strt+1:row,col:]
                df_final=pd.concat([df1,df3,df2],axis=0)
                rows_to_delete = df[0].isin(["Doubtful*","Total","Particulars", "Gross Standard Restructured Assets*"])
                # Delete rows
                df_final = df_final[~rows_to_delete]
                df_final.reset_index(drop=True,inplace=True)
                df = df_final.T  # Transpose the DataFrame
                df.columns = df.iloc[0]  # Set the first row as column names
                df = df[1:]
                df.columns=['Sub_Standard_In_Mn','Doubtful_1_In_Mn','Doubtful_2_In_Mn','Doubtful_3_In_Mn','Loss_In_Mn','Net_NPA_In_Mn','Gross_NPA_To_Gross_Advances_Pct','Net_NPA_To_NET_Advances_Pct']
                df.reset_index(drop = True, inplace = True)
                df=df.astype('float')
                df['Gross_NPA_Mn']=round(float(df['Sub_Standard_In_Mn'])+float(df['Doubtful_1_In_Mn'])+float(df['Doubtful_2_In_Mn'])+float(df['Doubtful_3_In_Mn'])+float(df['Loss_In_Mn']),4)
                df['Provisions_In_Mn']=round(df['Gross_NPA_Mn']-df['Net_NPA_In_Mn'],4)
                df['Bank']=bank
                df['Bank_Type']=bank_type
                df['Relevant_Date']=date
                df['Runtime']=datetime.datetime.now()
                #print(df)
                df.to_sql("BANK_OVERALL_NPA_QTLY_DATA", index=False, if_exists='append', con=engine)
            except Exception as e:
                if loop < 2:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='sub-standard'
                    end_text='doubtful 1'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_rbl(df,heading,link,max_date,bank_type,bank,date,'ilovepdf',2,table_name,catch_error_df)
            return df

        bank='RBL BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['substandard','net advances']
        if links_capital.empty:
            print('No new data for RBL')
        else:
            print('New data for RBL')
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)

                max_date,lnk=get_table_max_date_and_links(table_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'stream',10,300,70)
                        df=data_collection_rbl(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1,table_name,catch_error_df)
                    except:
                        df=data_collection_rbl('',search_str,link,max_date,bank_type,bank,date,'camelot',1,table_name,catch_error_df)
                    update_basel_iii(bank,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        def data_collection_pnb_city(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop,table_name,catch_error_df):
            
            try:
                try:
                    df=get_desired_table(tables,search_str)
                except:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                try:
                    row_strt=row_col_index_locator(df,['sub standard'])[1]
                except:
                    row_strt=row_col_index_locator(df,['substandard'])[1]
                row=row_col_index_locator(df,['to net advances'])[1]
                col=row_col_index_locator(df,['to gross advances'])[0]
                df=df.iloc[row_strt:row+1,col:]
                df=df.loc[:, (df != '').any(axis=0)]
                df=df.iloc[:,:2]
                rows_to_delete = df[0].isin(["Doubtful","Total NPAs (Gross)","Particulars", "Gross Standard Restructured Assets*"])
                # Delete rows
                df = df[~rows_to_delete]
                df.replace("#", 0, inplace=True)
                print(df)
                df.columns = [str(i) for i in range(len(df.columns))]
                df=df[df.iloc[:,1]!='']
                df=df[df.iloc[:,0]!='Total']
                df.reset_index(drop=True,inplace=True)
                df = df.T  # Transpose the DataFrame
                df.columns = df.iloc[0]  # Set the first row as column names
                df = df[1:]
                if bank=='CITY UNION BANK':
                    df.columns=['Sub_Standard_In_Mn','Doubtful_1_In_Mn','Doubtful_2_In_Mn','Doubtful_3_In_Mn','Loss_In_Mn','Gross_NPA_Mn',
                    'Gross_NPA_To_Gross_Advances_Pct','Net_NPA_To_NET_Advances_Pct']
                    df['Net_NPA_In_Mn']=0
                else:
                    df.columns=['Sub_Standard_In_Mn','Doubtful_1_In_Mn','Doubtful_2_In_Mn','Doubtful_3_In_Mn','Loss_In_Mn','Gross_NPA_Mn',
                        'Net_NPA_In_Mn','Gross_NPA_To_Gross_Advances_Pct','Net_NPA_To_NET_Advances_Pct']

                df=df.astype('float')
                df['Provisions_In_Mn']=round(df['Gross_NPA_Mn']-df['Net_NPA_In_Mn'],4)
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Bank_Type']=bank_type
                df['Runtime']=datetime.datetime.now()
                print(df)
                df.to_sql("BANK_OVERALL_NPA_QTLY_DATA", index=False, if_exists='append', con=engine)
            except Exception as e:
                if loop < 2:
                    print(e)
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
                    catch_error_df.to_sql("BANK_ERROR_TABLE", index=False, if_exists='append', con=engine)
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='sub standard'
                    end_text='doubtful 1'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_pnb_city(df,heading,link,max_date,bank_type,bank,date,'ilovepdf',2,table_name,catch_error_df)
            return df

        bank='PUNJAB NATIONAL BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['sub standard','net npa','net advances']
        if links_capital.empty:
            print('No new data for PNB')
        else:
            print('New data for PNB')
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)

                max_date,lnk=get_table_max_date_and_links(table_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'lattice',10,300,70)
                        df=data_collection_pnb_city(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1,table_name,catch_error_df)
                    except:
                        df=data_collection_pnb_city('',search_str,link,max_date,bank_type,bank,date,'camelot',1,table_name,catch_error_df)
                    update_basel_iii(bank,date)
                except Exception as e:
                    print(e)
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)

        bank='CITY UNION BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['sub standard','net npa','net advances']
        if links_capital.empty:
            print('No new data for CUB')
        else:
            print('New data for CUB')
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                link=link.replace(' ','%20')
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                print(link)

                max_date,lnk=get_table_max_date_and_links(table_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,True,'lattice',10,300,70)
                        print('city:', tables)
                        df=data_collection_pnb_city(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1,table_name,catch_error_df)
                    except:
                        df=data_collection_pnb_city('',search_str,link,max_date,bank_type,bank,date,'camelot',1,table_name,catch_error_df)
                    update_basel_iii(bank,date)
                except Exception as e:
                    print(e)
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)

        def data_collection_ujjivan(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop,table_name,catch_error_df):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                indexes=[]
                indexes.append(df1[df1[req_col].str.lower().str.contains('substandard')].index[0])
                indexes.append(df1[df1[req_col].str.lower().str.contains('doubtful')].index[0])
                indexes.append(df1[df1[req_col].str.lower().str.contains('loss')].index[0])
                df1=df1.iloc[indexes]
                df1=df1.replace('',np.nan).dropna(how='all',thresh=3,axis=1)
                df1=df1.T

                df1=df1.iloc[[-1]]
                for col in df1.columns:
                    df1[col]=df1[col].str.replace(' ','')
                df1=df1.astype('float')
                df1.columns=['Sub_Standard_In_Mn','Dropped','Loss_In_Mn']
                df1['Gross_NPA_Mn']=df1.sum(axis=1)
                df1.drop('Dropped',axis=1,inplace=True)
                df1=df1*0.1
                df1.reset_index(drop=True,inplace=True)


                for i in range(tables.n):
                    for col in range(tables[i].df.shape[1]):
                        if (tables[i].df[col].str.lower()=='net npa').any():
                            print(i)
                            df2=tables[i].df.copy()
                            req_col=col
                            df2=df2.iloc[:,req_col:]
                            break


                indexes=[]
                indexes.append(df2[df2[req_col].str.lower().str.contains('net npa')].index[0])
                df2=df2.iloc[indexes]
                df2=df2.replace('',np.nan).dropna(axis=1)
                df2=df2.T
                df2.columns=['Net_NPA_In_Mn']
                df2=df2.iloc[[-1]]
                df2['Net_NPA_In_Mn']=df2['Net_NPA_In_Mn'].str.replace(' ','')
                df2=df2.astype('float')
                df2=df2*0.1
                df2.reset_index(drop=True,inplace=True)

                for i in range(tables.n):
                    for col in range(tables[i].df.shape[1]):
                        if tables[i].df[col].str.lower().str.contains('net advances').any():
                            print(i)
                            df3=tables[i].df.copy()
                            req_col=col
                            df3=df3.iloc[:,req_col:]
                            break


                indexes=[]
                indexes.append(df3[df3[req_col].str.lower().str.contains('gross advances')].index[0])
                indexes.append(df3[df3[req_col].str.lower().str.contains('net advances')].index[0])
                df3=df3.iloc[indexes]
                df3=df3.replace('',np.nan).dropna(how='all',axis=1)
                df3=df3.T

                df3=df3.iloc[[-1]]
                for col in df3.columns:
                    df3[col]=df3[col].str.replace(' ','')
                df3=df3.astype('float')
                df3.columns=['Gross_NPA_To_Gross_Advances_Pct','Net_NPA_To_NET_Advances_Pct']
                df3.reset_index(drop=True,inplace=True)


                final_df=pd.concat([df1,df2,df3],axis=1)
                final_df=final_df.replace('',np.nan)
                final_df=final_df.fillna(method='bfill')
                final_df['Doubtful_1_In_Mn']=np.nan
                final_df['Doubtful_2_In_Mn']=np.nan
                final_df['Doubtful_3_In_Mn']=np.nan
                final_df['Provisions_In_Mn']=round(final_df['Gross_NPA_Mn']-final_df['Net_NPA_In_Mn'],4)
                final_df['Bank']=bank
                final_df['Relevant_Date']=date
                final_df['Bank_Type']=bank_type
                final_df['Runtime']=datetime.datetime.now()
                print(final_df)
                final_df.to_sql("BANK_OVERALL_NPA_QTLY_DATA", index=False, if_exists='append', con=engine)
                print('NPA DF Uploaded')
            except:
                if loop <2:
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='coal'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=ddata_collection_ujjivan(tables,search_str,link,max_date,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return df

        bank='UJJIVAN SMALL FINANCE BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['doubtful', 'gross npa']
        if links_capital.empty:
            print('No new data for UJJIVAN')
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
                        tables = extract_tables_from_pdf(link,True,'lattice',10,300,70)
                        df=data_collection_ujjivan(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1,table_name,catch_error_df)

                    except:
                        df=data_collection_ujjivan('',search_str,link,max_date,bank_type,bank,date,'camelot',1,table_name,catch_error_df)
                    update_basel_iii(bank,date)
                except Exception as e:
                    print(e)
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)

        MySql_CH.ch_truncate_and_insert('BANK_OVERALL_NPA_QTLY_DATA')
        log.job_end_log(table_name,job_start_time, no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')