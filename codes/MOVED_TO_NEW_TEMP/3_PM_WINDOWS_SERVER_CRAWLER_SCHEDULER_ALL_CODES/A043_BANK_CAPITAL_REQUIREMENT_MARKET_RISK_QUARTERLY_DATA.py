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
    connection.execute("update BANK_BASEL_III_QUARTERLY_LINKS set Bank_Capital_Requirement_Market_Risk_Status = 'COMPLETED' where Relevant_Date = '"+date+"' and Bank= '"+bank_name+"'")
    

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
    table_name = 'BANK_CAPITAL_REQUIREMENT_MARKET_RISK_QUARTERLY_DATA'
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
        table_name='BANK_CAPITAL_REQUIREMENT_MARKET_RISK_QUARTERLY_DATA'
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
                col=row_col_index_locator(df,['market risk'])[0]
                df=row_modificator(df,['market risk'],col,row_del=False,keep_row=True)
                df=df.iloc[:,col:]
                df.columns=['Standardised_Duration_Approach','Amounts_In_Million']
                df['Amounts_In_Million']=pd.to_numeric(df['Amounts_In_Million'])

                df['Bank']=bank
                df['Relevant_Date']=date
                df['Runtime']=pd.to_datetime('now')
                df=df[['Bank','Standardised_Duration_Approach','Amounts_In_Million','Relevant_Date','Runtime']]
                df['Bank_Type']=bank_type
                df.reset_index(drop=True,inplace=True)
                df=clean_table(df)
                df_final=df['Relevant_Date']>max_date
                df_final.to_sql("BANK_CAPITAL_REQUIREMENT_MARKET_RISK_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print(len(df_final))
                print('Done for BANK---> ',bank)
                print('Done for DATE---> ',date)
            except:
                if loop < 2:
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

        def data_collection_common(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                    df=tables
                    col=row_col_index_locator(df,['equity risk'])[0]
                    df=df.iloc[:,col:]
                    df.reset_index(drop=True,inplace=True)
                if bank=='ICICI BANK' or bank=='KOTAK MAHINDRA BANK'  or bank=='UJJIVAN SMALL BANK':
                    row1=row_col_index_locator(df,['equity position risk'])[1]
                    col=row_col_index_locator(df,['interest rate risk'])[0]
                    row=row_col_index_locator(df,['interest rate risk'])[1]
                else:
                    if bank=='BANK OF BARODA':
                        row1=row_col_index_locator(df,['equity'])[1]
                    else:
                        row1=row_col_index_locator(df,['equity risk'])[1]
                    col=row_col_index_locator(df,['interest rate risk'])[0]
                    row=row_col_index_locator(df,['interest rate risk'])[1]
                if bank=='KOTAK MAHINDRA BANK' or bank=='CITY UNION BANK':
                    df1=df.iloc[row:row1+2,col:]
                else:
                    df1=df.iloc[row:row1+1,col:]
                df1=clean_dataframe(df1)
                df1.reset_index(drop=True,inplace=True)
                df=df1
                try:
                    df.columns=['Standardised_Duration_Approach','Amounts_In_Million']
                except:
                    if bank=='HDFC BANK':
                        df=df.iloc[:,:len(df.columns)-1]
                        df.columns=['Standardised_Duration_Approach','Amounts_In_Million']
                df['Standardised_Duration_Approach'] = df['Standardised_Duration_Approach'].str.replace('•', '')
                df['Amounts_In_Million']=pd.to_numeric(df['Amounts_In_Million'],errors='coerce')
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Runtime']=pd.to_datetime('now')
                df=df[['Bank','Standardised_Duration_Approach','Amounts_In_Million','Relevant_Date','Runtime']]
                df['Bank_Type']=bank_type
                df.reset_index(drop=True,inplace=True)
                df_final=df['Relevant_Date']>max_date
                df_final.to_sql("BANK_CAPITAL_REQUIREMENT_MARKET_RISK_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print(len(df_final))
                print('Done for BANK---> ',bank)
                print('Done for DATE---> ',date)
            except:
                if loop < 2:
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='equity risk'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_common(df,search_str,link,max_date,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return df

        bank='HDFC BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['equity risk']
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

        bank='AXIS BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['equity risk']
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

        bank='INDIAN OVERSEAS BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['equity risk']
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
                        df=data_collection_common(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_common('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
                    
        def data_collection_sbi_central_union(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                col=row_col_index_locator(df,['interest rate'])[0]
                row1=row_col_index_locator(df,['interest rate'])[1]
                row2=row_col_index_locator(df,['foreign exchange'])[1]
                if bank=='UNION BANK OF INDIA':
                    row3=row_col_index_locator(df,['equity position risk'])[1]
                else:
                    row3=row_col_index_locator(df,['equity risk'])[1]
                if row1==row2==row3:
                    df1=df.loc[[row1]]
                    columns = ['Standardised_Duration_Approach', 'Amounts_In_Million']
                    df = pd.DataFrame(columns=columns)
                    index=row_col_index_locator(df1,['interest'])
                    Values=list(df1[index[0]][index[1]].split('  '))
                    act_values=['interest rate risk','foreign exchange risk(including gold)','equity position risk','equity risk','foreign exchange risk (including gold)']
                    index=row_col_index_locator(df1,['rs'])
                    try:
                        value=re.findall('\d+.{3}',df1[index[0]][index[1]])
                    except:
                        value=re.findall('\d+.{3}',df1[1][0])
                    i=0
                    for val in range(len(Values)):
                        com_val=remove_special_characters(Values[val])
                        print(com_val)
                        if com_val.lower().strip() in act_values:
                            df.loc[i, 'Standardised_Duration_Approach']=com_val.strip()
                            df.loc[i, 'Amounts_In_Million']=value[i]
                            i+=1

                elif row1==row2!=row3:
                    df1=df.iloc[row1:row1+1,col:]
                    df2=df.iloc[row3:row3+1,col:]
                    try:
                        df1.columns=['Standardised_Duration_Approach','Amounts_In_Million']
                    except:
                        df1=df1.replace('',np.nan)
                        df1.dropna(axis=1,how='all',inplace=True)
                        df1.columns=['Standardised_Duration_Approach','Amounts_In_Million']
                    print(df1)
                    df1.reset_index(drop=True,inplace=True)
                    columns = ['Standardised_Duration_Approach', 'Amounts_In_Million']
                    df = pd.DataFrame(columns=columns)
                    Values=list(df1['Amounts_In_Million'][0].split('  '))
                    print(df)
                    print(Values)
                    numeric_values = []
                    pattern = r'(?<!\S)\d+(?:\.\d+)?(?!\S)'  # regex pattern to match numeric values
                    for item in Values:
                        matches = re.findall(pattern, item)
                        numeric_values.extend(matches)
                    particulars=['Interest Rate Risk','Foreign Exchange Risk(including gold)']
                    i=0
                    for val in range(len(numeric_values)):
                        df.loc[i, 'Standardised_Duration_Approach']=particulars[i]
                        df.loc[i, 'Amounts_In_Million']=numeric_values[i]
                        i+=1
                    df2=df2.replace('',np.nan)
                    df2.dropna(axis=1,how='all',inplace=True)
                    df2.reset_index(drop=True,inplace=True)
                    value_equity=df2[1].str.extract(r'(\d+(\.\d+)?)')
                    df.loc[i, 'Amounts_In_Million']=value_equity[0][0]
                    df.loc[i, 'Standardised_Duration_Approach']=df2[0][0]
                else:
                    df1=df.iloc[row1:row3+1,col:]
                    df1=df1.replace('',np.nan)
                    df1.dropna(axis=1,how='all',inplace=True)
                    df1.reset_index(drop=True,inplace=True)
                    df=df1
                    try:
                        df.columns=['Standardised_Duration_Approach','Amounts_In_Million']
                    except:
                        df=df.iloc[:,:len(df.columns)-1]
                        df.columns=['Standardised_Duration_Approach','Amounts_In_Million']
                    df['Amounts_In_Million']=pd.to_numeric(df['Amounts_In_Million'],errors='coerce')
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Runtime']=datetime.datetime.now()
                df=df[['Bank','Standardised_Duration_Approach','Amounts_In_Million','Relevant_Date','Runtime']]
                df['Bank_Type']=bank_type
                df.reset_index(drop=True,inplace=True)
                df['Amounts_In_Million']=df['Amounts_In_Million'].apply(lambda x:str(x))
                df=clean_table(df)
                df_final=df['Relevant_Date']>max_date
                df_final.to_sql("BANK_CAPITAL_REQUIREMENT_MARKET_RISK_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print(len(df_final))
                print('Done for BANK---> ',bank)
                print('Done for DATE---> ',date)
            except:
                if loop < 2:
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='equity risk'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_sbi_central_union(tables,heading,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return df 


        # In[ ]:


        bank='STATE BANK OF INDIA'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['equity risk']
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
                        tables = extract_tables_from_pdf(link,False,'stream',20,100,40)
                        df=data_collection_sbi_central_union(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_sbi_central_union('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        # In[13]:


        bank='CENTRAL BANK OF INDIA'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['equity risk']
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
                        tables = extract_tables_from_pdf(link,False,'lattice',10,200,40)
                        df=data_collection_sbi_central_union(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_sbi_central_union('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        # In[14]:


        bank='UNION BANK OF INDIA'
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
                        tables = extract_tables_from_pdf(link,False,'lattice',10,200,40)
                        df=data_collection_sbi_central_union(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_sbi_central_union('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        # In[15]:


        bank='CANARA BANK'
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


        # In[16]:


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


        # In[17]:


        bank='FEDERAL BANK'
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
                        tables = extract_tables_from_pdf(link,False,'stream',10,100,40)
                        df=data_collection_common(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_common('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        # In[59]:


        bank='CITY UNION BANK'
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
                        tables = extract_tables_from_pdf(link,False,'stream',10,100,40)
                        df=data_collection_common(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_common('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        # In[61]:


        bank='IDBI BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['interest rate risk']
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


        # In[62]:


        bank='RBL BANK'
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


        # In[64]:


        bank='INDUSIND BANK'
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


        # In[67]:


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


        # In[69]:


        bank='UJJIVAN SMALL BANK'
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


        # In[20]:


        def data_collection_bandhan(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                search_str=['market risk']
                col=row_col_index_locator(df,['market risk:'])[0]
                df=row_modificator(df,['interest rate risk','Foreign exchange Risk','Equity Risk'],col,row_del=False,keep_row=True)

                df.columns=['Standardised_Duration_Approach','Amounts_In_Million']
                df['Amounts_In_Million']=pd.to_numeric(df['Amounts_In_Million'])*10
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Runtime']=pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
                df=df[['Bank','Standardised_Duration_Approach','Amounts_In_Million','Relevant_Date','Runtime']]
                df['Bank_Type']=bank_type
                df.reset_index(drop=True,inplace=True)
                df=clean_table(df)
                df_final=df['Relevant_Date']>max_date
                df_final.to_sql("BANK_CAPITAL_REQUIREMENT_MARKET_RISK_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print(len(df_final))
                print('Done for BANK---> ',bank)
                print('Done for DATE---> ',date)
            except:
                if loop<2:
                    print('This might happen because camelot failed trying with ilovepdf')
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='market risk'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_bandhan(df,heading,link,max_date,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return df


        # In[21]:


        bank='BANDHAN BANK'
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
                        tables = extract_tables_from_pdf(link,False,'stream',10,100,40)
                        df=data_collection_bandhan(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_bandhan('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        # In[22]:


        def data_collection_punjab(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                col=row_col_index_locator(df,['interest rate'])[0]
                col_last=row_col_index_locator(df,['Amount'])[0]

                d1=[{'interest rate risk':'Interest rate risk'},
                    {'Foreign exchange Risk':'Foreign exchange risk including gold'},
                    {'Equity Risk':'Equity position risk'},
                   ]

                df=row_modificator(df,d1,col,row_del=False,keep_row=True,update_row=True)
                df=df.iloc[:,:col_last+1]
                df.columns=['Standardised_Duration_Approach','Amounts_In_Million']

                df['Bank']='PUNJAB NATIONAL BANK'
                df['Relevant_Date']=date
                df['Runtime']=pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
                df=df[['Bank','Standardised_Duration_Approach','Amounts_In_Million','Relevant_Date','Runtime']]
                df['Bank_Type']=bank_type
                df.reset_index(drop=True,inplace=True)
                df=clean_table(df)
                df_final=df['Relevant_Date']>max_date
                df_final.to_sql("BANK_CAPITAL_REQUIREMENT_MARKET_RISK_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print(len(df_final))
                print('Done for BANK---> ',bank)
                print('Done for DATE---> ',date)

            except:
                if loop<2:
                    print('This might happen because camelot failed trying with ilovepdf')
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='interest rate'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_punjab(df,heading,link,max_date,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return df


        # In[23]:


        bank='PUNJAB NATIONAL BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['interest rate']
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


        # In[30]:


        def data_collection_uco(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
            
                df=make_data_from_rows(df,'Capital requirements for market risk')
                col=row_col_index_locator(df,['standardized duration approach'])[0]

                d1=[{'interest rate risk':'Interest rate risk'},
                    {'Foreign exchange Risk':'Foreign exchange risk including gold'},
                    {'Equity Risk':'Equity position risk'},
                   ]

                df=row_modificator(df,d1,col,row_del=False,keep_row=True,update_row=True)
                df.columns=['Standardised_Duration_Approach','Amounts_In_Million']

                df['Bank']=bank
                df['Relevant_Date']=date
                df['Runtime']=datetime.datetime.now()
                df['Amounts_In_Million']=df['Amounts_In_Million'].apply(lambda x:round(pd.to_numeric(re.match(r'\d+\.\d{2}',x)[0])*10,2))
                df=df[['Bank','Standardised_Duration_Approach','Amounts_In_Million','Relevant_Date','Runtime']]
                df.reset_index(drop=True,inplace=True)
                for j in df.columns:
                    if j!='Runtime':
                        df[j]=df[j].apply(lambda x:str(x))
                df['Bank_Type']=bank_type
                df=clean_table(df)
                df_final=df['Relevant_Date']>max_date
                df_final.to_sql("BANK_CAPITAL_REQUIREMENT_MARKET_RISK_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print(len(df_final))
                print('Done for BANK---> ',bank)
                print('Done for DATE---> ',date)
            
            except:
                if loop<2:
                    print('This might happen because camelot failed trying with ilovepdf')
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='market risk'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_uco(df,heading,link,max_date,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return df


        # In[32]:


        bank='UCO BANK'
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
                        tables = extract_tables_from_pdf(link,False,'stream',15,200,40)
                        df=data_collection_uco(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_uco('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)



        def data_collection_idfc(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                col=row_col_index_locator(df,['interest'])[0]
                df=row_modificator(df,['interest rate risk','Foreign exchange Risk','Equity Risk'],col,2,row_del=False,keep_row=True,update_row=False)
                df.columns=['Standardised_Duration_Approach','Amounts_In_Million']
                df['Amounts_In_Million']=pd.to_numeric(df['Amounts_In_Million'])*10

                df['Bank']=bank
                df['Relevant_Date']=date
                df['Runtime']=pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
                df=df[['Bank','Standardised_Duration_Approach','Amounts_In_Million','Relevant_Date','Runtime']]
                df['Bank_Type']=bank_type
                df.reset_index(drop=True,inplace=True)
                df=clean_table(df)
                df_final=df['Relevant_Date']>max_date
                df_final.to_sql("BANK_CAPITAL_REQUIREMENT_MARKET_RISK_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print(len(df_final))
                print('Done for BANK---> ',bank)
                print('Done for DATE---> ',date)

            except:
                if loop<2:
                    print('This might happen because camelot failed trying with ilovepdf')
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='interest rate'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_uco(df,heading,link,max_date,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return df


        bank='IDFC FIRST BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['interest rate']
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


        def data_collection_maharashtra(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                col=row_col_index_locator(market_risk_df,['Forex and Gold'])[0]
                df=row_modificator(market_risk_df,['Interest Rate Risk','Forex and Gold','Equity Risk'],col,2,row_del=False,keep_row=True)
                df.columns=['Standardised_Duration_Approach','Amounts_In_Million']
                df['Amounts_In_Million']=pd.to_numeric(market_risk_df['Amounts_In_Million'])

                df['Bank']=bank
                df['Relevant_Date']=date
                df['Runtime']=pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
                df=df[['Bank','Standardised_Duration_Approach','Amounts_In_Million','Relevant_Date','Runtime']]
                df['Bank_Type']=bank_type
                df.reset_index(drop=True,inplace=True)
                df=clean_table(df)
                df_final=df['Relevant_Date']>max_date
                df_final.to_sql("BANK_CAPITAL_REQUIREMENT_MARKET_RISK_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print(len(df_final))
                print('Done for BANK---> ',bank)
                print('Done for DATE---> ',date)

            except:
                if loop<2:
                    print('This might happen because camelot failed trying with ilovepdf')
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='interest rate risk'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_maharashtra(df,heading,link,max_date,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return df


        bank='BANK OF MAHARASHTRA'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['interest rate risk']
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

