
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
    table_name = 'BANK_COMMON_EQUITY_CAPITAL_RATIO_QUARTERLY_DATA'
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

        bank_name='BANK_COMMON_EQUITY_CAPITAL_RATIO_QUARTERLY_DATA'

        catch_error_df=pd.DataFrame(columns=['Table_name','Bank_Name','Error_message','Relevant_Date','Runtime'])
        sql="Delete from BANK_ERROR_TABLE where Table_Name = '" +bank_name+"'"
        engine.execute(sql)
        def delete_collected_data(bank_name,bank,date):
            sql="Delete from BANK_ERROR_TABLE where Table_Name = '" +bank_name+"' and Bank_Name = '" +bank+"' and Relevant_Date='" +date+"'"
            engine.execute(sql)
        table_name='BANK_COMMON_EQUITY_CAPITAL_RATIO_QUARTERLY_DATA'

        def data_collection_au(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                try:
                    indexes=common_equity_df[common_equity_df.iloc[:,1].str.lower().str.contains('ratio').replace(np.nan,False)].index.values
                    if len(indexes)<1:
                        indexes=common_equity_df[common_equity_df.iloc[:,0].str.lower().str.contains('ratio').replace(np.nan,False)].index.values
                except:
                    indexes=common_equity_df[common_equity_df.iloc[:,1].str.lower().str.contains('ratio').replace(np.nan,False)].index.values
                common_equity_df=common_equity_df.loc[[indexes[0],indexes[1]]]
                common_equity_df=common_equity_df.replace('',np.nan)
                common_equity_df.dropna(axis=1,inplace=True)
                common_equity_df.columns=['Particulars','Standalone_In_Percentage']
                common_equity_df['Standalone_In_Percentage']=round(pd.to_numeric(common_equity_df['Standalone_In_Percentage']),2)
                common_equity_df['Consolidated_In_Percentage']=np.nan
                common_equity_df['Bank']=bank
                common_equity_df['Relevant_Date']=date
                common_equity_df['Runtime']=datetime.datetime.now()
                common_equity_df=common_equity_df[['Bank','Particulars','Standalone_In_Percentage','Consolidated_In_Percentage','Relevant_Date','Runtime']]
                common_equity_df.reset_index(drop=True,inplace=True)
                for j in common_equity_df.columns:
                    if j!='Runtime':
                        common_equity_df[j]=common_equity_df[j].apply(lambda x:str(x))
                common_equity_df['Bank_Type']=bank_type
                common_equity_df=clean_table(common_equity_df)
                engine = adqvest_db.db_conn()
                common_equity_df.to_sql("BANK_COMMON_EQUITY_CAPITAL_RATIO_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print(common_equity_df)
                print('uploaded')
                print('\n')
            except:
                if loop>2:
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='ratio'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    common_equity_df=data_collection_au(df,search_str,link,max_date,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return common_equity_df  


        bank='AU SMALL FINANCE BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        # print(max_date_capital,links_capital)
        links_capital=get_correct_links(links_capital)
        search_str=['total capital funds']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                # print(link)
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


        def data_collection_sbi(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                df.reset_index(drop=True,inplace=True)
                index_sbi=row_col_index_locator(df,['state bank of india'])
                index_canada=row_col_index_locator(df,['canada'])
                index_california=row_col_index_locator(df,['california'])
                common_equity_df=df.copy()
                common_equity_df=common_equity_df.iloc[index_sbi[1]:index_california[1]+1,index_sbi[0]:]
                common_equity_df=common_equity_df.replace('',np.nan)
                common_equity_df.dropna(axis=1,how='all',inplace=True)
                common_equity_df.columns=['Bank','Cet 1 capital ratio','Tier 1 capital ratio','Total']
                common_equity_df=common_equity_df.melt('Bank')
                common_equity_df.columns=['Bank','Particulars','Standalone_In_Percentage']
                common_equity_df=common_equity_df[common_equity_df['Particulars']!='Total']
                common_equity_df=common_equity_df[~common_equity_df['Bank'].str.lower().str.contains('mauritius')]
                common_equity_df.reset_index(inplace=True,drop=True)
                common_equity_df['Standalone_In_Percentage']=pd.to_numeric(common_equity_df['Standalone_In_Percentage'])
                common_equity_df['Consolidated_In_Percentage']=np.nan
                common_equity_df['Relevant_Date']=date
                common_equity_df['Runtime']=datetime.datetime.now()
                common_equity_df['Bank_Type']=bank_type
                common_equity_df=clean_table(common_equity_df)
                common_equity_df.to_sql("BANK_COMMON_EQUITY_CAPITAL_RATIO_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print('Common Equity Uploaded')
            except:
                if loop<2:
                    print('This might happen because camelot failed trying with ilovepdf')
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='adequacy ratio'
                    end_text='tier 1'
                    df=search(heading,end_text,path,sheets)
                    common_equity_df=data_collection_sbi(df,search_str,link,max_date,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return common_equity_df  



        bank='STATE BANK OF INDIA'
        print(table_name)
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        print(max_date_capital,links_capital)
        links_capital=get_correct_links(links_capital)
        search_str=['tier 1']
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
                        df=data_collection_sbi(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_sbi('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        def data_collection_ujjivan(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                df.reset_index(drop=True,inplace=True)
                index_tier_i=row_col_index_locator(df,['tier i ratio'])
                index_tier_ii=row_col_index_locator(df,['tier ii ratio'])
                common_equity_df=df.iloc[index_tier_i[1]:index_tier_ii[1]+1,index_tier_i[0]:]
                common_equity_df=common_equity_df.replace('',np.nan)
                common_equity_df.dropna(axis=1,how='all',inplace=True)
                index_value=row_col_index_locator(df,['complied'])
                common_equity_df.reset_index(drop=True,inplace=True)
                common_equity_df=df.iloc[index_tier_i[1]:index_tier_ii[1]+1,[index_tier_i[0],index_value[0]]]
                common_equity_df.columns=['Particulars','Standalone_In_Percentage']
                common_equity_df['Standalone_In_Percentage'] = common_equity_df['Standalone_In_Percentage'].apply(lambda x: re.sub(r'[^0-9.]', '', str(x)))
                common_equity_df['Consolidated_In_Percentage']=np.nan
                common_equity_df['Bank']=bank
                common_equity_df['Relevant_Date']=date
                common_equity_df['Runtime']=datetime.datetime.now()
                common_equity_df=common_equity_df[['Bank','Particulars','Standalone_In_Percentage','Consolidated_In_Percentage','Relevant_Date','Runtime']]
                common_equity_df['Bank_Type']=bank_type
                common_equity_df.reset_index(drop=True,inplace=True)
                common_equity_df=clean_table(common_equity_df)
                engine = adqvest_db.db_conn()
                common_equity_df.to_sql("BANK_COMMON_EQUITY_CAPITAL_RATIO_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print('uploaded')
                print('\n')
            except:
                if loop<2:
                    print('This might happen because camelot failed trying with ilovepdf')
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='tier i'
                    end_text='crar'
                    df=search(heading,end_text,path,sheets)
                    common_equity_df=data_collection_ujjivan(df,search_str,link,max_date,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return common_equity_df


        bank='UJJIVAN SMALL FINANCE BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['tier i ratio']
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
                        df=data_collection_ujjivan(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_ujjivan('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        def data_collection_punjab(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                df.reset_index(drop=True,inplace=True)
                index_tier_1=row_col_index_locator(df,['tier 1'])
                crar=row_col_index_locator(df,['crar'])
                capital_ratio_df=df.iloc[index_tier_1[1]:crar[1]+1,index_tier_i[0]:]
                capital_ratio_df=common_equity_df.replace('',np.nan)
                common_equity_df.dropna(axis=1,how='all',inplace=True)
                capital_ratio_df.reset_index(drop=True,inplace=True)
                capital_ratio_df.columns=['Particulars','Consolidated_In_Percentage','Standalone_In_Percentage']
                capital_ratio_df['Standalone_In_Percentage']=capital_ratio_df['Standalone_In_Percentage'].str.replace('%','')
                capital_ratio_df['Standalone_In_Percentage']=pd.to_numeric(capital_ratio_df['Standalone_In_Percentage'])
                capital_ratio_df['Bank']='PUNJAB NATIONAL BANK'

                # capital_ratio_df['Consolidated_In_Percentage']=np.nan
                # capital_ratio_df['Bank']='PUNJAB NATIONAL BANK'
                capital_ratio_df['Relevant_Date']=rel_date
                capital_ratio_df['Runtime']=datetime.datetime.now()
                capital_ratio_df=capital_ratio_df[['Bank','Particulars','Standalone_In_Percentage','Consolidated_In_Percentage','Relevant_Date','Runtime']]
                capital_ratio_df['Bank_Type']=bank_type
                capital_ratio_df.reset_index(drop=True,inplace=True)
                capital_ratio_df=clean_table(capital_ratio_df)
                capital_ratio_df.to_sql("BANK_COMMON_EQUITY_CAPITAL_RATIO_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print('\n')
            except:
                if loop<2:
                    print('This might happen because camelot failed trying with ilovepdf')
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='tier 1'
                    end_text='crar'
                    df=search(heading,end_text,path,sheets)
                    common_equity_df=data_collection_punjab(df,search_str,link,max_date,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return common_equity_df


        bank='PUNJAB NATIONAL BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['tier 1']
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
                        df=data_collection_punjab(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_punjab('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)



        def data_collection_idbi(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                df.reset_index(drop=True,inplace=True)
                first_index=common_equity_df[common_equity_df.iloc[:,0].str.lower()=='cet 1'].index.values[0]
                second_index=common_equity_df[common_equity_df.iloc[:,0].str.lower()=='tier 1'].index.values[0]
                third_index=common_equity_df[common_equity_df.iloc[:,0].str.lower()=='tier 2'].index.values[0]
                common_equity_df=common_equity_df.loc[[first_index,second_index,third_index]]
                common_equity_df.columns=["Particulars","Standalone_In_Percentage"]
                common_equity_df['Standalone_In_Percentage']=pd.to_numeric(common_equity_df['Standalone_In_Percentage'])
                common_equity_df['Consolidated_In_Percentage']=np.nan
                common_equity_df['Bank']=bank
                common_equity_df['Relevant_Date']=date
                common_equity_df['Runtime']=datetime.datetime.now()
                common_equity_df=common_equity_df[['Bank','Particulars','Standalone_In_Percentage','Consolidated_In_Percentage','Relevant_Date','Runtime']]
                common_equity_df['Bank_Type']=bank_type
                common_equity_df.reset_index(drop=True,inplace=True)
                common_equity_df=clean_table(common_equity_df)
                common_equity_df.to_sql("BANK_COMMON_EQUITY_CAPITAL_RATIO_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                #print(common_equity_df)
                print('uploaded')
                print('\n')
            except:
                if loop<2:
                    print('This might happen because camelot failed trying with ilovepdf')
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='common equity tier 1'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    common_equity_df=data_collection_idbi(df,search_str,link,max_date,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return common_equity_df


        bank='IDBI BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['common equity tier 1','total(tier']
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
                        df=data_collection_idbi(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_idbi('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        def data_collection_maharastra(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                df.reset_index(drop=True,inplace=True)
                indexes=common_equity_df[common_equity_df.iloc[:,1].str.lower().str.contains('tier 1')].index.values
                common_equity_df=common_equity_df.loc[[indexes[0],indexes[1]]]
                common_equity_df=common_equity_df.replace('',np.nan)
                common_equity_df.dropna(inplace=True,how='all',axis=1)
                common_equity_df=common_equity_df.iloc[:,[0,2]]
                common_equity_df.columns=['Particulars','Standalone_In_Percentage']
                common_equity_df['Standalone_In_Percentage']=pd.to_numeric(common_equity_df['Standalone_In_Percentage'])
                common_equity_df['Consolidated_In_Percentage']=np.nan
                common_equity_df['Bank']=bank
                common_equity_df['Relevant_Date']=date
                common_equity_df['Runtime']=datetime.datetime.now()
                common_equity_df=common_equity_df[['Bank','Particulars','Standalone_In_Percentage','Consolidated_In_Percentage','Relevant_Date','Runtime']]
                common_equity_df['Bank_Type']=bank_type
                common_equity_df.reset_index(drop=True,inplace=True)
                common_equity_df=clean_table(common_equity_df)
                common_equity_df.to_sql("BANK_COMMON_EQUITY_CAPITAL_RATIO_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print(common_equity_df)
                print('uploaded')
                print('\n')
            except:
                if loop<2:
                    print('This might happen because camelot failed trying with ilovepdf')
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='common equity tier 1'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    common_equity_df=data_collection_maharastra(df,search_str,link,max_date,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return common_equity_df


        bank='BANK OF MAHARASHTRA'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['common equity tier 1','total(tier']
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
                        tables = extract_tables_from_pdf(link,False,'stream',20,100,40)
                        df=data_collection_maharastra(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_maharastra('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        def data_collection_baroda(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                df.reset_index(drop=True,inplace=True)
                start_index=common_equity_df[common_equity_df[0].str.lower().str.contains('common equity')].index[0]
                end_index=common_equity_df[common_equity_df[0].str.lower().str.contains('crar')].index[0]
                common_equity_df=common_equity_df.loc[start_index:end_index-1]
                common_equity_df.columns=['Particulars','Consolidated_In_Percentage','Standalone_In_Percentage']
                common_equity_df['Standalone_In_Percentage']=pd.to_numeric(common_equity_df['Standalone_In_Percentage'])
                common_equity_df['Consolidated_In_Percentage']=pd.to_numeric(common_equity_df['Consolidated_In_Percentage'])
                common_equity_df['Bank']='BANK OF BARODA'
                common_equity_df['Relevant_Date']=date
                common_equity_df['Runtime']=datetime.datetime.now()
                common_equity_df=common_equity_df[['Bank','Particulars','Standalone_In_Percentage','Consolidated_In_Percentage','Relevant_Date','Runtime']]
                common_equity_df.reset_index(drop=True,inplace=True)
                for j in common_equity_df.columns:
                    if j!='Runtime':
                        common_equity_df[j]=common_equity_df[j].apply(lambda x:str(x))

                common_equity_df['Bank_Type']=bank_type
                common_equity_df=clean_table(common_equity_df)
                common_equity_df.to_sql("BANK_COMMON_EQUITY_CAPITAL_RATIO_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print('Uploaded')
            except:
                if loop<2:
                    print('This might happen because camelot failed trying with ilovepdf')
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='common equity tier 1'
                    end_text='crar'
                    df=search(heading,end_text,path,sheets)
                    common_equity_df=data_collection_baroda(df,search_str,link,max_date,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return common_equity_df



        bank='BANK OF BARODA'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['common equity tier i']
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
                        tables = extract_tables_from_pdf(link,False,'stream',20,100,40)
                        df=data_collection_baroda(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_baroda('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        def data_collection_hdfc(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                df.reset_index(drop=True,inplace=True)
                start_index=common_equity_df[common_equity_df[1].str.lower().str.contains('cet1 capital')].index[0]
                end_index=common_equity_df[common_equity_df[1].str.lower().str.contains('total capital ratio')].index[1]
                common_equity_df=common_equity_df.iloc[start_index-1:end_index,:]
                common_equity_df=row_modificator(common_equity_df,['Total capital ratio','Particulars'],col_idx=1,row_del=True)
                common_equity_df=row_modificator(common_equity_df,['March '],col_idx=3,row_del=True)
                common_equity_df = common_equity_df.reset_index(drop=True)
                common_equity_df=common_equity_df.drop(columns=[0,4,2])


                df1=common_equity_df.iloc[:2,:]
                df2=common_equity_df.iloc[3:,:]
                df1.columns=['Particulars','Standalone_In_Percentage']
                df2.columns=['Particulars','Consolidated_In_Percentage']
                df2 = df2.reset_index(drop=True)

                common_equity_df=df1.merge(df2[['Particulars','Consolidated_In_Percentage']], on = 'Particulars', how = 'left')

                common_equity_df['Standalone_In_Percentage']=pd.to_numeric(common_equity_df['Standalone_In_Percentage'])
                common_equity_df['Consolidated_In_Percentage']=pd.to_numeric(common_equity_df['Consolidated_In_Percentage'])
                common_equity_df['Bank']='HDFC BANK'
                common_equity_df['Relevant_Date']=date
                common_equity_df['Runtime']=datetime.datetime.now()
                common_equity_df=common_equity_df[['Bank','Particulars','Standalone_In_Percentage','Consolidated_In_Percentage','Relevant_Date','Runtime']]
                common_equity_df.reset_index(drop=True,inplace=True)
                for j in common_equity_df.columns:
                    if j!='Runtime':
                        common_equity_df[j]=common_equity_df[j].apply(lambda x:str(x))
                common_equity_df['Bank_Type']=bank_type
                common_equity_df=clean_table(common_equity_df)
                common_equity_df.to_sql("BANK_COMMON_EQUITY_CAPITAL_RATIO_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print("common_equity_df")
                print('Uploaded')
            except:
                if loop<2:
                    print('This might happen because camelot failed trying with ilovepdf')
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='stadalone'
                    end_text='crar'
                    df=search(heading,end_text,path,sheets)
                    common_equity_df=data_collection_hdfc(df,search_str,link,max_date,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return common_equity_df



        bank='HDFC BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['Standalone']
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
                        df=data_collection_hdfc(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_hdfc('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)

        def data_collection_sbi(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                df.reset_index(drop=True,inplace=True)
                try:
                    df1=df.loc[[df[df[1].str.lower().str.contains('sbi group')].index[0]]]
                except:
                    df1=df.loc[[df[df[0].str.lower().str.contains('sbi group')].index[0]]]
                df1.reset_index(drop=True,inplace=True)
                if len(df1.columns)>2:
                    df1 = df1.iloc[:,2:-1]
                    v1=re.findall('\d+.{3}',df1.iloc[0,0])
                    v2=re.findall('\d+.{3}',df1.iloc[0,1])
                    df1.columns=['CET 1 capital ratio','Tier 1 capital ratio']
                    df1['CET 1 capital ratio']=float(v1[0])
                    df1['Tier 1 capital ratio']=float(v2[0])
                else:
                    v1=re.findall('\d+.{3}',df1.iloc[0,0])
                    df1.columns=['CET 1 capital ratio','Tier 1 capital ratio']
                    df1['CET 1 capital ratio']=float(v1[0])
                    df1['Tier 1 capital ratio']=float(v1[1])
                df1['Bank']='SBI GROUP'

                try:
                    df2=df.loc[[df[df[0].str.lower().str.contains('state bank of india')].index[0]]]
                except:
                    df2=df.loc[[df[df[1].str.lower().str.contains('state bank of india')].index[0]]]
                df2.reset_index(drop=True,inplace=True)
                if len(df2.columns)>2:
                    df2 = df2.iloc[:,2:-1]
                    v1=re.findall('\d+.{3}',df2.iloc[0,0])
                    v2=re.findall('\d+.{3}',df2.iloc[0,1])
                    df2.columns=['CET 1 capital ratio','Tier 1 capital ratio']
                    df2['CET 1 capital ratio']=float(v1[0])
                    df2['Tier 1 capital ratio']=float(v2[0])
                else:
                    v1=re.findall('\d+.{3}',df2.iloc[0,0])
                    df2.columns=['CET 1 capital ratio','Tier 1 capital ratio']
                    df2['CET 1 capital ratio']=float(v1[0])
                    df2['Tier 1 capital ratio']=float(v1[1])
                df2['Bank']='STATE BANK OF INDIA'


                try:
                    df3=df.loc[[df[df[1].str.lower().str.contains('mauritius')].index[0]]]
                except:
                    df3=df.loc[[df[df[0].str.lower().str.contains('mauritius')].index[0]]]
                df3.reset_index(drop=True,inplace=True)
                if len(df3.columns)>2:
                    df3 = df3.iloc[:,2:-1]
                    v1=re.findall('\d+.{3}',df3.iloc[0,0])
                    v2=re.findall('\d+.{3}',df3.iloc[0,1])
                    df3.columns=['CET 1 capital ratio','Tier 1 capital ratio']
                    df3['CET 1 capital ratio']=float(v1[0])
                    df3['Tier 1 capital ratio']=float(v2[0])
                else:
                    v1=re.findall('\d+.{3}',df3.iloc[0,0])
                    df3.columns=['CET 1 capital ratio','Tier 1 capital ratio']
                    df3['CET 1 capital ratio']=float(v1[0])
                    df3['Tier 1 capital ratio']=float(v1[1])
                df3['Bank']='SBI (MAURITIUS) LTD'

                try:
                    df4=df.loc[[df[df[1].str.lower().str.contains('canada')].index[0]]]
                except:
                    df4=df.loc[[df[df[0].str.lower().str.contains('canada')].index[0]]]
                df4.reset_index(drop=True,inplace=True)
                if len(df4.columns)>2:
                    df4 = df4.iloc[:,2:-1]
                    v1=re.findall('\d+.{3}',df4.iloc[0,0])
                    v2=re.findall('\d+.{3}',df4.iloc[0,1])
                    df4.columns=['CET 1 capital ratio','Tier 1 capital ratio']
                    df4['CET 1 capital ratio']=float(v1[0])
                    df4['Tier 1 capital ratio']=float(v2[0])
                else:
                    v1=re.findall('\d+.{3}',df4.iloc[0,0])
                    df4.columns=['CET 1 capital ratio','Tier 1 capital ratio']
                    df4['CET 1 capital ratio']=float(v1[0])
                    df4['Tier 1 capital ratio']=float(v1[1])
                df4['Bank']='STATE BANK OF INDIA (CANADA)'

                try:
                    df5=df.loc[[df[df[1].str.lower().str.contains('california')].index[0]]]
                except:
                    df5=df.loc[[df[df[0].str.lower().str.contains('california')].index[0]]]
                df5.reset_index(drop=True,inplace=True)
                if len(df5.columns)>2:
                    df5 = df5.iloc[:,2:-1]
                    v1=re.findall('\d+.{3}',df5.iloc[0,0])
                    v2=re.findall('\d+.{3}',df5.iloc[0,1])
                    df5.columns=['CET 1 capital ratio','Tier 1 capital ratio']
                    df5['CET 1 capital ratio']=float(v1[0])
                    df5['Tier 1 capital ratio']=float(v2[0])
                else:
                    v1=re.findall('\d+.{3}',df5.iloc[0,0])
                    df5.columns=['CET 1 capital ratio','Tier 1 capital ratio']
                    df5['CET 1 capital ratio']=float(v1[0])
                    df5['Tier 1 capital ratio']=float(v1[1])
                df5['Bank']='STATE BANK OF INDIA (CALIFORNIA)'

                try:
                    df6=df.loc[[df[df[1].str.lower().str.contains('indonesia')].index[0]]]
                except:
                    df6=df.loc[[df[df[0].str.lower().str.contains('indonesia')].index[0]]]
                df6.reset_index(drop=True,inplace=True)
                if len(df6.columns)>2:
                    df6 = df6.iloc[:,2:-1]
                    v1=re.findall('\d+.{3}',df6.iloc[0,0])
                    v2=re.findall('\d+.{3}',df6.iloc[0,1])
                    df6.columns=['CET 1 capital ratio','Tier 1 capital ratio']
                    df6['CET 1 capital ratio']=float(v1[0])
                    df6['Tier 1 capital ratio']=float(v2[0])
                else:
                    v1=re.findall('\d+.{3}',df6.iloc[0,0])
                    df6.columns=['CET 1 capital ratio','Tier 1 capital ratio']
                    df6['CET 1 capital ratio']=float(v1[0])
                    df6['Tier 1 capital ratio']=float(v1[1])
                df6['Bank']='BANK SBI INDONESIA'

                try:
                    df7=df.loc[[df[df[1].str.lower().str.contains('commercial indo')].index[0]]]
                    df7.reset_index(drop=True,inplace = True)
                    if bool(re.search(r'\d',df7.loc[0,1])) == False:
                        df7=df.loc[[df[df[1].str.lower().str.contains('commercial indo')].index[0]+1]]
                except:
                    df7=df.loc[[df[df[0].str.lower().str.contains('commercial indo')].index[0]]]
                    df7.reset_index(drop=True,inplace = True)
                    if bool(re.search(r'\d',df7.loc[0,1])) == False:
                        df7=df.loc[[df[df[1].str.lower().str.contains('commercial indo')].index[0]+1]]
                df7.reset_index(drop=True,inplace=True)
                if len(df7.columns)>2:
                    df7 = df7.iloc[:,2:-1]
                    v1=re.findall('\d+.{3}',df7.iloc[0,0])
                    v2=re.findall('\d+.{3}',df7.iloc[0,1])
                    df7.columns=['CET 1 capital ratio','Tier 1 capital ratio']
                    df7['CET 1 capital ratio']=float(v1[0])
                    df7['Tier 1 capital ratio']=float(v2[0])
                else:
                    v1=re.findall('\d+.{3}',df7.iloc[0,0])
                    df7.columns=['CET 1 capital ratio','Tier 1 capital ratio']
                    df7['CET 1 capital ratio']=float(v1[0])
                    df7['Tier 1 capital ratio']=float(v1[1])
                df7['Bank']='COMMERCIAL INDO BANK LLC,MOSCOW'

                try:
                    df8=df.loc[[df[df[1].str.lower().str.contains('nepal sbi')].index[0]]]
                except:
                    df8=df.loc[[df[df[0].str.lower().str.contains('nepal sbi')].index[0]]]
                df8.reset_index(drop=True,inplace=True)
                if len(df8.columns)>2:
                    df8 = df8.iloc[:,2:-1]
                    v1=re.findall('\d+.{3}',df8.iloc[0,0])
                    v2=re.findall('\d+.{3}',df8.iloc[0,1])
                    df8.columns=['CET 1 capital ratio','Tier 1 capital ratio']
                    df8['CET 1 capital ratio']=float(v1[0])
                    df8['Tier 1 capital ratio']=float(v2[0])
                else:
                    v1=re.findall('\d+.{3}',df8.iloc[0,0])
                    df8.columns=['CET 1 capital ratio','Tier 1 capital ratio']
                    df8['CET 1 capital ratio']=float(v1[0])
                    df8['Tier 1 capital ratio']=float(v1[1])
                df8['Bank']='NEPAL SBI BANK LTD'


                try:
                    df9=df.loc[[df[df[1].str.lower().str.contains('uk')].index[0]]]
                except:
                    df9=df.loc[[df[df[0].str.lower().str.contains('uk')].index[0]]]
                df9.reset_index(drop=True,inplace=True)
                if len(df9.columns)>2:
                    df9 = df9.iloc[:,2:-1]
                    v1=re.findall('\d+.{3}',df9.iloc[0,0])
                    v2=re.findall('\d+.{3}',df9.iloc[0,1])
                    df9.columns=['CET 1 capital ratio','Tier 1 capital ratio']
                    df9['CET 1 capital ratio']=float(v1[0])
                    df9['Tier 1 capital ratio']=float(v2[0])
                else:
                    v1=re.findall('\d+.{3}',df9.iloc[0,0])
                    df9.columns=['CET 1 capital ratio','Tier 1 capital ratio']
                    df9['CET 1 capital ratio']=float(v1[0])
                    df9['Tier 1 capital ratio']=float(v1[1])
                df9['Bank']='SBI (UK) LTD'


                common_equity_df=pd.concat([df1,df2,df3,df4,df5,df6,df7,df9])
                common_equity_df.reset_index(drop=True,inplace=True)

                common_equity_df=common_equity_df.melt(id_vars='Bank').sort_values(['Bank','variable'])
                common_equity_df.reset_index(inplace=True,drop=True)
                common_equity_df['value']=pd.to_numeric(common_equity_df['value'])

                common_equity_df.columns=['Bank','Particulars','Standalone_In_Percentage']
                common_equity_df['Consolidated_In_Percentage']=np.nan
                common_equity_df['Relevant_Date']=date

                common_equity_df['Runtime']=datetime.datetime.now()
                #print(common_equity_df)
                common_equity_df['Bank_Type']=bank_type
                common_equity_df=clean_table(common_equity_df)
                common_equity_df.to_sql("BANK_COMMON_EQUITY_CAPITAL_RATIO_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print('Common Equity Uploaded')
            except:
                if loop<2:
                    print('This might happen because camelot failed trying with ilovepdf')
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='california'
                    end_text='uk'
                    df=search(heading,end_text,path,sheets)
                    common_equity_df=data_collection_sbi(df,search_str,link,max_date,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return common_equity_df


        bank='STATE BANK OF INDIA'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['california']
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
                        df=data_collection_sbi(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_sbi('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        


        def data_collection_indusind(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                df.reset_index(drop=True,inplace=True)
                indexes=common_equity_df[common_equity_df.iloc[:,0].str.lower().str.contains('crar #')].index.values
                common_equity_df=common_equity_df.loc[indexes]

                try:
                    common_equity_df=common_equity_df.replace('',np.nan).dropna(axis=1,how='all')
                except:
                    pass
                common_equity_df.columns=['Particulars','Standalone_In_Percentage']
                common_equity_df['Consolidated_In_Percentage']=np.nan
                common_equity_df['Standalone_In_Percentage']=pd.to_numeric(common_equity_df['Standalone_In_Percentage'])
                common_equity_df['Bank']=bank
                common_equity_df['Relevant_Date']=date
                common_equity_df['Runtime']=datetime.datetime.now()
                common_equity_df['Particulars']=common_equity_df['Particulars'].str.replace('#','').str.strip()
                common_equity_df['Particulars']=common_equity_df['Particulars'].str.replace(r'\s\s+',' ')
                common_equity_df=common_equity_df[['Bank','Particulars','Standalone_In_Percentage','Consolidated_In_Percentage','Relevant_Date','Runtime']]
                common_equity_df['Bank_Type']=bank_type
                common_equity_df.reset_index(drop=True,inplace=True)
                common_equity_df.to_sql("BANK_COMMON_EQUITY_CAPITAL_RATIO_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                #print(common_equity_df)
                print('uploaded')
            except:
                if loop<2:
                    print('This might happen because camelot failed trying with ilovepdf')
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='total capital funds'
                    end_text='crar'
                    df=search(heading,end_text,path,sheets)
                    common_equity_df=data_collection_indusind(df,search_str,link,max_date,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return common_equity_df


        



        bank='INDUSIND BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['total capital funds']
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
                        df=data_collection_indusind(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_indusind('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        


        def data_collection_idfc(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                df.reset_index(drop=True,inplace=True)
                columns=common_equity_df.iloc[0]
                start_index=common_equity_df[common_equity_df[0].str.lower().str.contains('cet1')].index[0]
                end_index=common_equity_df[common_equity_df[0].str.lower().str.contains('crar')].index[0]
                common_equity_df=common_equity_df.loc[start_index:end_index-1]
                common_equity_df.columns=columns
                common_equity_df=common_equity_df.rename(columns={'Standalone':'Standalone_In_Percentage','Consolidated':'Consolidated_In_Percentage'})
                common_equity_df['Standalone_In_Percentage']=pd.to_numeric(common_equity_df['Standalone_In_Percentage'])
                common_equity_df['Consolidated_In_Percentage']=pd.to_numeric(common_equity_df['Consolidated_In_Percentage'])
                common_equity_df['Bank']='IDFC FIRST BANK'
                common_equity_df['Relevant_Date']=date
                common_equity_df['Runtime']=datetime.datetime.now()
                common_equity_df=common_equity_df[['Bank','Particulars','Standalone_In_Percentage','Consolidated_In_Percentage','Relevant_Date','Runtime']]
                common_equity_df.reset_index(drop=True,inplace=True)
                common_equity_df['Standalone_In_Percentage']=common_equity_df['Standalone_In_Percentage'].apply(lambda x:str(x))
                common_equity_df['Consolidated_In_Percentage']=common_equity_df['Consolidated_In_Percentage'].apply(lambda x:str(x))
                common_equity_df['Bank_Type']=bank_type
                common_equity_df=clean_table(common_equity_df)
                common_equity_df.to_sql("BANK_COMMON_EQUITY_CAPITAL_RATIO_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                #print(common_equity_df)
                print('Uploaded')
            except:
                if loop<2:
                    print('This might happen because camelot failed trying with ilovepdf')
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='capital adequacy ratios'
                    end_text='crar'
                    df=search(heading,end_text,path,sheets)
                    common_equity_df=data_collection_idfc(df,search_str,link,max_date,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return common_equity_df


        


        bank='IDFC FIRST BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['capital adequacy ratios']
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
                        df=data_collection_idfc(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_idfc('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        


        def data_collection_rbl(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                df.reset_index(drop=True,inplace=True)
                capital_ratio=pd.DataFrame()
                capital_ratio=capital_ratio.append(df[df.iloc[:,2].str.contains('Tier1 Capital Adequacy Ratio')])
                capital_ratio.iloc[0,2]='Tier I capital ratio'
                capital_ratio.iloc[1,2]='CET 1 capital ratio'
                capital_ratio=capital_ratio[[2,3]]
                capital_ratio.columns=['Particulars','Standalone_In_Percentage']
                capital_ratio['Standalone_In_Percentage']=pd.to_numeric(capital_ratio['Standalone_In_Percentage'])
                capital_ratio['Bank']=bank
                capital_ratio['Consolidated_In_Percentage']=np.nan
                capital_ratio['Relevant_Date']=date

                capital_ratio['Runtime']=datetime.datetime.now()
                capital_ratio=capital_ratio[['Bank','Particulars','Standalone_In_Percentage','Consolidated_In_Percentage','Relevant_Date','Runtime']]
                capital_ratio['Bank_Type']=bank_type
                #print(capital_ratio)
                capital_ratio=clean_table(capital_ratio)
                capital_ratio.to_sql("BANK_COMMON_EQUITY_CAPITAL_RATIO_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print(capital_ratio)
                print('uploaded')
            except:
                if loop<2:
                    print('This might happen because camelot failed trying with ilovepdf')
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='capital adequacy ratios'
                    end_text='crar'
                    df=search(heading,end_text,path,sheets)
                    common_equity_df=data_collection_rbl(df,search_str,link,max_date,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return common_equity_df


        


        bank='RBL BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['capital adequacy ratios']
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
                        df=data_collection_rbl(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_rbl('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        


        def data_collection_axis(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    capital_ratio_df=get_desired_table(tables,search_str)
                else:
                    capital_ratio_df=tables
                capital_ratio_df.reset_index(drop=True,inplace=True)
                capital_ratio_df.columns=capital_ratio_df.iloc[0]
                capital_ratio_df=capital_ratio_df.iloc[1:-1]
                capital_ratio_df['Consolidated']=capital_ratio_df['Consolidated'].apply(lambda x:''.join(re.findall(r'\d*\.?\d+', x)))
                capital_ratio_df['Standalone']=capital_ratio_df['Standalone'].apply(lambda x:''.join(re.findall(r'\d*\.?\d+', x)))
                capital_ratio_df['Consolidated']=pd.to_numeric(capital_ratio_df['Consolidated'])
                capital_ratio_df['Standalone']=pd.to_numeric(capital_ratio_df['Standalone'])
                capital_ratio_df.columns=['Particulars','Consolidated_In_Percentage','Standalone_In_Percentage']
                capital_ratio_df['Bank']='AXIS BANK'
                capital_ratio_df['Relevant_Date']=date
                capital_ratio_df['Runtime']=datetime.datetime.now()
                capital_ratio_df=capital_ratio_df[['Bank','Particulars','Standalone_In_Percentage','Consolidated_In_Percentage','Relevant_Date','Runtime']]
                capital_ratio_df.iloc[0,1]='CET 1 capital ratio'
                capital_ratio_df.iloc[1,1]='Tier 1 capital ratio'
                #print(capital_ratio_df)
                capital_ratio_df['Bank_Type']=bank_type
                capital_ratio_df=clean_table(capital_ratio_df)
                capital_ratio_df.to_sql("BANK_COMMON_EQUITY_CAPITAL_RATIO_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print('uploaded')
            except:
                if loop<2:
                    print('This might happen because camelot failed trying with ilovepdf')
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='capital adequacy ratios'
                    end_text='total crar'
                    df=search(heading,end_text,path,sheets)
                    capital_ratio_df=data_collection_axis(df,search_str,link,max_date,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    capital_ratio_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return capital_ratio_df


        


        bank='AXIS BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['capital adequacy ratios']
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
                        df=data_collection_axis(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_axis('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        


        def data_collection_fed(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                df.reset_index(drop=True,inplace=True)
                start_index=df[df[1].str.lower().str.contains('common equity tier')].index[0]
                common_equity_df=df.loc[[start_index+1,len(df)-2],[1,2,3]]
                common_equity_df.columns=['Particulars','Standalone_In_Percentage','Consolidated_In_Percentage']
                common_equity_df['Standalone_In_Percentage']=pd.to_numeric(common_equity_df['Standalone_In_Percentage'])
                common_equity_df['Consolidated_In_Percentage']=pd.to_numeric(common_equity_df['Consolidated_In_Percentage'])
                common_equity_df['Bank']='FEDERAL BANK'
                common_equity_df['Relevant_Date']=date
                common_equity_df['Runtime']=datetime.datetime.now()
                common_equity_df=common_equity_df[['Bank','Particulars','Standalone_In_Percentage','Consolidated_In_Percentage','Relevant_Date','Runtime']]
                common_equity_df.reset_index(drop=True,inplace=True)
                for j in common_equity_df.columns:
                    if j!='Runtime':
                        common_equity_df[j]=common_equity_df[j].apply(lambda x:str(x))
                common_equity_df['Bank_Type']=bank_type
                common_equity_df=clean_table(common_equity_df)
                common_equity_df.to_sql("BANK_COMMON_EQUITY_CAPITAL_RATIO_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                #print(common_equity_df)
                #common_equity_df.to_csv('Bank of Baroda.csv',mode='a',header=True)
                print('Uploaded')
            except:
                if loop<2:
                    print('This might happen because camelot failed trying with ilovepdf')
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='common equity tier i capital'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    common_equity_df=data_collection_fed(df,search_str,link,max_date,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    common_equity_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return common_equity_df


        


        bank='FEDERAL BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['common equity tier i capital']
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
                        df=data_collection_fed(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_fed('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        


        def data_collection_iob(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                df.reset_index(drop=True,inplace=True)
                start_index=df[df[0].str.lower().str.contains('capital ratio')].index[2]
                common_equity_df=df.iloc[start_index:,:]
                common_equity_df.columns=['Particulars','Standalone_In_Percentage']
                common_equity_df = common_equity_df.reset_index(drop=True)
                values=[float(j) for j in common_equity_df['Standalone_In_Percentage'] if j!='']

                common_equity_df=common_equity_df.append({'Particulars':'Tier I Capital Ratio','Standalone_In_Percentage':values[0]},ignore_index=True)
                common_equity_df=common_equity_df.append({'Particulars':'Common Equity Tier-I Capital Ratio','Standalone_In_Percentage':values[1]},ignore_index=True)
                common_equity_df=common_equity_df.iloc[1:]
                common_equity_df['Standalone_In_Percentage']=pd.to_numeric(common_equity_df['Standalone_In_Percentage'])
                common_equity_df['Consolidated_In_Percentage']=' '
                common_equity_df['Bank']=bank
                common_equity_df['Relevant_Date']=date
                common_equity_df['Runtime']=datetime.datetime.now()
                common_equity_df=common_equity_df[['Bank','Particulars','Standalone_In_Percentage','Consolidated_In_Percentage','Relevant_Date','Runtime']]
                common_equity_df.reset_index(drop=True,inplace=True)
                for j in common_equity_df.columns:
                    if j!='Runtime':
                        common_equity_df[j]=common_equity_df[j].apply(lambda x:str(x))
                common_equity_df['Bank_Type']=bank_type
                common_equity_df=clean_table(common_equity_df)
                common_equity_df.to_sql("BANK_COMMON_EQUITY_CAPITAL_RATIO_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print('uploaded')
            except:
                if loop<2:
                    print('This might happen because camelot failed trying with ilovepdf')
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='tier 1 capital ratio'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    common_equity_df=data_collection_iob(df,search_str,link,max_date,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    common_equity_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return common_equity_df


        


        bank='INDIAN OVERSEAS BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['tier 1 capital ratio']
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
                        df=data_collection_iob(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_iob('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        


        def data_collection_canara(tables,search_str,link,max_date,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                df.reset_index(drop=True,inplace=True)
                first_index=df[df.loc[:,2].str.lower()=='cet 1 ratio'].index.values
                second_index=df[df.loc[:,2].str.lower()=='tier 1 ratio'].index.values
                third_index=df[df.loc[:,2].str.lower()=='tier 2 ratio'].index.values
                common_equity_df1=df.loc[[first_index[0],second_index[0],third_index[0]]]
                common_equity_df2=df.loc[[first_index[1],second_index[1],third_index[1]]]
                common_equity_df1=df.loc[[first_index[0],second_index[0],third_index[0]]]
                common_equity_df2=df.loc[[first_index[1],second_index[1],third_index[1]]]
                common_equity_df2.reset_index(inplace=True,drop=True)
                common_equity_df1.reset_index(inplace=True,drop=True)
                common_equity_df1[4]=common_equity_df2[3]
                common_equity_df=common_equity_df1.loc[:,2:5]
                #common_equity_df.columns=[0,1]
                common_equity_df=common_equity_df.replace('',np.nan)
                common_equity_df.dropna(axis=1,how='all',inplace=True)
                common_equity_df.fillna(' ',inplace=True)
                common_equity_df.columns=["Particulars","Consolidated_In_Percentage","Standalone_In_Percentage"]
                common_equity_df['Standalone_In_Percentage']=pd.to_numeric(common_equity_df['Standalone_In_Percentage'])
                common_equity_df['Consolidated_In_Percentage']=pd.to_numeric(common_equity_df['Consolidated_In_Percentage'])
                common_equity_df['Bank']=bank
                common_equity_df['Relevant_Date']=date
                common_equity_df['Runtime']=datetime.datetime.now()
                common_equity_df=common_equity_df[['Bank','Particulars','Standalone_In_Percentage','Consolidated_In_Percentage','Relevant_Date','Runtime']]
                common_equity_df['Bank_Type']=bank_type
                common_equity_df.reset_index(drop=True,inplace=True)
                common_equity_df=clean_table(common_equity_df)
                common_equity_df.to_sql("BANK_COMMON_EQUITY_CAPITAL_RATIO_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                #print(common_equity_df)
                print('uploaded')
            except:
                if loop<2:
                    print('This might happen because camelot failed trying with ilovepdf')
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='cet 1 ratio'
                    end_text='crar'
                    df=search(heading,end_text,path,sheets)
                    common_equity_df=data_collection_canara(df,search_str,link,max_date,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    common_equity_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return common_equity_df


        


        bank='CANARA BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['CET 1 Ratio']
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
                        df=data_collection_canara(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_canara('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)

        print('Done')
        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:

        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_type)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')







