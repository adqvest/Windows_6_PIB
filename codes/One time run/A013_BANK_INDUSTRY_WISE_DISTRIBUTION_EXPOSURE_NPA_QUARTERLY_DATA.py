
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
    table_name = 'BANK_INDUSTRY_WISE_DISTRIBUTION_EXPOSURE_NPA_QUARTERLY_DATA'
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


        bank_name='BANK_INDUSTRY_WISE_DISTRIBUTION_EXPOSURE_NPA_QUARTERLY_DATA'
        catch_error_df=pd.DataFrame(columns=['Table_name','Bank_Name','Error_message','Relevant_Date','Runtime'])
        sql="Delete from BANK_ERROR_TABLE where Table_Name = '" +bank_name+"'"
        engine.execute(sql)
        def delete_collected_data(bank_name,bank,date):
            sql="Delete from BANK_ERROR_TABLE where Table_Name = '" +bank_name+"' and Bank_Name = '" +bank+"' and Relevant_Date='" +date+"'"
            engine.execute(sql)

        def data_collection_sbi(tables,search_str,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                str_index=row_col_index_locator(df,['industry'])[1]
                end_index=row_col_index_locator(df,['total'])[1]
                df=df.iloc[str_index:end_index,:]
                df=column_values_clean(df)
                code_code=row_col_index_locator(df,['code'])
                col_indus=row_col_index_locator(df,['industry'])
                industry_col=row_col_index_locator(df,['npa'])
                df=df[[code_code[0],col_indus[0],industry_col[0]]]
                df.reset_index(drop=True,inplace=True)
                code_code_2=row_col_index_locator(df,['coal'])
                df=df.iloc[code_code_2[1]:,:]
                df.columns=['Industry_Code','Industry','Total_Gross_Npa_In_Millions']
                df['Sub_Industry']=df.apply(lambda _:'',axis=1)
                df.reset_index(drop=True,inplace=True)
                for row in range(df.shape[0]):
                    if (re.match(r'\d+\.',str(df['Industry_Code'][row]))):
                        df['Sub_Industry'].iloc[row]=df['Industry'][row]
                        df['Industry'].iloc[row]=np.nan
                df['Industry'].fillna(method='ffill',inplace=True)
                df['Bank']='STATE BANK OF INDIA'
                df['Relevant_Date']=date
                df['Runtime']=datetime.datetime.now()
                df=df[['Bank','Industry','Sub_Industry','Total_Gross_Npa_In_Millions',
                                                                'Relevant_Date','Runtime','Industry_Code']]
                df['Total_Provisions_For_Npa_In_Millions']=np.nan
                df['Total_Provision_For_Standard_Assets_In_Millions']=np.nan
                df['Qtrly_Write_Offs_In_Millions']=np.nan
                df['Qtrly_Provisions_For_Npa_In_Millions']=np.nan
                df.reset_index(drop=True,inplace=True)
                for j in df.columns:
                    if j!='Runtime':
                        df[j]=df[j].apply(lambda x:str(x))
                df['Bank_Type']=bank_type
                df=clean_table(df)
                df['Total_Gross_Npa_In_Millions'] = df['Total_Gross_Npa_In_Millions'].str.replace(',', '')
                df['Total_Gross_Npa_In_Millions'] = df['Total_Gross_Npa_In_Millions'].astype(float)

                df.to_sql("BANK_INDUSTRY_WISE_DISTRIBUTION_EXPOSURE_NPA_QUARTERLY_DATA", index=False, if_exists='append', con=engine)

                print('NPA DF Uploaded')
            except:
                if loop <2:
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='coal'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_sbi(df,heading,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return df 


        bank='STATE BANK OF INDIA'
        search_str=['code']
        # tables = extract_tables_from_pdf(link,True,'stream',10,200,70)
        # df=data_collection_sbi(tables,search_str,bank_type,bank,date,'camelot',1)
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['code']
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
                        df=data_collection_sbi(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_sbi('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        def data_collection_hdfc(tables,search_str,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                str_index=row_col_index_locator(df,['industry'])[1]
                end_index=row_col_index_locator(df,['total'])[1]
                df=df.iloc[str_index:end_index,:]
                df=column_values_clean(df)
                str_index=row_col_index_locator(df,['agriculture'])[1]
                df=df.iloc[str_index:end_index,:]
                df['Sub_Industry']=np.nan
                df.columns=['Industry','Total_Gross_Npa_In_Millions','Total_Provisions_For_Npa_In_Millions',
                                                'Total_Provision_For_Standard_Assets_In_Millions','Qtrly_Write_Offs_In_Millions',
                                                'Qtrly_Provisions_For_Npa_In_Millions','Sub_Industry']
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Runtime']=datetime.datetime.now()
                df=df[['Bank','Industry','Sub_Industry','Total_Gross_Npa_In_Millions','Total_Provisions_For_Npa_In_Millions',
                                'Total_Provision_For_Standard_Assets_In_Millions','Qtrly_Write_Offs_In_Millions',
                                'Qtrly_Provisions_For_Npa_In_Millions','Relevant_Date','Runtime']]
                df.reset_index(drop=True,inplace=True)
                df['Industry_code']=range(len(df))
                for j in df.columns:
                    if j!='Runtime':
                        df[j]=df[j].apply(lambda x:str(x))

                #npa_df=npa_df.drop(npa_df[npa_df['Industry']=='nan'].index[0])
                df['Bank_Type']=bank_type
                df.reset_index(drop=True,inplace=True)
                df=clean_table(df)
                df.to_sql("BANK_INDUSTRY_WISE_DISTRIBUTION_EXPOSURE_NPA_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print('Uploaded')
            except:
                if loop <2:
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='industry'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_sbi(df,heading,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return df 


        
        bank='HDFC BANK'
        # max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['provision for npa','write offs']
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
                        df=data_collection_hdfc(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_hdfc('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)


        bank='AXIS BANK'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['gross npa']
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

        def data_collection_axis(tables,search_str,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                str_index=row_col_index_locator(df,['particulars'])[1]
                end_index=row_col_index_locator(df,['total'])[1]
                col1=row_col_index_locator(df,['particulars'])[0]
                col2=row_col_index_locator(df,['Gross NPA'])[0]
                col3=row_col_index_locator(df,['specific provision'])[0]
                df=df.iloc[str_index+1:end_index,[col1,col2,col2+1]]
                df.columns=['Industry','Total_Gross_Npa_In_Millions','Total_Provisions_For_Npa_In_Millions']
                df['Bank']='AXIS BANK'
                df['Sub_Industry']=np.nan
                df['Total_Provision_For_Standard_Assets_In_Millions']=np.nan
                df['Qtrly_Write_Offs_In_Millions']=np.nan
                df['Qtrly_Provisions_For_Npa_In_Millions']=np.nan
                df=df[['Bank','Industry','Sub_Industry','Total_Gross_Npa_In_Millions','Total_Provisions_For_Npa_In_Millions','Total_Provision_For_Standard_Assets_In_Millions','Qtrly_Write_Offs_In_Millions','Qtrly_Provisions_For_Npa_In_Millions']]
                df['Relevant_Date']=date
                df['Runtime']=datetime.datetime.now()
                df['Bank_Type']=bank_type
                #print(npa_df)
                df=clean_table(df)
                df.to_sql("BANK_INDUSTRY_WISE_DISTRIBUTION_EXPOSURE_NPA_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                print('uploaded')
            except:
                if loop <2:
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='infrastructure'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_axis(df,heading,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return df 

        def data_collection_rbl(tables,search_str,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                str_index=row_col_index_locator(df,['gross npa'])[1]
                df=df.iloc[str_index:,:]
                df.reset_index(drop=True,inplace=True)
                end_index=row_col_index_locator(df,['other residuary advances'])[1]
                col1=row_col_index_locator(df,['mining and quarrying'])[0]
                col2=row_col_index_locator(df,['gross npa'])[0]
                col3=row_col_index_locator(df,['provision for npa'])[0]
                col4=row_col_index_locator(df,['write offs'])[0]
                col5=row_col_index_locator(df,['additional provision'])[0]
                df=df.iloc[1:end_index+1,[col1,col2,col3,col4,col5]]
                df.columns=['Industry','Total_Gross_Npa_In_Millions','Total_Provisions_For_Npa_In_Millions','Qtrly_Write_Offs_In_Millions','Qtrly_Provisions_For_Npa_In_Millions',]
                df['Industry_Code'] = df['Industry'].str.extract(r'([A-Z]\.[a-zA-Z\d]+\.\s*\d+|[A-Z]\.[a-zA-Z\d]+|[A-Z]\.\s*\d+|[A-Z]\.)')
                df['Total_Provision_For_Standard_Assets_In_Millions']=np.nan
                df['Sub_Industry']=np.nan
                df.reset_index(drop=True,inplace=True)
                stop_index=df[df['Industry'].str.lower().str.contains('residuary')].index[0]
                df=df.iloc[:stop_index+1]
                df.drop(df[df['Industry']==''].index.values,inplace=True)
                df.drop(df[df['Industry']=='Industry Name'].index.values,inplace=True)
                df['Industry']=df['Industry'].apply(lambda x:re.sub("[\(\[].*?[\)\]]","",x))
                for row in range(df.shape[0]):
                    try:
                        if (re.match(r'^[A-Z]\.\d',df.iloc[row,0])):
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
                df['Bank']=bank

                df['Relevant_Date']=date

                df['Runtime']=datetime.datetime.now()
                df=df[['Bank','Industry','Sub_Industry','Total_Gross_Npa_In_Millions','Total_Provisions_For_Npa_In_Millions','Total_Provision_For_Standard_Assets_In_Millions','Qtrly_Write_Offs_In_Millions','Qtrly_Provisions_For_Npa_In_Millions','Relevant_Date','Runtime']]
                df['Bank_Type']=bank_type
                df.reset_index(drop=True,inplace=True)
                #print(npa_df)
                df=clean_table(df)
                df.to_sql("BANK_INDUSTRY_WISE_DISTRIBUTION_EXPOSURE_NPA_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                # print('uploaded')
                df
            except:
                if loop <2:
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='industry name'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    df=data_collection_rbl(df,heading,bank_type,bank,date,'ilovepdf',2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return df 

        bank='RBL BANK'
        # max_date_capital,links_capital=get_table_max_date_and_links(bank_name,bank)
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['industry name']
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
                        tables = extract_tables_from_pdf(link,False,'lattice',10,100,40)
                        df=data_collection_rbl(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_rbl('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    update_basel_iii(bank_name,date)
                except Exception as e:
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)

        
        def data_collection_union(tables,search_str,bank_type,bank,date,type_read,loop):
            try:
                if type_read=='camelot':
                    df=get_desired_table(tables,search_str)
                else:
                    df=tables
                    df.reset_index(drop=True,inplace=True)
                str_index=row_col_index_locator(df,['industry name'])[1]
                end_index=row_col_index_locator(df,['total'])[1]
                df=df.iloc[str_index:end_index]
                df.reset_index(drop=True,inplace=True)
                col=row_col_index_locator(df,['dsb code'])[0]
                col1=row_col_index_locator(df,['industry name'])[0]
                col2=row_col_index_locator(df,['Gross NPA'])[0]
                col3=row_col_index_locator(df,['write off'])[0]
                col4=row_col_index_locator(df,['prov'])[0]
                df=df.iloc[1:,[col,col1,col2,col3,col4]]
                df.columns=['Industry_Code','Industry','Total_Gross_Npa_In_Millions','Qtrly_Write_Offs_In_Millions','Total_Provisions_For_Npa_In_Millions']
                df['Sub_Industry']=np.nan
                for row in range(df.shape[0]):
                    if (re.match(r'[A-Z]\.\d',df.iloc[row,0])):
                        df['Sub_Industry'].iloc[row]=df.iloc[row,1].partition('.')[0].strip()
                        df['Industry'].iloc[row]=np.nan
                    elif (re.match(r'[a-z]\.',df.iloc[row,0])):
                        df['Sub_Industry'].iloc[row]=df.iloc[row,1].partition('.')[0].strip()
                        df['Industry'].iloc[row]=np.nan
                df['Industry'].fillna(method='ffill',inplace=True)
                df['Sub_Industry']=df['Sub_Industry'].apply(lambda x:re.sub('\d\.\d*','',str(x)))
                df['Sub_Industry']=df['Sub_Industry'].apply(lambda x:re.sub('\d','',str(x)))
                df['Runtime']=datetime.datetime.now()
                df['Bank']=bank
                df['Relevant_Date']=date
                df['Total_Provision_For_Standard_Assets_In_Millions']=np.nan
                df['Qtrly_Provisions_For_Npa_In_Millions']=np.nan
                df=df[['Bank','Industry_Code','Industry','Sub_Industry','Total_Gross_Npa_In_Millions','Total_Provisions_For_Npa_In_Millions','Total_Provision_For_Standard_Assets_In_Millions','Qtrly_Write_Offs_In_Millions','Qtrly_Provisions_For_Npa_In_Millions','Relevant_Date','Runtime']]
                df['Bank_Type']=bank_type
                df=clean_table(df)
                df.to_sql("BANK_INDUSTRY_WISE_DISTRIBUTION_EXPOSURE_NPA_QUARTERLY_DATA", index=False, if_exists='append', con=engine)
                # print(npa_df)
                print('uploaded')
            except:
                if loop<2:
                    print('This might happen because camelot failed trying with ilovepdf')
                    path,sheets=read_data_using_ilovepdf(link,bank)
                    heading='gross npa'
                    end_text='total'
                    df=search(heading,end_text,path,sheets)
                    npa_df= data_collection_sbi(df,2)
                else:
                    e='Nothing worked try manually'
                    catch_error_df=get_error_df(table_name,bank,e,date,catch_error_df)
            return industry_df


        bank='UNION BANK OF INDIA'
        max_date_capital,links_capital=get_table_max_date_and_links(table_name,bank)
        links_capital=get_correct_links(links_capital)
        search_str=['gross npa']
        if links_capital.empty:
            print('No new data')
        else:
            for i in range(len(links_capital)):
                link=links_capital['Links'][i]
                bank_type = links_capital['Bank_Type'][i]
                date = links_capital['Relevant_Date'][i]
                link=link.replace(' ','%20')
                print(link)
                max_date,lnk=get_table_max_date_and_links(table_name,bank)
                try:
                    try:
                        tables = extract_tables_from_pdf(link,False,'lattice',20,100,40)
                        df=data_collection_union(tables,search_str,link,max_date,bank_type,bank,date,'camelot',1)
                    except:
                        df=data_collection_union('',search_str,link,max_date,bank_type,bank,date,'camelot',1)
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




