import pandas as pd
import numpy as np
import sys
import sqlalchemy
import re
import datetime
from pytz import timezone
# sys.path.insert(0, '/home/ubuntu/AdQvestDir/Adqvest_Function')
sys.path.insert(0, r'C:\Users\Administrator\AdQvestDir\Adqvest_Function')
import JobLogNew as log
import adqvest_db
engine = adqvest_db.db_conn()
connection = engine.connect()
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days

# class Mapping:

    # def __init__(self,py_file_name,run_by,scheduler,job_start_time, no_of_ping):

    #     self.py_file_name = py_file_name
    #     self.run_by = run_by
    #     self.scheduler = scheduler
    #     self.job_start_time = job_start_time
    #     self.no_of_ping = no_of_ping


def comp_clean(raw_df: pd.DataFrame, to_map: str, js_file: str, clean_col:str, table_name:str):
    """
    Generic function to map raw company names to clean

    If no clean name is present in look up table it will copy raw company name

    Parameters
    ----------
    raw_df : dataframe
        (Use dataframe after all cleaning opertions are done)
        
    to_map : str
        column name from raw_df that is to be mapped
    
    tbl_name : str
        table name to filter rows from  GENERIC_COMPANY_LOOK_UP_TABLE

    js_file : str
        js_file of the sector to which table belongs
        (eg:. 'LIFE_INSURANCE_LIST_OF_AGENTS_MONTHLY_DATA' belongs to life insurance sector, thus js_file for that is 'life_insurance')

    clean_col : str
        name of clean column to rename Company_Clean_Name

    table_name : str
        name of the table to be cleaned

    Returns
    --------
    dataframe
        dataframe with cleaned company names

    Raises
    --------
    Error
        exception in case company names are unmapped and logs into error log

    """

    lookup_tbl = pd.read_sql(f"select * from AdqvestDB.GENERIC_COMPANY_LOOK_UP_TABLE where js_file = '{js_file}'",con=engine)
    # lookup_tbl = lookup_tbl.drop(columns=['js_file','Relevant_Date', 'Runtime'])
    lookup_tbl['Company_Name_temp'] = lookup_tbl['Company_Name'].str.lower()
    raw_df['to_map_temp'] = raw_df[to_map].str.lower()
    lookup_tbl = lookup_tbl.drop(columns=['js_file','Relevant_Date', 'Runtime','Company_Name'])
    lookup_tbl.drop_duplicates(inplace=True)
    map_df = pd.merge(raw_df, lookup_tbl, left_on='to_map_temp', right_on='Company_Name_temp', how='left')
    map_df = map_df.drop(['Company_Name_temp', 'to_map_temp'], axis=1)
    map_df = map_df.rename(columns={'Company_Clean_Name': clean_col})
    un_mapped = list(map_df[to_map][map_df[clean_col].isna()].values)
    map_df[clean_col] = np.where(map_df[clean_col].isna(), map_df[to_map], map_df[clean_col])
    map_df = map_df.reset_index(drop=True)
    unmapped_df = pd.DataFrame()
    unmapped_df['Company_Name'] = un_mapped
    unmapped_df['Table_Name'] = table_name
    unmapped_df['js_file'] = js_file
    unmapped_df['Status'] = np.nan
    unmapped_df['Comments'] = np.nan
    unmapped_df['Relevant_Date'] = today.date()
    unmapped_df['Runtime'] = today

    unmapped_df.to_sql('GENERIC_COMPANY_UNMAPPED_TABLE', index=False, if_exists='append', con=engine)
    # try:
    #     if len(un_mapped) > 0:
    #         raise Exception(f'--------Complete Mapping Failed--------\nList of unmapped companies:\n{un_mapped}')
    #     else:
    #         print('Mapping Successful')
    # except:

    #     error_type = str(re.search("'(.+?)'", str(sys.exc_info()[0])).group(1))
    #     error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
    #     print(error_msg)
    #     log.job_error_log(tbl_name, self.job_start_time, error_type, error_msg, self.no_of_ping)
    return map_df, un_mapped

            # if self.run_by == 'Adqvest_Bot':
            #     run = "'Adqvest_Bot'"
            # else:
            #     run = 'null'
            # query = f"Insert into TRANSACTION_LOG_AND_ERROR_LOG_DAILY_DATA value ('{tbl_name}', '{self.py_file_name}', '{self.scheduler}', {run}, '{str(today.strftime('%Y-%m-%d %H:%M:%S'))}', '{self.job_start_time}','{datetime.datetime.now(india_time)}',null,null,'{error_type}', '{error_msg}', null, null,'','','', '{str(today.strftime('%Y-%m-%d'))}', '{str(today.strftime('%Y-%m-%d %H:%M:%S'))}')"
            # connection.execute(sqlalchemy.text(query))
            # connection.execute('commit')

