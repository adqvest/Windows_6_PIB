from typing import Optional, Dict
import pandas as pd
import numpy as np
import sys
import sqlalchemy
import re
import datetime
from pytz import timezone
# sys.path.insert(0, '/home/ubuntu/AdQvestDir/Adqvest_Function')
# sys.path.insert(0, r'C:\Users\Administrator\AdQvestDir\Adqvest_Function')
sys.path.insert(0, r'C:\Users\Administrator\AdQvestDir\Adqvest_Function')
# import JobLogNew as log
import adqvest_db
engine = adqvest_db.db_conn()
connection = engine.connect()
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days
job_start_time = datetime.datetime.now(india_time)


def alertlog(lookup_table:str, error_type:str, error_msg:str):
    global engine
    global job_start_time
    
    print('Updating alert/error in Transaction log')
    relevant_date = job_start_time.strftime("%Y-%m-%d")
    job_end_time = datetime.datetime.now(india_time)
    job_execution_time = (job_end_time - job_start_time).total_seconds()
    job_start_time1 = job_start_time.strftime("%Y-%m-%d %H:%M:%S")
    
    df=pd.DataFrame()
    df['Table_Name']=[lookup_table]
    df['Python_File_Name']=''
    df['Scheduler']=''
    df['Run_By']='Adqvest_Bot'
    df['Schedular_Start_Time']=np.nan
    df['Start_Time']=job_start_time1
    
    df['End_Time']=job_end_time.strftime("%Y-%m-%d %H:%M:%S")
    df['Execution_Time_Seconds']=job_execution_time
    df['No_Of_Ping']=0
    df['Error_Type']=error_type
    
    df['Error_Msg'] = error_msg
    df['Relevant_Date']=relevant_date
    df['Runtime']=datetime.datetime.now(india_time)
    df.to_sql('TRANSACTION_LOG_AND_ERROR_LOG_DAILY_DATA', con=engine, if_exists='append', index=False) # type: ignore

    
def map(raw_col: str, clean_col:str , raw_df: pd.DataFrame, lookup_table: str, engine, today, context = None):
    unmapped_df = pd.DataFrame()  # Initialize outside the loop to collect unmapped entries

    # Read lookup table
    if context==None:
        lookup_tbl = pd.read_sql("SELECT * FROM AdqvestDB.GENERIC_LOCATION_LOOK_UP_TABLE GROUP BY UPPER(Location_Name)", con=engine)

    else:
        lookup_tbl = pd.read_sql(f"SELECT * FROM AdqvestDB.GENERIC_LOCATION_LOOK_UP_TABLE where Context='{context}'  GROUP BY UPPER(Location_Name)", con=engine)

    # print(f"SELECT * FROM AdqvestDB.GENERIC_LOCATION_LOOK_UP_TABLE where Context='{context}'  GROUP BY UPPER(Location_Name)")
    lookup_tbl['Location_Name_temp'] = lookup_tbl['Location_Name'].apply(lambda x: x.upper() if isinstance(x, str) else x)
    lookup_tbl = lookup_tbl.drop(columns=['js_file', 'Context', 'Relevant_Date', 'Runtime', 'Location_Name'])
    lookup_tbl.drop_duplicates(inplace=True)
    
    # Create temporary cleaned column in raw_df
    raw_df['raw_col_temp'] = raw_df[raw_col].apply(lambda x: x.upper() if isinstance(x, str) else x)
    
    # Merge with lookup table
    map_df = pd.merge(raw_df, lookup_tbl, left_on='raw_col_temp', right_on='Location_Name_temp', how='left')
    map_df.drop(columns=['Location_Name_temp', 'raw_col_temp'], inplace=True)
    map_df.rename(columns={'Location_Clean_Name': clean_col}, inplace=True)
    
    # Identify unmapped locations
    un_mapped = map_df[raw_col][map_df[clean_col].isna()].unique()
    map_df[clean_col] = map_df[clean_col].fillna(map_df[raw_col].str.title())
    if '_temp' in raw_col:
        map_df.drop(columns=raw_col, inplace=True)
    
    # Append unmapped entries to unmapped_df
    for location in un_mapped:
        unmapped_df = unmapped_df._append({'Location_Name': location,
                                        'Table_Name': lookup_table,
                                        'Context': context,
                                        'Status': np.nan,
                                        'Comments': np.nan,
                                        'Relevant_Date': today.date(),
                                        'Runtime': today}, ignore_index=True) # type: ignore
    # Write unmapped locations to SQL table if there are any
    if not unmapped_df.empty:
        unmapped_df.to_sql('GENERIC_LOCATION_UNMAPPED_TABLE', index=False, if_exists='append', con=engine)
        alertlog(lookup_table, 'Alert: Clean Name Unavailable', f'Location Clean Mapping not available for {len(unmapped_df)} locations, Please check GENERIC_LOCATION_UNMAPPED_TABLE for more details')
    
    # print(map_df)
    return map_df
    
def geo_clean(raw_df: pd.DataFrame, lookup_table:str, input_cols:list, output_cols:list, cleaning: bool = True,context: Optional[str] = None):

    
    """
    Generic function to map raw location names to clean

    If no clean name is present in look up table it will copy raw location name in title case

    Parameters
    ----------
    raw_df : dataframe
        input dataframe

    lookup_table : str
        name of the table to be cleaned


    input_cols : list
        columns to be cleaned or mapped ['raw_1']
        
    output_cols : list
        columns to be cleaned or mapped ['raw_1']
        
    context : Optional[str] == None
        context by which lookup table records are filtered (eg:. 'State', 'Dist',City) default None
        
    cleaning : bool = False
        default False
        
    mapping : bool = True
        default True
        
    Returns
    --------
    dataframe
        dataframe with cleaned location names

    Raises
    --------
    Alert
        Alert in case location names are unmapped and logs into error log
    """
    col_dict=dict(zip(input_cols,output_cols))
    if context == None:
        if cleaning:
            print('------------Cleaning & Mapping in process------------')
            for raw, cln in col_dict.items():
                base_cln = cln + '_temp'
                raw_df[base_cln] = raw_df[raw].apply(lambda x: re.sub('  +', ' ', re.sub('[^A-z0-9 ]+', '', x)).title().strip() if isinstance(x, str) else x)
                clean_map_df = map(base_cln, cln, raw_df, lookup_table, engine, today, context = None)
                raw_df = clean_map_df.copy()
                raw_df.drop(columns=['Table_Name'],inplace=True)
            print('------------Process Finished------------')
        else:
            print('------------Mapping in process------------')
            for raw, cln in col_dict.items():
                clean_map_df = map(raw, cln, raw_df, lookup_table, engine, today, context = None)
                raw_df = clean_map_df.copy()
                raw_df.drop(columns=['Table_Name'],inplace=True)
            print('------------Process Finished------------')
        
        
    
    elif context != None:
        if cleaning:
            print('------------Cleaning & Mapping in process------------')
            for raw, cln in col_dict.items():
                base_cln = cln + '_temp'
                raw_df[base_cln] = raw_df[raw].apply(lambda x: re.sub('  +', ' ', re.sub('[^A-z0-9 ]+', '', x)).title().strip() if isinstance(x, str) else x)
                clean_map_df = map(base_cln, cln, raw_df, lookup_table, engine, today, context = context)
                raw_df = clean_map_df.copy()
                raw_df.drop(columns=['Table_Name'],inplace=True)
            print('------------Process Finished------------')
        else:
            print('------------Mapping in process------------')
            for raw, cln in col_dict.items():
                clean_map_df = map(raw, cln, raw_df, lookup_table, engine, today, context = context)
                raw_df = clean_map_df.copy()
                raw_df.drop(columns=['Table_Name'],inplace=True)
            print('------------Process Finished------------')

    return raw_df

        # alertlog(lookup_table, 'ValueError', 'Context filter is not yet specified will be used as future functionality') 
        # lookup_tbl = pd.read_sql(f"select * from AdqvestDB.GENERIC_LOCATION_LOOK_UP_TABLE where Type = '{type}' GROUP BY UPPER(Location_Name)",con=engine) To be used when declared