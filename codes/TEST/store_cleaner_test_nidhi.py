import datetime
import os
import re
import sys
import warnings
import pandas as pd
from pytz import timezone
warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
from GetState import find_state
from GetState import find_district
import cleancompanies
# import ClickHouse_db
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/State_Function')
import state_rewrite
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/District_Function')
import district_rewrite

def run_program(run_by='Adqvest_Bot', py_file_name=None):

    # os.chdir('/home/ubuntu/AdQvestDir')
    engine = adqvest_db.db_conn()
    # client = ClickHouse_db.db_conn()

    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    # yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'STORE_LOCATOR_WEEKLY_CLEAN_DATA_Temp2_Nidhi'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        # if(run_by=='Adqvest_Bot'):
        #     log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        # else:
        #     log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        # Get last date of STORE LOCATOR BASE TABLE
        query = "select * from AdqvestDB.STORE_LOCATOR_COMPANY_BRAND_LIST;"
        store_df = pd.read_sql(query, con=engine)
           
        for i in range(len(store_df)):
            engine = adqvest_db.db_conn()
            df_final = pd.DataFrame()
            brand_new = store_df['Brand'][i]
            company_new = store_df['Company'][i]
            try:
                brand = store_df['Current_Brand'][i].replace("'","''")
            except:
                brand = None
                    
            try:
                company = store_df['Current_Company'][i].replace("'","''")
            except:
                company = None

            query = f"select max(Relevant_Date) as Relevant_Date from AdqvestDB.STORE_LOCATOR_WEEKLY_CLEAN_DATA_Temp2_Nidhi where Brand = '{brand_new}' and Company = '{company_new}'"
            max_date_clean = pd.read_sql(query, con=engine)['Relevant_Date'][0]
                
            
            if  brand is None:
                query2 = f"select max(Relevant_Date) as Relevant_Date from AdqvestDB.STORE_LOCATOR_WEEKLY_DATA where Brand is NULL and Company = '{company}'"
                max_date_base = pd.read_sql(query2, con=engine)['Relevant_Date'][0]
            elif company is None:
                query2 = f"select max(Relevant_Date) as Relevant_Date from AdqvestDB.STORE_LOCATOR_WEEKLY_DATA where Brand = '{brand}' and Company is NULL"
                max_date_base = pd.read_sql(query2, con=engine)['Relevant_Date'][0]
            else:
                query2 = f"select max(Relevant_Date) as Relevant_Date from AdqvestDB.STORE_LOCATOR_WEEKLY_DATA where Brand = '{brand}' and Company = '{company}'"
                max_date_base = pd.read_sql(query2, con=engine)['Relevant_Date'][0]  

            print(f"{brand}, {company} : Last Cleaned on : {max_date_clean}")

            if max_date_clean is None or max_date_clean < max_date_base:
                print("THERE IS DATA TO BE CLEANED")
                
                if max_date_clean is None:
                    if  brand is None:
                        file = pd.read_sql(f"Select * from AdqvestDB.STORE_LOCATOR_WEEKLY_DATA where  Brand is NULL and Company = '{company}'",con=engine)
                    elif company is None:
                        file = pd.read_sql(f"Select * from AdqvestDB.STORE_LOCATOR_WEEKLY_DATA where  Brand = '{brand}' and Company IS NULL",con=engine)
                    else:
                        file = pd.read_sql(f"Select * from AdqvestDB.STORE_LOCATOR_WEEKLY_DATA where  Brand = '{brand}' and Company = '{company}'",con=engine)
                else:
                    if  brand is None:
                        file = pd.read_sql(f"Select * from AdqvestDB.STORE_LOCATOR_WEEKLY_DATA where Relevant_Date > '{max_date_clean}' and Brand is NULL and Company = '{company}'",con=engine)
                    elif company is None:
                        file = pd.read_sql(f"Select * from AdqvestDB.STORE_LOCATOR_WEEKLY_DATA where Relevant_Date > '{max_date_clean}' and Brand = '{brand}' and Company IS NULL",con=engine)
                    else:
                        file = pd.read_sql(f"Select * from AdqvestDB.STORE_LOCATOR_WEEKLY_DATA where Relevant_Date > '{max_date_clean}' and Brand = '{brand}' and Company = '{company}'",con=engine)
                        
                rdates = file['Relevant_Date'].unique()
                
                for rdate in rdates:
                    df = file[file['Relevant_Date'] == rdate]
                    df.drop_duplicates(subset=df.columns.difference(['Runtime']),inplace=True)
                    df.fillna('',inplace=True)
                    df.reset_index(drop=True,inplace=True)
                    for x in range(len(df)):
                        if df['State'][x] == '' or df['State'][x] is None:
                            if df['Pincode'][x] is None:
                                df['Pincode'][x] = ''
                            df['State'][x] = find_state(df['Pincode'][x],df['Address'][x] + ", " + df['Locality'][x] + ", " + df['City'][x]).strip()
                            
                        if df['State'][x] == '':
                            if df['Address'][x] == '':
                                df['State'][x] = state_rewrite.state(df['Address'][x] + ", " + df['Locality'][x] + ", " + df['City'][x]).split('|')[-1].title().strip()
                            else:
                                try:
                                    df['State'][x] = state_rewrite.state(df['Address'][x]).split('|')[-1].title().strip()
                                except:
                                    pass
                        if df['District'][x] == '' or df['District'][x] is None:
                            df['District'][x] =find_district(df['Pincode'][x],df['Address'][x] + ", " + df['Locality'][x] + ", " + df['City'][x]).strip()
                        
                        
                        if df['District'][x] == '':
                            try:
                                df['District'][x] = district_rewrite.district(df['Address'][x] + ", " + df['Locality'][x] + ", " + df['City'][x]).split('|')[-1].title().strip()
                            except:
                                pass
                        
                    df['Brand'] = df['Brand'].replace("'","").replace("&","and").replace("-"," ").replace("/"," ")
                    df['Company'] = df['Company'].replace("'","").replace("&","and").replace("-"," ").replace("/"," ")                    
                    df['Brand'] = store_df['Brand'][i]
                    df['Company'] = store_df['Company'][i]
                    df['Category'] = store_df['Category'][i]
                    
                    df["State_Clean"] = [state_rewrite.state(x).split('|')[-1].strip().title() if x != '' else x for x in df["State"]]   
                    df["District_Clean"] = [district_rewrite.district(x).split('|')[-1].strip().title() if x != '' else x for x in df["District"]]  
                    df,unmapped = cleancompanies.comp_clean(df, 'Company', 'stores', 'Company_Clean', 'STORE_LOCATOR_WEEKLY_DATA')
                    # print(unmapped)
                    df['Act_Runtime'] = datetime.datetime.now()
                    df.to_sql(name = "STORE_LOCATOR_WEEKLY_CLEAN_DATA_Temp2_Nidhi",con = engine,if_exists = 'append',index = False)
    
                print("DONE CLEANED AND PUSHED INTO SQL")
            else:
                print(f"{brand} up to date!")
    
        

            #####################
            # click_max_date = client.execute("select max(Relevant_Date) from AdqvestDB.STORE_LOCATOR_WEEKLY_CLEAN_DATA_Temp2_Nidhi")
            # click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])

            # query = "select * from AdqvestDB.STORE_LOCATOR_WEEKLY_CLEAN_DATA_Temp2_Nidhi WHERE Relevant_Date > '" +click_max_date+ "'"
            # df = pd.read_sql(query, engine)

            # client.execute("INSERT INTO STORE_LOCATOR_WEEKLY_CLEAN_DATA_Temp2_Nidhi values",df.values.tolist())

            # print("DATA PUSHED INTO CLICKHOUSE")
            # log.job_end_log(table_name, job_start_time, no_of_ping)

        # else:
        #     print("NO NEW DATA TO BE CLEANED")
        #     # log.job_end_log(table_name, job_start_time, no_of_ping)

        print("CODE RAN SUCESSFULLY")

    except:
        error_type = str(re.search("'(.+?)'", str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        # log.job_error_log(table_name, job_start_time, error_type, error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
