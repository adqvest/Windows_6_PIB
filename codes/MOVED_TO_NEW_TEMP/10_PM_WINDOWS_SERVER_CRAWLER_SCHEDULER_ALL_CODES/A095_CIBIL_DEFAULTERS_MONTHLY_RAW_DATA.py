import sqlalchemy
import pandas as pd
from pandas.io import sql
import os
from dateutil import parser
import requests
from bs4 import BeautifulSoup
import urllib
import re
import datetime as datetime
from pytz import timezone
import sys
import warnings
import json
import time
from dateutil.relativedelta import relativedelta
warnings.filterwarnings('ignore')
import numpy as np
import time
requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = 'ALL:@SECLEVEL=1'
from csv import reader
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
#%%
warnings.filterwarnings('ignore')
#%%
# sys.path.insert(0, '/home/ubuntu/AdQvestDir/Adqvest_Function')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
#sys.path.insert(0, r'C:\Adqvest')
import numpy as np
import adqvest_db
import JobLogNew as log

from adqvest_robotstxt import Robots
robot = Robots(__file__)
engine = adqvest_db.db_conn()
# d = datetime.datetime.now()
#%%
engine = adqvest_db.db_conn()
connection = engine.connect()
#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday =  today - days

#%%
my_dictionary={
     'lacs':{'st_ut':'lakhAccount','dt_rng':'quarterIdLakh','clk':4,'cat':'25 Lacs and above','file_type':"1"},
     'crore':{'st_ut':'croreAccount','dt_rng':'quarterIdCrore','clk':3,'cat':'1 Cr and above','file_type':"2"}
     }

def clean_values(x):
    x=float(str(x).replace(',',''))
    return x

def get_json_from_soup(soup):
    a =str(soup)
    a = a.split("var json = ")[-1]
    a = a.split(";")[0]
    a = eval(a)
    a = json.loads(a)
    b=a['rows']
    return b

def get_page_content(url,cate_type='',layer_1=False,layer_2=False,layer_3=False,layer_4=False,rel_month='',st_row='',bnk_id=''):
    driver_path = r'C:\Users\Administrator\AdQvestDir\chromedriver.exe'
    download_path = r"C:\Users\Administrator\Junk_One_Time"
    
    prefs = {
        "download.default_directory": download_path,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True
    }

    options = webdriver.ChromeOptions()
    options.add_argument("start-maximized")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--disable-blink-features")
    options.add_argument("--disable-blink-features=AutomationControlled")
    driver = webdriver.Chrome(executable_path=driver_path, chrome_options=options)
    driver.get(url)
    time.sleep(5)

    if layer_1==True:
       soup=BeautifulSoup(driver.page_source)
       return soup
    if layer_2==True:
        elem1=driver.find_element(By.XPATH, f'//*[@id="{my_dictionary[cate_type]["st_ut"]}"]/option[4]')
        time.sleep(3)
        elem1.click()

        dropdown_element = driver.find_element(By.ID, f'{my_dictionary[cate_type]["dt_rng"]}') 
        dropdown = Select(dropdown_element)
        dropdown.select_by_value(rel_month)
        
        elem3=driver.find_element(By.XPATH, f'//*[@id="loadSuitFiledDataSearchAction"]/div[1]/div[{my_dictionary[cate_type]["clk"]}]/div[4]/img')
        time.sleep(3)
        elem3.click()
        time.sleep(5)
        soup1=BeautifulSoup(driver.page_source)
       
        if layer_3==True:
            ele2=driver.find_element(By.XPATH, f'//*[@id="{st_row}"]/td[3]/a')
            time.sleep(2)
            ele2.click()
            time.sleep(5)
            soup1=BeautifulSoup(driver.page_source)
            
        if layer_4==True:
            ele4=driver.find_element(By.XPATH, f'//*[@id="{bnk_id}"]/td[4]/a')
            time.sleep(2)
            ele4.click()
            time.sleep(5)
            df=pd.read_html(driver.page_source)
            time.sleep(5)

        driver.delete_all_cookies()
        if layer_4==True:
           return df
        else:
            return soup1


   
      

def process_cibil_data(df,bk_name,dt_val,category):
     df = pd.DataFrame(df['rows'])
     df['Registered_Address'] = df['importDataBean'].apply(lambda x : x['regaddr'])
     df['Outstanding_Amt_Lacs'] = pd.to_numeric(df['totalAmount'],errors='ignore')
     df['State'] = df['stateName']
     df['Party'] = df['borrowerName']
     df['Director_Name'] = df['directorName']
     df['Branch'] = df['branchBean'].apply(lambda x:x['branchName'])
     df['Credit_Institution'] = bk_name
     df['Relevant_Date'] = dt_val
     # df['Source'] = 'CIBIL'
     df['Category'] = category
     df['Runtime'] = datetime.datetime.now(india_time).strftime('%Y-%m-%d %H:%M:%S')
     df = df[['State','Registered_Address','Credit_Institution','Branch','Party','Director_Name','Outstanding_Amt_Lacs','Category','Relevant_Date', 'Runtime']]
     
     
     df['Credit_Institution']=df['Credit_Institution'].apply(lambda x:str(x).upper().strip())
     df['Branch']=df['Branch'].apply(lambda x:str(x).upper().strip())
     df['State']=df['State'].apply(lambda x:str(x).upper().strip())
     df['Party']=df['Party'].apply(lambda x:str(x).upper().strip())
     df['Registered_Address']=df['Registered_Address'].apply(lambda x:str(x).upper().strip().replace('"','').replace('#', ''))
     df['Outstanding_Amt_Lacs']=df['Outstanding_Amt_Lacs'].apply(lambda x:clean_values(x))
     
     return df
 
def Upload_Data_mysql(table_name, data, category):
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    
    query=f"select max(Relevant_Date) as Max from {table_name} where Category='{category}'"
    db_max_date = pd.read_sql(query,engine)["Max"][0]
    # data=data.loc[data['Relevant_Date'] > db_max_date]
    data.to_sql(table_name, con=engine, if_exists='append', index=False)
    print('Data loded in mysql')
    
def update_status_table(record_df):
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    for index,row in record_df.iterrows():
        update_query = f"""UPDATE CIBIL_EQUIFAX_CRIF_DEFAULTERS_MONTHLY_QTRLY_DATA_STATUS_TABLE
               set Category='{row['Category']}',Records='{row['Records']}',Outstanding_Amt_Lacs='{row['Outstanding_Amt_Lacs']}',Source='{row['Source']}',Runtime='{row['Runtime']}'
               WHERE Relevant_Date ='{row['Relevant_Date']}'"""

        engine.execute(update_query)
                          
#%%
def run_program(run_by='Adqvest_Bot', py_file_name=None):

    # os.chdir('/home/ubuntu/AdQvestDir')
    os.chdir('C:/Users/Administrator/AdQvestDir')
    

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'CIBIL_DEFAULTERS_MONTHLY_RAW_DATA'
    scheduler = ''
    no_of_ping = 0

    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)\

        
        os.chdir('C:/Users/Administrator/CIBIL_EQUIFAX_CRIF')
        driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
        download_path=os.getcwd()
        url = 'https://suit.cibil.com/loadSuitFiledDataSearchAction'
        robot.add_link(url)

#%%
        soup=get_page_content(url,layer_1=True)
       
      
        #%%
       
        for ct in my_dictionary.keys():
            engine = adqvest_db.db_conn()
            connection = engine.connect()
            #%%
            # ct='crore'
            # print(my_dictionary[ct]['cat'])

            ## On this Particular day of month only Status table will be updated
            if today.day==10:
                col_only_records=True
            else:
                col_only_records=False

            print(f"Working on--{my_dictionary[ct]['cat']}")
            db_max_dt_1 = pd.read_sql(f"select max(Relevant_Date) as Max from CIBIL_DEFAULTERS_MONTHLY_RAW_DATA where  Category ='{my_dictionary[ct]['cat']}'",engine)["Max"][0]


            q1=f"""select Relevant_Date,Count_diff from (select t1.Relevant_Date as Relevant_Date,t2.Relevant_Date as Not_Col_Dates,t1.Records as Actual_Records,t2.col_records as col_records,(t1.Records-t2.col_records) as Count_diff from (select Relevant_Date,Records from CIBIL_EQUIFAX_CRIF_DEFAULTERS_MONTHLY_QTRLY_DATA_STATUS_TABLE where `Source` ='CIBIL' and Category ='{my_dictionary[ct]['cat']}' order by Relevant_Date desc) as t1
                    LEFT join (select  Relevant_Date,count(*) as col_records  from  CIBIL_DEFAULTERS_MONTHLY_RAW_DATA 
                    where `Source` ='CIBIL' and Category ='{my_dictionary[ct]['cat']}' group by Relevant_Date order by Relevant_Date desc) as t2
                    on t1.Relevant_Date=t2.Relevant_Date) as t3 where Count_diff>0 or Not_Col_Dates is Null;"""

            q1=f"""select Relevant_Date,Amt_diff,Count_diff,Amt_Pct from (select t1.Relevant_Date as Relevant_Date,t2.Relevant_Date as Not_Col_Dates,t1.Records as Actual_Records,t2.col_records as col_records,(t1.Records-t2.col_records) as Count_diff,(t1.Amount-t2.Col_Amt) as Amt_diff,((t2.Col_Amt/t1.Amount)-1)*100 as Amt_Pct
                    from (select Relevant_Date,Sum(Outstanding_Amt_Lacs) as Amount,Category,SUM(Record_No) as Records  from CIBIL_INST_WISE_DEFAULTERS_CNT_N_OS_AMOUNT_MONTHLY_RAW_DATA
                    where Category ='{my_dictionary[ct]['cat']}' and Credit_Institution!='Grand Total' group by Relevant_Date order by Relevant_Date desc) as t1
                    LEFT join (select  Relevant_Date,count(*) as col_records,sum(Outstanding_Amt_Lacs) as Col_Amt  from  CIBIL_DEFAULTERS_MONTHLY_RAW_DATA 
                    where Category ='{my_dictionary[ct]['cat']}' group by Relevant_Date order by Relevant_Date desc) as t2
                    on t1.Relevant_Date=t2.Relevant_Date) as t3 where  Amt_diff is null or Amt_Pct>5 or Amt_Pct<-5 order by t1.Relevant_Date DESC;"""
                    

            already_col_df=pd.read_sql(q1,engine)
            already_col_df['Relevant_Date']=already_col_df['Relevant_Date'].apply(lambda x:str(x))
            

            if today.day==13:
                already_col_df=pd.DataFrame()
                print(f'----------')
                four_months_ago = db_max_dt_1 - relativedelta(months=3)

                del_dates = pd.date_range(str(four_months_ago),str(db_max_dt_1), freq='M')[::-1]
                del_dates=[str(i.date()) for i in del_dates]
                already_col_df['Relevant_Date']=del_dates

            already_col_df=already_col_df.drop_duplicates(keep="first")
            print(already_col_df)
           

            db_max_dt = pd.read_sql(f"select max(Relevant_Date) as Max from CIBIL_DEFAULTERS_MONTHLY_RAW_DATA where  Category ='{my_dictionary[ct]['cat']}'",engine)["Max"][0]
            org_tbl_col_dt = pd.read_sql(f"select Distinct Relevant_Date as Relevant_Date from CIBIL_DEFAULTERS_MONTHLY_RAW_DATA where  Category ='{my_dictionary[ct]['cat']}'",engine)
            org_tbl_col_dt['Relevant_Date']=org_tbl_col_dt['Relevant_Date'].apply(lambda x:str(x))
            
            status_tbl_col_dt = pd.read_sql(f"select Distinct Relevant_Date as Relevant_Date from CIBIL_EQUIFAX_CRIF_DEFAULTERS_MONTHLY_QTRLY_DATA_STATUS_TABLE where Source = 'CIBIL' AND Category ='{my_dictionary[ct]['cat']}'",engine)
            status_tbl_col_dt['Relevant_Date']=status_tbl_col_dt['Relevant_Date'].apply(lambda x:str(x))
            base_date=pd.to_datetime('2014-01-31',format='%Y-%m-%d').date()
            
            st_ut = soup.find("select",{"id":f"{my_dictionary[ct]['st_ut']}"}).find_all("option")
            st_ut_str = [x.text for x in st_ut if 'state' in x.text.lower()]
            st_ut_code = [x['value'] for x in st_ut if 'state' in x.text.lower()]


            
            date_id = soup.find("select",{"id":f"{my_dictionary[ct]['dt_rng']}"}).find_all("option")
            date_id = [x for x in date_id if "select" not in x.text.lower()]
            
            if col_only_records==True:
                # date_id_val={x['value']:pd.to_datetime(x.text).date() for x in date_id if (str(pd.to_datetime(x.text).date())  in set(status_tbl_col_dt.Relevant_Date.to_list()))}
                date_id_val={x['value']:pd.to_datetime(x.text).date() for x in date_id if ((pd.to_datetime(x.text).date()>base_date) and str(pd.to_datetime(x.text).date()) not in set(status_tbl_col_dt.Relevant_Date.to_list()) )}

            else:
                # date_id_val={x['value']:pd.to_datetime(x.text).date() for x in date_id if (str(pd.to_datetime(x.text).date()) in set(status_tbl_col_dt.Relevant_Date.to_list()))}
                date_id_val={x['value']:pd.to_datetime(x.text).date() for x in date_id if (str(pd.to_datetime(x.text).date())  in already_col_df.Relevant_Date.to_list() or (pd.to_datetime(x.text).date()>db_max_dt))}
                
                # date_id_val={k:v for k,v in date_id_val.items() if v<=pd.to_datetime('2020-12-31',format='%Y-%m-%d').date()}
                # date_id_val={k:v for k,v in date_id_val.items() if v>=pd.to_datetime('2019-01-31',format='%Y-%m-%d').date()}
                # date_id_val={k:v for k,v in date_id_val.items() if v==pd.to_datetime('2015-03-31',format='%Y-%m-%d').date()}

            #%%
            if len(date_id_val)>0:
                for dt_id,dt_val in date_id_val.items():
                    #%%
                    dt_val=date_id_val[dt_id]
                    print(f"Working on--{dt_val}")
                    soup1=get_page_content(url,cate_type=ct,layer_2=True,rel_month=dt_id)

                    if (('No Records' in str(soup1)) & (today.day<10)):
                        break
                        
                    state_info=get_json_from_soup(soup1)
                    states = [x['stateBean'] for x in state_info]
                    states_id_names ={x['stateName']:x['stateId'] for x in states}
                    
                    state_row_index=soup1.find_all('tr',class_='ui-widget-content jqgrow ui-row-ltr')
                    state_row_index={i.get('id'):i.find('td', {'aria-describedby':'projectTable_stateBean.stateName'}).get('title')  for i in state_row_index if i.find('td', {'aria-describedby':'projectTable_stateBean.stateName'})!=None}
                      
                    records=soup1.find_all('td',{'aria-describedby':'projectTable_totalRecords'})[-1].text

                    try:
                        amount=float(soup1.find_all('td',{'aria-describedby':'projectTable_totalAmount'})[-1].text.replace(',',''))
                    except:
                        amount=np.nan
                    print(amount)
                    record_count_df=pd.DataFrame()
                    record_count_df=pd.DataFrame.from_dict([{'Category':str(my_dictionary[ct]['cat']),'Records':clean_values(records),'Outstanding_Amt_Lacs':amount,'Source':'CIBIL','Relevant_Date':dt_val,'Runtime':pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))}])
                    # print(record_count_df)
                    record_count_df=record_count_df.drop_duplicates(keep="first")
                    
                    if str(dt_val) in status_tbl_col_dt.Relevant_Date.to_list():
                        engine.execute(f"Delete from CIBIL_EQUIFAX_CRIF_DEFAULTERS_MONTHLY_QTRLY_DATA_STATUS_TABLE where Relevant_Date='{dt_val}' and `Source` ='CIBIL' and Category ='{my_dictionary[ct]['cat']}'")

                    record_count_df.to_sql('CIBIL_EQUIFAX_CRIF_DEFAULTERS_MONTHLY_QTRLY_DATA_STATUS_TABLE', con=engine, if_exists='append', index=False)
                    print(record_count_df)

                    if col_only_records==True:
                        continue
                    
                    if str(dt_val) in org_tbl_col_dt.Relevant_Date.to_list():
                        col_states = pd.read_sql(f"select Distinct State as State from CIBIL_DEFAULTERS_MONTHLY_RAW_DATA where Category ='{my_dictionary[ct]['cat']}' and Relevant_Date='{dt_val}'",engine)
                        state_row_index={k:v for k,v in state_row_index.items() if v not in col_states.State.to_list()}
                        print(state_row_index)

                    # if str(dt_val) in already_col_df.Relevant_Date.to_list():
                    #     engine.execute(f"Delete from CIBIL_DEFAULTERS_MONTHLY_RAW_DATA where Relevant_Date='{dt_val}' and `Source` ='CIBIL' and Category ='{my_dictionary[ct]['cat']}'")

                        
                    #%%
                    for st_row,st in state_row_index.items():
                        print(f"Working on--{st}")
                    #%%
                        try:
                            st=state_row_index[st_row]
                            st_id=states_id_names[st]
                            soup2=get_page_content(url,cate_type=ct,layer_2=True,layer_3=True,rel_month=dt_id,st_row=st_row)
                            
                            bank_info=get_json_from_soup(soup2)
                            banks = [x['bankBean'] for x in bank_info]
                            bank_id_names ={x['bankId']:x['bankName'] for x in banks}
                            bank_id_records ={x['bankId']:x['bankNoRecords'] for x in banks}
                            
                            bank_row_index=soup2.find_all('tr',class_='ui-widget-content')
                            bank_row_index={i.find('td', {'aria-describedby':'projectTable_bankBean.bankName'}).get('title'):i.get('id')  for i in bank_row_index if i.find('td', {'aria-describedby':'projectTable_bankBean.bankName'})!=None}
                     
                            banks = [x['bankBean'] for x in bank_info]
                            bank_names = [x['bankName'] for x in banks]
                            bank_id = [x['bankId'] for x in banks]
                            bank_records = [x['bankNoRecords'] for x in banks]
                            print(bank_records)
                            # print(bank_names)
               
                            df_final=pd.DataFrame()
                            for bk,bk_id,bk_r in zip(bank_names,bank_id,bank_records):
                                bank_id=bank_row_index[bk]
                              
                                d = datetime.datetime.now()
                                unixtime = int(time.mktime(d.timetuple()))
                                data = {"fileType": my_dictionary[ct]["file_type"],
                                        "suitSearchBeanJson": '{"borrowerName":"","costAmount":"","stateName":"","directorName":"","branchBean":null,"dunsNumber":"","city":"","bankBean":{"bankId":'+str(bk_id)+',"bankName":"","categoryBean":{"categoryId":0,"categoryName":"","categoryAllotedId":"","active":0,"enable":false},"bankNoRecords":0,"bankTotalAmount":"","enable":false,"active":0},"quarterBean":{"quarterId":'+str(dt_id)+',"quarterDate":null,"quarterDateStr":"","quarterName":"","quarterMonthStr":"","quarterYearStr":"","isPush":0},"stateBean":{"stateId":'+str(st_id)+',"stateName":"","stateNoRecords":0,"stateTotalAmount":"","category":"","enable":false,"isActive":0},"borrowerAddress":null,"borrowerId":0,"sort":0,"totalRecords":0,"totalAmount":"","quarterCol":"","categoryBean":null,"noOFCGs1Cr":0,"records1Cr":0,"noOFCGs25Lac":0,"records25Lac":0,"cat":"","catGroup":"","fromQuarterId":0,"toQuarterId":0,"partyTypeId":0,"quarterId":0,"srNo":"","userComments":"","rejected":0,"rejectComment":"","lastLimit":0,"firstLimit":0,"reject":null,"edit":null,"modify":null,"editedTotalAmount":null,"editedDirectorNames":null,"rejectComments":null,"updateReject":"","userId":0,"isReview":"","sortBy":null,"sortOrder":null,"summaryState":"1","summaryType":"2","directorId":0,"directorSuffix":"","dinNumber":"","editedDirectorDin":null,"dirPan":"","editedDirectorPan":null,"title":"","directorBean":null,"user":null,"importDataBean":null,"uploadBatchBean":null}',
                                      "_search": False,
                                      "nd": str(unixtime),
                                      "rows": str(bk_r),
                                      "page": "1",
                                      "sidx": "",
                                      "sord": "asc"}

                                query_parameters = urllib.parse.urlencode(data)
                                url_data = 'https://suit.cibil.com/loadSearchResultPage?'+query_parameters
                                print(bk)

                                final = json.loads(requests.get(url_data).content)
                                # robot.add_link(url)
                                if len(final['rows'])==0:
                                    final_df=get_page_content(url,cate_type=ct,layer_2=True,layer_3=True,layer_4=True,rel_month=dt_id,st_row=st_row,bnk_id=bank_id)
                                    print(final_df)
                                    final_df=[i for i in final_df if i.shape[0]>0]
                                    df_bnk=pd.concat(final_df)
                                    df_bnk.dropna(axis=0,inplace=True)
                                    df_bnk.columns= ['Credit_Institution','Branch','Relevant_Date','Party','Registered_Address','Director_Name','Outstanding_Amt_Lacs']
                                    df_bnk['State']=str(st).upper().strip()
                                    df_bnk['Relevant_Date'] = dt_val
                                    # df_bnk['Source'] = 'CIBIL'
                                    df_bnk['Category'] = str(my_dictionary[ct]['cat'])
                                    df_bnk['Runtime'] = datetime.datetime.now(india_time).strftime('%Y-%m-%d %H:%M:%S')
                                    
                                else:
                                    df_bnk=process_cibil_data(final,bk,dt_val,category=str(my_dictionary[ct]['cat']))
                                
                                if len(df_bnk)>0:
                                    df_final=pd.concat([df_final,df_bnk])

                            if len(df_final)>0:
                                Upload_Data_mysql('CIBIL_DEFAULTERS_MONTHLY_RAW_DATA',df_final,str(my_dictionary[ct]['cat']))
                                time.sleep(2)
                        except:
                            pass
                     # 
            # print('--------------------')


#%%
        log.job_end_log(table_name,job_start_time,no_of_ping)

    except:
        try:
            connection.close()
        except:
            pass
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)


        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
