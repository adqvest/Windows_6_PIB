import datetime as datetime
import os
import re
import sys
import warnings
import pandas as pd
import requests
from bs4 import BeautifulSoup
from pytz import timezone
warnings.filterwarnings('ignore')
import time
import json
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import JobLogNew as log
import adqvest_db
import ClickHouse_db

def run_program(run_by = 'Adqvest_Bot', py_file_name = None):
    os.chdir('C:/Users/Administrator/AdQvestDir')
    engine = adqvest_db.db_conn()
    connection = engine.connect()

    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    job_start_time = datetime.datetime.now(india_time)
    table_name = "STORE_LOCATOR_WEEKLY_DATA"
    scheduler = ''
    no_of_ping = 0

    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]

    try :
        if(run_by == 'Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        Latest_Date = pd.read_sql("select max(Relevant_Date) as Max from STORE_LOCATOR_WEEKLY_DATA where Brand = 'Maruti Suzuki'",engine)
        Latest_Date= Latest_Date["Max"][0]

        if(today.date() - Latest_Date) >= datetime.timedelta(7):
            url = "https://www.marutisuzuki.com/dealer-showrooms"
            dealer_code = []
            dealer_name = []
            src = requests.get(url)
            content = src.text
            soup = BeautifulSoup(content,'lxml')
            dealer_options = [i.findAll('option') for i in soup.findAll('select',attrs = {'id':"select-dealer-locator"})][0]
            for l in dealer_options:
                # time.sleep(1)
                dealer_code.append(l['value'])
                dealer_name.append(l.text)
            dealer_code = dealer_code[1:]
            dealer_name = dealer_name[1:]

            state_code = []
            state_name = []
            state_options = [i.findAll('option') for i in soup.findAll('select',attrs = {'id':"dealer-state"})][0]
            for l in state_options:
                # time.sleep(1)
                state_code.append(l['value'])
                state_name.append(l.text)
            state_code = state_code[1:]
            state_name = state_name[1:]

            headers = {
                "user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
                    }
            url = 'https://www.marutisuzuki.com/api/sitecore/QuickLinks/GetCitiesForDealers'
            df_final = pd.DataFrame()
            for i,j in zip(state_code,state_name):
                time.sleep(1)
                post_data = {'stateCode':i}
                limit = 0
                while True:
                    try:
                        r = requests.post(url,headers = headers,data = post_data)
                        print(r)
                        if r.status_code != 200:
                            raise Exception
                        else:
                            break
                    except:
                        time.sleep(5)
                        if(limit < 10):
                            continue
                        else:
                            raise Exception
                city=json.loads(r.text)
                df = pd.json_normalize(city)
                df['State']=j
                df_final=df_final.append(df)
                # break
            df_final1=df_final[['State','CityName']]
            df_final1.reset_index(inplace = True,drop = True)

            headers = {
                "user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
                'accept': 'application/json, text/javascript, */*; q=0.01','accept-encoding': 'gzip, deflate, br',
                'origin': 'https://www.marutisuzuki.com','content-length': '54',
                'Content-Type': 'application/json; charset=UTF-8'
                    }
            url = 'https://www.marutisuzuki.com/api/dealerlocators'
            maruti_df = pd.DataFrame()
            list1=['dealer','tvs','ss','mds','MASS','mgp']

            for row in list1:
                for i in range(0,df_final1.shape[0]):
                    addr_list = []
                    print(row,i)
                    time.sleep(1)
                #try:
                    #data ='{StateName: "'+df_final1['State'].iloc[i]+'", CityName: "'+df_final1['Title'].iloc[i].lower()+'", Category: "'+row+'", Radius:30}'
                #except:
                    # data = '{CityName: "'+df_final1['CityName'].iloc[i].lower()+'", Category: "'+row+'", Radius: 30}'
                    data = {'CityName': df_final1['CityName'].iloc[i].lower(),
                            'Category': row,
                            'Radius':30,
                            'StateName' : df_final1['State'].iloc[i]}
                    limit = 0
                    while True:
                        try:
                            r = requests.post(url,headers = headers,data = data)
                            print(r)
                            if r.status_code != 200:
                                raise Exception
                            else:
                                break
                        except:
                            time.sleep(5)
                            if(limit < 10):
                                continue
                            else:
                                raise Exception

                    val=json.loads(r.text)['Result']['Response']
                    val1=json.loads(val)['list']

                    for j in val1:
                        addr = list(j['dealeraddress'])
                        address = ''.join(addr)
                        addr_list.append(address)
                    df = pd.DataFrame({"Address":addr_list,
                           "State":df_final1['State'].iloc[i],
                           "City":df_final1['CityName'].iloc[i].lower(),
                           "Sub_Category_1":row})
                    maruti_df = pd.concat([maruti_df,df])
                # break
            maruti_df['Brand'] = 'Maruti Suzuki'
            maruti_df['Comments'] = 'Crawler Data'
            maruti_df['Relevant_Date'] = today.date()
            maruti_df['Runtime'] = datetime.datetime.now()
            maruti_df.to_sql(name = "STORE_LOCATOR_WEEKLY_DATA",index = False,con = engine,if_exists = "append")

            client1 = ClickHouse_db.db_conn()
            click_max_date = client1.execute("select max(Relevant_Date) from AdqvestDB.STORE_LOCATOR_WEEKLY_DATA where Brand = 'Maruti Suzuki'")
            click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])
            query = 'select * from AdqvestDB.STORE_LOCATOR_WEEKLY_DATA where Brand = "Maruti Suzuki" and Relevant_Date > "' + click_max_date + '"'
            df = pd.read_sql(query,engine)
            client1.execute("INSERT INTO AdqvestDB.STORE_LOCATOR_WEEKLY_DATA VALUES",df.values.tolist())
        log.job_end_log(table_name,job_start_time, no_of_ping)

    except:
        try:
            connection.close()
        except:
            pass
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by = 'manual')
