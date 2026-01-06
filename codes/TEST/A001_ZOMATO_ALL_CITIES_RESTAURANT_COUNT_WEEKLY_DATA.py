import datetime as datetime
from pytz import timezone
from bs4 import BeautifulSoup
import json
import re
import requests
import pandas as pd
import unidecode
import sys
import ast
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import ClickHouse_db
import JobLogNew as log
from adqvest_robotstxt import Robots
robot = Robots(__file__)

         

def run_program(run_by='Adqvest_Bot', py_file_name=None):

    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'ZOMATO_ALL_CITIES_RESTAURANT_COUNT_WEEKLY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = '1_PM_WINDOWS_SERVER_SCHEDULER'
    no_of_ping = 0
    engine = adqvest_db.db_conn()

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        query = "select max(Relevant_Date) as Date from AdqvestDB.ZOMATO_ALL_CITIES_RESTAURANT_COUNT_WEEKLY_DATA where Country='India'"
        max_date = pd.read_sql(query, con=engine)['Date'][0]
        print('IN 1')
        if((today.date() - max_date).days >= 0):
            print('IN 2')
            headers = {"Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,applicationsigned-exchange;v=b3;q=0.7",
                        "Accept-Encoding":"gzip, deflate, br",
                        "Accept-Language":"en-GB,en-US;q=0.9,en;q=0.8",
                        "Connection":"keep-alive",
                        "Host":"www.zomato.com",
                        "sec-ch-ua":'"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
                        "sec-ch-ua-platform":"macOS",
                        "Sec-Fetch-Dest":"document",
                        "Sec-Fetch-Mode":"navigate",
                        "Sec-Fetch-Site":"none",
                        "Sec-Fetch-User":"?1",
                        "Upgrade-Insecure-Requests":"1",
                        "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"}
            
            url = 'https://www.zomato.com/delivery-cities'
            r = requests.get(url,headers = headers)
            soup = BeautifulSoup(r.content, "lxml")
            print('IN SOUP')
            
            b = soup.find_all("script")
            b = [x for x in b if "window.__PRELOADED_STATE__" in x.text]
            b = r.text.split(" = JSON.parse(")[-1].split(");")[0]
            print(b)
            data = json.loads(ast.literal_eval(b))
            cities = data['pages']['deliverycities']['allO2Cities']
            city_urls = [x['url'] for x in cities]
            city_name = [x['name'] for x in cities]
            city_name = [x.replace("\n","") for x in city_name]
            print(city_name[0])
                    
            for url,city in zip(city_urls,city_name):   
                city = city
                r = requests.get(url,headers = headers)
                soup = BeautifulSoup(r.content, "lxml")
                b = soup.find_all("script")
                b = [x for x in b if "window.__PRELOADED_STATE__" in x.text]
                b = r.text.split(" = JSON.parse(")[-1].split(");")[0]
                data = json.loads(ast.literal_eval(b))
                # print(data)
                city_key = list(data['pages']['search'].keys())[0]#
                act_data = data['pages']['search'][city_key]['sections']['SECTION_POPULAR_LOCATIONS']['locations']
                # print('<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n',act_data)
                area_list = []
                count_list = []
                for i in act_data:
                    print(i['name'], '------>', i['count'])
                    area_list.append(i['name'].lower().replace('locality', '').strip().title())
                    count_list.append(i['count'].split()[0])
                    
                for area,count in zip(area_list,count_list):
                    item = {}
                    item['Country'] = "India"
                    item['City'] = city
                    item["Area"] = area
                    try:
                        item["Area_Clean"] = item["Area"].apply(lambda x:x.split("(")[0].strip())
                    except:
                        item["Area_Clean"] = item["Area"]
                    item["Count"] = count
                    item['Relevant_Date'] = today.date()
                    item['Runtime'] = today.strftime("%Y-%m-%d %H:%M:%S")
                    # zomato_df = pd.DataFrame(item)
                    zomato_df = pd.DataFrame([{"Country":item['Country'],"City":item['City'],"Area":item["Area"],"Area_Clean":item["Area_Clean"],"Count":item["Count"],"Relevant_Date":item['Relevant_Date'],"Runtime":item['Runtime']}])
                    # zomato_df["Area"] = zomato_df["Area"].apply(lambda x: x.split(" in")[-1].strip())
                    # zomato_df["Count"] = zomato_df["Count"].apply(lambda x: x.split("(")[-1].split("places")[0])
                    # zomato_df["City"] = zomato_df["City"].apply(lambda x: x.split("Restaurants")[0].strip())
                    zomato_df["City"] = zomato_df["City"].apply(lambda x: unidecode.unidecode(x))
                    zomato_df["Area"] = zomato_df["Area"].apply(lambda x: unidecode.unidecode(x))
            
                    zomato_df["Relevant_Week"] = today.date().strftime("%V")
                    zomato_df["Relevant_Week"] = zomato_df["Relevant_Week"].apply(lambda x:"Week "+ x +"-"+today.date().strftime("%Y"))
                    zomato_df.to_sql(name='ZOMATO_ALL_CITIES_RESTAURANT_COUNT_WEEKLY_DATA',con=engine,if_exists='append',index=False)
                    client = ClickHouse_db.db_conn()
                    click_max_date = client.execute("select max(Relevant_Date) from AdqvestDB.ZOMATO_ALL_CITIES_RESTAURANT_COUNT_WEEKLY_DATA WHERE Country IN ('INDIA')")
                    click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])
                    query = 'select * from AdqvestDB.ZOMATO_ALL_CITIES_RESTAURANT_COUNT_WEEKLY_DATA WHERE Country IN ("INDIA") AND Relevant_Date > "' + click_max_date + '"'
                    df = pd.read_sql(query,engine)
                    client.execute("INSERT INTO ZOMATO_ALL_CITIES_RESTAURANT_COUNT_WEEKLY_DATA VALUES",df.values.tolist())

        log.job_end_log(table_name,job_start_time,no_of_ping)


    except:

        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        error_msg = str(sys.exc_info()[1]) + " line " + str(exc_tb.tb_lineno)
        print(error_msg)

        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)


if(__name__=='__main__'):
    run_program(run_by='manual')
