from bs4 import BeautifulSoup
import pandas as pd
import time
import re
import sys
import datetime
from pytz import timezone
import warnings
warnings.filterwarnings('ignore')
from selenium import webdriver
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
from GetState import find_district
from GetState import find_state
import adqvest_db
import JobLogNew as log
import ClickHouse_db
from adqvest_robotstxt import Robots
robot = Robots(__file__)
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/State_Function')
import state_rewrite

from geopy.geocoders import Nominatim
geolocator = Nominatim(user_agent="geoapiExercises")
from geopy.exc import GeocoderInsufficientPrivileges

def data_collect(today):
    
    zeno = pd.DataFrame(columns = ['Category','Company','Brand','Address','City','State','Pincode','Country','Latitude','Longitude','Relevant_Date','Runtime'])
    
    chrome_driver = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
    download_file_path = r"C:\Users\Administrator\AdQvestDir\codes\One time run"
    # chrome_driver = r"D:\Adqvest\chrome_path\chromedriver.exe"
    # download_file_path = r"D:\Adqvest\Junk"
    prefs = {
        "download.default_directory": download_file_path,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True
        }

    options = webdriver.ChromeOptions()
    options.add_experimental_option('prefs', prefs)
    driver = webdriver.Chrome(executable_path=chrome_driver, chrome_options=options)
    
    url = "https://web.zeno.health/corporate/store-locator"
    driver.get(url)
    robot.add_link(url)
    time.sleep(3)
    
    html_content = driver.page_source
    soup = BeautifulSoup(html_content, 'lxml')
    
    list_buttons = soup.find_all('address', class_='store-address')
    time.sleep(1)
    
    for i in list_buttons:
        address = i.text.strip()
        address = address.replace('\n', ' ')
        time.sleep(1)
        href = i.find('a')['href']
        destination = href.split('destination=')[1]
        lat, lon = map(float, destination.split(','))
        pincode_match = re.search(r'(?<!\d)\d{6}(?!\d)|\d{3}\s\d{3}', address)
        pincode = None
        city_new= None
        state = None
        if pincode_match:
            pincode = pincode_match.group(0)
            city_new = find_district(pincode, address)
            state = find_state(pincode, address)
            zeno.loc[len(zeno)] = ['Healthcare','Zeno Health','Zeno Health',address,city_new,state.title(),pincode,'India',lat,lon,today.date(),today]
            zeno = zeno.drop_duplicates(subset='Address', keep='first')
        
        else: 
            location = geolocator.reverse(destination)
            total_address = location.raw['address']

            city = total_address.get('city', '')
            city = city.split("City")[0].split("Suburban")[0].strip()
            state = total_address.get('state', '')
            pincode = total_address.get('postcode')
            zeno.loc[len(zeno)] = ['Healthcare','Zeno Health','Zeno Health',address,city,state,pincode,'India',lat,lon,today.date(),today]
            zeno = zeno.drop_duplicates(subset='Address', keep='first')
        
    driver.quit()         

    return zeno

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    # os.chdir('/home/ubuntu/AdQvestDir')
    engine = adqvest_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'STORE_LOCATOR_WEEKLY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
        
        #last_rel_date = pd.read_sql("select max(Relevant_Date) as Max from STORE_LOCATOR_WEEKLY_DATA where Brand = 'Zeno Health'",engine)
        #last_rel_date = last_rel_date["Max"][0]
        #print("Last Data Updation Date : ",last_rel_date)
        
        #if today.date()-last_rel_date >= datetime.timedelta(7):
            
        engine = adqvest_db.db_conn()
        zeno_final = data_collect(today)
        zeno_final.to_sql(name = "STORE_LOCATOR_WEEKLY_DATA",if_exists="append",index = False,con = engine)
        print("Data uploaded to SQL") 
        
        #client1 = ClickHouse_db.db_conn()
        #click_max_date = client1.execute("select max(Relevant_Date) from AdqvestDB.STORE_LOCATOR_WEEKLY_DATA where Brand = 'Zeno Health'")
        #click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])
        #query = 'select * from AdqvestDB.STORE_LOCATOR_WEEKLY_DATA where Brand = "Zeno Health" and Relevant_Date > "' + click_max_date + '"'
        #df = pd.read_sql(query,engine)
        #client1.execute("INSERT INTO AdqvestDB.STORE_LOCATOR_WEEKLY_DATA VALUES",df.values.tolist())
        #print("Data uploaded to Clickhouse")
    
        #else:
         #   print("Data already present")
        log.job_end_log(table_name,job_start_time,no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')     