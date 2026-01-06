import requests
import xml.etree.ElementTree as ET
import pandas as pd
from bs4 import BeautifulSoup
import time
import re
from selenium import webdriver
import warnings
warnings.filterwarnings('ignore')
import datetime
import sys
from pytz import timezone
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
from webdriver_manager.chrome import ChromeDriverManager
from adqvest_robotstxt import Robots
robot = Robots(__file__)

def clean(input_string):
    input_string = input_string.replace('&', 'and')
    
    input_string = re.sub(r'[^a-zA-Z0-9\s]', '', input_string)
    
    return input_string

def parse_url(url_element):
    data = {}
    for child in url_element:
        if child.tag.endswith('loc'):
            data['loc'] = child.text
        elif child.tag.endswith('lastmod'):
            data['lastmod'] = child.text
        elif child.tag.endswith('changefreq'):
            data['changefreq'] = child.text
        elif child.tag.endswith('priority'):
            data['priority'] = child.text
    return data

def data_collection(today):
    
    dmart_df = pd.DataFrame(columns = ['Company_Name','Category','Sub_Category_1','Sub_Category_2','Product','Quantity','Units','Discounted_Price','Max_Retail_Price','Relevant_Date','Runtime'])

    response = requests.get('https://www.dmart.in/sitemap.xml')

    if response.status_code == 200:
        root = ET.fromstring(response.content)
        data_list = []
     
        for url_element in root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}url'):
            data = parse_url(url_element)
            data_list.append(data)

        df = pd.DataFrame(data_list)

    else:
        print("Failed to fetch XML data from the URL")
        
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-infobars")
    options.add_argument("start-maximized")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-notifications")
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--no-sandbox')  
    driver_path = r'C:\Users\Administrator\AdQvestDir\chromedriver.exe'    
    driver = webdriver.Chrome(driver_path, options = options)
    driver.get('https://www.dmart.in/')
    time.sleep(10)
    for cat_link in df['loc']:
        time.sleep(2)
        driver.get(cat_link)
        print(cat_link)
        time.sleep(10)
        soup = BeautifulSoup(driver.page_source,'lxml')
        products = soup.find_all('div',class_=re.compile(r'vertical-card_card.*'))
        
        for product in products:
            product_details= product.find('div',class_ = re.compile(r'vertical-card_title.*')).text.split(':')
            product_name = product_details[0].strip()
            try:
                try:
                    product_qty = product_details[1].strip()
                except:
                    product_qty = product.find('div',class_ = re.compile(r'.*MuiInputBase-input.*')).text
                pattern = r'(\d+)\s*(\w+)'
                match = re.match(pattern, product_qty)
                try:
                    qty = match.group(1)
                    qty_units = match.group(2)
                except:
                    pattern = r'(\w+)\s*(\d+)'
                    match = re.match(pattern, product_qty)
                    qty = match.group(2)
                    qty_units = match.group(1)
            except:
                qty = None
                qty_units = None
            price_details= product.find('div',class_ = re.compile(r'vertical-card_price-left.*')).text.split('DMart')
            mrp = price_details[0].strip().replace('₹','').replace('MRP','').strip()
            dmart_price = price_details[1].replace('₹','').strip()
            
            cat_levels = soup.find_all('li',class_ = re.compile(r'MuiBreadcrumbs-li'))
            category = None
            sub_category_1 = None
            sub_category_2 = None

            for i in range(len(cat_levels)):
                if i==0:
                    category = cat_levels[i].text
                elif i==1:
                    sub_category_1 = cat_levels[i].text
                elif i==2:
                    sub_category_2 = cat_levels[i].text

            print(category,sub_category_1,sub_category_2)
            dmart_df.loc[len(dmart_df)] = ['DMART',clean(category),clean(sub_category_1),clean(sub_category_2),product_name,qty,qty_units,dmart_price,mrp,today.date(),today]
    driver.close()
    return dmart_df

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir')
    engine = adqvest_db.db_conn()

    # ****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today = datetime.datetime.now(india_time)

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = "DMART_ALL_CATEGORIES_PRODUCTS_WEEKLY_DATA"

    if (py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if (run_by == 'Adqvest_Bot'):
            log.job_start_log_by_bot(table_name, py_file_name, job_start_time)
        else:
            log.job_start_log(table_name, py_file_name, job_start_time, scheduler)

        # max_rel_date = pd.read_sql('SELECT MAX(Relevant_Date) as max from DMART_ALL_CATEGORIES_PRODUCTS_WEEKLY_DATA',con=engine)['max'][0]

        # if today.date() - max_rel_date >= datetime.timedelta(7):
        dmart = data_collection(today)
        dmart.to_sql('DMART_ALL_CATEGORIES_PRODUCTS_WEEKLY_DATA', if_exists='replace', index=False, con = engine)
        # else:
        #     print('No new data')
        log.job_end_log(table_name, job_start_time, no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'", str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name, job_start_time, error_type, error_msg, no_of_ping)
        
if (__name__ == '__main__'):
    run_program(run_by='manual')