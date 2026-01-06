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
import JobLogNew as log


def spaces(x):
  try:
    return re.sub(' +', ' ', x)
  except:
    return None

def splchar(x):
  try:
    return re.sub('[^A-Za-z0-9]+', ' ', x)
  except:
    return None

def run_program(run_by='Adqvest_Bot', py_file_name=None):

    engine = adqvest_db.db_conn()

    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'STORE_LOCATOR_WEEKLY_CLEAN_DATA_Temp2_Nidhi'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        # Get last date of STORE LOCATOR BASE TABLE
        query = "select max(Relevant_Date) as Relevant_Date from AdqvestDB.STORE_LOCATOR_WEEKLY_DATA"
        max_date_base = pd.read_sql(query, con=engine)['Relevant_Date'][0]

        # Get last date of STORE LOCATOR BASE TABLE
        # query = "select max(Relevant_Date) as Relevant_Date from AdqvestDB.STORE_LOCATOR_WEEKLY_CLEAN_DATA_Temp2_Nidhi"
        # max_date_clean = pd.read_sql(query, con=engine)['Relevant_Date'][0]

        # print("BASE TABLE MAX DATE:",max_date_base,"CLEAN TABLE MAX DATE:",max_date_clean)

        # if (max_date_base > max_date_clean):
        #     print("THERE IS DATA TO BE CLEANED")

        file = pd.read_sql("Select * from AdqvestDB.STORE_LOCATOR_WEEKLY_DATA",con=engine)

        file.loc[file['Company'].str.contains('MF',na = False), 'Brand'] = 'SBI MF'
        file.loc[file['Company'].str.contains('MF',na = False), 'Company'] = 'SBI'

        file.loc[file['Company'].str.contains('Lombard',na = False), 'Brand'] = 'ICICI Lombard'
        file.loc[file['Company'].str.contains('Lombard',na = False), 'Company'] = 'ICICI'

        file.loc[file['Company'].str.contains('wheelers',na = False), 'Brand'] = 'Mahindra Two Wheelers'
        file.loc[file['Company'].str.contains('wheelers',na = False), 'Company'] = 'Mahindra'

        file.loc[file['Company'].str.contains('Magma',na = False), 'Brand'] = 'Magma HDI'
        file.loc[file['Company'].str.contains('Magma',na = False), 'Company'] = 'Magma Fincorp Ltd & HDI Global SE'

        file.loc[file['Company'].str.contains('Royal',na = False), 'Brand'] = 'Royal Sundaram'
        file.loc[file['Company'].str.contains('Royal',na = False), 'Company'] = 'Sundaram Finances'

        file.loc[file['Company'].str.contains('Allianz',na = False), 'Brand'] = 'Bajaj Allianz Life Insurance'
        file.loc[file['Company'].str.contains('Allianz',na = False), 'Company'] = 'Bajaj Finserv Limited'

        file.loc[file['Company'].str.contains('United',na = False), 'Brand'] = 'United India Insurance '
        file.loc[file['Company'].str.contains('United',na = False), 'Company'] = 'United India'

        file.loc[file['Company'].str.contains('Hyundai',na = False), 'Brand'] = 'Hyundai'
        file.loc[file['Company'].str.contains('Hyundai',na = False), 'Company'] = 'Hyundai Motor Company'

        file.loc[file['Company'].str.contains('Liberty',na = False), 'Brand'] = 'Liberty Finance'
        file.loc[file['Company'].str.contains('Liberty',na = False), 'Company'] = 'LMHC Massachusetts Holdings'

        file.loc[file['Company'].str.contains('Hero',na = False), 'Brand'] = 'Hero'
        file.loc[file['Company'].str.contains('Hero',na = False), 'Company'] = 'Hero Motors Company'

        file.loc[file['Company'].str.contains('Honda',na = False), 'Brand'] = 'Honda'
        file.loc[file['Company'].str.contains('Honda',na = False), 'Company'] = 'Honda Motors'

        file.loc[file['Company'].str.contains('Career',na = False), 'Brand'] = 'Career Launcher'
        file.loc[file['Company'].str.contains('Career',na = False), 'Company'] = 'CL Educate'

        file.loc[file['Company'].str.contains('Suzuki',na = False), 'Brand'] = 'Maruti Suzuki'
        file.loc[file['Company'].str.contains('Suzuki',na = False), 'Company'] = 'Suzuki Motor'

        file.loc[file['Company'].str.contains('Aakash',na = False), 'Brand'] = 'Aakash Coaching Centres'
        file.loc[file['Company'].str.contains('Aakash',na = False), 'Company'] = 'Aakash'

        file.loc[file['Company'].str.contains('Vespa',na = False), 'Brand'] = 'Vespa'
        file.loc[file['Company'].str.contains('Vespa',na = False), 'Company'] = 'Piaggio Group'

        file.loc[file['Company'].str.contains('Religare',na = False), 'Brand'] = 'Religare Health Insurance'
        file.loc[file['Company'].str.contains('Religare',na = False), 'Company'] = 'Religare Enterprises Limited'

        file.loc[file['Company'].str.contains('Home',na = False), 'Brand'] = 'Home Centre Stores'
        file.loc[file['Company'].str.contains('Home',na = False), 'Company'] = 'Home Centre'

        file.loc[file['Company'].str.contains('MS',na = False), 'Brand'] = 'Chola MS'
        file.loc[file['Company'].str.contains('MS',na = False), 'Company'] = 'Cholamandalam Investment And Finance Company'

        file.loc[file['Company'].str.contains('Belgian',na = False), 'Brand'] = 'Belgian Waffle'
        file.loc[file['Company'].str.contains('Belgian',na = False), 'Company'] = 'Bloombay Enterprises'

        file.loc[file['Company'].str.contains('Pantaloons',na = False), 'Brand'] = 'Pantaloons'
        file.loc[file['Company'].str.contains('Pantaloons',na = False), 'Company'] = 'ADITYA BIRLA FASION AND RETAIL'

        file.loc[file['Company'].str.contains('Burger',na = False), 'Brand'] = 'Burger King'
        file.loc[file['Company'].str.contains('Burger',na = False), 'Company'] = 'Restaurant Brands International & 3G Capital'

        file.loc[file['Company'].str.contains('Metropolis',na = False), 'Brand'] = 'Metropolis Labs'
        file.loc[file['Company'].str.contains('Metropolis',na = False), 'Company'] = 'Metropolis'

        file.loc[file['Company'].str.contains('Biba',na = False), 'Brand'] = 'Biba'
        file.loc[file['Company'].str.contains('Biba',na = False), 'Company'] = 'Biba Apparels Private limited'

        file.loc[file['Company'].str.contains('Desi',na = False), 'Brand'] = 'Global Desi'
        file.loc[file['Company'].str.contains('Desi',na = False), 'Company'] = 'Ochre & Black Private Limited'

        file.loc[file['Company'].str.contains('KFC',na = False), 'Brand'] = 'KFC'
        file.loc[file['Company'].str.contains('KFC',na = False), 'Company'] = 'Yum Brands'

        file.loc[file['Company'].str.contains('Spencers',na = False), 'Brand'] = 'Spencers Retail'
        file.loc[file['Company'].str.contains('Spencers',na = False), 'Company'] = 'RPSG Group'

        file.loc[file['Company'].str.contains('Jawa',na = False), 'Brand'] = 'Jawa Moto'
        file.loc[file['Company'].str.contains('Jawa',na = False), 'Company'] = 'Jihostroj'

        file.loc[file['Company'].str.contains('Sharekhan',na = False), 'Brand'] = 'Sharekhan Limited'
        file.loc[file['Company'].str.contains('Sharekhan',na = False), 'Company'] = 'BNP Paribas'

        file.loc[file['Company'].str.contains('Oriental',na = False), 'Brand'] = 'Oriental'
        file.loc[file['Company'].str.contains('Oriental',na = False), 'Company'] = 'Mandarin Oriental International Limited'

        file.loc[file['Company'].str.contains('OBC',na = False), 'Brand'] = 'Canara HSBC OBC Life Insurance'
        file.loc[file['Company'].str.contains('OBC',na = False), 'Company'] = 'Canara Bank'

        file.loc[file['Company'].str.contains('House',na = False), 'Brand'] = 'House of Indya'
        file.loc[file['Company'].str.contains('House',na = False), 'Company'] = 'FabAlley & Indya High Street Essentials'

        file.loc[file['Company'].str.contains('Keventers',na = False), 'Brand'] = 'Keventers'
        file.loc[file['Company'].str.contains('Keventers',na = False), 'Company'] = 'Super Milk Products Private Limited'

        file.loc[file['Company'].str.contains('Lenskart',na = False), 'Brand'] = 'Lenskart'
        file.loc[file['Company'].str.contains('Lenskart',na = False), 'Company'] = 'VALYOO Technologies'

        file.loc[file['Company'].str.contains('Transport',na = False), 'Brand'] = 'Shriram Transport Finance Company'
        file.loc[file['Company'].str.contains('Transport',na = False), 'Company'] = 'Shriram Group'

        file.loc[file['Company'].str.contains('Pizza',na = False), 'Brand'] = 'Pizza Hut'
        file.loc[file['Company'].str.contains('Pizza',na = False), 'Company'] = 'Yum Brands'

        file.loc[file['Company'].str.contains('Finserv',na = False), 'Brand'] = 'Bajaj Allianz Life Insurance'
        file.loc[file['Company'].str.contains('Finserv',na = False), 'Company'] = 'Bajaj Finserv Limited'

        file.loc[file['Company'].str.contains('Jockey',na = False), 'Brand'] = 'Jockey'
        file.loc[file['Company'].str.contains('Jockey',na = False), 'Company'] = 'Page Industries'

        file.loc[file['Company'].str.contains('Planet',na = False), 'Brand'] = 'Planet Fitness'
        file.loc[file['Company'].str.contains('Planet',na = False), 'Company'] = 'Planet Fitness Inc'

        file.loc[file['Company'].str.contains('Stop',na = False), 'Brand'] = 'SHOPPERS STOP'
        file.loc[file['Company'].str.contains('Stop',na = False), 'Company'] = 'K Raheja Corp'

        file.loc[file['Company'].str.contains('Maruti',na = False), 'Brand'] = 'Maruti Suzuki'
        file.loc[file['Company'].str.contains('Maruti',na = False), 'Company'] = 'Suzuki Motor'

        file.loc[file['Company'].str.contains('Legends',na = False), 'Brand'] = 'Classic Legends'
        file.loc[file['Company'].str.contains('Legends',na = False), 'Company'] = 'Mahindra'

        file.loc[file['Company'].str.contains('Ampere',na = False), 'Brand'] = 'Ampere Vehicles'
        file.loc[file['Company'].str.contains('Ampere',na = False), 'Company'] = 'Greaves Electric Mobility'


        file.loc[file['Company'].str.contains('Reliance General Insurance',na = False), 'Brand'] = 'Reliance General Insurance'
        file.loc[file['Company'].str.contains('Reliance General Insurance',na = False), 'Company'] = 'Reliance Capital'

        file.loc[file['Company'].str.contains('Future Generali General Insurance',na = False), 'Brand'] = 'Future General Life Insurance'
        file.loc[file['Company'].str.contains('Future Generali General Insurance',na = False), 'Company'] = 'Future Group'

        file.loc[file['Company'].str.contains('Bharti AXA GI',na = False), 'Brand'] = 'Bharti AXA General Insurance'
        file.loc[file['Company'].str.contains('Bharti AXA GI',na = False), 'Company'] = 'AXA'

        file.loc[file['Company'].str.contains('Reliance Life',na = False), 'Brand'] = 'Reliance Life'
        file.loc[file['Company'].str.contains('Reliance Life',na = False), 'Company'] = 'Reliance Capital'

        file.loc[file['Company'].str.contains('IFFCO Tokio',na = False), 'Brand'] = 'IFFCO Tokio'
        file.loc[file['Company'].str.contains('IFFCO Tokio',na = False), 'Company'] = 'Indian Farmers Fertiliser Cooperative'

        file.loc[file['Company'].str.contains('Shriram General Insurance',na = False), 'Brand'] = 'Shriram General Insurance'
        file.loc[file['Company'].str.contains('Shriram General Insurance',na = False), 'Company'] = 'Shriram Group'

        file.loc[file['Company'].str.contains('Future Generali Life insurance',na = False), 'Brand'] = 'Future General Life insurance'
        file.loc[file['Company'].str.contains('Future Generali Life insurance',na = False), 'Company'] = 'Future Group'

        file.loc[file['Company'].str.contains('Bharti AXA LI',na = False), 'Brand'] = 'Bharti AXA Life Insurance'
        file.loc[file['Company'].str.contains('Bharti AXA LI',na = False), 'Company'] = 'AXA'

        file.loc[file['Company'].str.contains('Tata',na = False), 'Brand'] = 'Tata'
        file.loc[file['Company'].str.contains('Tata',na = False), 'Company'] = 'Tata'

        file.loc[file['Company'].str.contains('Bajaj',na = False), 'Brand'] = 'Bajaj Auto'
        file.loc[file['Company'].str.contains('Bajaj',na = False), 'Company'] = 'Bajaj Group'

        file.loc[file['Company'].str.contains('Shriram Life Insurance',na = False), 'Brand'] = 'Shriram Life Insurance'
        file.loc[file['Company'].str.contains('Shriram Life Insurance',na = False), 'Company'] = 'Shriram Group'


        file.loc[file['Company'].str.contains('Kotak Securities Limited',na = False), 'Brand'] = 'Kotak Mahindra'
        file.loc[file['Company'].str.contains('Kotak Securities Limited',na = False), 'Company'] = 'Kotak Securities Limited'

        file.loc[file['Company'].str.contains('HDFC Ergo',na = False), 'Brand'] = 'HDFC Ergo'
        file.loc[file['Company'].str.contains('HDFC Ergo',na = False), 'Company'] = 'HDFC'

        file.loc[file['Company'].str.contains('Lifestyle Stores',na = False), 'Brand'] = 'Landmark Group'
        file.loc[file['Company'].str.contains('Lifestyle Stores',na = False), 'Company'] = 'Lifestyle Stores'

        file.loc[file['Company'].str.contains('AND',na = False), 'Brand'] = '& Co'
        file.loc[file['Company'].str.contains('AND',na = False), 'Company'] = 'AND Clothing'

        file.loc[file['Company'].str.contains('DHFL Pramerica Life Insurance',na = False), 'Brand'] = 'DHFL Investments & Prudential International Insurance Holdings'
        file.loc[file['Company'].str.contains('DHFL Pramerica Life Insurance',na = False), 'Company'] = 'DHFL Pramerica Life Insurance'

        file.loc[file['Company'].str.contains('IFFCO Tokio',na = False), 'Brand'] = 'Indian Farmers Fertiliser Cooperative'
        file.loc[file['Company'].str.contains('IFFCO Tokio',na = False), 'Company'] = 'IFFCO Tokio'

        file.loc[file['Company'].str.contains('HDFC Ergo General Insurance',na = False), 'Brand'] = 'HDFC Ergo'
        file.loc[file['Company'].str.contains('HDFC Ergo General Insurance',na = False), 'Company'] = 'HDFC'

        file.loc[file['Company'].str.contains('Apollo',na = False), 'Company'] = 'Apollo Hospitals Group'
        file.loc[file['Company'].str.contains('Apollo',na = False), 'Brand'] = 'Apollo Hospitals'

        

        file.loc[file['Brand'].str.contains('Gold',na = False), 'Company'] = 'RSG Group GmbH'
        file.loc[file['Brand'].str.contains('Gold',na = False), 'Brand'] = 'Golds Gym'

        file.loc[file['Brand'].str.contains('Metropolis',na = False), 'Company'] = 'Metropolis'
        file.loc[file['Brand'].str.contains('Metropolis',na = False), 'Brand'] = 'Metropolis Labs'

        file.loc[file['Brand'].str.contains('Maruti Suzuki',na = False), 'Company'] = 'Suzuki Motor'
        file.loc[file['Brand'].str.contains('Maruti Suzuki',na = False), 'Brand'] = 'Maruti Suzuki'

        file.loc[file['Brand'].str.contains('Nexa',na = False), 'Company'] = 'Suzuki Motor'
        file.loc[file['Brand'].str.contains('Nexa',na = False), 'Brand'] = 'Nexa Maruti Suzuki'

        file.loc[file['Brand'].str.contains('Bajaj',na = False), 'Company'] = 'Bajaj Group'
        file.loc[file['Brand'].str.contains('Bajaj',na = False), 'Brand'] = 'Bajaj Auto'

        file.loc[file['Brand'].str.contains('HDFC Life',na = False), 'Company'] = 'HDFC'
        file.loc[file['Brand'].str.contains('HDFC Life',na = False), 'Brand'] = 'HDFC Life'

        file.loc[file['Brand'].str.contains('McDonalds',na = False), 'Company'] = 'Westlife'
        file.loc[file['Brand'].str.contains('McDonalds',na = False), 'Brand'] = 'McDonalds'

        file.loc[file['Brand'].str.contains('Burger',na = False), 'Company'] = 'Restaurant Brands International & 3G Capital'
        file.loc[file['Brand'].str.contains('Burger',na = False), 'Brand'] = 'Burger King'

        file.loc[file['Brand'].str.contains('Fab',na = False), 'Company'] = 'Fab India'
        file.loc[file['Brand'].str.contains('Fab',na = False), 'Brand'] = 'Fab India Clothing'

        file.loc[file['Brand'].str.contains('Hyundai',na = False), 'Company'] = 'Hyundai Motors'
        file.loc[file['Brand'].str.contains('Hyundai',na = False), 'Brand'] = 'Hyundai'

        file.loc[file['Brand'].str.contains('ICICI Securities Limited',na = False), 'Company'] = 'ICICI'
        file.loc[file['Brand'].str.contains('ICICI Securities Limited',na = False), 'Brand'] = 'ICICI Securities Limited'

        file.loc[file['Brand'].str.contains('Future Generali Life insurance',na = False), 'Company'] = 'Future Group'
        file.loc[file['Brand'].str.contains('Future Generali Life insurance',na = False), 'Brand'] = 'Future General Life insurance'

        file.loc[file['Brand'].str.contains('Lombard',na = False), 'Company'] = 'ICICI'
        file.loc[file['Brand'].str.contains('Lombard',na = False), 'Brand'] = 'ICICI Lombard'

        file.loc[file['Brand'].str.contains('Bharti AXA GI',na = False), 'Company'] = 'AXA'
        file.loc[file['Brand'].str.contains('Bharti AXA GI',na = False), 'Brand'] = 'Bharti AXA General Insurance'

        file.loc[file['Brand'].str.contains('Kotak Securities Limited',na = False), 'Company'] = 'Kotak Mahindra'
        file.loc[file['Brand'].str.contains('Kotak Securities Limited',na = False), 'Brand'] = 'Kotak Securities Limited'

        file.loc[file['Brand'].str.contains('Spencers',na = False), 'Company'] = 'RPSG Group'
        file.loc[file['Brand'].str.contains('Spencers',na = False), 'Brand'] = 'Spencers Retail'

        file.loc[file['Brand'].str.contains('Sharekhan Limited',na = False), 'Company'] = 'BNP Paribas'
        file.loc[file['Brand'].str.contains('Sharekhan Limited',na = False), 'Brand'] = 'Sharekhan Limited'

        file.loc[file['Brand'].str.contains('KFC',na = False), 'Company'] = 'Yum Brands'
        file.loc[file['Brand'].str.contains('KFC',na = False), 'Brand'] = 'KFC'

        file.loc[file['Brand'].str.contains('Keventers',na = False), 'Company'] = 'Super Milk Products Private Limited'
        file.loc[file['Brand'].str.contains('Keventers',na = False), 'Brand'] = 'Keventers'

        file.loc[file['Brand'].str.contains('Global Desi',na = False), 'Company'] = 'Ochre & Black Private Limited'
        file.loc[file['Brand'].str.contains('Global Desi',na = False), 'Brand'] = 'Global Desi'

        file.loc[file['Brand'].str.contains('AND',na = False), 'Company'] = '& Co'
        file.loc[file['Brand'].str.contains('AND',na = False), 'Brand'] = 'AND Clothing'

        file.loc[file['Brand'].str.contains('Jawa',na = False), 'Company'] = 'Jihostroj'
        file.loc[file['Brand'].str.contains('Jawa',na = False), 'Brand'] = 'Jawa Moto'

        file.loc[file['Brand'].str.contains('House of Indya',na = False), 'Company'] = 'FabAlley & Indya High Street Essentials'
        file.loc[file['Brand'].str.contains('House of Indya',na = False), 'Brand'] = 'House of Indya'

        file.loc[file['Brand'].str.contains('DHFL Pramerica Life Insurance',na = False), 'Company'] = 'DHFL Investments & Prudential International Insurance Holdings'
        file.loc[file['Brand'].str.contains('DHFL Pramerica Life Insurance',na = False), 'Brand'] = 'DHFL Pramerica Life Insurance'

        file.loc[file['Brand'].str.contains('IFFCO Tokio',na = False), 'Company'] = 'Indian Farmers Fertiliser Cooperative'
        file.loc[file['Brand'].str.contains('IFFCO Tokio',na = False), 'Brand'] = 'IFFCO Tokio'

        file.loc[file['Brand'].str.contains('Reliance Life',na = False), 'Company'] = 'Reliance Capital'
        file.loc[file['Brand'].str.contains('Reliance Life',na = False), 'Brand'] = 'Reliance Life'

        file.loc[file['Brand'].str.contains('Reliance General Insurance',na = False), 'Company'] = 'Reliance Capital'
        file.loc[file['Brand'].str.contains('Reliance General Insurance',na = False), 'Brand'] = 'Reliance General Insurance'

        file.loc[file['Brand'].str.contains('Magma HDI',na = False), 'Company'] = 'Magma Fincorp Ltd & HDI Global SE'
        file.loc[file['Brand'].str.contains('Magma HDI',na = False), 'Brand'] = 'Magma HDI'

        file.loc[file['Brand'].str.contains('Suzuki',na = False), 'Company'] = 'Suzuki Motor'
        file.loc[file['Brand'].str.contains('Suzuki',na = False), 'Brand'] = 'Maruti Suzuki'

        file.loc[file['Brand'].str.contains('Liberty',na = False), 'Company'] = 'LMHC Massachusetts Holdings'
        file.loc[file['Brand'].str.contains('Liberty',na = False), 'Brand'] = 'Liberty Finance'

        file.loc[file['Brand'].str.contains('Future Generali General Insurance',na = False), 'Company'] = 'Future Group'
        file.loc[file['Brand'].str.contains('Future Generali General Insurance',na = False), 'Brand'] = 'Future General Life Insurance'

        file.loc[file['Brand'].str.contains('Bharti AXA LI',na = False), 'Company'] = 'AXA'
        file.loc[file['Brand'].str.contains('Bharti AXA LI',na = False), 'Brand'] = 'Bharti AXA Life Insurance'

        file.loc[file['Brand'].str.contains('Vespa',na = False), 'Company'] = 'Piaggio Group'
        file.loc[file['Brand'].str.contains('Vespa',na = False), 'Brand'] = 'Vespa'

        file.loc[file['Brand'].str.contains('Shriram Life Insurance',na = False), 'Company'] = 'Shriram Group'
        file.loc[file['Brand'].str.contains('Shriram Life Insurance',na = False), 'Brand'] = 'Shriram Life Insurance'

        file.loc[file['Brand'].str.contains('Shriram General Insurance',na = False), 'Company'] = 'Shriram Group'
        file.loc[file['Brand'].str.contains('Shriram General Insurance',na = False), 'Brand'] = 'Shriram General Insurance'

        file.loc[file['Brand'].str.contains('Religare Health',na = False), 'Company'] = 'Religare Enterprises Limited'
        file.loc[file['Brand'].str.contains('Religare Health',na = False), 'Brand'] = 'Religare Health Insurance'

        file.loc[file['Brand'].str.contains('Hero',na = False), 'Company'] = 'Hero Motors'
        file.loc[file['Brand'].str.contains('Hero',na = False), 'Brand'] = 'Hero'

        file.loc[file['Brand'].str.contains('Bajaj Allianz Life Insurance',na = False), 'Company'] = 'Bajaj Finserv Limited'
        file.loc[file['Brand'].str.contains('Bajaj Allianz Life Insurance',na = False), 'Brand'] = 'Bajaj Allianz Life Insurance'

        file.loc[file['Brand'].str.contains('Lifestyle Stores',na = False), 'Company'] = 'Landmark Group'
        file.loc[file['Brand'].str.contains('Lifestyle Stores',na = False), 'Brand'] = 'Lifestyle Stores'

        file.loc[file['Brand'].str.contains('Biba',na = False), 'Company'] = 'Biba Apparels Private limited'
        file.loc[file['Brand'].str.contains('Biba',na = False), 'Brand'] = 'Biba'

        file.loc[file['Brand'].str.contains('Shriram Transport Finance Co. Ltd',na = False), 'Company'] = 'Shriram Group'
        file.loc[file['Brand'].str.contains('Shriram Transport Finance Co. Ltd',na = False), 'Brand'] = 'Shriram Transport Finance Co. Ltd'

        file.loc[file['Brand'].str.contains('Harley Davidson',na = False), 'Company'] = 'Harley Davidson Motor Co Group'
        file.loc[file['Brand'].str.contains('Harley Davidson',na = False), 'Brand'] = 'Harley Davidson'

        file.loc[file['Brand'].str.contains('W for Women',na = False), 'Company'] = 'TCNS Clothing'
        file.loc[file['Brand'].str.contains('W for Women',na = False), 'Brand'] = 'W for Women'

        file.loc[file['Brand'].str.contains('Aurelia',na = False), 'Company'] = 'TCNS Clothing'
        file.loc[file['Brand'].str.contains('Aurelia',na = False), 'Brand'] = 'Aurelia'

        file.loc[file['Brand'].str.contains('Color Plus',na = False), 'Company'] = 'RAYMOND GROUP'
        file.loc[file['Brand'].str.contains('Color Plus',na = False), 'Brand'] = 'Color Plus'

        file.loc[file['Brand'].str.contains('Raymond Ready To Wear',na = False), 'Company'] = 'RAYMOND GROUP'
        file.loc[file['Brand'].str.contains('Raymond Ready To Wear',na = False), 'Brand'] = 'Raymond Ready To Wear'

        file.loc[file['Brand'].str.contains(' HDFC Life',na = False), 'Company'] = 'HDFC'
        file.loc[file['Brand'].str.contains(' HDFC Life',na = False), 'Brand'] = 'HDFC Life'


        file.loc[file['Brand'].str.contains('Gold',na = False), 'Company'] = 'RSG Group GmbH'
        file.loc[file['Brand'].str.contains('Gold',na = False), 'Brand'] = 'Golds Gym'

        file.loc[file['Brand'].str.contains('Pink',na = False), 'Company'] = 'Pink Fitness'
        file.loc[file['Brand'].str.contains('Pink',na = False), 'Brand'] = 'Pink Ladies Gym'

        file.loc[file['Brand'].str.contains('SLAM',na = False), 'Company'] = 'Grand Slam Fitness'
        file.loc[file['Brand'].str.contains('SLAM',na = False), 'Brand'] = 'SLAM'

        file.loc[file['Brand'].str.contains('Royal Enfield',na = False), 'Company'] = 'EICHER MOTORS'
        file.loc[file['Brand'].str.contains('Royal Enfield',na = False), 'Brand'] = 'Royal Enfield'

        file.loc[file['Brand'].str.contains('DR LAL PATH LABS',na = False), 'Company'] = 'DR LAL PATH LABS'
        file.loc[file['Brand'].str.contains('DR LAL PATH LABS',na = False), 'Brand'] = 'DR LAL PATH LABS'

        file.loc[file['Brand'].str.contains('Vidyamandir',na = False), 'Company'] = 'Vidyamandir'
        file.loc[file['Brand'].str.contains('Vidyamandir',na = False), 'Brand'] = 'Vidyamandir'

        file.loc[file['Brand'].str.contains('TIME',na = False), 'Company'] = 'Triumphant Institute of Management Education'
        file.loc[file['Brand'].str.contains('TIME',na = False), 'Brand'] = 'TIME'

        file.loc[file['Brand'].str.contains('Van Heusen',na = False), 'Company'] = 'ADITYA BIRLA FASION AND RETAIL'
        file.loc[file['Brand'].str.contains('Van Heusen',na = False), 'Brand'] = 'Van Heusen'

        file.loc[file['Brand'].str.contains('Louis Philippe',na = False), 'Company'] = 'ADITYA BIRLA FASION AND RETAIL'
        file.loc[file['Brand'].str.contains('Louis Philippe',na = False), 'Brand'] = 'Louis Philippe'

        file.loc[file['Brand'].str.contains('Peter England',na = False), 'Company'] = 'ADITYA BIRLA FASION AND RETAIL'
        file.loc[file['Brand'].str.contains('Peter England',na = False), 'Brand'] = 'Peter England'

        file.loc[file['Brand'].str.contains('Allen Solly',na = False), 'Company'] = 'ADITYA BIRLA FASION AND RETAIL'
        file.loc[file['Brand'].str.contains('Allen Solly',na = False), 'Brand'] = 'Allen Solly'

        file.loc[file['Brand'].str.contains('BATA',na = False), 'Company'] = 'BATA INDIA'
        file.loc[file['Brand'].str.contains('BATA',na = False), 'Brand'] = 'Bata Footwear'

        file.loc[file['Brand'].str.contains('HUSH PUPPIES',na = False), 'Company'] = 'Bata India & Wolverine World Wide '
        file.loc[file['Brand'].str.contains('HUSH PUPPIES',na = False), 'Brand'] = 'Hush Puppies'

        file.loc[file['Brand'].str.contains("Domino's Pizza",na = False), 'Company'] = 'JUBILANT FOODWORKS'
        file.loc[file['Brand'].str.contains("Domino's Pizza",na = False), 'Brand'] = 'Dominos Pizza'

        file.loc[file['Brand'].str.contains('Central',na = False), 'Company'] = 'City Centre Mall Management Limited'
        file.loc[file['Brand'].str.contains('Central',na = False), 'Brand'] = 'Central Mall'

        file.loc[file['Brand'].str.contains('Brand Factory',na = False), 'Company'] = 'Future Group'
        file.loc[file['Brand'].str.contains('Brand Factory',na = False), 'Brand'] = 'Brand Factory'

        file.loc[file['Brand'].str.contains('FBB',na = False), 'Company'] = 'Future Group'
        file.loc[file['Brand'].str.contains('FBB',na = False), 'Brand'] = 'Food Big Bazaar'

        file.loc[file['Brand'].str.contains('Big Bazaar',na = False), 'Company'] = 'Future Group'
        file.loc[file['Brand'].str.contains('Big Bazaar',na = False), 'Brand'] = 'Big Bazaar'

        file.loc[file['Brand'].str.contains('Hypercity',na = False), 'Company'] = 'Future Group'
        file.loc[file['Brand'].str.contains('Hypercity',na = False), 'Brand'] = 'Hypercity'

        file.loc[file['Brand'].str.contains('Tanishq',na = False), 'Company'] = 'TITAN COMPANY'
        file.loc[file['Brand'].str.contains('Tanishq',na = False), 'Brand'] = 'Tanishq'

        file.loc[file['Brand'].str.contains('Fastrack',na = False), 'Company'] = 'TITAN COMPANY'
        file.loc[file['Brand'].str.contains('Fastrack',na = False), 'Brand'] = 'Fastrack'

        file.loc[file['Brand'].str.contains('Titan',na = False), 'Company'] = 'TITAN COMPANY'
        file.loc[file['Brand'].str.contains('Titan',na = False), 'Brand'] = 'Titan'

        file.loc[file['Brand'].str.contains('Ethos',na = False), 'Company'] = 'KDDL'
        file.loc[file['Brand'].str.contains('Ethos',na = False), 'Brand'] = 'Ethos Watches'

        file.loc[file['Brand'].str.contains('Forever 21',na = False), 'Company'] = 'Authentic Brands Group & Simon Property Group & Brookfield Properties'
        file.loc[file['Brand'].str.contains('Forever 21',na = False), 'Brand'] = 'Forever 21'

        file.loc[file['Brand'].str.contains('Audi',na = False), 'Company'] = 'Volkswagen Group'
        file.loc[file['Brand'].str.contains('Audi',na = False), 'Brand'] = 'Audi'

        file.loc[file['Brand'].str.contains('Kawasaki',na = False), 'Company'] = 'Kawasaki Heavy Industries'
        file.loc[file['Brand'].str.contains('Kawasaki',na = False), 'Brand'] = 'Kawasaki'

        file.loc[file['Brand'].str.contains('Benelli',na = False), 'Company'] = 'Qianjiang Motorcycle'
        file.loc[file['Brand'].str.contains('Benelli',na = False), 'Brand'] = 'Benelli'

        file.loc[file['Brand'].str.contains('Barbecue Nation',na = False), 'Company'] = 'SAYAJI HOTELS'
        file.loc[file['Brand'].str.contains('Barbecue Nation',na = False), 'Brand'] = 'Barbecue Nation'

        file.loc[file['Brand'].str.contains('Style Play',na = False), 'Company'] = 'RAYMOND GROUP'
        file.loc[file['Brand'].str.contains('Style Play',na = False), 'Brand'] = 'Style Play'

        file.loc[file['Brand'].str.contains('The Raymond Shop',na = False), 'Company'] = 'RAYMOND GROUP'
        file.loc[file['Brand'].str.contains('The Raymond Shop',na = False), 'Brand'] = 'The Raymond Shop'

        file.loc[file['Brand'].str.contains('Park Avenue',na = False), 'Company'] = 'RAYMOND GROUP'
        file.loc[file['Brand'].str.contains('Park Avenue',na = False), 'Brand'] = 'Park Avenue'

        file.loc[file['Brand'].str.contains('Parx',na = False), 'Company'] = 'RAYMOND GROUP'
        file.loc[file['Brand'].str.contains('Parx',na = False), 'Brand'] = 'Parx'

        file.loc[file['Brand'].str.contains('Croma',na = False), 'Company'] = 'Infiniti Retail'
        file.loc[file['Brand'].str.contains('Croma',na = False), 'Brand'] = 'Croma'

        file.loc[file['Brand'].str.contains('METRO SHOES',na = False), 'Company'] = 'Metro Brands Limited'
        file.loc[file['Brand'].str.contains('METRO SHOES',na = False), 'Brand'] = 'Metro Shoes'

        file.loc[file['Brand'].str.contains('Reliance Digital',na = False), 'Company'] = 'Reliance Retail'
        file.loc[file['Brand'].str.contains('Reliance Digital',na = False), 'Brand'] = 'Reliance Digital'


        file['Act_Runtime'] = datetime.datetime.now(india_time)
        file.to_sql(name = "STORE_LOCATOR_WEEKLY_CLEAN_DATA_Temp2_Nidhi",con = engine,if_exists = 'append',index = False)

        print("DONE CLEANED AND PUSHED INTO SQL")

        #####################
        # click_max_date = client.execute("select max(Relevant_Date) from AdqvestDB.STORE_LOCATOR_WEEKLY_CLEAN_DATA_Temp2_Nidhi")
        # click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])

        # query = "select * from AdqvestDB.STORE_LOCATOR_WEEKLY_CLEAN_DATA_Temp2_Nidhi WHERE Relevant_Date > '" +click_max_date+ "'"
        # df = pd.read_sql(query, engine)

        # client.execute("INSERT INTO STORE_LOCATOR_WEEKLY_CLEAN_DATA_Temp2_Nidhi values",df.values.tolist())

        print("DATA PUSHED INTO CLICKHOUSE")
        log.job_end_log(table_name, job_start_time, no_of_ping)


        print("CODE RAN SUCESSFULLY")

    except:
        error_type = str(re.search("'(.+?)'", str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name, job_start_time, error_type, error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
