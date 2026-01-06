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
import json
import datetime as datetime
from pytz import timezone
import requests
import sys
import time
import os
import calendar
import re
import glob
import sqlalchemy
from pandas.tseries.offsets import MonthEnd
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
import numpy as np
import PyPDF2
from pandas.core.common import flatten
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
#sys.path.insert(0, 'C:/Adqvest')
import adqvest_db
import ClickHouse_db
import JobLogNew as log
from adqvest_robotstxt import Robots
robot = Robots(__file__)
import calendar


def pdf_to_excel(file_path,download_path,OCR_doc=False):
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=10000,
                                    chromium_sandbox=True,
                                    downloads_path=download_path)
        page = browser.new_page()
        page.goto("https://www.ilovepdf.com/",timeout=30000*5)

        page.locator("//*[contains(text(),'Log in')]").click()
        email = page.locator("//*[@id='loginEmail']")
        email.fill("kartmrinal101@outlook.com")
        password = page.locator("//*[@id='inputPasswordAuth']")
        password.fill("zugsik-zuqzuH-jyvno4")
        page.locator("//*[@id='loginBtn']").click()
        page.get_by_title("PDF to Excel").click()

        for i in file_path:
            with page.expect_file_chooser() as fc_info:
                
                page.get_by_text("Select PDF file").click()
                file_chooser = fc_info.value
                file_chooser.set_files(i)
                if OCR_doc==True:
                    page.locator("//*[@id='processTask']").click()
                    time.sleep(7)
                    # page.get_by_text("Continue without OCR").click()
                    # page.locator("//*[@id='confirmOcr']/main/div/div[1]/div/div[2]/div/button[1]").click()
                    page.locator(":nth-match(:text('Continue without OCR'), 1)").click()


                    
                else:
                    page.locator("//*[@id='processTask']").click()


               
                with page.expect_download() as download_info:
                    page.get_by_text("Download EXCEL").click()
                # Wait for the download to start
                download = download_info.value
                # Wait for the download process to complete
                print(download.path())
                file_name = download.suggested_filename
                # wait for download to complete
                download.save_as(os.path.join(download_path, file_name))
                page.go_back()

def get_date(x):
    try:
        try:
            x=datetime.datetime.strptime(x,"%d-%m-%Y").date()
        except:
            x=datetime.datetime.strptime(x,"%d/%m/%Y").date()
    except:
        x=datetime.datetime.strptime(x,"%d.%m.%Y").date()

    return x

def parse_date(x, fmts=("%b %Y", "%B %Y")):
    try:
      for fmt in fmts:
          try:
              return datetime.datetime.strptime(x, fmt)
          except ValueError:
              pass
    except:
      raise Exception("Error While Parsing Dates")
def ldm(any_day):
    # this will never fail
    # get close to the end of the month for any day, and add 4 days 'over'
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
    # subtract the number of remaining 'overage' days to get last day of current month, or said programattically said, the previous day of the first of next month
    return next_month - datetime.timedelta(days=next_month.day)

def get_prod_last_date(title):
    date_pattern = r'\b([A-Za-z]+),*\s*(\d{4})\b'
    match = re.search(date_pattern, title)
    if match:
        month_str, year_str = match.groups()
        year = int(year_str)
        try:
            month = datetime.datetime.strptime(month_str, '%B').month
        except ValueError:
            month = datetime.datetime.strptime(month_str, '%b').month
        last_day = calendar.monthrange(year, month)[1]
        return datetime.datetime(year, month, last_day)
    return None

def getLinks(r):
    
    soup = BeautifulSoup(r,'lxml')
    
    rows = soup.findAll('div', class_ = 'nmdc-press-releases-box')
    
    data = []
    
    for row in rows:
        
        temp = {
            'title' : row.find('a').text.strip(),
            'Links' : row.find('a')['href']
            }
        
        data.append(temp)
        
    return pd.DataFrame(data)

#%%
def run_program(run_by='Adqvest_Bot', py_file_name=None):

    engine = adqvest_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    #add table name
    table_name = 'BSE_PUBLISHED_RANDOM_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = '3_PM_WINDOWS_SERVER_2_SCHEDULER'
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

#        r=requests.get('https://www.nmdc.co.in/financial%20information/default.aspx')
        os.chdir('C:/Users/Administrator/AdQvestDir/BSE_Random')

        client = ClickHouse_db.db_conn()
        '''
        New URL = https://www.nmdc.co.in/investors/financial-details/details-of-production-and-sales-and-prices-of-iron-ore
        Old URL = https://www.nmdc.co.in/financial%20information/default.aspx
        Change API key if error
        '''
        # headers = {"user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36"}

        # # url = 'https://www.nmdc.co.in/cms-admin/api/investornews/productiondetails'
        # url_prod= 'https://www.nmdc.co.in/cms-admin/api/investornews/productiondetails'
        # url_price="https://www.nmdc.co.in/cms-admin/api/investornews/ironoresales"

        # data = {"lang": "En",
        #         "index": "0",
        #         "pagesize": "50"}
        # headers['ApiKey'] = 'weryewrtuewshwfuyrtgergg'
        # # r = requests.post(url,data=data,headers=headers)
        # # print(r)

        # r_price = requests.post(url_price,data=data,headers=headers, timeout = 60)
        # r_prod=requests.post(url_prod,data=data,headers=headers, timeout = 60)
        # robot.add_link(url_price)
        # robot.add_link(url_prod)
        # # print(r_price)
        # # print(r_prod)

        # data_price = pd.DataFrame(json.loads(r_price .text)['data']['list'])
        # data_prod = pd.DataFrame(json.loads(r_prod.text)['data']['list'])

        # price_df=data_price
        # prod_df=data_prod
        # # prod_df = data[data['title'].str.lower().str.contains("production")]

        # price_df['Links'] = "https://www.nmdc.co.in/cms-admin"+price_df['url']
        # prod_df['Links'] = "https://www.nmdc.co.in/cms-admin"+prod_df['url']


        # data = pd.DataFrame(json.loads(r.text)['data']['list'])
        # price_df = data[data['title'].str.lower().str.contains("prices")]
        # prod_df = data[data['title'].str.lower().str.contains("production")]
        # price_df['Links'] = "https://www.nmdc.co.in/cms-admin"+price_df['url']
        # prod_df['Links'] = "https://www.nmdc.co.in/cms-admin"+prod_df['url']


        driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
    

        
        
        options = webdriver.ChromeOptions()
        options.add_argument("--disable-infobars")
        options.add_argument("start-maximized")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-notifications")
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--no-sandbox')

        # driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options = options)
        driver = webdriver.Chrome(executable_path=driver_path,chrome_options=options)


        headers = {"user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36"}

        url_prod= 'https://www.nmdc.co.in/investors/financial-details/details-of-production-and-sales-and-prices-of-iron-ore'
        url_price='https://www.nmdc.co.in/investors/financial-details/prices-of-iron-ore'


        driver.get(url_price)
        time.sleep(10)
        r_price = driver.page_source

        driver.get(url_prod)
        time.sleep(10)
        r_prod = driver.page_source


        price_df=getLinks(r_price)
        prod_df=getLinks(r_prod)
        
        driver.quit()
        

        #######################Changes Done Here##################################
        try:
            price_df['Relevant_Date'] = price_df['title'].str.split("f.").str[-1].str.strip().replace(r'\u200b',"",regex=True).apply(lambda x: get_date(x[:11].strip()))  #Replace x with x[:11].strip() (Added by Nidhi on 15-09-2023)
        except:
            price_df['Relevant_Date'] = price_df['title'].str.split(".").str[-1].str.strip().replace(r'\u200b|f',"",regex=True)
            price_df['Relevant_Date']=np.where(price_df.Relevant_Date.str.contains('/', regex=True),price_df.Relevant_Date.apply(lambda x: x.replace('/','-').strip()),price_df['Relevant_Date'])            
            price_df['Relevant_Date']=price_df['Relevant_Date'].apply(lambda x: datetime.datetime.strptime(x,"%d-%m-%Y").date())
        try:
            prod_df['Relevant_Date'] = [get_prod_last_date(title) for title in prod_df['title']] #(Added by Nidhi on 02-04-2024)
            prod_df['Relevant_Date'] = prod_df['Relevant_Date'].apply(lambda x: x.date() if x else None)
        except:
            prod_df['Relevant_Date'] = prod_df['title'].str.split("of").str[-1].apply(lambda x : re.sub('[^A-Za-z0-9]+', ' ', x)).str.strip().apply(lambda x : ldm(parse_date(x)).date())
        ###########################################################################################################################
        # price_df['Relevant_Date'] = price_df['title'].str.split(".").str[-1].str.strip().replace(r'\u200b',"",regex=True).apply(lambda x: datetime.datetime.strptime(x,"%d-%m-%Y").date())
        

        max_rel_date = pd.read_sql("select max(Relevant_Date) as Max_Date from BSE_PUBLISHED_RANDOM_DATA where Sector is not null and Company='NMDC'",engine)["Max_Date"][0]
        max_price_date=pd.read_sql("select max(Relevant_Date) as Max_Date from BSE_PUBLISHED_RANDOM_DATA where Sector is null and Company='NMDC'",engine)["Max_Date"][0]
        price_df=price_df[price_df['Relevant_Date']>max_price_date]
        prod_df=prod_df[prod_df['Relevant_Date']>max_rel_date]


        if prod_df.empty:
            print('No new data')
        else:
            for i,row in prod_df.iterrows():
                robot.add_link(row['Links'])
                tables=camelot.read_pdf(row['Links'],pages='1-end',line_scale=40,shift_text=[''],strip_text=['\n'])
                for i in range(tables.n):
                    if tables[i].df[0].str.lower().str.contains('sector').any():
                        df=tables[i].df.copy()
                        #print(credit_risk_df)
                        break
                df[0]=df[0].str.replace(r' ','')
                df=df.iloc[1:,[0,1,3]]
                df=df.melt(id_vars=[0],
                        value_name="Value")[[0,'Value']]

                #index1=df[df[0].str.lower().str.contains('chhattisgarh')].index
                index1=df[((df[0].str.lower().str.contains('chhattisgarh')) | (df[0].str.lower().str.contains('chhatisgarh')) | (df[0].str.lower().str.contains('chhaftisgarh')))].index
                print(index1)
                index2=df[((df[0].str.lower().str.contains('karnataka')) | (df[0].str.lower().str.contains('kamataka')))].index
                print(index2)
                date=row['Relevant_Date']

                print(df)
                # print(df.Sector.iloc[0,3])
                df=df.loc[[index1[0],index1[1],index2[0],index2[1]]]
                df['Category']=['Production','Sales','Production','Sales']
                df.columns=['Sector','Value','Category']

                df['Company']='NMDC'
                df['Sector'] = np.where(df.Sector == 'Chhaftisgarh', 'Chhattisgarh', df.Sector)
                df['Sector'] = np.where(df.Sector == 'Kamataka','karnataka', df.Sector)
                df['Commodity']='Iron Ore'
                df['Unit']='MMT'
                df['Relevant_Date']=date
                df['Runtime']=datetime.datetime.now()
                df['Published_Date']=np.nan
                df['Value']=df['Value'].str.replace(r' ','')
                df=df[['Company','Sector','Category','Commodity','Value','Unit','Relevant_Date','Published_Date','Runtime']]
                df.reset_index(inplace=True,drop=True)
                print(df)
                df.to_sql("BSE_PUBLISHED_RANDOM_DATA", index=False, if_exists='append', con=engine)


        if price_df.empty:
            print('No new data')
        else:

            for i,row in price_df.iterrows():
                headers = {"user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36"}

                r =  requests.get(row['Links'],verify = False,headers = headers, timeout = 60)
                robot.add_link(row['Links'])
                os.chdir('C:/Users/Administrator/AdQvestDir/BSE_Random')
                with open("nmdc_price.pdf",'wb') as f:
                    f.write(r.content)
                    f.close()
                
               
                os.chdir('C:/Users/Administrator/AdQvestDir/BSE_Random')
                pdfFileObj = open('nmdc_price.pdf', 'rb')
                pdfReader = PyPDF2.PdfFileReader(pdfFileObj)
                pageObj = pdfReader.getPage(0)
                words=pageObj.extractText()
                pdfFileObj.close()
                print('Bef',len(words.strip()))
                ######################################################################################
                # if words.strip()=='':
                #     os.chdir('C:/Users/Administrator/AdQvestDir/BSE_Random')
                #     path=os.getcwd()
                #     download_path=os.getcwd()
                #     pdf_list = glob.glob(os.path.join(path, "*.pdf"))
                #     print(pdf_list)

                #     matching = [s for s in pdf_list if "nmdc_price" in s]
                #     print('Matching')
                #     print(matching)
                #     pdf_to_excel(matching,download_path,OCR_doc=False)
                
                  
            
                #     files = glob.glob(os.path.join(path, "*.XLSX"))
                
                #     for f in files:
                #             print('this is:',f)
                #             data=pd.read_excel(f,sheet_name='Table 5')
                #             words=data.columns[0]
                #             os.remove(f)
                ################################################

 
                start=words.find('Rs')
                end=words.find('/-')
                cost=words[start:end]
                print(cost.strip())
                print('words',words.strip())
                start = [i for i in range(len(words)) if words.startswith('Rs', i)]
                end = [i for i in range(len(words)) if words.startswith('/-', i)]
                print('start2:',start)
                print('end2:',end)
                prices=[]
                prices.append(words[start[0]:end[0]].strip())
                prices.append(words[start[1]:end[1]].strip())
                req_df=pd.DataFrame(columns=['Company','Sector','Category','Commodity','Value','Unit','Relevant_Date','Published_Date','Runtime'])
                req_df['Commodity']=['Lump Ore','Fines']
                req_df['Value']=prices
                req_df['Unit']='INR per Ton'
                req_df['Company']='NMDC'
                req_df['Sector']=np.nan
                req_df['Category']='Price'
                req_df['Value']=req_df['Value'].str.replace(r' ','').str.replace(r',','')
                req_df['Value']=req_df['Value'].str.extract(r'(\d+)')
                req_df['Relevant_Date']=row['Relevant_Date']
                req_df['Runtime']=datetime.datetime.now()
                print(req_df)
                os.remove('nmdc_price.pdf')
                req_df.to_sql("BSE_PUBLISHED_RANDOM_DATA", index=False, if_exists='append', con=engine)

        ##Prod, Sales data to CH##
        click_max_date = client.execute("select max(Relevant_Date) from BSE_PUBLISHED_RANDOM_DATA where Sector is not null and Company='NMDC'")
        click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])
        query = "select * from AdqvestDB.BSE_PUBLISHED_RANDOM_DATA where Sector is not null and Company='NMDC' AND Relevant_Date > '" + click_max_date + "'"
        prod_df = pd.read_sql(query,engine)
        client.execute("INSERT INTO BSE_PUBLISHED_RANDOM_DATA VALUES", prod_df.values.tolist())

        ##Price data to CH##
        click_max_date = client.execute("select max(Relevant_Date) from BSE_PUBLISHED_RANDOM_DATA where Sector is null and Company='NMDC'")
        click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])
        query = "select * from AdqvestDB.BSE_PUBLISHED_RANDOM_DATA where Sector is null and Company='NMDC' AND Relevant_Date >'" + click_max_date + "'"
        price_df = pd.read_sql(query,engine)
        client.execute("INSERT INTO BSE_PUBLISHED_RANDOM_DATA VALUES", price_df.values.tolist())


        log.job_end_log(table_name,job_start_time, no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)

        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
