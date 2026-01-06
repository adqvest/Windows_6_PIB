from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import datetime as datetime
import re
import os
import requests
import warnings
warnings.filterwarnings('ignore')
import sqlalchemy
from selenium import webdriver
from pytz import timezone
import time
import pytesseract
from PIL import Image
import sys
from selenium.common.exceptions import ElementNotInteractableException,NoSuchElementException
from selenium.webdriver.common.proxy import Proxy, ProxyType
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.by import By

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')

import adqvest_db
import ClickHouse_db
import JobLogNew as log
from adqvest_robotstxt import Robots
robot = Robots(__file__)


def run_program(run_by='Adqvest_Bot', py_file_name=None):

    engine = adqvest_db.db_conn()
    connection = engine.connect()
    client = ClickHouse_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'MCA_INDEX_OF_CHARGES'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        def clean_txt(text):
            #text = text.title()
            text = text.upper()
            text = text.replace('  ',' ').replace(',','').replace('-','').replace('(','').replace(')','').replace('\n','').replace('.','')
            text = text.replace('LTD','').replace('LIMITED','')
            text = text.replace('THE','').replace('&','AND')
            text = re.sub(r'  +',' ', text).strip()
            return text

        def get_captcha(driver, element, path):
            location = element.location
            size = element.size
            driver.get_screenshot_as_file(path)


            image = Image.open(path)

            left = location['x']
            top = location['y']
            right = location['x'] + size['width']
            bottom = location['y'] + size['height']

            image = image.crop((left, top, right, bottom))
            image.save('capt1.png')

        def captcha(cin):
            try:
                # img = driver.find_element_by_xpath("//*[@id='captcha']")
                img =  driver.find_element(By.XPATH,"//*[@id='captcha']")
                # except NoSuchElementException:
                #     raise Exception ('Web Page Error')
                #get_captcha(driver, img, "captcha.png")
                img.screenshot("capt1.png")
                pytesseract.pytesseract.tesseract_cmd=r'C:\Program Files\Tesseract-OCR\tesseract.exe'
                text = str(((pytesseract.image_to_string(Image.open('capt1.png')))))
                print(text)
                text1=text.replace('\n','').strip()
                print(text1)
                # driver.find_element_by_xpath("//*[@id='companyID']").send_keys(cin)
                # driver.find_element_by_xpath("//*[@id='userEnteredCaptcha']").send_keys(text1)
                # driver.find_element_by_xpath("//*[@id='chargeForm_0']").click()

                driver.find_element(By.XPATH,"//*[@id='companyID']").send_keys(cin)
                driver.find_element(By.XPATH,"//*[@id='userEnteredCaptcha']").send_keys(text1)
                driver.find_element(By.XPATH,"//*[@id='companyLLPMasterData_0']").click()
            except:
                print('error 1')
        chrome_driver_path=r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
        download_file_path = r"C:\Users\Administrator\AdQvestDir"

        options=webdriver.ChromeOptions()
        prefs = {
                    "download.default_directory": download_file_path,
                    "download.prompt_for_download": False,
                    "download.directory_upgrade": True
                    }
        options.add_experimental_option('prefs', prefs)
        options.add_argument('--ignore-certificate-errors')
        # options.add_experimental_option('useAutomationExtension', False)
        # options.headless = True
        # options.add_argument('--disable-gpu')

        prox = Proxy()
        prox.proxy_type = ProxyType.MANUAL
        prox.http_proxy = "http://5c2fcd5a03ad47a8b87f3cc83450c4a7:@proxy.crawlera.com:3218"
        # prox.socks_proxy = "http://5c2fcd5a03ad47a8b87f3cc83450c4a7:@proxy.crawlera.com:8011"
        prox.ssl_proxy = "http://5c2fcd5a03ad47a8b87f3cc83450c4a7:@proxy.crawlera.com:3218"

        # headless_proxy = "http://5c2fcd5a03ad47a8b87f3cc83450c4a7:@proxy.crawlera.com:8011"
        # proxy = {
        #     "proxyType": "manual",
        #     "httpProxy": headless_proxy,
        #     "ftpProxy": headless_proxy,
        #     "sslProxy": headless_proxy,
        #     "noProxy": "",
        # }
        #
        capabilities = dict(DesiredCapabilities.CHROME)
        # options.set_capability("proxy", prox)
        prox.add_to_capabilities(capabilities)
        driver = webdriver.Chrome(executable_path=chrome_driver_path,options = options,desired_capabilities=capabilities)
        # driver.get("http://www.mca.gov.in/mcafoportal/showIndexOfCharges.do#")
        # options.add_argument('--load-extension={}'.format(unpacked_extension_path))

        # driver = webdriver.Chrome(executable_path=chrome_driver_path,options = options)
        count=0
        loop=0
        status_df=pd.read_sql("select Cin,Company_Name,Status from AdqvestDB.MCA_COMPANY_MASTER_DATA_WEEKLY_STATUS ",engine)
        print("1")
        if status_df['Status'].isnull().sum()==0:
            status_df['Status']=None
            connection = engine.connect()
        #     connection.execute("UPDATE AdqvestDB.MCA_COMPANY_MASTER_DATA_WEEKLY_STATUS set Status=NULL")
        while (loop<=5):
            try:
                cin_list=pd.read_sql("select Cin,Company_Name,Status from AdqvestDB.MCA_COMPANY_MASTER_DATA_WEEKLY_STATUS where Status is null",engine)

                cin_list1=cin_list['Cin'][:500]
                cin_company=cin_list['Company_Name'][:500]
                mca_df1=pd.DataFrame()
                for cin,company in zip(cin_list1,cin_company):
                    print(cin)
                    limit=0
                    while(limit<=10):
                        driver.get("https://www.mca.gov.in/mcafoportal/viewCompanyMasterData.do")
                        # driver.maximize_window()
                #         no_of_ping+=1
                        captcha(cin)
                        time.sleep(3)
                        try:
                            err_msg=''
                            # err_msg=driver.find_element_by_class_name("errorMessage").text
                            err_msg=driver.find_element(By.CLASS_NAME, "errorMessage").text
                            # driver.find_element_by_class_name('boxclose').click()
                            driver.find_element(By.CLASS_NAME,'boxclose').click()
                            print(err_msg)
                            if 'No Charges' in err_msg   :
                                print(err_msg)
                                connection.execute("Update AdqvestDB.MCA_COMPANY_MASTER_DATA_WEEKLY_STATUS set Status='No Charges' where Cin='"+cin+"'")
                                raise ElementNotInteractableException
                            elif 'CIN/LLPIN/FLLPIN/FCRN' in err_msg:
                                print(err_msg)
                                connection.execute("Update AdqvestDB.MCA_COMPANY_MASTER_DATA_WEEKLY_STATUS set Status='No CIN' where Cin='"+cin+"'")
                                print('STOPPED')
                                raise ElementNotInteractableException
                        except (ElementNotInteractableException, NoSuchElementException) as e:
                            print(e)
                            error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
                            print(error_msg)
                            break
                        limit+=1
                    # robot.add_link("https://www.mca.gov.in//content/mca/global/en/mca/master-data/view-index-of-charges.html")    
                    data=[]
                    heading=[]
                    err_list=['No Charges','CIN/LLPIN/FLLPIN/FCRN','No Matches']
                    ismatch=[True for x in err_list if x in err_msg]
                    print(ismatch)
                    if True not in ismatch:
                            # company_name=driver.find_element_by_xpath('//*[@id="companyInfo"]/tbody/tr[2]/td[2]').text //*[@id="resultTab1"]/tbody/tr[2]/td[2]
                        company_name=driver.find_element(By.XPATH,'//*[@id="resultTab1"]/tbody/tr[2]/td[2]').text
                        soup=BeautifulSoup(driver.page_source)
                        # cin_new=driver.find_element_by_xpath('//*[@id="companyInfo"]/tbody/tr[1]/td[2]').text
                        cin_new=driver.find_element(By.XPATH,'//*[@id="resultTab1"]/tbody/tr[1]/td[2]').text
                        table_data = pd.read_html(driver.page_source)
                        mca_df=table_data[len(table_data)-1]
                        mca_df.insert(0,'Cin',cin_new)
                        mca_df.insert(2,'Company_Name',company_name)
                        mca_df.columns=['Cin','DIN/PAN','Company_Name','Name','Begin_Date','End_Date','Surrendered_DIN']
                        mca_df['Relevant_Date']=pd.to_datetime('now').date()
                        mca_df['Runtime']=pd.to_datetime('now')
                        # connection.execute("Delete from AdqvestDB.MCA_COMPANY_MASTER_DATA  where Cin='"+cin+"'")
                        mca_df.to_sql('MCA_COMPANY_MASTER_DATA',index=False, if_exists='append', con=engine)
                        connection.execute("Update AdqvestDB.MCA_COMPANY_MASTER_DATA_WEEKLY_STATUS set Status='Yes',Runtime = '"+datetime.datetime.now(india_time).strftime("%Y-%m-%d %H:%M:%S")+"',Relevant_Date = '"+datetime.datetime.now(india_time).strftime("%Y-%m-%d")+"' where Cin='"+cin+"'")

                    else:
                        continue
            except:
                if loop>5:
                    #raise Exception("Captcha Limit Reached")
                    error_type = str(re.search("'(.+?)'", str(sys.exc_info()[0])).group(1))
                    error_msg = str(sys.exc_info()[1])
                    error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
                    print(error_msg)
                    log.job_error_log(table_name, job_start_time, error_type, error_msg, no_of_ping)
                error_type = str(re.search("'(.+?)'", str(sys.exc_info()[0])).group(1))
                error_msg = str(sys.exc_info()[1])
                error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
                print(error_msg)
            if loop == 5:
                connection.execute("Update AdqvestDB.MCA_COMPANY_MASTER_DATA_WEEKLY_STATUS set Status='No CIN' where Cin='"+cin+"'")
            print(loop)
            loop+=1
        print(loop, limit )
        log.job_end_log(table_name, job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'", str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name, job_start_time, error_type, error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
