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

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log



def run_program(run_by='Adqvest_Bot', py_file_name=None):

    engine = adqvest_db.db_conn()
    connection = engine.connect()
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
            # try:
            img = driver.find_element_by_xpath("//*[@id='captcha']")
            # except NoSuchElementException:
            #     raise Exception ('Web Page Error')
            #get_captcha(driver, img, "captcha.png")
            img.screenshot("capt1.png")
            pytesseract.pytesseract.tesseract_cmd=r'C:\Program Files\Tesseract-OCR\tesseract.exe'
            text = str(((pytesseract.image_to_string(Image.open('capt1.png')))))
            text1=text.replace('\n','').strip()
            driver.find_element_by_xpath("//*[@id='companyID']").send_keys(cin)
            driver.find_element_by_xpath("//*[@id='userEnteredCaptcha']").send_keys(text1)
            driver.find_element_by_xpath("//*[@id='chargeForm_0']").click()
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
        status_df=pd.read_sql("select Cin,Company_Name,Status from AdqvestDB.MCA_INDEX_OF_CHARGES_STATUS ",engine)
        if status_df['Status'].isnull().sum()==0:
            # status_df['Status']=None
            connection = engine.connect()
            #connection.execute("UPDATE AdqvestDB.MCA_INDEX_OF_CHARGES_STATUS set Status=NULL")
        while (loop<=5):
            #try:
                #cin_list=pd.read_sql("select Cin,Company_Name,Status from AdqvestDB.MCA_INDEX_OF_CHARGES_STATUS where Status is null",engine)
                cin_list=pd.read_sql("SELECT * FROM AdqvestDB.MCA_INDEX_OF_CHARGES_STATUS where char_length(Company_Name)<=30 and Status is null;",engine)
                cin_list1=cin_list['Cin'][:100-count]
                cin_company=cin_list['Company_Name'][:100-count]

                #cin_list1=cin_list['Cin']
                #cin_company=cin_list['Company_Name']
                mca_df1=pd.DataFrame()
                for cin,company in zip(cin_list1,cin_company):
                    print(cin)
                    limit=0
                    while(limit<=10):
                        driver.get("https://www.mca.gov.in//content/mca/global/en/mca/master-data/view-index-of-charges.html")
                        no_of_ping+=1
                        captcha(cin)
                        time.sleep(4)
                        try:
                            err_msg=''
                            err_msg=driver.find_element_by_class_name("errorMessage").text
                            driver.find_element_by_class_name('boxclose').click()
                            if 'No Charges' in err_msg   :
                                print(err_msg)
                                connection.execute("Update AdqvestDB.MCA_INDEX_OF_CHARGES_STATUS set Status='No Charges' where Cin='"+cin+"'")
                                raise ElementNotInteractableException
                            elif 'CIN/LLPIN/FLLPIN/FCRN' in err_msg:
                                print(err_msg)
                                connection.execute("Update AdqvestDB.MCA_INDEX_OF_CHARGES_STATUS set Status='No CIN' where Cin='"+cin+"'")
                                raise ElementNotInteractableException
                        except (ElementNotInteractableException, NoSuchElementException) as e:
                            print(e)
                            error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
                            print(error_msg)
                            break
                        limit+=1
                    data=[]
                    heading=[]
                    err_list=['No Charges','CIN/LLPIN/FLLPIN/FCRN','No Matches']
                    ismatch=[True for x in err_list if x in err_msg]
                    if True not in ismatch:
                        company_name=driver.find_element_by_xpath('//*[@id="companyInfo"]/tbody/tr[2]/td[2]').text
                        soup=BeautifulSoup(driver.page_source)
                        cin_new=driver.find_element_by_xpath('//*[@id="companyInfo"]/tbody/tr[1]/td[2]').text
                        table=soup.findAll('table',{'class':'result-forms dataTable'})[0]
                        thead=table.findAll('thead')
                        for row in thead:
                            tr=row.findAll('tr')
                            for i in tr:
                                th=i.findAll('th')
                                for j in th:
                                    if 'Charges' not in j.text:
                                        heading.append(j.text)
                    else:
                        continue
                    while True:
                        if True not in ismatch:
                            soup=BeautifulSoup(driver.page_source)
                            table=soup.findAll('tbody',{'role':'alert'})[0]
                            tr=table.findAll('tr')
                            for i in tr:
                                td=i.findAll('td')
                                th=i.findAll('th')
                                for j in td:
                                    data.append(j.text)
                            try:
                                driver.find_element_by_xpath('//*[@class="next paginate_button"]').click()
                            except (NoSuchElementException,ElementNotInteractableException):
                                break
                        else:
                            break
                        time.sleep(1)
                    mca=[data[x:x+9] for x in range(0,len(data),9)]
                    mca_df=pd.DataFrame(mca)
                    mca_df.columns=['S_No','Srn','Charge_Id','Charge_Holder_Name','Date_Of_Creation','Date_Of_Modification','Date_Of_Satisfaction','Amount','Address']
                    mca_df.drop('S_No',axis=1,inplace=True)
                    for i in range(mca_df.shape[0]):
                        try:
                            mca_df['Date_Of_Creation'].iloc[i]=datetime.datetime.strptime(str(mca_df['Date_Of_Creation'].iloc[i]),'%d/%m/%Y').date()
                        except:
                            pass
                        try:
                            mca_df['Date_Of_Modification'].iloc[i]=datetime.datetime.strptime(str(mca_df['Date_Of_Modification'].iloc[i]),'%d/%m/%Y').date()
                        except:
                            pass
                        try:
                            mca_df['Date_Of_Satisfaction'].iloc[i]=datetime.datetime.strptime(str(mca_df['Date_Of_Satisfaction'].iloc[i]),'%d/%m/%Y').date()
                        except:
                            pass
                    mca_df.insert(0,'Cin',cin)
                    mca_df.insert(2,'Company_Name',company_name)
        #             mca_df.insert(1,'Cin_New',cin_new)
                    mca_df['Runtime']=datetime.datetime.now()
                    print(mca_df.head())
                    mca_df.to_sql('MCA_INDEX_OF_CHARGES_PIT',index=False, if_exists='append', con=engine)
                    connection.execute("Update AdqvestDB.MCA_INDEX_OF_CHARGES_STATUS set Status='Yes',Runtime = '"+datetime.datetime.now(india_time).strftime("%Y-%m-%d %H:%M:%S")+"',Relevant_Date = '"+datetime.datetime.now(india_time).strftime("%Y-%m-%d")+"' where Cin='"+cin+"'")
                    connection.execute("Delete from AdqvestDB.MCA_INDEX_OF_CHARGES_NO_PIT  where Cin='"+cin+"'")
                    mca_df.to_sql('MCA_INDEX_OF_CHARGES_NO_PIT',index=False, if_exists='append', con=engine)
                    count+=1
                    mca_df1=mca_df1.append(mca_df,ignore_index=True)
            # except:
            #     if loop>5:
            #         #raise Exception("Captcha Limit Reached")
            #         error_type = str(re.search("'(.+?)'", str(sys.exc_info()[0])).group(1))
            #         error_msg = str(sys.exc_info()[1])
            #         error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
            #         print(error_msg)
            #         log.job_error_log(table_name, job_start_time, error_type, error_msg, no_of_ping)
                loop+=1
        log.job_end_log(table_name, job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'", str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name, job_start_time, error_type, error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
