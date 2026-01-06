# -*- coding: utf-8 -*-
"""
Created on Wed May 24 10:08:47 2023

@author: Abdulmuizz
"""

from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import time
from bs4 import BeautifulSoup
import pandas as pd
import re
import sys
import datetime
from pytz import timezone
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
import ClickHouse_db
from adqvest_robotstxt import Robots
robot = Robots(__file__)


def run_program(run_by='Adqvest_Bot', py_file_name=None):
    # os.chdir('/home/ubuntu/AdQvestDir')
    engine = adqvest_db.db_conn()
    client = ClickHouse_db.db_conn()
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
        
        last_rel_date = pd.read_sql("select max(Relevant_Date) as Max from STORE_LOCATOR_WEEKLY_DATA where Brand = 'Maruti Suzuki'",engine)
        last_rel_date = last_rel_date["Max"][0]
        print(last_rel_date)
        
        if(today.date()-last_rel_date >= datetime.timedelta(7)):

            options = webdriver.ChromeOptions()
            options.add_argument("--disable-infobars")
            options.add_argument("start-maximized")
            options.add_argument("--disable-extensions")
            options.add_argument("--disable-notifications")
            options.add_argument('--ignore-certificate-errors')
            options.add_argument('--no-sandbox')


            driver_path=r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"

            driver = webdriver.Chrome(executable_path=driver_path,chrome_options=options)

            driver.get("https://www.marutisuzuki.com/dealer-showrooms")
            robot.add_link("https://www.marutisuzuki.com/dealer-showrooms")
            
            time.sleep(5)
            
            
            states = [{'Text': j.text.strip(), 'Value' : j.get_attribute('value')} for j in driver.find_element(By.XPATH, '//select[@id="dealer-state"]').find_elements(By.XPATH, './/option')]
            
            states = pd.DataFrame(states).drop_duplicates(subset = ['Value'],  keep = 'last').to_dict('records')
            
            data = []
            for state in states[1:]:
                
                Select(driver.find_element(By.XPATH, '//select[@id="dealer-state"]')).select_by_value(state['Value'])
                
                time.sleep(2)
                
                cities = [{'Text': j.text.strip(), 'Value' : j.get_attribute('value')} for j in driver.find_element(By.XPATH, '//select[@id="dealercity-dealer"]').find_elements(By.XPATH, './/option')]
                
                cities = pd.DataFrame(cities).drop_duplicates(subset = ['Value'], keep = 'last').to_dict('records')
                
                for city in cities[1:]:
                    # driver.refresh()
                    # time.sleep(2)
                    # Select(driver.find_element(By.XPATH, '//select[@id="dealer-state"]')).select_by_value(state['Value'])

                    # time.sleep(2)

                    try:
                        Select(driver.find_element(By.XPATH, '//select[@id="dealercity-dealer"]')).select_by_value(city['Value'])
                    except:
                        try:
                            # driver.refresh()
                            time.sleep(5)
                            Select(driver.find_element(By.XPATH, '//select[@id="dealercity-dealer"]')).select_by_value(city['Value'])
                        except:
                            continue
                    time.sleep(2)        
                    
                    driver.find_element(By.XPATH, '//a[contains(text(),"Search")]').click()
                                        
                    try:
                        driver.switch_to.alert.accept()
                    except:
                        pass

                    try:
                        driver.refresh() 
                    except:
                        time.sleep(5)
                        driver.refresh()    

                    time.sleep(1)

                    try:
                        driver.switch_to.alert.accept()
                    except:
                        pass
                    
                    soup = BeautifulSoup(driver.page_source, 'lxml')


                    try:
                        showrooms = soup.find('ul', class_ = 'delaer-locator-list').findAll('li')

                        print(state['Text'], '--->' , city['Text'].lower())
                        
                        for showroom in showrooms:
                            
                            temp = {
                                'State' : state['Text'],
                                'City' : city['Text'].lower(),
                                'Address' : showroom.find('div', class_ = 'address').text.strip(),
                                }
                            
                            data.append(temp)
                    except:
                        pass
                        
            
            driver.quit()            
            
            final_df = pd.DataFrame(data)
            final_df['Brand'] = 'Maruti Suzuki'
            final_df['Comments'] = 'Crawler Data'
            
            final_df.to_sql(name = "STORE_LOCATOR_WEEKLY_DATA",index = False,con = engine,if_exists = "append")

            client1 = ClickHouse_db.db_conn()
            click_max_date = client1.execute("select max(Relevant_Date) from AdqvestDB.STORE_LOCATOR_WEEKLY_DATA where Brand = 'Maruti Suzuki'")
            click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])
            query = 'select * from AdqvestDB.STORE_LOCATOR_WEEKLY_DATA where Brand = "Maruti Suzuki" and Relevant_Date > "' + click_max_date + '"'
            df = pd.read_sql(query,engine)
            client1.execute("INSERT INTO AdqvestDB.STORE_LOCATOR_WEEKLY_DATA VALUES",df.values.tolist())

        else:
            print("Data already present")
            
        log.job_end_log(table_name,job_start_time,no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')







