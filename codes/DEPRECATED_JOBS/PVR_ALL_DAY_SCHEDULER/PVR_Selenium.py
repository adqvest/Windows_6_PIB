# -*- coding: utf-8 -*-
"""
Created on Wed May  3 13:36:52 2023

@author: Abdulmuizz
"""

from selenium import webdriver
from selenium.webdriver.support.ui import Select
# from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import time
import datetime
from bs4 import BeautifulSoup
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from pytz import timezone
import pandas as pd
import sys
import re
import sys
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
engine = adqvest_db.db_conn()

#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days




def getSeatData(soup):
    
    not_occupied = len(soup.findAll('span', class_ = 'seat current'))
    occupied = len(soup.findAll('span', class_ = 'seat disable current'))
    
    return  {
        'Not_Occupied' : not_occupied,
        'Occupied' : occupied,
        'Total_Seats' : occupied + not_occupied
              }

class infinite_scroll(object):
   
    def __init__(self, last):
      
      self.last = last
    
    def __call__(self, driver):
      new = driver.execute_script('return document.body.scrollHeight')  
      if new > self.last:
          return new
      else:
          return False
    
def scroll(scroll_limit = None):    
    last_height = driver.execute_script('return document.body.scrollHeight')
    
    limit = 0
    
    flag=1
    while flag==1:
    
        driver.execute_script('window.scrollTo(0,document.body.scrollHeight)')
        
        try:
            wait = WebDriverWait(driver, 10)
               
            new_height = wait.until(infinite_scroll(last_height))
            last_height = new_height
            
            limit += 1
            
            if scroll_limit != None:
                if limit >= scroll_limit:
                    flag = 0
               
        except:
            print("End of page reached")
            flag = 0
            
def movetoelement(element):
        
    desired_y = (element.size['height'] / 2) + element.location['y']
    current_y = (driver.execute_script('return window.innerHeight') / 2) + driver.execute_script('return window.pageYOffset')
    scroll_y_by = desired_y - current_y
    driver.execute_script("window.scrollBy(0, arguments[0]);", scroll_y_by)
    time.sleep(2)
    element.click()
    
def clickOK(wait_time = 5):
    
    time.sleep(wait_time)
    
    try:
        driver.find_element(By.XPATH, '//button[contains(text(),"OK")]').click()
    except:
        try:
            driver.find_element(By.XPATH, '//button[contains(text(),"Ok")]').click()
        except:
            pass
        
url = 'https://www.pvrcinemas.com'

options = webdriver.ChromeOptions()
# options.add_argument("--headless")
# options.add_argument('--window-size=1920,1080')
options.add_argument("--disable-infobars")
options.add_argument("start-maximized")
options.add_argument("--disable-extensions")
options.add_argument("--disable-notifications")
options.add_argument('--ignore-certificate-errors')
options.add_argument('--no-sandbox')
chrome_driver =r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
## 'Mumbai', 'Pune','Chennai','Kolkata','Hyderabad',
cities = ['Lucknow','Ahmedabad','Bengaluru','Delhi-NCR', 'Coimbatore']

for city in cities:
    
    driver = webdriver.Chrome(executable_path=chrome_driver, chrome_options = options)
    
    driver.get(url)
    
    time.sleep(10)
    
    driver.find_element(By.XPATH, '//input[@role="searchbox" and @placeholder="Search your city"]').click()
    
    time.sleep(10)
    
    driver.find_element(By.XPATH, f'//a[contains(text(),"{city}")]').click()
    
    time.sleep(5)
    driver.refresh()
    
    driver.get(url + "/nowshowing")
    time.sleep(5)
    
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(2)
    
    movies = []
    for link,name,box in zip(driver.find_elements(By.XPATH, '//a[@class="btn btn-primary-white text-uppercase ng-star-inserted" and contains(text(),"Book Tickets")]'),driver.find_elements(By.XPATH, '//h4[@class="m-title"]'),driver.find_elements(By.XPATH, '//movie-box')):
        if 'Releasing on' not in box.text:
            movies.append(
                {'Link' : link.get_attribute('href'),
                 'Name' : name.text}
                )
         
    driver.quit()
    
    
    class CustomClickError(Exception):
        
        pass
    
    for movie_data in movies:
        
        driver = webdriver.Chrome(executable_path=chrome_driver, chrome_options = options)
         
        driver.get(movie_data['Link'])
        
        movie = movie_data['Name']
        
        clickOK()
                    
        time.sleep(3)
        
        # scroll()
        
        cinemas = driver.find_elements(By.XPATH,'//div[@class="cinema-holder ng-star-inserted"]')[:5]
            
        total_cinemas = len(cinemas)
        
        
        for i in range(total_cinemas):
            
            movetoelement(cinemas[i])
            
            theater = cinemas[i].text.split('\n')[0]
            
            shows = cinemas[i].find_elements(By.XPATH, './/span[@class="slot text-success"]')
            
            total_shows = len(shows)
            
            if total_shows == 0:
                continue
            
            for j in range(total_shows):
                
                click_limit = 0
                error_limit = 0
                invalid_show = False
                while True:
                    try:
                        today = datetime.datetime.now(india_time).replace(tzinfo = None)
                        
                        show_time = datetime.datetime.strptime(shows[j].text,'%I:%M %p')
                        
                        show_time = show_time.replace(year = today.year, month = today.month, day = today.day, tzinfo= None)
                        
                        if ((show_time - today).total_seconds()/60 <= 30) and (show_time > today):
                            pass
                        else:
                            invalid_show = True
                            break
                        
                        
                        try:
                            shows[j].click()
                        except:
                            try:
                                time.sleep(2)
                                movetoelement(shows[j])
                            except:
                                raise CustomClickError()
                        
                        time.sleep(1)
                
                        clickOK(wait_time = 2)
                        
                        time.sleep(2)
                        
                        try:
                            driver.find_element(By.XPATH, '//span[contains(text(),"SKIP")]').click()
                        except:
                            pass
                            
                        WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.XPATH, '//span[@class="seat current"]')))
                        
                        break
                    except:
                        
                        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
                        print(error_type)
                        
                        
                        driver.quit()
                        
                        if 'CustomClickError' in error_type:
        
                            time.sleep(5) 
                            
                            click_limit += 1
                            
                            if click_limit > 3:
                                break
                        else:
                            error_limit += 1
                            if error_limit > 3:
                                break
                            time.sleep(120)
                            
                        
                        driver = webdriver.Chrome(executable_path=chrome_driver, chrome_options = options)
                        
                        driver.get(movie_data['Link'])
                        
                        time.sleep(2)
                        
                        clickOK()
                            
                        time.sleep(3)
                        
                        # scroll()
                                
                        cinemas = driver.find_elements(By.XPATH,'//div[@class="cinema-holder ng-star-inserted"]')
                        
                        theater = cinemas[i].text.split('\n')[0]
                        
                        movetoelement(cinemas[i])
                        
                        shows = cinemas[i].find_elements(By.XPATH, './/span[@class="slot text-success"]')
                        
                        time.sleep(2)
                        continue
                        
                
                if invalid_show == True:
                    continue
                
                if click_limit > 3 or error_limit > 3:
                    break
                        
                show_soup = BeautifulSoup(driver.page_source, 'lxml')
                
                data = getSeatData(show_soup)
                
                data.update({
                    'City' : city,
                    'Theater' : theater,
                    'Movie' : movie,
                    'Show_Date' : datetime.datetime(today.year,today.month,today.day,show_time.hour,show_time.minute,show_time.second),
                    'Show_Time' : datetime.datetime.strftime(show_time,'%H:%M:%S'),
                    "Relevant_Date":today.date(),
                    "Runtime":pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
                    })
                
                print(data)
                
                final_df = pd.DataFrame(data, index = [0])
                
                final_df.to_sql(name='PVR_INDIA_DAILY_SEATING_Temp_Abdul',con=engine,if_exists='append',index=False)
                
                del data
                del final_df
                del show_time
                
                driver.back()
                
                clickOK()
                    
                time.sleep(3)
                
                # scroll()
                        
                cinemas = driver.find_elements(By.XPATH,'//div[@class="cinema-holder ng-star-inserted"]')
        
                if j != range(total_shows)[-1]:
                    movetoelement(cinemas[i])
                    
                    shows = cinemas[i].find_elements(By.XPATH, './/span[@class="slot text-success"]')
                    
                    time.sleep(2)
                    
            if click_limit > 3 or error_limit > 3:
                break
            
        driver.quit()






        
        
        




