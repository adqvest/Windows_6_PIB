#!/usr/bin/env python
# coding: utf-8

# In[21]:


#LINKEDIN_JOBS_NEW
import sqlalchemy
import pandas as pd
import os
import requests
from bs4 import BeautifulSoup
import re
import datetime as datetime
from pytz import timezone
import sys
import warnings
import time
import re
import calendar
warnings.filterwarnings('ignore')
import numpy as np
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
import ClickHouse_db
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
# In[22]:


os.chdir('C:/Users/Administrator/AdQvestDir/')
engine = adqvest_db.db_conn()
client = ClickHouse_db.db_conn()
#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days

## job log details
job_start_time = datetime.datetime.now(india_time)
table_name = 'NAUKRI_TOTAL_JOBS_DAILY_DATA_3_TABLE'
scheduler = ''
no_of_ping = 0

def xpath_soup(element):
    # type: (typing.Union[bs4.element.Tag, bs4.element.NavigableString]) -> str
    """
    Generate xpath from BeautifulSoup4 element.
    :param element: BeautifulSoup4 element.
    :type element: bs4.element.Tag or bs4.element.NavigableString
    :return: xpath as string
    :rtype: str
    Usage
    -----
    >>> import bs4
    >>> html = (
    ...     '<html><head><title>title</title></head>'
    ...     '<body><p>p <i>1</i></p><p>p <i>2</i></p></body></html>'
    ...     )
    >>> soup = bs4.BeautifulSoup(html, 'html.parser')
    >>> xpath_soup(soup.html.body.p.i)
    '/html/body/p[1]/i'
    >>> import bs4
    >>> xml = '<doc><elm/><elm/></doc>'
    >>> soup = bs4.BeautifulSoup(xml, 'lxml-xml')
    >>> xpath_soup(soup.doc.elm.next_sibling)
    '/doc/elm[2]'
    """
    components = []
    child = element if element.name else element.parent
    for parent in child.parents:  # type: bs4.element.Tag
        siblings = parent.find_all(child.name, recursive=False)
        components.append(
            child.name if 1 == len(siblings) else '%s[%d]' % (
                child.name,
                next(i for i, s in enumerate(siblings, 1) if s is child)
                )
            )
        child = parent
    components.reverse()
    return '/%s' % '/'.join(components)

def clean_data(naukri_df,engine):

    dict_df = pd.read_sql("select * from AdqvestDB.GENERIC_DICTIONARY_TABLE where Input_Table = 'NAUKRI_TOTAL_JOBS_COMPANY_DATA' and Output_Table = 'NAUKRI_TOTAL_JOBS_COMPANY_DATA_CLEAN_DATA' and Output_Col = 'Industry' group by Input",engine)

    dict_df = dict_df[["Input","Output"]]
    dict_df.columns = ["Company","Industry"]


    #max_relevant_date = pd.read_sql("select max(Relevant_Date) as Max from AdqvestDB.NAUKRI_TOTAL_JOBS_COMPANY_DATA_CLEAN_DATA",engine)["Max"][0]
    #print(max_relevant_date)
    #if(max_relevant_date == None):
    #    naukri_df = pd.read_sql("select * from AdqvestDB.NAUKRI_TOTAL_JOBS_COMPANY_DATA",engine)
    #else:
    #    naukri_df = pd.read_sql("select * from AdqvestDB.NAUKRI_TOTAL_JOBS_COMPANY_DATA where Relevant_Date >= '"+max_relevant_date.strftime("%Y-%m-%d")+"'",engine)

    naukri_df = naukri_df.merge(dict_df,on = "Company",how = 'left')
    naukri_df["Industry"] = np.where(naukri_df["Industry"].isnull(),"",naukri_df["Industry"])
    naukri_df["Runtime"] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
    naukri_df = naukri_df.drop_duplicates(['State','City','Company','Number','Relevant_Date'])
    print(naukri_df.shape)

    return naukri_df


def clean_industry_names(df,engine):
    names_df = pd.read_sql("select * from GENERIC_DICTIONARY_TABLE where Input_Table = 'NAUKRI_TOTAL_JOBS_INDUSTRY_WISE_DAILY_DATA' and Output is not null", engine)
    df.insert(1,'Industries_Mapped','')
    df['Industries_Mapped'] = df.merge(names_df,how = 'left', left_on = 'Industries', right_on = 'Input')['Output']
    return df

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    global no_of_ping

    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]


    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        max_relevant_date = pd.read_sql("select max(Relevant_Date) as Max from AdqvestDB.NAUKRI_TOTAL_JOBS_INDUSTRY_WISE_DAILY_DATA",engine)["Max"][0]

        print(max_relevant_date)

        if max_relevant_date == today.date():
            print('Data Collected')

        else:

            options = webdriver.ChromeOptions()

            options.add_argument("start-maximized")
            #options.add_argument("--disable-extensions")
            #options.add_argument("--disable-notifications")
            options.add_argument('--ignore-certificate-errors')
            #options.add_argument('--no-sandbox')
            #options.add_argument("--disable-infobars")

            driver = webdriver.Chrome(executable_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe", chrome_options=options)
            url = 'https://www.naukri.com/jobs-by-location'
            no_of_ping += 1
            driver.get(url)
            driver.maximize_window()
            time.sleep(5)
            try:
                driver.find_element("xpath","//button[text()='GOT IT']").click()
            except:
                pass

            soup = BeautifulSoup(driver.page_source, 'lxml')
            all_columns = soup.findAll('div', class_='column')
            zipped = []
            for data in all_columns:
                for state_data in data.findAll('div', class_='section_white_title'):
                    city_data = state_data.findAll('a')
                    for i in range(len(city_data)):
                        if(i==0):
                            state = city_data[i].text.split(' in ')[1].strip()
                        else:
                            city = city_data[i].text.split(' in ')[1].strip()
                            link = city_data[i]['href']
                            zipped.append([state, city, link])
            # zipped = zipped[30:33]
            data = []
            industry_data = []
            company_data = []
            for i in range(len(zipped)):
                state = zipped[i][0]
                city = zipped[i][1]
                url = zipped[i][2]
                print(state,city)
                no_of_ping += 1

                limit = 0
                while True:
                    try:
                        driver.quit()
                        time.sleep(2.5)
                        driver = webdriver.Chrome(executable_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe",chrome_options=options)
                        driver.get(url)
                        break
                    except:
                        limit += 1
                        if(limit < 8):
                            continue
                        else:
                            break



                time.sleep(3)
                try:
                  time.sleep(2)
                  chat_bot = "//div[@class='crossIcon chatBot chatBot-ic-cross']"
                  chat_bot = driver.find_element("xpath",chat_bot)
                  driver.execute_script("arguments[0].click();", chat_bot)
                except:
                  pass
                soup = BeautifulSoup(driver.page_source, 'lxml')
                try:
                    text = soup.findAll('span', title=re.compile(".*of.*\d+", re.IGNORECASE))[0].text
                except:
                    continue
                text = text.lower().split(' of ')[1]
                num = float(re.findall(r'-?\d+\.?\d*',text.replace(',',''))[0])
                data.append([state, city, num, today.date(), pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))])
                print(data)

                # Get Top Companies data
                limit = 0
                while True:
                    try:
                        soup = BeautifulSoup(driver.page_source, 'lxml')
                        elem = soup.findAll('span', class_='fw500', text='Top Companies')
                        # print(elem)
                        if(elem==[]):
                            break
                        elem = elem[0].parent.parent
                        # print(elem)
                        if(elem.findAll('a', class_='blue-text filter-more-link')==[]):
                            ls = elem.get_text('$$')
                            # print(ls)
                            if(ls.split('$$')[0] == "Top Companies"):
                                ls = ls.split('$$')[1:]
                            else:
                                # print('no elem')
                                raise Exception("error in Companies"+''+state+''+city)
                            if((len(ls)%2) != 0):
                                raise Exception("error in Companies"+''+state+''+city)
                            for i in range(int(len(ls)/2)):
                                st = ls[2*i].strip()
                                num = int(re.findall("\d+", ls[2*i + 1].replace(",",""))[0])
                                company_data.append([state, city, st, num, today.date(), pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))])
                        else:
                            xpath = xpath_soup(elem) + '//a[@class="blue-text filter-more-link"]'
                            no_of_ping += 1
                            #driver.find_element("xpath",xpath)[0].click()

                            time.sleep(3)
                            driver.execute_script("arguments[0].click();", driver.find_element("xpath",xpath))
                            # driver.execute_script("arguments.click();", driver.find_element("xpath",xpath))
                            time.sleep(5)
                            soup = BeautifulSoup(driver.page_source, 'lxml')
                            ls = soup.findAll('div', class_='heading')[0].parent.get_text("$$")
                            if(ls.split('$$')[0] == "Top Companies"):
                                ls = ls.split('$$')[1:]
                                del ls[-1]
                            else:
                                raise Exception("error in Companies"+''+state+''+city)
                            if((len(ls)%2) != 0):
                                raise Exception("error in Companies"+''+state+''+city)
                            for i in range(int(len(ls)/2)):
                                st = ls[2*i].strip()
                                num = int(re.findall("\d+", ls[2*i + 1].replace(",",""))[0])
                                company_data.append([state, city, st, num, today.date(), pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))])
                            no_of_ping += 1
                            head = "//div[@class='heading']//i"
                            head = driver.find_element("xpath",head)
                            driver.execute_script("arguments[0].click();", head)
                            # driver.execute_script("arguments.click();", head)    

                            #driver.find_element("xpath","//div[@class='heading']//i")[0].click()
                        break
                    except:
                        try:
                          time.sleep(2)
                          chat_bot = "//div[@class='crossIcon chatBot chatBot-ic-cross']"
                          chat_bot = driver.find_element("xpath",chat_bot)
                          driver.execute_script("arguments[0].click();", chat_bot)
                          # driver.execute_script("arguments.click();", chat_bot)
                        except:
                          pass
                        limit += 1
                        if(limit>3):
                            raise Exception
                        time.sleep(1)

                # Get Industries data
                time.sleep(1)
                limit = 0
                while True:
                    try:
                        soup = BeautifulSoup(driver.page_source, 'lxml')
                        elem = soup.findAll('span', class_='fw500', text='Industries')
                        if(elem==[]):
                            break
                        elem = elem[0].parent.parent
                        if(elem.findAll('a', class_='blue-text filter-more-link')==[]):
                            ls = elem.get_text('$$')
                            if(ls.split('$$')[0] == "Industries"):
                                ls = ls.split('$$')[1:]
                            else:
                                raise Exception("error in Industries"+''+state+''+city)
                            if((len(ls)%2) != 0):
                                raise Exception("error in Industries"+''+state+''+city)
                            for i in range(int(len(ls)/2)):
                                st = ls[2*i].strip()
                                num = int(re.findall("\d+", ls[2*i + 1].replace(",",""))[0])
                                industry_data.append([state, city, st, num, today.date(), pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))])
                        else:

                            xpath = xpath_soup(elem) + '//a[@class="blue-text filter-more-link"]'
                            no_of_ping += 1
                            time.sleep(2)
                            # driver.execute_script("arguments[0].click();", driver.find_element("xpath",xpath)[0])
                            driver.execute_script("arguments.click();", driver.find_element("xpath",xpath))
 
                            # time.sleep(2)
                            #driver.find_element("xpath",xpath)[0].click()


                            soup = BeautifulSoup(driver.page_source, 'lxml')
                            ls = soup.findAll('div', class_='heading')[0].parent.get_text("$$")
                            if(ls.split('$$')[0] == "Industries"):
                                ls = ls.split('$$')[1:]
                                del ls[-1]
                            else:
                                raise Exception("error in Industries"+''+state+''+city)
                            if((len(ls)%2) != 0):
                                raise Exception("error in Industries"+''+state+''+city)
                            for i in range(int(len(ls)/2)):
                                st = ls[2*i].strip()
                                num = int(re.findall("\d+", ls[2*i + 1].replace(",",""))[0])
                                industry_data.append([state, city, st, num, today.date(), pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))])
                            no_of_ping += 1
                            head = "//div[@class='heading']//i"
                            head = driver.find_element("xpath",head)
                            # driver.execute_script("arguments[0].click();", head)
                            driver.execute_script("arguments.click();", head)

                            #driver.find_element("xpath","//div[@class='heading']//i")[0].click()
                        break
                    except:
                        try:
                          time.sleep(2)
                          chat_bot = "//div[@class='crossIcon chatBot chatBot-ic-cross']"
                          chat_bot = driver.find_element("xpath",chat_bot)
                          # driver.execute_script("arguments[0].click();", chat_bot)
                          driver.execute_script("arguments.click();", chat_bot)

                        except:
                          pass
                        limit += 1
                        if(limit>3):
                            raise Exception("error"+''+state+''+city)
                        time.sleep(1)





            NAUKRI_TOTAL_JOBS_DATA = pd.DataFrame(data, columns=['State', 'City', 'Number', 'Relevant_Date', 'Runtime'])
            NAUKRI_TOTAL_JOBS_INDUSTRY_DATA = pd.DataFrame(industry_data, columns=['State', 'City', 'Industries', 'Number', 'Relevant_Date', 'Runtime'])
            NAUKRI_TOTAL_JOBS_COMPANY_DATA = pd.DataFrame(company_data, columns=['State', 'City', 'Company', 'Number', 'Relevant_Date', 'Runtime'])

            # NAUKRI_TOTAL_JOBS_DATA["Relevant_Date"] = yesterday.date()
            # NAUKRI_TOTAL_JOBS_INDUSTRY_DATA["Relevant_Date"] = yesterday.date()
            # NAUKRI_TOTAL_JOBS_COMPANY_DATA["Relevant_Date"] = yesterday.date()
            NAUKRI_TOTAL_JOBS_COMPANY_DATA = NAUKRI_TOTAL_JOBS_COMPANY_DATA.drop_duplicates(['State','City','Company','Number','Relevant_Date'])
            NAUKRI_TOTAL_JOBS_INDUSTRY_DATA = NAUKRI_TOTAL_JOBS_INDUSTRY_DATA.drop_duplicates(['State', 'City', 'Industries', 'Number', 'Relevant_Date'])
            NAUKRI_TOTAL_JOBS_DATA = NAUKRI_TOTAL_JOBS_DATA.drop_duplicates(['State', 'City', 'Number', 'Relevant_Date'])

            NAUKRI_TOTAL_JOBS_COMPANY_DATA = clean_data(NAUKRI_TOTAL_JOBS_COMPANY_DATA,engine)
            NAUKRI_TOTAL_JOBS_INDUSTRY_DATA.reset_index(drop = True, inplace = True)
            NAUKRI_TOTAL_JOBS_INDUSTRY_DATA = clean_industry_names(NAUKRI_TOTAL_JOBS_INDUSTRY_DATA,engine)

            NAUKRI_TOTAL_JOBS_DATA.to_sql(name='NAUKRI_TOTAL_JOBS_STATE_CITY_DAILY_DATA',con=engine,if_exists='append',index=False)



            if max_relevant_date == today.date():
                print('Data Collected')
            else:
                print('worked')
            NAUKRI_TOTAL_JOBS_INDUSTRY_DATA.to_sql("NAUKRI_TOTAL_JOBS_INDUSTRY_WISE_DAILY_DATA", con=engine, if_exists='append', index=False)
            click_max_date = client.execute("select max(Relevant_Date) from AdqvestDB.NAUKRI_TOTAL_JOBS_INDUSTRY_WISE_DAILY_DATA")
            click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])
            query = 'select * from AdqvestDB.NAUKRI_TOTAL_JOBS_INDUSTRY_WISE_DAILY_DATA WHERE Relevant_Date > "' + click_max_date + '"'
            ind_df = pd.read_sql(query,engine)
            client.execute("INSERT INTO NAUKRI_TOTAL_JOBS_INDUSTRY_WISE_DAILY_DATA VALUES", ind_df.values.tolist())
            NAUKRI_TOTAL_JOBS_COMPANY_DATA.to_sql("NAUKRI_TOTAL_JOBS_COMPANY_WISE_DAILY_DATA", con=engine, if_exists='append', index=False)
            click_max_date = client.execute("select max(Relevant_Date) from AdqvestDB.NAUKRI_TOTAL_JOBS_COMPANY_WISE_DAILY_DATA")
            click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])
            query = 'select * from AdqvestDB.NAUKRI_TOTAL_JOBS_COMPANY_WISE_DAILY_DATA WHERE Relevant_Date > "' + click_max_date + '"'
            com_df = pd.read_sql(query,engine)
            client.execute("INSERT INTO NAUKRI_TOTAL_JOBS_COMPANY_WISE_DAILY_DATA VALUES", com_df.values.tolist())
            try:
                driver.quit()
            except:
                pass
        # log.check_data("NAUKRI_TOTAL_JOBS_STATE_CITY_DAILY_DATA", NAUKRI_TOTAL_JOBS_DATA.shape[0],thresh = 0.7)
        log.job_end_log(table_name,job_start_time,no_of_ping)
    except:
        try:
            driver.quit()
        except:
            pass
        # a = input()
        # print(a)
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        error_msg = str(sys.exc_info()[1]) + " line " + str(exc_tb.tb_lineno)
        print(error_type)
        print(error_msg)

        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
