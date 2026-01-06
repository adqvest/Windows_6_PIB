# -*- coding: utf-8 -*-
"""
Created on Tue Jan  5 19:26:59 2021

@author: abhis
"""


import unidecode
import os
import sys
import datetime
import re
from quantities import units
import unidecode
import re
from quantulum3 import parser
from pytz import timezone
from bs4 import BeautifulSoup
from scrapy import Selector
import json
import requests
import os
import sqlalchemy
import pandas as pd
from selenium.common.exceptions import NoSuchElementException
import re
import os
import sqlalchemy
import requests
import pandas as pd
import numpy as np
import datetime
from pytz import timezone
from selenium import webdriver
import random
import time
import numpy as np
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import math
import requests, json
from bs4 import BeautifulSoup
import adqvest_db
import JobLogNew as log
from fuzzywuzzy import process
from itertools import product
os.chdir(r'C:\Users\Administrator\AdQvestDir\codes\One time run\CAG')

def get_fuzz_score(str1, str2):

    partial_ratio = fuzz.partial_ratio(str1, str2)

    return partial_ratio


def pattern_searcher(search_str:str, search_list:str):

    search_obj = re.match(search_list, search_str)
    if search_obj :
        return_str = search_str[search_obj.start(): search_obj.end()]
    else:
#        return_str = search_str.partition(' ')[0]+' FLAG'
         return_str = 'NA'
    return return_str

def get_data(url):
    time.sleep(0.5)
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    print(url)
    try:
        table = soup.find('table')

        records = []
        columns = []
        for tr in table.findAll("tr"):
            ths = tr.findAll("th")
            if ths != []:
                for each in ths:
                    columns.append(each.text)
            else:
                trs = tr.findAll("td")
                record = []
                for each in trs:
                    try:
                        link = each.find('a')['href']
                        text = each.text
                        record.append(link)
                        record.append(text)
                    except:
                        text = each.text
                        record.append(text)
                        state = soup.find_all("p",{'class':'bc'})[0].text.split(" > ")[1]
                        district = soup.find_all("p",{'class':'bc'})[0].text.split(" > ")[2]
                        record.append(state)
                        record.append(district)
                records.append(record)

        columns.insert(0, 'Link')
        columns.append("State")
        columns.append("District")
        df = pd.DataFrame(data=records, columns = columns)

    except:
        try:
            table = soup.find('table')

            records = []
            columns = []
            for tr in table.findAll("tr"):
                ths = tr.findAll("th")
                if ths != []:
                    for each in ths:
                        columns.append(each.text)
                else:
                    trs = tr.findAll("td")
                    record = []
                    for each in trs:
                        try:
                            link = each.find('a')['href']
                            text = each.text
                            record.append(link)
                            record.append(text)
                        except:
                            text = each.text
                            record.append(text)
#                            state = soup.find_all("p",{'class':'bc'})[0].text.split(" > ")[1]
#                            record.append(state)
                    records.append(record)

            columns.insert(0, 'Link')
#            columns.append("State")
            df = pd.DataFrame(data=records, columns = columns)
        except:
            try:
                records = soup.find_all("a",href=True)
                records = records[4:len(records)-1]
                names = [a.text for a in records]
                records = [a['href'] for a in records]
                state = soup.find_all("p",{'class':'bc'})[0].text.split(" > ")[1]
                district = soup.find_all("p",{'class':'bc'})[0].text.split(" > ")[2]
                block = soup.find_all("p",{'class':'bc'})[0].text.split(" > ")[3]
                df = pd.DataFrame({"Links":records,"Village_Name":names,"Block":block,"District":district,"State":state})
            except:
                pass


    df.iloc[:,0] = "https://villagemap.in" + df.iloc[:,0]

    return df

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir(r'C:\Users\Administrator\AdQvestDir\codes\One time run\CAG')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'VILLAGE_LAT_LONG_CAG'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
        #dfp = pd.read_sql("select * from AdqvestDB.CAG_SCHOOLS_VILLAGE_LEVEL_DATA_Abhi",con=engine)
        #df1 = pd.read_csv(r"CAG_NEW_SAT_IMAGES_V2.csv")
        #df2 = pd.read_csv(r"Villages_Coordinates.csv")
        #df2 = df2[list(df1.columns)]
        #dfp.to_csv("CAG_MAIN.csv",index=False)

        #dist1 = list(df1['District_Name'].unique())
        #dist2 = list(df2['District_Name'].unique())
        #new  = dist1+dist2
        #main_dist = list(set(dist1)^set(dist2))
        #main_dist = list(set(new))



        #df1 = pd.concat([df1,df2])

        #main_df = df1[df1['District_Name'].isin(main_dist)]
        #main_df['State_Name'] = main_df['State_Name'].apply(lambda x : x.title())
        #main_df['Pincode'] = main_df['Pincode'].apply(lambda x : int(x))
        main_df = pd.read_sql("Select * from AdqvestDB.VILLAGE_SHORTLISTED_CAG",con=engine)
        main_df = main_df[(main_df['Village_Name'].str.contains("WARD")==False)|(main_df['Village_Name'].str.contains("WORD")==False)|(main_df['Village_Name'].str.contains("WARD ")==False)]
        main_df.columns = ['Village', 'State', 'District', 'Block', 'Pincode']
        main_df = main_df[['Village', 'Block', 'District','State', 'Pincode']]
        main_df.iloc[:,0] = main_df.iloc[:,0].str.title()
        main_df.iloc[:,1] = main_df.iloc[:,1].str.title()
        main_df.iloc[:,2] = main_df.iloc[:,2].str.title()
        main_df.iloc[:,3] = main_df.iloc[:,3].str.title()
        main_df['District'] = main_df['District'].str.replace('Sundargarh','SUNDERGARH'.title(),regex=True)
        main_df['District'] = main_df['District'].str.replace('Balangir','Bolangir'.title(),regex=True)
        main_df['District'] = main_df['District'].str.replace('Nabarangapur','Nabarangpur'.title(),regex=True)



        url = 'https://villagemap.in/'
        r = requests.get(url,verify = False,headers = {"user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"})
        soup = BeautifulSoup(r.text,'html.parser')


        states = ['Bihar','Odisha','Jharkhand','Uttar Pradesh']
        df = get_data(url)
        df1 = df[df.iloc[:,1].isin(states)]

        districts = []
        for links in df1.iloc[:,0].to_list():

            dataframe = get_data(links)

            districts.append(dataframe)


        reqd_districts = list(main_df['District'].unique())
        reqd_districts = [x.title() for x in reqd_districts]+['Nabarangapur', 'Sundargarh', 'Balangir']#['Nabarangpur', 'Sundergarh', 'Bolangir']

        main = pd.DataFrame()
        for vals in districts:
            main = pd.concat([main,vals])

        main1 = main[main.iloc[:,1].isin(reqd_districts)]

        sub_districts = []
        for links in main1.iloc[:,0].to_list():

            dataframe = get_data(links)

            sub_districts.append(dataframe)

        main2 = pd.DataFrame()
        for vals in sub_districts:
            main2 = pd.concat([main2,vals])

        main2 = main2.reset_index()

        main2.columns = ['index', 'URL', 'Block', 'Number of Villages', 'State',
               'District']

        main2  = main2[['URL', 'Block',
               'District', 'State']]
        main2['District'] = main2['District'].str.replace('Sundargarh','SUNDERGARH'.title(),regex=True)
        main2['District'] = main2['District'].str.replace('Balangir','BOLANGIR'.title(),regex=True)
        main2['District'] = main2['District'].str.replace('Nabarangapur','Nabarangpur'.title(),regex=True)

        reqd_blocks = list(main_df['Block'].unique())
        reqd_blocks = [x.title() for x in reqd_blocks]
        t1 = main_df.drop_duplicates(subset=['Block'], keep='last')

        villages = []
        for links in main2.iloc[:,0].to_list():

            dataframe = get_data(links)

            villages.append(dataframe)

        overall_villages = pd.DataFrame()
        for vals in villages:
            overall_villages = pd.concat([overall_villages,vals])

        overall_villages.columns = ['Links', 'Village', 'Block', 'District', 'State']



        unable = pd.DataFrame()

        reqd_blocks = list(main_df['Block'].unique())
        reqd_blocks = [x.title() for x in reqd_blocks]
        reqd_districts = list(main_df['District'].unique())
        t1 = main_df.drop_duplicates(subset=['Block'], keep='last')
        from fuzzywuzzy import fuzz
        matched = []
        unique = []
        check = []
        older = []
        work_with = []
        for dist in reqd_districts:
            print(dist)
            l1 = t1[t1['District'] == dist]
            l2 = main2[main2['District'] == dist]
            l1 = l1.reset_index()
            l2 = l2.reset_index()
            if len(l1)>len(l2):
                check.append(dist)
        #    l1['Block'].to_list()
        #    l2['Block'].to_list()
            matched_vendors = []
            matched_vendors1 = []
            older1 = []
            work_with1 = []
            for row in l1.index:
                vendor_name = l1._get_value(row,"Block")
                for columns in l2.index:
                    regulated_vendor_name = l2._get_value(columns,"Block")
                    matched_token=fuzz.partial_ratio(vendor_name,regulated_vendor_name)
                    if matched_token>=75:
                        matched_vendors.append({vendor_name:[regulated_vendor_name,dist]})
                        matched_vendors1.append(regulated_vendor_name)
                        older1.append(vendor_name)
                        work_with1.append([vendor_name,regulated_vendor_name])
            matched.append(matched_vendors)
            unique.append(matched_vendors1)
            older.append(older1)
            work_with.append(work_with1)
        try:
            available = list(set(sum(matched,[])))
            print(len(list(set(sum(matched,[])))))
        except:
            available = list(sum(matched,[]))
            print(len(list(sum(matched,[]))))


        print(len(sum(unique,[])))
        sliced = sum(unique,[])
        sliced1 = sum(older,[])

        overall_villages = overall_villages[overall_villages['Block'].isin(sliced)]

        headers = {"user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"}
        driver=webdriver.Chrome(executable_path= r"C:\Users\Administrator\AdQvestDir\chromedriver.exe")
        cols = list(overall_villages.columns)
        for i,row in overall_villages.iterrows():
            print(row['Links'])

#            response = requests.get(row['Links'])
            #    soup = BeautifulSoup(response.text, 'html.parser')
            #    soup.find_all("a",{'target':'_blank'})[1]['href']
            driver.get(row['Links'])
            time.sleep(5)
            driver.execute_script("window.scrollTo(0, window.scrollY + 400)")
            driver.implicitly_wait(30)
            driver.switch_to.default_content()
            driver.switch_to.frame("gmap")
            driver.implicitly_wait(30)
            elems = driver.find_elements_by_xpath("//a[@href]")
            geocode = []
            for elem in elems:
                print(elem.get_attribute("href"))
                geocode.append(elem.get_attribute("href"))
            try:
                val = geocode[2].split("/")[4]
            except:
                try:
                    val =  geocode[2].split("/")[3]
                except:
                    pass
            try:
                if len(geocode) != 0:

                    df = {
                            cols[0]:row[cols[0]],
                            cols[1]:row[cols[1]],
                            cols[2]:row[cols[2]],
                            cols[3]:row[cols[3]],
                            cols[4]:row[cols[4]],
                            cols[5]:row[cols[5]],
                            'Geocode': val
                            }

                    df = pd.DataFrame(df,index=[0])
                    print(df)
                    df.to_sql(name = "VILLAGE_LAT_LONG_CAG",index = False,if_exists = 'append',con = engine)

                    print("uploaded")
                else:

                    print('Not Found')

                    df = {

                    cols[0]:row[cols[0]],
                    cols[1]:row[cols[1]],
                    cols[2]:row[cols[2]],
                    cols[3]:row[cols[3]],
                    cols[4]:row[cols[4]],
                    cols[5]:row[cols[5]]

                    }
                    df = pd.DataFrame(df,index=[0])

                    unable = pd.concat([unable,df])

            except:
                    print('Not Found')

                    df = {

                    cols[0]:row[cols[0]],
                    cols[1]:row[cols[1]],
                    cols[2]:row[cols[2]],
                    cols[3]:row[cols[3]],
                    cols[4]:row[cols[4]],
                    cols[5]:row[cols[5]]

                    }
                    df = pd.DataFrame(df,index=[0])

                    unable = pd.concat([unable,df])
        unable.to_csv("NOT FOUND.csv",index=False)



        connection.close()
        log.job_end_log(table_name,job_start_time,no_of_ping)

    except:

        try:
            connection.close()
        except:
            pass
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])

        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
