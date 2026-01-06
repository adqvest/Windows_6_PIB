
import sqlalchemy
import pandas as pd

import calendar
import os
import requests
import json
from bs4 import BeautifulSoup

import re
import ast
import datetime as datetime
from pytz import timezone
import requests

import numpy as np
import time

import sys
from selenium.common.exceptions import NoSuchElementException
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import camelot
import shutil
from selenium.webdriver.common.action_chains import ActionChains
from selenium import webdriver
from pyxlsb import open_workbook as open_xlsb

import warnings
warnings.filterwarnings('ignore')

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log




#log location
os.chdir('C:/Users/Administrator/AMC/')
#os.chdir('E:/Adqvest files')
#os.chdir('/home/shivam/adqvest file/')

india_time = timezone('Asia/Kolkata')
today = datetime.datetime.now(india_time)
yesterday = datetime.datetime.now(india_time) - datetime.timedelta(1)


# In[3]:



#DB Connection
properties = pd.read_csv('C:/Users/Administrator/AdQvestDir/AdQvest_properties.txt',delim_whitespace=True)
#properties = pd.read_csv('AdQvest_properties.txt',delim_whitespace=True)

host    = list(properties.loc[properties['Env'] == 'Host'].iloc[:,1])[0]
port    = list(properties.loc[properties['Env'] == 'port'].iloc[:,1])[0]
db_name = list(properties.loc[properties['Env'] == 'DBname'].iloc[:,1])[0]

con_string = 'mysql+pymysql://' + host + ':' + port + '/' + db_name + '?charset=utf8'
engine     = sqlalchemy.create_engine(con_string,encoding='utf-8')
connection = engine.connect()

#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days

## job log details
job_start_time = datetime.datetime.now(india_time)
table_name = 'AMC_MGMT_FEE'
py_file_name = 'AMC_MGMT_FEE'
scheduler = '12_PM_WINDOWS_SERVER_SCHEDULER'


#functions
def date_value(x):
    x = x.strip()
    try:
        x = datetime.datetime.strptime(x, '%B %Y').date()
    except:
        x = datetime.datetime.strptime(x, '%b %Y').date()
    return datetime.date(x.year, x.month, calendar.monthrange(x.year, x.month)[1])

def main_function():
    global no_of_ping
    try:
        try:
            icici_df = ICICI("https://www.icicipruamc.com/about-us/financials/financial-reports/unaudited-half-yearly-financial-reports")
            print(icici_df)
        except:
            icici_df = pd.DataFrame()
        no_of_ping += 1
        try:
            reliance_df = Reliance("https://www.nipponindiamf.com/investor-service/downloads/annual-half-yearly-reports")
            print(reliance_df)
        except:
            reliance_df = pd.DataFrame()
        no_of_ping += 1

        try:
            idfc_df = IDFC("https://www.idfcmf.com/statutory-disclosure/financials")
            print(idfc_df)
        except:
            idfc_df = pd.DataFrame()
        no_of_ping += 1

        try:
            hdfc_df = HDFC("https://www.hdfcfund.com/statutory-disclosure/scheme-financials")
            print(hdfc_df)
        except:
            hdfc_df = pd.DataFrame()
        no_of_ping += 1

        try:
            edelweiss_df = Edelweiss("https://www.edelweissmf.com/statutory")
            print(edelweiss_df)
        except:
            edelweiss_df = pd.DataFrame()
        no_of_ping += 1
        df = pd.concat([icici_df,reliance_df,idfc_df,hdfc_df,edelweiss_df])
        #df = pd.concat([reliance_df,hdfc_df,edelweiss_df])

        if(df.empty==False):
            return df
        else:
            return(pd.DataFrame())
    except:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        raise Exception(str(sys.exc_info()[1]) +" line " +str(exc_tb.tb_lineno))

def Reliance(url):
    r = requests.get(url)
    soup = BeautifulSoup(r.content)
    pdf_list = soup.findAll(class_ = "pdf")
    title_list = pdf_list[0].findAll(class_ = "lhsLbl")
    link_list = pdf_list[0].findAll(class_ = "rhsLbl")


    for i in range(len(title_list)):
        title_list[i] = title_list[i].text
    title_list = [x for x in title_list if 'voting policy' not in x.lower()]
    for i in range(len(link_list)):
        link_list[i] = link_list[i].find(class_ = "pdf SelectedLink").get("href")

    data = {"Titles":title_list,"Links":link_list}
    links_df = pd.DataFrame(data)
    links_df = links_df[links_df["Titles"].str.contains("Unaudited")]
    links_df["Links"] = "https://www.nipponindiamf.com"  + links_df["Links"]
    links_df.reset_index(drop=True, inplace=True)

    links_df["Titles"] = links_df["Titles"].apply(lambda x: x.split("-")[-1].strip())
    links_df["Titles"] = links_df["Titles"].apply(lambda x: x.split("–")[-1].strip())
    links_df["Titles"] = links_df["Titles"].apply(lambda x: x.replace(".","").replace(" ","-").replace(",",""))

    links_df["Relevant_Date"] = links_df["Titles"].apply(lambda x: datetime.datetime.strptime(x,"%B-%d-%Y"))
    links_df["Relevant_Date_Str"] = links_df["Relevant_Date"].apply(lambda x: datetime.datetime.strftime(x,"%Y-%m-%d"))

    links_df["Month"] = links_df["Relevant_Date"].apply(lambda x: x.month)
    links_df["Year"] = links_df["Relevant_Date"].apply(lambda x: x.year)

    links_df = links_df[:1]

    links_df["Relevant_Date"] = np.where((links_df["Relevant_Date_Str"].str.endswith("-03-31")),datetime.date(links_df["Year"] - 1,12,31),links_df["Relevant_Date"])
    links_df["Relevant_Date"] = np.where((links_df["Relevant_Date_Str"].str.endswith("-09-30")),datetime.date(links_df["Year"],6,30),links_df["Relevant_Date"])


    Latest_Date = pd.read_sql("select max(Relevant_Date) as Max from AMC_MGMT_FEE where AMC_Name = 'Reliance Nippon MF'",engine)
    Latest_Date = Latest_Date["Max"][0]
    links_df = links_df[links_df["Relevant_Date"]>Latest_Date]

    if(links_df.empty == False):
        for link in links_df["Links"]:
            print(link)
            reliance_mgmt_fee_df = pd.DataFrame()
            reliance_total_mgmt_fee_df = pd.DataFrame()
            limit = 0
            for i in range(1,50):
                try:
                    #page = camelot.read_pdf("https://www.nipponindiamf.com/InvestorServices/AnnualHalfYearlyReportsFY201920/Unaudited-Half-Yearly-Financial-Results-September-30-2019.pdf",pages = str(i),flavor='stream',row_tol = 20)
                    page = camelot.read_pdf(link,pages = str(i),flavor = "lattice")
                    df = page[0].df
                    df.columns = list(range(df.shape[1]))
                    mgmt_fee_df = df[df[1].str.contains("Percentage of")]
                    mgmt_fee_df.reset_index(drop = True,inplace =True)
                    column_df = df.loc[0]
                    mgmt_fee_df = mgmt_fee_df.loc[0]
                    reliance_mgmt_fee_df = pd.concat([mgmt_fee_df,column_df],axis = 1)
                    reliance_mgmt_fee_df["Relevant_Date"] = links_df["Relevant_Date"][0]
                    reliance_total_mgmt_fee_df = pd.concat([reliance_total_mgmt_fee_df,reliance_mgmt_fee_df])
                    print("Page:")
                    print(i)
                    print(reliance_total_mgmt_fee_df.shape)
                except:
                    limit = limit + 1
                    if(limit>1):
                        break

            reliance_total_mgmt_fee_df.columns = ["Mgmt_Fee","Scheme_Name","Relevant_Date"]
            reliance_total_mgmt_fee_df.reset_index(drop = True,inplace = True)
            reliance_total_mgmt_fee_df["Mgmt_Fee"] = reliance_total_mgmt_fee_df["Mgmt_Fee"].apply(lambda x:x.split("\n")[-2])
            reliance_total_mgmt_fee_df = reliance_total_mgmt_fee_df[reliance_total_mgmt_fee_df["Mgmt_Fee"]!='6.4']
            reliance_total_mgmt_fee_df = reliance_total_mgmt_fee_df[reliance_total_mgmt_fee_df["Scheme_Name"]!="Particulars"]
            reliance_total_mgmt_fee_df["Scheme_Name"] = reliance_total_mgmt_fee_df["Scheme_Name"].apply(lambda x: x.replace("\n",""))
            reliance_total_mgmt_fee_df["Mgmt_Fee"] = reliance_total_mgmt_fee_df["Mgmt_Fee"].apply(lambda x:x.replace("%",""))
            reliance_total_mgmt_fee_df["Mgmt_Fee"] = reliance_total_mgmt_fee_df["Mgmt_Fee"].apply(lambda x:x.replace("  ",""))
            reliance_total_mgmt_fee_df = reliance_total_mgmt_fee_df.replace("",np.nan)
            reliance_total_mgmt_fee_df.reset_index(drop = True,inplace = True)


            query = "select Scheme_Name, Scheme_Type from AMC_MONTHLY_AAUM where Relevant_Date='"+datetime.datetime.strftime(reliance_total_mgmt_fee_df["Relevant_Date"][0],"%Y-%m-%d")+"'"+" and AMC_Name = 'Reliance Nippon MF'"
            df = pd.read_sql(query, con=engine)

            reliance_total_mgmt_fee_merged_df = reliance_total_mgmt_fee_df.merge(df,how='left',on='Scheme_Name')
            reliance_total_mgmt_fee_merged_df['Runtime']=pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))


            reliance_total_mgmt_fee_merged_df["Mgmt_Fee"] = reliance_total_mgmt_fee_merged_df["Mgmt_Fee"].apply(lambda x: x.strip())
            reliance_total_mgmt_fee_merged_df["Mgmt_Fee"] = np.where((reliance_total_mgmt_fee_merged_df["Mgmt_Fee"] == ""),"0",reliance_total_mgmt_fee_merged_df["Mgmt_Fee"])

            reliance_total_mgmt_fee_merged_df["Mgmt_Fee"] = reliance_total_mgmt_fee_merged_df["Mgmt_Fee"].apply(lambda x:float(x))
            reliance_total_mgmt_fee_merged_df["Mgmt_Fee"] = reliance_total_mgmt_fee_merged_df["Mgmt_Fee"].apply(lambda x: x/100)
            reliance_total_mgmt_fee_merged_df["Mgmt_Fee"] = reliance_total_mgmt_fee_merged_df["Mgmt_Fee"].replace(np.nan,0)
            reliance_total_mgmt_fee_merged_df["Scheme_Type"] = reliance_total_mgmt_fee_merged_df["Scheme_Type"].replace(np.nan,"")
            reliance_total_mgmt_fee_merged_df["Comments"] = ""
            reliance_total_mgmt_fee_merged_df["AMC_Name"] = "Reliance Nippon MF"
            return(reliance_total_mgmt_fee_merged_df)

    else:
        print("No new Data in Reliance")

        return(pd.DataFrame())

def ICICI(url):

    chrome_driver = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
    download_file_path = r"C:\Users\Administrator\AMC"
    os.chdir('C:/Users/Administrator/AMC')
    prefs = {
        "download.default_directory": download_file_path,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True
        }
    options = webdriver.ChromeOptions()
    #options.add_argument("headless")
    options.add_experimental_option('prefs', prefs)

    driver = webdriver.Chrome(executable_path=chrome_driver, chrome_options=options)
    driver.get(url)
    time.sleep(5)

    driver.maximize_window()
    time.sleep(0.5)
    driver.find_element_by_xpath("//*[@id='divUnauditedreport']/div[2]/div/div[2]/div/div/div[2]/div[1]/div[1]/div/div/button").click()
    time.sleep(1)

    if(today.month <= 7):
        driver.find_element_by_xpath("//*[text() = '2019-2020']").click()
    else:
        driver.find_element_by_xpath("//*[text() = '2020-2021']").click()

    soup = BeautifulSoup(driver.page_source, 'lxml')
    files_list = soup.findAll(text=re.compile(r'(.*?)financial +results(.*?)',re.IGNORECASE))

    files_list = [x for x in files_list if 'notes' not in x.lower()]
    links_df = pd.DataFrame({"Files":files_list})

    if(links_df.shape[0] > 1):
        links_df = links_df[:1]
        print(links_df)

    links_df["Relevant_Date"] = links_df["Files"].apply(lambda x: x.split("ended")[-1].strip().replace(" ","-").replace(",",""))
    links_df["Relevant_Date"] = links_df["Relevant_Date"].apply(lambda x : datetime.datetime.strptime(x,"%B-%d-%Y").date())

    links_df["Month"] = links_df["Relevant_Date"].apply(lambda x: x.month)
    links_df["Year"] = links_df["Relevant_Date"].apply(lambda x: x.year)
    links_df["Relevant_Date"] = np.where((links_df["Month"] == 9),datetime.date(links_df["Year"],6,30),links_df["Relevant_Date"])
    links_df["Relevant_Date"] = np.where((links_df["Month"] == 3),datetime.date(links_df["Year"]-1,12,31),links_df["Relevant_Date"])

    Latest_Date = pd.read_sql("select max(Relevant_Date) as Max from AMC_MGMT_FEE where AMC_Name = 'ICICI Prudential MF'",engine)
    Latest_Date = Latest_Date["Max"][0]
    links_df = links_df[links_df["Relevant_Date"]>Latest_Date]
    print("Latest_Date",Latest_Date)
    print(links_df)


    if(links_df.empty == False):
        print("here")

        delete_list = os.listdir()  #remove file after reading
        for file in delete_list:
            os.remove(file)

        for file in links_df["Files"]:
            driver.find_element_by_xpath("//*[contains(text(), '"+file+"')]").click()
        time.sleep(5)
        for file in links_df["Files"]:
            link = os.listdir()[0]

        df = pd.read_excel(link)
        df.reset_index(inplace=True, drop=True)
        df.dropna(axis=0, how='all', inplace=True)
        df.columns = list(range(df.shape[1]))

        first = df[df[0].isnull() & df[1].isnull() & df[2].str.lower().str.contains('icici')]
        second = df[df[1].str.lower().str.contains('percentage of management fees to average')==True]


        df1 = pd.concat([first,second])
        df1.dropna(how='all',inplace=True, axis=1)
        df1[1] = df1[1].str.strip()
        df1.drop([0],inplace=True,axis=1)
        df1.iloc[0] = df1.iloc[0].str.strip()
        df1 = df1.transpose()
        df1.reset_index(drop=True,inplace=True)
        df1.drop([0],axis=0,inplace=True)

        df1["Relevant_Date"] = links_df["Relevant_Date"][0]
        df1['Runtime'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))

        df1.columns = ['Scheme_Name','Management_Fees', 'Relevant_Date', 'Runtime']
        df1.reset_index(drop = True,inplace= True)

        query = "select Scheme_Name, Scheme_Type from AMC_MONTHLY_AAUM where Relevant_Date='"+datetime.datetime.strftime(df1["Relevant_Date"][0],"%Y-%m-%d")+"'"+" and AMC_Name = 'ICICI Prudential MF'"
        df = pd.read_sql(query, con=engine)



        new_df = df1.merge(df,how='left',on='Scheme_Name')
        new_df = new_df[['Scheme_Name', 'Scheme_Type', 'Management_Fees', 'Relevant_Date', 'Runtime']]
        new_df = new_df.rename(columns={'Management_Fees':'Mgmt_Fee'})
        new_df["Scheme_Type"] = new_df["Scheme_Type"].replace(np.nan,"")
        new_df["Comments"] = ""
        new_df["AMC_Name"] = "ICICI Prudential MF"
        delete_list = os.listdir()  #remove file after reading
        for file in delete_list:
            os.remove(file)
        driver.quit()
        return(new_df)
    else:
        print("No new Data in ICICI")
        driver.quit()
        return(pd.DataFrame())



def IDFC(url):
    chrome_driver = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
    download_file_path = r"C:\Users\Administrator\AMC"
    os.chdir('C:/Users/Administrator/AMC')
    prefs = {
        "download.default_directory": download_file_path,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True
        }
    options = webdriver.ChromeOptions()
    #options.add_argument("headless")
    options.add_experimental_option('prefs', prefs)

    driver = webdriver.Chrome(executable_path=chrome_driver, chrome_options=options)
    driver.get(url)
    time.sleep(5)
    soup = BeautifulSoup(driver.page_source, 'lxml')

    files_list = soup.findAll(text=re.compile(r'(.*)Unaudited Financial Results(.*)'))

    links_df = pd.DataFrame({"Files":files_list})
    links_df["Relevant_Date"] = links_df["Files"].apply(lambda x: x.split("ended")[-1].strip())
    links_df["Relevant_Date"] = links_df["Relevant_Date"].apply(lambda x: datetime.datetime.strptime(x,"%B %d, %Y").date())

    links_df["Month"] = links_df["Relevant_Date"].apply(lambda x: x.month)
    links_df["Year"] = links_df["Relevant_Date"].apply(lambda x: x.year)
    if(links_df.shape[0] > 1):
        links_df = links_df[:1]
        print(links_df)
    links_df["Relevant_Date"] = np.where((links_df["Month"] == 9),datetime.date(links_df["Year"],6,30),links_df["Relevant_Date"])
    links_df["Relevant_Date"] = np.where((links_df["Month"] == 3),datetime.date(links_df["Year"] -1,12,31),links_df["Relevant_Date"])


    Distinct_Dates = pd.read_sql('select distinct(Relevant_Date) from AMC_MGMT_FEE where AMC_Name = "HDFC AMC" ',engine)
    Distinct_Dates.columns = ["Distinct_Dates"]

    Distinct_Dates_IDFC = pd.read_sql('select distinct(Relevant_Date) from AMC_MGMT_FEE where AMC_Name = "IDFC MF" ',engine)
    Distinct_Dates_IDFC.columns = ["Distinct_Dates"]


    if((links_df["Relevant_Date"].isin(Distinct_Dates["Distinct_Dates"])[0]) & (links_df["Relevant_Date"].isin(Distinct_Dates_IDFC["Distinct_Dates"])[0] == False)):
        print("YES")

        delete_list = os.listdir()  #remove file after reading
        for file in delete_list:
            os.remove(file)

        for file in links_df["Files"]:
            driver.find_element_by_xpath("//*[contains(text(), '"+file+"')]").click()
        time.sleep(5)
        for file in links_df["Files"]:
            link = os.listdir()[0]
            print(link)
            df = pd.read_excel(link)
            df.reset_index(inplace=True, drop=True)
            df.dropna(axis=0, how='all', inplace=True)
            df.columns = list(range(df.shape[1]))
            df.reset_index(inplace=True, drop=True)

            first = df[df[2].str.lower().str.contains('particulars')==True]
            #first = [val for val in first.values[0] if('idfc' in str(val).lower())]
            second = df[(df[2].str.lower().str.contains('percentage of management fees to')==True) | (df[2].str.lower().str.contains('direct plan')==True)]
            a = df[(df[2].str.lower().str.contains('percentage of management fees to')==True)].index[0]
            second = df.iloc[a+2:a+3]

            df1 = pd.concat([first,second])
            df1.drop([0,1,2], axis =1, inplace=True)
            df1 = df1.transpose()
            df1.columns = list(range(df1.shape[1]))
            df1[0]=df1[0].str.replace("^","")
            df1[0]=df1[0].str.replace("#","")
            df1[0] = df1[0].str.strip()

            def change_string(x):
                try:
                    return float(x)
                except:
                    x = x.replace("-",'0')
                    return float(x)

            df1[1] = df1[1].apply(change_string)
            df1.reset_index(drop=True,inplace=True)
            df1.columns = ["Scheme_Name","Mgmt_Fee"]
            df1['Relevant_Date'] = links_df["Relevant_Date"][0]
            df1['Runtime'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))

            query = "select Scheme_Name, Scheme_Type from AMC_MONTHLY_AAUM where Relevant_Date='"+datetime.datetime.strftime(df1["Relevant_Date"][0],"%Y-%m-%d")+"'"+" and AMC_Name = 'IDFC MF'"
            df = pd.read_sql(query, con=engine)

            new_df = df1.merge(df,how='left',on='Scheme_Name')
            new_df = new_df.rename(columns={'Management_Fees':'Mgmt_Fee'})
            new_df["Scheme_Type"] = new_df["Scheme_Type"].replace(np.nan,"")
            new_df["Comments"] = ""
            new_df["AMC_Name"] = "IDFC MF"
            delete_list = os.listdir()  #remove file after reading
            for file in delete_list:
                os.remove(file)
            driver.quit()
            return(new_df)
            print(new_df)
    else:
        print("No new Data in IDFC")
        driver.quit()
        return(pd.DataFrame())

def HDFC(url):
    try:

        chrome_driver = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
        download_file_path = r"C:\Users\Administrator\AMC"
        os.chdir('C:/Users/Administrator/AMC')
        prefs = {
            "download.default_directory": download_file_path,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True
            }
        options = webdriver.ChromeOptions()
        #options.add_argument("headless")
        options.add_experimental_option('prefs', prefs)

        driver = webdriver.Chrome(executable_path=chrome_driver, chrome_options=options)


        driver.get(url)



        limit_2 = 0
        while True:
            try:
                driver.find_element_by_xpath("//button[@class='rounded-pink-rect'][@data-attr='false']").click()
                time.sleep(5)
                break
            except:
                limit_2 = limit_2 + 1
                print('error')
                if(limit_2<3):
                    driver.refresh()
                if(limit_2>8):
                    driver.quit()
                    break
        soup = BeautifulSoup(driver.page_source, 'lxml')
        time.sleep(2)

        driver.quit()
        files_list = soup.findAll(class_ = "files")[0].findAll(class_ = "container-930 list-content alternate-colored-rows")
        title_list = []
        links_list = []
        for i in range(len(files_list)):
            links_list.append(files_list[i].find("a",href = True).get("href"))
            title_list.append(files_list[i].text)
        links_df = pd.DataFrame({"Links":links_list,"Title":title_list})
        links_df = links_df[links_df["Title"].str.contains("Unaudited")]
        links_df.reset_index(drop = True,inplace = True)
        links_df["Year"] = links_df["Title"].apply(lambda x : re.findall(r'[0-9]{4}',x)[0])
        links_df = links_df[0:1]
        links_df["Title"] = links_df["Title"].apply(lambda x:x.split("Ended")[-1].strip().split(",")[0])
        day = links_df["Title"][0].split(" ")[1]
        month = links_df["Title"][0].split(" ")[0]
        year = links_df["Year"][0]
        date = day + "-" + month + "-" +year
        date = datetime.datetime.strptime(date,"%d-%B-%Y").date()
        links_df["Relevant_Date"] = ""
        links_df["Relevant_Date"][0] = date


        if(links_df.shape[0] > 1):
            links_df = links_df[:1]
            print(links_df)
        links_df["Month"] = links_df["Relevant_Date"].apply(lambda x: x.month)
        links_df["Year"] = links_df["Relevant_Date"].apply(lambda x: x.year)
        links_df["Relevant_Date"] = np.where((links_df["Month"] == 9),datetime.date(links_df["Year"],6,30),links_df["Relevant_Date"])
        links_df["Relevant_Date"] = np.where((links_df["Month"] == 3),datetime.date(links_df["Year"] - 1,12,31),links_df["Relevant_Date"])


        Latest_Date = pd.read_sql("select max(Relevant_Date) as Max from AMC_MGMT_FEE where AMC_Name = 'HDFC AMC'",engine)
        Latest_Date = Latest_Date["Max"][0]
        links_df = links_df[links_df["Relevant_Date"]>Latest_Date]
        #links_df = links_df[links_df["Relevant_Date"] == Latest_Date] #remove later

        if(links_df.empty == False):
            for link in links_df["Links"]:
                df = pd.read_excel(link,sheet_name = "new format")
                df.reset_index(inplace=True)
                df.columns = list(range(df.shape[1]))

                first = df[df[3].str.lower().str.contains('particulars')==True]
                second = df[df[3].str.lower().str.contains('percentage of management fees to average')==True]

                df1 = pd.concat([first,second])
                df1.dropna(how='all',inplace=True, axis=1)
                df1.reset_index(inplace=True)
                df1.columns = list(range(df1.shape[1]))

                df1.drop([0,1,2,3],inplace=True,axis=1)
                df1 = df1.transpose()
                df1.reset_index(drop=True,inplace=True)

                df1[0] = df1[0].str.strip()
                df1[1] = df1[1]/100

                df1.columns = df1.iloc[0]
                df1['Relevant_Date'] = links_df["Relevant_Date"][0]
                df1['Runtime'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
                df1.columns = ['Scheme_Code','Management_Fees', 'Relevant_Date', 'Runtime']

                df_scheme_List = pd.read_excel(link,sheet_name = "Scheme list")
                df_scheme_List.reset_index(inplace=True)
                df_scheme_List.columns = list(range(df_scheme_List.shape[1]))
                df_scheme_List.drop([0,1],inplace=True,axis=1)
                df_scheme_List.columns = df_scheme_List.iloc[0]
                df_scheme_List.columns = ['Scheme_Name','Scheme_Code',]

                df_scheme_name = df1.merge(df_scheme_List,how='left',on='Scheme_Code')
                df_scheme_name.drop(['Scheme_Code'],inplace=True,axis=1)

                query = "select Scheme_Name, Scheme_Type from AMC_MONTHLY_AAUM where Relevant_Date='"+datetime.datetime.strftime(df1["Relevant_Date"][0],"%Y-%m-%d")+"'"+" and AMC_Name = 'HDFC AMC'"
                df = pd.read_sql(query, con=engine)

                new_df = df_scheme_name.merge(df,how='left',on='Scheme_Name')
                new_df = new_df[['Scheme_Name', 'Scheme_Type', 'Management_Fees', 'Relevant_Date', 'Runtime']]
                new_df = new_df.rename(columns={'Management_Fees':'Mgmt_Fee'})
                new_df["Scheme_Type"] = new_df["Scheme_Type"].replace(np.nan,"")
                new_df["Comments"] = ""
                new_df["AMC_Name"] = "HDFC AMC"
                return(new_df)
        else:
            print("No new Data in HDFC")
            return(pd.DataFrame())
    except:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        raise Exception(str(sys.exc_info()[1]) +" line " +str(exc_tb.tb_lineno))

def Edelweiss(url):
    chrome_driver = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
    download_file_path = r"C:\Users\Administrator\AMC"
    os.chdir('C:/Users/Administrator/AMC')
    prefs = {
        "download.default_directory": download_file_path,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True
        }
    options = webdriver.ChromeOptions()
    #options.add_argument("headless")
    options.add_experimental_option('prefs', prefs)

    driver = webdriver.Chrome(executable_path=chrome_driver, chrome_options=options)
    driver.get(url)
    time.sleep(5)
    soup = BeautifulSoup(driver.page_source, 'lxml')
    time.sleep(2)
    driver.quit()

    links = []
    title = []
    for row in soup.findAll(href=re.compile(r'http(.*?)xlsb')):
        links.append(row['href'])
        title.append(row.text)


    links_df = pd.DataFrame({"Links":links,"Title":title})

    links_df["Title"] = links_df["Title"].apply(lambda x:x.replace("\n","").replace("  ",""))
    links_df = links_df[links_df["Title"]!=""]
    links_df.reset_index(drop = True,inplace = True)
    links_df["Date"] = links_df["Title"].apply(lambda x:x.split("–")[-1])
    links_df["Relevant_Date"] = links_df['Date'].apply(date_value)


    if(links_df.shape[0] > 1):
        links_df = links_df[:1]
        print(links_df)
    links_df["Month"] = links_df["Relevant_Date"].apply(lambda x: x.month)
    links_df["Year"] = links_df["Relevant_Date"].apply(lambda x: x.year)
    links_df["Relevant_Date"] = np.where((links_df["Month"] == 9),datetime.date(links_df["Year"],6,30),links_df["Relevant_Date"])
    links_df["Relevant_Date"] = np.where((links_df["Month"] == 3),datetime.date(links_df["Year"]-1,12,31),links_df["Relevant_Date"])

    Latest_Date = pd.read_sql("select max(Relevant_Date) as Max from AMC_MGMT_FEE where AMC_Name = 'Edelweiss MF'",engine)
    Latest_Date = Latest_Date["Max"][0]
    links_df = links_df[links_df["Relevant_Date"]>Latest_Date]
    #links_df = links_df[links_df["Relevant_Date"] == Latest_Date] #remove later
    if(links_df.empty == False):
        for link in links_df["Links"]:
            headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36"}
            r = requests.get(links_df["Links"][0],headers = headers)
            f = open("mgmt.xlsb","wb")
            f.write(r.content)
            f.close()

            df = []

            with open_xlsb("mgmt.xlsb") as wb:
                with wb.get_sheet(1) as sheet:
                    for row in sheet.rows():
                        df.append([item.v for item in row])
            df = pd.DataFrame(df[1:], columns=df[0])

            df.columns = list(range(df.shape[1]))
            df.fillna(value=pd.np.nan, inplace=True)
            df.dropna(axis=0, how='any',thresh = 10,inplace = True)
            df.dropna(axis=1, how='any',thresh = 50,inplace = True)
            df.reset_index(inplace=True, drop=True)
            df.columns = list(range(df.shape[1]))

            # first = df[df[0].str.lower().str.contains('particulars')==True]
            # second = df[(df[0].str.lower().str.contains('percentage of management fees to')==True) | (df[0].str.lower().str.contains('direct plan')==True)]
            # a = df[(df[0]=='Direct Plan')][0].index[0]
            # second = df.iloc[a:a+1]

            first = df[df[1].str.lower().str.contains('particulars')==True]
            second = df[(df[1].str.lower().str.contains('percentage of management fees to')==True) | (df[1].str.lower().str.contains('direct plan')==True)]
            a = df[(df[1]=='Direct Plan')][0].index[0]
            second = df.iloc[a:a+1]


            df1 = pd.concat([first,second])
            df1.dropna(how='all',inplace=True, axis=1)
            df1.reset_index(inplace=True, drop=True)
            df1.columns = list(range(df1.shape[1]))
            df1.drop([0],inplace=True,axis=1)
            df1.columns = list(range(df1.shape[1]))
            df1.iloc[0] = df1.iloc[0].str.strip()
            df1 = df1.transpose()
            df1[1] = df1[1].replace("N.A.",0)

            df1.reset_index(drop=True,inplace=True)
            df1[1].fillna(0, inplace=True)

            #df1[1] = df1[1].apply(change_string)
            df1.reset_index(drop=True,inplace=True)

            df1['Relevant_Date'] = links_df["Relevant_Date"][0]
            df1['Runtime'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
            df1.columns = ['Scheme_Name','Management_Fees', 'Relevant_Date', 'Runtime']

            query = "select Scheme_Name, Scheme_Type from AMC_MONTHLY_AAUM where Relevant_Date='"+datetime.datetime.strftime(df1["Relevant_Date"][0],"%Y-%m-%d")+"'"+" and AMC_Name = 'Edelweiss MF'"
            df = pd.read_sql(query, con=engine)

            new_df = df1.merge(df,how='left',on='Scheme_Name')
            new_df = new_df[['Scheme_Name', 'Scheme_Type', 'Management_Fees', 'Relevant_Date', 'Runtime']]
            new_df = new_df.rename(columns={'Management_Fees':'Mgmt_Fee'})
            new_df["Scheme_Type"] = new_df["Scheme_Type"].replace(np.nan,'')
            new_df["Scheme_Name"] = new_df["Scheme_Name"].apply(lambda x:x.replace("\n","").replace("@","").replace("#","").replace("$",""))
            new_df["AMC_Name"] = "Edelweiss MF"
            new_df["Comments"] = ""
        return(new_df)
    else:
        print("No new Data in Edelweiss")
        return(pd.DataFrame())

#**************************************************************************************
no_of_ping = 0
os.chdir('C:/Users/Administrator/AdQvestDir/')
engine = adqvest_db.db_conn()
#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days
job_start_time = datetime.datetime.now(india_time)
table_name = 'AMC_MGMT_FEE'
scheduler = ''

def run_program(run_by='Adqvest_Bot', py_file_name=None):

    ## job log details
    global no_of_ping
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]




    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)


        amc_mgmt_fee_df = main_function()
        print(amc_mgmt_fee_df)
        if(amc_mgmt_fee_df.empty==False):
            print("new data")
            #if(today.date() == datetime.date(2020, 3, 31)):
            #raise Exception("New data present") #this line raises an exception if new data is uploaded so that we can check the data quality before uploading
            #amc_mgmt_fee_df.to_sql(name = "AMC_MGMT_FEE",if_exists = "append",con = engine,index = False)
            log.job_end_log(table_name,job_start_time,no_of_ping)
        else:
            print("New Data not available")
            log.job_end_log(table_name,job_start_time,no_of_ping)
    except:
        try:
             driver.quit()
        except:
            pass
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        error_msg = str(sys.exc_info()[1]) + " line " + str(exc_tb.tb_lineno)
        print(error_type)
        print(error_msg)

        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
