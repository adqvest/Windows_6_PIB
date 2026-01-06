# -*- coding: utf-8 -*-
"""
Created on Wed Apr 27 14:40:52 2022

@author: Santonu
"""
from scholarly import scholarly
import ast
import sqlalchemy
import pandas as pd
from pandas.io import sql
import os
from dateutil import parser
import requests
from bs4 import BeautifulSoup
import re
import datetime as datetime
from pytz import timezone
import sys
import warnings
import json
import time
import numpy as np
warnings.filterwarnings('ignore')
from playwright.sync_api import sync_playwright

#%%
# sys.path.insert(0, '/home/ubuntu/AdQvestDir/Adqvest_Function')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
#sys.path.insert(0, r'C:\Adqvest')
import numpy as np
import adqvest_db
import JobLogNew as log
requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = 'ALL:@SECLEVEL=1'
import ClickHouse_db


#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days

engine = adqvest_db.db_conn()
connection = engine.connect()
client = ClickHouse_db.db_conn()

os.chdir('C:/Users/Administrator')
driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
download_path=os.getcwd()

#%%
def drop_duplicates(df):
    columns = df.columns.to_list()
    columns.remove('Runtime')
    df = df.drop_duplicates(subset=columns, keep='last')
    return df
def get_renamed_columns(df,col_dict):
    df.fillna('#', inplace=True)
    for k,v in col_dict.items():
        print(k)
        try:
            ele=[item for item in df.columns if k.lower() in item.lower()]
            k1=df.columns.to_list().index(ele[0])
            df=df.rename(columns={f"{df.columns[k1]}":f"{col_dict[k]}"})
        except:
            pass
    
    df.reset_index(drop=True,inplace=True)
    df=df.replace('#',np.nan)
    return df

def Upload_Data_MySQL(table,data):
        data.to_sql(table, con=engine, if_exists='append', index=False)
        print("Data uploded in MySQL")
        print(data.info())

def string_reverse(s):
    s1=s.split(',')
    if len(s1)==2:
        rt=s1[1]+' '+s1[0]
        return rt
    else:
        return s

def get_page_content(url,source=''):
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=1000,
                                    chromium_sandbox=True,
                                    downloads_path=download_path)
        page = browser.new_page()
        page.goto(url)
        #---------------------------Google Scholar -------
        if source=='Google Scholar':
            for i in range(1):
                try:
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    page.locator('//*[@id="gsc_bpf_more"]/span/span[2]').click()
                except:
                    pass
            soup=BeautifulSoup(page.content())
            df = pd.read_html(page.content())
        #--------------------------------Scopus-------
        elif source=='Scopus_1':
            page.locator('//*[@id="authorSubmitBtn"]/span[1]').click()
            if 'No authors were found.' not in str(page.content()):
                page.locator('//*[@id="resultsPerPage-button"]/span[2]').click()
                page.locator('//*[@id="ui-id-4"]').click()
                page.wait_for_timeout(1000)  # Time is in milliseconds
                soup=BeautifulSoup(page.content())
                df = pd.read_html(page.content())
            else:
                print('Check Authors name')
                df =pd.DataFrame()
                soup=''
        else:
            page.wait_for_timeout(30000)  # Time is in milliseconds
            # page.wait_for_selector('//*[@id="scopus-author-profile-page-control-microui__ScopusAuthorProfilePageControlMicroui"]')
            soup=BeautifulSoup(page.content())
            df = pd.read_html(page.content())

        page.close()
        browser.close()
        return df,soup

def get_gc_Pub_Citation_Year(author):
    p=author['publications']
    my_dict={}
    for i in p:
        try:
            k=1
            yr=i['bib']['pub_year']
            if yr in my_dict.keys():
                my_dict[str(yr)].append(k)
            else:
                my_dict[str(yr)]=[k]
        except:
            pass
    my_dict={k:str(len(v))+' document' for k,v in my_dict.items() if int(k)>=2018}
    my_dict1={k:str(v)+' citation' for k,v in author['cites_per_year'].items() if int(k)>=2018}
    merged_dict={**my_dict1,**my_dict}
    return merged_dict
def google_api_data_claening(author):
    author_info=get_specfic_dict_key(author,['affiliation','citedby','interests','hindex','name','i10index','publications','scholar_id'])
    author_info['publications']=len(author_info['publications'])
    author_info['interests']=','.join(author_info['interests']).strip()

    yr_citation={k:v for k,v in author['cites_per_year'].items() if int(k)>=2016}
    for k,v in yr_citation.items():
        author_info[f'Year_{k}']=v

    # author_info['Pub_Citation_Year']=str(get_gc_Pub_Citation_Year(author))
    autor_df=pd.DataFrame.from_dict([author_info])

    
    autor_df['Source']='Google Scholar'
    autor_df['City']=np.nan
    autor_df['Country']=np.nan
    autor_df['Relevant_Date'] = today.date()
    autor_df["Runtime"] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
    autor_df=get_renamed_columns(autor_df,{'hindex':'h-index','citedby':'Citations','name':'Authors_Name','name':'Authors_Name',
                                            'i10index':'i10-index','citedby':'Citations','affiliation':'Institute',
                                            'publications':'No_Publications','interests':'Field_of_Interest','scholar_id':'Author_ID'})

    try:
        autor_df=autor_df[['Authors_Name','Author_ID','h-index','i10-index','Citations','No_Publications','Year_2016', 'Year_2017', 'Year_2018',
       'Year_2019', 'Year_2020', 'Year_2021', 'Year_2022', 'Year_2023','Field_of_Interest','Institute','Source','City','Country','Relevant_Date','Runtime']]
        print(autor_df)
    except Exception as e:
        exception_variable =ast.literal_eval(e.args[0].replace('not in index','')) 
        for col in exception_variable:
            autor_df[col]=np.nan

    autor_df=autor_df[['Authors_Name','Author_ID','h-index','i10-index','Citations','Year_2016', 'Year_2017', 'Year_2018',
       'Year_2019', 'Year_2020', 'Year_2021', 'Year_2022', 'Year_2023','No_Publications','Field_of_Interest','Institute','Source','City','Country','Relevant_Date','Runtime']]
    return autor_df

def google_org_search(org_name,scholar_type):
    kpmg_data=pd.read_sql(f"select distinct Full_Name as authors from KPMG_SCHOLARS_DATA_STATIC where Scholar_Type='{scholar_type}' and Institute_Name='{org_name}' order by Full_Name desc",con=engine)
    scholars=kpmg_data.authors.tolist()
    
    org=scholarly.search_org(org_name)
    if len(org)!=0:
        search_query = scholarly.search_author_by_organization(org[0]['id'])
        org_authors=[get_specfic_dict_key(i,['name','scholar_id']) for i in list(search_query)]
        scholars=kpmg_data.authors.tolist()
        
        scholars=[i.replace('Mr.','').replace('Ms.','').lower().strip() for i in scholars]
        my_list=[]
        for a in org_authors:
            if a['name'].lower().strip() in scholars:
                print(a)
                author_id=scholarly.search_author_id(a['scholar_id'])
                my_list.append(author_id)

    else:
        my_list=[]

    return my_list





def get_google_api_df(info,sch_type,search_type):
    if search_type=='Author_Name':
       search_query = scholarly.search_author(info)
       my_list1 = list(search_query)
       print(my_list1)
       if len(my_list1)>0:
          my_list=my_list1
       else:
          my_list=[]

    elif search_type=='Org_Name':
       my_list=google_org_search(info,sch_type)

    df_list=pd.DataFrame()
    for i in my_list:
        author = scholarly.fill(i)
        autor_df=google_api_data_claening(author)
  
        df_list=pd.concat([df_list,autor_df])

    return df_list
def get_google_df(df1,soup):

    author_name= soup.find_all(id="gsc_prf_inw")[0].text
    try:
        institute=[i.find('a',class_="gsc_prf_ila") for i in soup.find_all(class_="gsc_prf_il")][0].text
    except:
        institute=soup.find_all(class_="gsc_prf_il")[0].text

    field=[i.text for i in soup.find_all(class_="gsc_prf_il",id='gsc_prf_int')[0].find_all('a')]
    
    author_info={}
    author_info['Institute']=institute.title()
    author_info['Fied_of_Interest']=', '.join(field)
    # author_info['Fied_of_Interest']=author_info['Fied_of_Interest'].apply(lambda x:x.title())
    author_info['Authors_Name']=author_name
    autor_df1=pd.DataFrame.from_dict([author_info])
    autor_df2 =pd.DataFrame()
    
    if len(df1)==2:
        no_publication=df1[1].shape[0]
        autor_df2=[i for i in df1 if i.shape[0]==3][0]
        autor_df2=autor_df2[['Unnamed: 0', 'All']]
        autor_df2=autor_df2.T
        autor_df2.columns=autor_df2.iloc[0,:]
        autor_df2=autor_df2.iloc[1:,:]
        autor_df2['Authors_Name']=author_name
        # autor_df2['h-index'] = pd.to_numeric(autor_df2['h-index'], errors='coerce', downcast='integer')
        # autor_df2['i10-index'] = pd.to_numeric(autor_df2['i10-index'], errors='coerce', downcast='integer')

    else:
        no_publication=df1[0].shape[0]
        autor_df2['Authors_Name']=author_name
        autor_df2['Citations']=np.nan
        autor_df2['i10-index']=np.nan
        autor_df2['h-index']=np.nan
        
      
    autor_df=pd.merge(autor_df1, autor_df2,on='Authors_Name')
    autor_df['Source']='Google Scholar'
    autor_df['No_Publications']=int(no_publication)
    autor_df['Relevant_Date'] = today.date()
    autor_df["Runtime"] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
    
    autor_df['City']=np.nan
    autor_df['Country']=np.nan
    autor_df=autor_df[['Authors_Name','h-index','i10-index','Citations','No_Publications','Fied_of_Interest','Institute','Source','City','Country','Relevant_Date','Runtime']]
    return autor_df

def get_Scopus_author_df(s1):
    # _,s1=get_page_content(url)
    author=s1.find_all('tr',class_="searchArea")
    # print(author)
    df=pd.DataFrame()
    for i in author:
        try:
            citation_link=i.find(class_="authorResultsNamesCol col20").find(class_="docTitle",href=True)['href']
            _,s2=get_page_content(citation_link)
            citation=int(s2.find_all('data-testid'=="unclickable-count",'data-testid'=="metrics-section-citations-count",class_="info-field-module___OAMG")[0].find('span').text)
            author_name=string_reverse(s2.find_all('data-testid'=="author-profile-name",class_="Stack-module__tT3r4 Stack-module___CTfk")[0].find('h1').text.strip())

            merged_list=[i.find_all(attrs={"aria-label": True,'class':"highcharts-point"}) for i in s2.find_all(class_="highcharts-series-group")]
            merged_list=[i for j in merged_list for i in j]
            merged_list=[i['aria-label'] for i in merged_list]
            pub_cite_pr_yr=[{i.split(':')[0]:i.split(':')[1].strip()} for i in merged_list if int(i.split(':')[0])>=2018 ]
            merged_dict={}
            for d in pub_cite_pr_yr:
                merged_dict.update(d)
            
        except:
            citation=np.nan
            merged_dict=np.nan
            author_name=string_reverse(i.find(class_="authorResultsNamesCol col20").text.strip())

        author_info={'Authors_Name':author_name,
                     'Author_ID':re.findall(r'\d+',str(i.find(class_="authorResultsNamesCol col20").find(class_="hidden")))[0],
                     'Citations':citation,
                     'Pub_Citation_Year':str(merged_dict),
                     'No_Publications':i.find(class_="dataCol3 alignRight").text.strip(),
                     'h-index':i.find(class_="dataCol4 alignRight").text.strip(),
                     'Institute':i.find(class_="dataCol5").text.strip(),
                     'City':i.find(class_="dataCol6").text.strip(),
                     'Country':i.find(class_="dataCol7 alignRight").text.strip()
                    
        }
        df=pd.concat([df,pd.DataFrame.from_dict([author_info])])
    df['Fied_of_Interest']=np.nan
    df['i10-index']=np.nan
    df['Field_of_Interest']=np.nan
    df['Source']='Scopus'
    df['Relevant_Date'] = today.date()
    df["Runtime"] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
    df=df[['Authors_Name','Author_ID','h-index','i10-index','Citations','No_Publications','Pub_Citation_Year','Field_of_Interest','Institute','Source','City','Country','Relevant_Date','Runtime']]
    print(df)
    return df


def get_specfic_dict_key(di,l1): 
    multi_value_dict={}
    for k,v in di.items():
        if k in l1:
            multi_value_dict[k] =v   
    return multi_value_dict


#%%
def run_program(run_by='Adqvest_Bot', py_file_name=None):
    # os.chdir('/home/ubuntu/AdQvestDir')
    os.chdir('C:/Users/Administrator/AdQvestDir')
    engine = adqvest_db.db_conn()

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'KPMG_SCOPUS_SCHOLAR_AUTHOR_DATA_ONE_TIME'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
        
        # driver_path="C:/Users/Santonu/Desktop/ADQvest/Chrome Driver/chromedriver.exe"
        # os.chdir(r"C:\Users\Santonu\Desktop\ADQvest\Error files\today error")
        # download_path=os.getcwd()
        
    
        # authors=['Diksha Vats','Pushpender gupta','Abhitesh Sachdeva','Ashwini Tiwari','Dikshant Bodana','Antra Choubey']
        # authors=['Abhitesh Sachdeva']
        # author_name='somsubhra chakraborty'
        kpmg_data=pd.read_sql("select distinct Full_Name as authors,Scholar_Type,Institute_Name from KPMG_SCHOLARS_DATA_STATIC where Scholar_Type='PMRF' and Full_Name not in (select distinct KPMG_Author from KPMG_GOOGLE_SCHOLAR_AUTHOR_DATA  where Scholar_Type='PMRF' and Runtime>='2023-12-07 00:00:25' order by KPMG_Author asc) and Full_Name is not NULL order by Full_Name asc",con=engine)
        authors=kpmg_data.authors.tolist()
        Scholar_Type=kpmg_data.Scholar_Type.unique()[0]
        #---------------------------------------------------------------------------------------------------------------------
        # scopus_data=pd.read_sql("select distinct KPMG_Author from KPMG_GOOGLE_SCHOLAR_AUTHOR_DATA  where Scholar_Type='PMRF' and where  Runtime>='2023-12-07 00:00:25' order by KPMG_Author asc",con=engine)
        # authors2=scopus_data.authors.tolist()
        # authors=[i for i in authors if i not in authors2]

        df4=pd.DataFrame()
        for au,inst in zip(kpmg_data['authors'],kpmg_data['Institute_Name']):
            author_name=au
            au1=au.replace('Mr.','').replace('Ms.','').lower().strip()
            # if len(au1.split())==3:
            #     fn=au1.split()[0].lower()
            #     ln=au1.split()[2].lower()
            #     au1=''.join([fn,ln])
            #     author_name1=au1.replace('Mr.','').replace('Ms.','').lower().strip()+' '+inst.lower()
            # else:
            author_name1=au1.replace('Mr.','').replace('Ms.','').lower().strip()+' '+inst.lower()
            
            print(author_name1)
            df5=pd.DataFrame()
            for sc in ['Google Scholar']:
                if sc=='Google Scholar':
                    try:
                      df_gschl=get_google_api_df(author_name1,Scholar_Type,search_type='Author_Name')
                    except:
                        df_gschl=pd.DataFrame()
                        
                        
                        # df5['NOC_Authors']=author_name
                        # df5['Relevant_Date'] = sc
                        # df5['Relevant_Date'] = today.date()
                        # df4=pd.concat([df4,df5])

                        print(f"No results found. for--->{author_name} in {sc}")
                        #---------------Api is there this part not req----------------
                        # search_url = f"https://scholar.google.com/scholar?q={author_name}"
                        # response = requests.get(search_url)
                    
                        # if response.status_code == 200:
                        #     soup = BeautifulSoup(response.content, 'html.parser')
                        #     link=[i.find('a',href=True)['href'] for i in soup.find_all(class_="gs_rt2")]
                        #     if len(link)>0:
                        #         link='https://scholar.google.com'+link[0]
                        #         df1,s1=get_page_content(link,download,source='Google Scholar')
                        #         df_gschl=get_google_df(df1,s1)
                        #     else:
                        #         df_gschl=pd.DataFrame()
                        # else:
                        #     print(f"No results found. for--->{author_name} in {sc}")
                        #-----------------------------------------------------------------
                
                elif sc=='Scopus':
                    try:
                        if len(author_name.split())==3:
                            fn=author_name.split()[0].lower()
                            ln=author_name.split()[2].lower()
                        else:
                            fn=author_name.split()[0].lower()
                            ln=author_name.split()[1].lower()

                        url=f'https://www.scopus.com/freelookup/form/author.uri?st1={ln}&st2={fn}'
                        df2,s2=get_page_content(url,source='Scopus_1')
                        if s2!='':
                           df_scopus=get_Scopus_author_df(s2)
                        else:
                            print(f"No results found. for--->{author_name} in {sc}")
                            df_scopus=pd.DataFrame()
                    except:
                        df_scopus=pd.DataFrame()
                        df5['NOC_Authors']=author_name
                        df5['Relevant_Date'] = sc
                        df5['Relevant_Date'] = today.date()
                        df4=pd.concat([df4,df5])

                   
            if len(df_gschl)==0:
                continue
            # authir_final=pd.concat([df_gschl,df_scopus])
            authir_final=pd.concat([df_gschl])
            authir_final['Scholar_Type']=Scholar_Type
            
            if len(authir_final)>0:
                authir_final['KPMG_Author']=author_name
            else:
                authir_final['KPMG_Author']=np.nan
            print(authir_final)
            Upload_Data_MySQL('KPMG_GOOGLE_SCHOLAR_AUTHOR_DATA',authir_final)

            # df4=pd.concat([df4,authir_final])
           
            # try:
            #  scopus=authir_final[(authir_final["Source"].isin(['Scopus']))]
            #  Upload_Data_MySQL('KPMG_SCOPUS_SCHOLAR_AUTHOR_DATA_ONE_TIME',scopus)
            # except:
            #   pass

        
        # google=authir_final[(authir_final["Source"].isin(['Google Scholar']))]
        # engine = adqvest_db.db_conn()
        # connection = engine.connect()   
        # Upload_Data_MySQL('KPMG_GOOGLE_SCHOLAR_AUTHOR_DATA',df4)


        # if len(df4)>0:
        #     os.chdir('C:/Users/Administrator/AGRI/DOES')
        #     df4.to_excel(f'KPMG_NOC_Authors_{today.date()}.xlsx')

           
#%%
        
        log.job_end_log(table_name,job_start_time, no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_type)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)


if(__name__=='__main__'):
    run_program(run_by='manual')

