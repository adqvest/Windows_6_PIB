''' @author : Joe '''

import sqlalchemy
import pandas as pd
from pandas.io import sql
import calendar
import os
import requests
import json
from bs4 import BeautifulSoup
from time import sleep
import random
import re
import ast
import datetime as datetime
from pytz import timezone
import requests, zipfile, io
import csv
import numpy as np
import zipfile
import sys
import time
import glob
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import warnings
warnings.filterwarnings('ignore')
from dateutil import parser
from pandas.tseries.offsets import MonthEnd
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import adqvest_s3
import JobLogNew as log
import ClickHouse_db
import cv2
import camelot
import time
import boto3
from botocore.config import Config
from adqvest_robotstxt import Robots
robot = Robots(__file__)
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    engine = adqvest_db.db_conn()
    client = ClickHouse_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days
    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'GENERAL_INSURANCE_14_NLS_QUARTERLY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = '2_30_AM_WINDOWS_SERVER_SCHEDULER_ALL_CODES'
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)



        def temp_sum(a,b,c):
            st = comp[comp.iloc[:,0].str.lower().str.contains(a,na=False)==True].index[0]+1
            if c =='SBI':
                st = comp[comp.iloc[:,0].str.lower().str.contains(a,na=False)==True].index[0]
                en = comp[comp.iloc[:,0].str.lower().str.contains(b,na=False)==True].index[0]+1
            elif c=='np':
                print('true')
                if b=='total':
                    en = comp[comp.iloc[:,0].str.lower().str.contains(b,na=False)==True].index[0]
                else:
                    en = comp[comp.iloc[:,0].str.lower().str.contains(b,na=False)==True].index[0]+1
            else :
                if b=='total':
                    en = comp[(comp.iloc[:,0].str.lower().str.contains(b,na=False)==True) & (comp.iloc[:,0].str.lower().str.contains(c,na=False)==True)].index[0]
                else:
                    en = comp[comp.iloc[:,0].str.lower().str.contains(b,na=False)==True].index[0]

            temp = comp.iloc[st:en]
            v = list(temp.index)
            temp = temp[temp[fyq]!='']
            temp[fyq] = temp[fyq].apply(float)
            return temp[fyq].sum(),v


        def clean_nl(comp_name,nl,tables):
            comp3 = pd.DataFrame()

            if nl =='NL6':
                df1 = tables[0].df
                df2 = tables[2].df
            else:
                df1 = tables[0].df
                df2 = tables[1].df

            comp1 = drop_upto(nl, df1)
            comp2 = drop_upto(nl, df2)

            comp1[fyq] = comp1[fyq].apply(str).apply(lambda x: x.replace(',','')).apply(lambda x: x.replace('-','')).apply(lambda x:'-'+x if '(' in x else x).apply(lambda x: x.replace('(','').replace(')',''))
            comp2[fyq] = comp2[fyq].apply(str).apply(lambda x: x.replace(',','')).apply(lambda x: x.replace('-','')).apply(lambda x:'-'+x if '(' in x else x).apply(lambda x: x.replace('(','').replace(')',''))

            #temporary
            for comp in [comp1,comp2]:
                comp['Segment'] = np.where(comp['Segment'].str.lower().str.contains('fire'),'Fire',comp['Segment'])
                comp['Segment'] = np.where(comp['Segment'].str.lower().str.contains('marine cargo'),'Marine Cargo',comp['Segment'])
                comp['Segment'] = np.where(comp['Segment'].str.lower().str.contains('total marine'),'Marine Total',comp['Segment'])
                comp['Segment'] = np.where(comp['Segment'].str.lower().str.contains('total motor'),'Motor Total',comp['Segment'])
                comp['Segment'] = np.where(comp['Segment'].str.lower().str.contains('total health'),'Health+PA+TI',comp['Segment'])
                comp['Segment'] = np.where(comp['Segment'].str.lower().str.contains('crop'),'Crop',comp['Segment'])
                comp['Segment'] = np.where(comp['Segment'].str.lower().str.contains('workmen'),'Workmen’s compensation',comp['Segment'])
                comp['Segment'] = np.where(comp['Segment'].str.lower().str.contains('credit'),'Credit Insurance',comp['Segment'])
                comp['Segment'] = np.where(comp['Segment'].str.lower().str.contains('public'),'Public/ Product Liability',comp['Segment'])
                comp['Segment'] = np.where(comp['Segment'].str.lower().str.contains('other miscellaneous'),'Other Miscellaneous',comp['Segment'])
                comp['Segment'] = np.where(comp['Segment'].str.lower().str.contains('total miscellaneous'),'Miscellaneous Total',comp['Segment'])
                comp['Segment'] = np.where(comp['Segment'].str.lower().str.contains('grand total'),'Total',comp['Segment'])

            seg = ['Health+PA+TI','Engineering','Crop','Aviation','Workmen’s compensation','Public/ Product Liability','Credit Insurance','Other Miscellaneous', 'Miscellaneous Total', 'Total']
            for se in seg:
                comp1 = pd.concat([comp1,comp2[comp2['Segment']==se]])
            #------------------------------------
            temp = comp1[(comp1['Segment']=='Fire') | (comp1['Segment']=='Marine Total') | (comp1['Segment']=='Miscellaneous Total')].reset_index(drop=True)
            te = comp1[(comp1['Segment']=='Total')]

            temp[fyq] = np.where(temp[fyq]=='',0,temp[fyq])
            temp[fyq] = temp[fyq].apply(int)
            te[fyq] = te[fyq].apply(int)
            clean_check = merge_df(nl, temp, te)
            comp1 = pd.concat([comp1,clean_check])
            comp1.insert(0,'Source',nl)
        #     comp1.columns = final_df.columns
            return comp1

        def drop_upto(nl,df1):

            to_drop = []
            try:
                case = df1[(df1.iloc[:,2].str.lower().str.contains('upto',na=False)) | (df1.iloc[:,2].str.lower().str.contains('up to',na=False))].index[0]
            except:
                case = df1[(df1.iloc[:,3].str.lower().str.contains('upto',na=False)) | (df1.iloc[:,3].str.lower().str.contains('up to',na=False)) | (df1.iloc[:,3].str.lower().str.contains('sr',na=False) & df1.iloc[:,3].str.lower().str.contains('no',na=False))].index[0]
            for cols, vals in enumerate(df1.loc[case]):
        #         if (type(vals) != np.float64) and (type(vals) != float):
                if type(vals) != float:
                    if ('upto' in vals.lower()) or ('up to' in vals.lower()):
                        to_drop.append(cols)
            df1.drop(df1.columns[to_drop],axis=1,inplace=True)
            comp = df1[~df1.iloc[:,1].str.lower().str.contains('for',na=False)]
            try:
                comp = comp[comp[comp.iloc[:,1].str.lower().str.contains('particular')==True].index[0]:].reset_index(drop=True)
            except:
                comp = comp[comp[comp.iloc[:,0].str.lower().str.contains('particular')==True].index[0]:].reset_index(drop=True)
            comp.columns = list(comp.loc[0])
            comp = comp[1:].reset_index(drop=True)

            if nl =='NL4':
                try:
                    comp = comp[comp[comp.iloc[:,0].str.lower().str.contains('gross direct premium')==True].index[0]:].reset_index(drop=True)
                    comp = comp[:comp[comp.iloc[:,0].str.lower().str.contains('net earned premium')==True].index[0]+1]
                except:
                    comp = comp[comp[comp.iloc[:,1].str.lower().str.contains('gross direct premium')==True].index[0]:].reset_index(drop=True)
                    comp = comp[:comp[comp.iloc[:,1].str.lower().str.contains('net earned premium')==True].index[0]+1]

            elif nl =='NL5':
                comp = comp[comp[comp.iloc[:,0].str.lower().str.contains('claims paid')==True].index[0]:].reset_index(drop=True)
                comp = comp[:comp[comp.iloc[:,0].str.lower().str.contains('net incurred')==True].index[0]+1]

            elif nl =='NL6':
                comp = comp[comp[comp.iloc[:,0].str.lower().str.contains('gross commission')==True].index[0]:].reset_index(drop=True)
                comp = comp[:comp[comp.iloc[:,0].str.lower().str.contains('net commission')==True].index[0]+1]
                #for 2 values in one cell
                if '   ' in comp.iloc[comp[comp.iloc[:,0].str.lower().str.contains('net commission',na=False)==True].index[0],7]:
                    comp.iloc[comp[comp.iloc[:,0].str.lower().str.contains('net commission',na=False)==True].index[0],7] =comp.iloc[comp[comp.iloc[:,0].str.lower().str.contains('net commission',na=False)==True].index[0],7].split()[1]

            elif nl == 'NL7':
                del comp[list(comp.columns)[0]]
                comp = comp[comp.iloc[:,0].str.lower().str.contains('total')]


            comp = pd.melt(comp,id_vars={comp.columns[0]} ,value_vars = list(comp.columns[1:]))
            comp.rename(columns={'Particulars':comp_name,'variable':'Segment','value':fyq},inplace=True)
            return comp


        def merge_df(nl,temp,te):
        #     if nl=='NL4':
            he_pa = temp.groupby([comp_name]).sum()
            clean_he_pa = he_pa.reset_index()
            total_clean = clean_he_pa.merge(te,on = comp_name, how = 'right')
            total_clean[fyq+'_y']=total_clean[fyq+'_y'].apply(float)
            total_clean.insert(1,fyq,total_clean[fyq+'_x']-total_clean[fyq+'_y'])
            total_clean = total_clean.iloc[:,:2]
            total_clean.insert(1,'Segment','Check')
            return total_clean

        def month_for_year(date):
            q=date.split(" ")[1]

            y='20'+date.split(" ")[0].split('Y')[1]
            dates={'Q1':'6',
                       'Q2':'9',
                    'Q3':'12',
                     'Q4':'3'}
            y = int(y)
            if dates[q] =='3':
                date = str(y) + '-' + dates[q]
            else:
                date = str(y-1) + '-' + dates[q]
            date = pd.to_datetime(date, format="%Y-%m") + MonthEnd(1)
            date = date.date()
            return date

        def spaces(x):
            try:
                return re.sub(' +', ' ', x)
            except:
                return x

        dis_fyq = pd.read_sql('Select distinct(FYQ) as Max from GENERAL_INSURANCE_14_NLS_QUARTERLY_DATA where Insurers="ICICI Lombard"',engine)['Max']
        dis_fyq = [month_for_year(i) for i in dis_fyq]

        max_date_db = max(dis_fyq)
        db_year = max_date_db.year
        year_range = str(db_year)+'-'+str(db_year+1)
        if max_date_db.month == 3:
            # year_range = str(db_year)+'-'+str(db_year+1)
            next_qr = 'Q1 '+str(db_year+1)
        elif max_date_db.month == 6:
            next_qr = 'H1 '+str(db_year+1)
        elif max_date_db.month == 9:
            next_qr = 'Q3 '+str(db_year+1)
        elif max_date_db.month == 12:
            next_qr = 'Q4 ' +str(db_year+1)
        print('year_range:',year_range)

        headers = {'authority': 'stats.g.doubleclick.net',
            'accept': '*/*',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,ta;q=0.7',
            'content-type': 'text/plain',
            'origin': 'https://www.icicilombard.com',
            'referer': 'https://www.icicilombard.com/',
            'sec-ch-ua': '"Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'cross-site',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
            'x-client-data': 'CJS2yQEIo7bJAQipncoBCJ73ygEIlqHLAQiFoM0BCI2nzQEIw6rNAQ=='}

        url = 'https://www.icicilombard.com/about-us/public-disclosure'
        r=requests.get(url, headers = headers,verify = False,timeout=180)
        robot.add_link(url)
        soup = BeautifulSoup(r.content,'lxml')

        fin_year = soup.find('div',{'id':'ddyear'}).find_all('option')
        fin_year = [i.text for i in fin_year][0]
        print('fin_year',fin_year)

        if year_range != fin_year:
            print('Site is not updated with latest financial year')
        else:
            print('Site is updated')

            url = 'https://www.icicilombard.com/about-us/public-disclosure#'+str(year_range)
            r=requests.get(url, headers = headers, timeout=60)
            act_link=[]
            soup = BeautifulSoup(r.content)
            tables = soup.find_all('div',class_='public-disclosure marB25')
            req_qr = [i for i in tables if next_qr in i.text]

            if len(req_qr)==0:
                print('No new data')
            else:
                print('New data')

                table_link = [j.find_all('tr', ) for j in req_qr][0]
                nls = [1,2,3,4,5,6,7,10,17,18,16,26,35,36]
                nls = ['NL '+str(i) for i in nls]
                final_nls = []
                for nl in nls:
                    for link in table_link:
                        if link.find('td',).text == nl:
                            final_nls.append(link)
                            break

                if len([k.get('href') for k in [j.find('a') for j in final_nls] if k.get('href')=='#']) != 0:
                    print('Files are not updated')
                else:
                    final_links = [f"https://www.icicilombard.com{k.get('href')}" for k in [j.find('a') for j in final_nls]]
                    title = [f"icici_{i}.pdf".replace(' ','-').lower() for i in nls]

                    os.chdir(r"C:\Users\Administrator\Junk_Cerc")
                    files_to_remove=[]
                    for link,name in zip(final_links,title):
                        r1=requests.get(link, headers = headers,timeout=60)
                        with open('C:\\Users\\Administrator\\Junk_Cerc\\'+name, 'wb') as f:
                            f.write(r1.content)
                            f.close()
                        files_to_remove.append('C:\\Users\\Administrator\\Junk_Cerc\\'+name)



                    comp_name = 'ICICI Lombard'
                    co = 'icici'
                    final_df = pd.DataFrame()
                    final_df['Source'] = np.nan

                    #nl-1
                    tables = camelot.read_pdf(co+'_nl-1.pdf',strip_text='\n', pages='all')
                    comp = tables[0].df
                    ind = []
                    for cols, vals in enumerate(comp.loc[0]):
                        if type(vals) != float:
                            if (('particulars' in vals.lower()) or ('total' in vals.lower())):
                                ind.append(cols)

                    comp = comp[ind]
                    fyq = 'FY' + comp.iloc[1,1].split('-')[-1]+ ' ' +comp.iloc[1,1].split(' ')[1]
                    comp.columns = [comp_name,fyq]
                    comp = comp[comp[comp.iloc[:,0].str.lower().str.contains('premiums',na=False)].index[0]:]
                    comp = comp[comp[comp_name]!=''].reset_index(drop=True)
                    comp[fyq] = comp[fyq].apply(lambda x: x.replace(',','')).apply(lambda x: x.replace('-','')).apply(lambda x:'-'+x if '(' in x else x).apply(lambda x: x.replace('(','').replace(')',''))
                    final_df.loc[len(final_df.index), comp_name] = 'Income Statement'
                    final_df['Segment'] = None
                    final_df[fyq] = np.nan

                    #others

                    other = comp[comp[comp_name].str.lower().str.contains('others')]
                    other = other[other[fyq]!='']
                    other[fyq]=other[fyq].apply(float)
                    comp = comp[~comp[comp_name].str.lower().str.contains('others')]
                    value = other[fyq].sum()
                    comp.loc[3] = ["Other income", value]  # adding a row
                    comp.index = comp.index + 1  # shifting index
                    comp = comp.sort_index()
                    comp.reset_index(drop=True,inplace=True)
                    clean = ['Premiums earned (Net)', 'Profit/ Loss on sale of Investments', 'Interest, Dividend & Rent – Gross', 'Other income - Policy',
                    'Total Income - Insurance', 'Claims Incurred (Net)', 'Commission','Operating Expenses','Others', 'Total Underwriting expenses',
                    'Operating profit / (loss)']
                    comp = comp[:comp[comp.iloc[:,0].str.lower().str.contains('c=',na=False)].index[0]+1]
                    comp[comp_name] = clean
                    comp.insert(1,'Segment',None)
                    comp.insert(0,'Source','NL1')
                    comp.columns = final_df.columns
                    final_df = pd.concat([final_df,comp])

                    #----------------------------------nl-2-----------------------------
                    tables = camelot.read_pdf(co+'_nl-2.pdf',strip_text = '\n')
                    comp = tables[0].df
                    comp = comp[comp[comp.iloc[:,1].str.lower().str.contains('particular')==True].index[0]:].reset_index(drop=True)

                    if comp.iloc[comp[comp.iloc[:,1].str.lower().str.contains('interest, dividend & rent',na=False)==True].index[0],3]=='':
                        comp.iloc[comp[comp.iloc[:,1].str.lower().str.contains('interest, dividend & rent',na=False)==True].index[0],3] =list(comp[comp.iloc[:,1].str.lower().str.contains('interest, dividend & rent',na=False)==True][4].str.split())[0][0]
                        comp.iloc[comp[comp.iloc[:,1].str.lower().str.contains('interest, dividend & rent',na=False)==True].index[0],4] =list(comp[comp.iloc[:,1].str.lower().str.contains('interest, dividend & rent',na=False)==True][4].str.split())[0][1]

                    ind=[]
                    for cols, vals in enumerate(comp.loc[0]):
                        if type(vals) != float:
                            b = 'for q' in vals.lower()
                            c = 'for t' in vals.lower()
                            if (('particulars' in vals.lower()) or (b) or (c)):
                                ind.append(cols)
                                if (b==True) or (c==True):
                                    break

                    comp = comp[ind]
                    comp = comp.iloc[comp[comp.iloc[:,0].str.lower().str.contains('income from investment')==True].index[0]+1:].reset_index(drop=True)
                    comp = comp.iloc[:comp[comp.iloc[:,0].str.lower().str.contains('after tax')==True].index[0]+1]
                    comp.columns = [comp_name,fyq]
                    comp = comp[comp[comp_name]!=''].reset_index(drop=True)
                    comp[fyq] = comp[fyq].apply(lambda x: x.replace(',','')).apply(lambda x: x.replace('-','0')).apply(lambda x:'-'+x if '(' in x else x).apply(lambda x: x.replace('(','').replace(')',''))

                    value = temp_sum('other income','total','np')[0]
                    to_drop = temp_sum('other income','total','np')[1]

                    comp.drop(to_drop,inplace=True)
                    comp[fyq] = np.where(comp[comp_name].str.lower().str.contains('other income'),value,comp[fyq])
                    comp = comp[~comp[comp_name].str.lower().str.contains('future recoverable')].reset_index(drop=True)
                    comp.reset_index(drop=True,inplace=True)

                    value = temp_sum('other expenses','total','b')[0]
                    to_drop = temp_sum('other expenses','total','b')[1]
                    comp.drop(to_drop,inplace=True)

                    comp[fyq] = np.where(comp[comp_name].str.lower().str.contains('other expense'),value,comp[fyq])
                    comp.reset_index(drop=True,inplace=True)

                    value = temp_sum('provision for taxation','deferred tax','np')[0]
                    to_drop = temp_sum('provision for taxation','deferred tax','np')[1]
                    comp.drop(to_drop,inplace=True)
                    comp[fyq] = np.where(comp[comp_name].str.lower().str.contains('provision for taxation'),value,comp[fyq])

                    clean = ['Interest, Dividend & Rent – Gross', 'Profit on sale of investments', 'Less: Loss on sale of investments',
                                'Amortization of Discount / (Premium)', 'Other Income', 'TOTAL', 'PROVISIONS (Other than taxation)',
                                         'Provision for diminution in value of investments', 'Provision for doubtful debts', 'Provision - Others',
                                         'Other expenses', 'Other expenses & Provisions', 'PBT', 'Taxes', 'PAT']
                    comp[comp_name] = clean
                    comp.insert(1,'Segment',None)
                    comp.insert(0,'Source','NL2')
                    comp.columns = final_df.columns
                    final_df = pd.concat([final_df,comp])

                    #------------------------------nl-3-------------------
                    final_df.loc[len(final_df.index), comp_name] = 'Balance Sheet'
                    tables = camelot.read_pdf(co+'_nl-3.pdf',strip_text = '\n',shift_text = [' -'])
                    comp = tables[0].df
                    comp = comp[comp[comp.iloc[:,0].str.lower().str.contains('particular')==True].index[0]:].reset_index(drop=True)
                    ind=[]
                    for cols, vals in enumerate(comp.loc[0]):
                        if type(vals) != float:
                            b = 'at ' in vals.lower()
                    #         c = 'for t' in vals.lower()
                            if (('particulars' in vals.lower()) or (b)):
                                ind.append(cols)
                                if (b==True):
                                    break
                    comp = comp[ind]
                    comp = comp.iloc[comp[comp.iloc[:,0].str.lower().str.contains('sources of fund')==True].index[0]:].reset_index(drop=True)
                    comp.columns = [comp_name,fyq]
                    if '   ' in comp.iloc[comp[comp.iloc[:,0].str.lower().str.contains('net current',na=False)==True].index[0],1]:
                        comp.iloc[comp[comp.iloc[:,0].str.lower().str.contains('net current',na=False)==True].index[0],1] =comp.iloc[comp[comp.iloc[:,0].str.lower().str.contains('net current',na=False)==True].index[0], 1].split()[0]

                    comp[fyq] = comp[fyq].apply(lambda x: x.replace(',','')).apply(lambda x: x.replace('-','0')).apply(lambda x:'-'+x if '(' in x else x).apply(lambda x: x.replace('(','').replace(')',''))

                    val = temp_sum('share capital','reserves and surplus','n')[0]
                    to_drop = temp_sum('share capital','reserves and surplus','n')[1]

                    comp.drop(to_drop,inplace=True)
                    comp.reset_index(drop=True,inplace=True)

                    value = temp_sum('fair value','policyholder','np')[0]
                    to_drop = temp_sum('fair value','policyholder','np')[1]
                    comp = comp[~comp[comp_name].str.lower().str.contains('fair value')]
                    comp.loc[6] = ["Fair value change account", value]

                    ex = comp[comp.iloc[:,0].str.lower().str.contains('borrowing')].index[0]+0.5
                    comp.loc[ex] = ['Others', val]  # adding a row
                    comp.index = comp.index + 1  # shifting index
                    comp = comp.sort_index()
                    comp.reset_index(drop=True,inplace=True)

                    value = temp_sum('application of fund','investments-policy','np')[0]
                    en = temp_sum('application of fund','investments-policy','np')[1][1]+0.5
                    comp.loc[en] = ['Total Investments', value]  # adding a row
                    comp.index = comp.index + 1  # shifting index
                    comp = comp.sort_index()
                    comp.reset_index(drop=True,inplace=True)

                    comp = comp[comp[comp_name]!=''].reset_index(drop=True)
                    comp = comp[~(comp[comp_name].str.lower().str.contains('deferred tax liability'))].reset_index(drop=True)

                    clean = ["SOURCES OF FUNDS" ,"Share Capital", "Reserves & Surplus",
                                         "FV Change- Shareholder's Funds", "FV Change- Policyholder's Funds",
                                "Fair Value Change Account", "Borrowings", "Others", "TOTAL", "APPLICATION OF FUNDS", "Investments- Shareholders Funds",
                                "Investments- Policyholders Funds", "Total Investments", "Loans",
                                "Fixed Assets", "Deferred Tax Assets", "Current Assets", "Cash and Bank Balances", "Advances and Other Assets", "Current Assets",
                                "Current Liabilities", "Provisions", "Current Liabilities & Provisions", "NET CURRENT ASSETS", "Miscellaneous Expenditure",
                                "P&L Debit balance", "TOTAL"]

                    comp[comp_name] = clean
                    comp.insert(1,'Segment',None)
                    comp.insert(0,'Source','NL3')
                    comp.columns = final_df.columns
                    final_df = pd.concat([final_df,comp])

                    #-----------------------------nl4----------------
                    final_df.loc[len(final_df.index), comp_name] = 'Premium'
                    tables = camelot.read_pdf(co+'_nl-4.pdf',strip_text = '\n', pages = 'all')
                    comp1 = clean_nl(comp_name,'NL4',tables)

                    comp1[comp_name] = np.where(comp1[comp_name].str.lower().str.contains('premium on reinsurance accepted'),'Add : Premium on reinsurance accepted',comp1[comp_name])
                    comp1[comp_name] = np.where(comp1[comp_name].str.lower().str.contains('premium on reinsurance ceded'),'Less : Premium on reinsurance ceded',comp1[comp_name])
                    comp1[comp_name] = np.where(comp1[comp_name].str.lower().str.contains('gross direct'),'Gross Direct Premium',comp1[comp_name])
                    comp1[comp_name] = np.where(comp1[comp_name].str.lower().str.contains('net written'),'Net Written Premium',comp1[comp_name])
                    comp1[comp_name] = np.where(comp1[comp_name].str.lower().str.contains('net earned'),'Net Earned Premium',comp1[comp_name])
                    comp1[comp_name] = np.where(comp1[comp_name].str.lower().str.contains('add') & comp1[comp_name].str.lower().str.contains('upr'),'Add : Opening balance of UPR',comp1[comp_name])
                    comp1[comp_name] = np.where(comp1[comp_name].str.lower().str.contains('less') & comp1[comp_name].str.lower().str.contains('upr'),'Less : Closing balance of UPR',comp1[comp_name])

                    final_df = pd.concat([final_df,comp1])

                    #------------------------nl5-------------------------
                    final_df.loc[len(final_df.index), comp_name] = 'Claims'
                    tables = camelot.read_pdf(co+'_nl-5.pdf',strip_text = '\n', pages = 'all')
                    comp1 = clean_nl(comp_name,'NL5',tables)

                    comp1[comp_name] = np.where(comp1[comp_name].str.lower().str.contains('claims paid') & comp1[comp_name].str.lower().str.contains('direct'),'Claims Paid (Direct)',comp1[comp_name])
                    comp1[comp_name] = np.where(comp1[comp_name].str.lower().str.contains('re-insurance accepted to'),'Add : Re-insurance accepted to direct claims',comp1[comp_name])
                    comp1[comp_name] = np.where(comp1[comp_name].str.lower().str.contains('re-insurance ceded to'),'Less : Re-insurance ceded to claims paid',comp1[comp_name])
                    comp1[comp_name] = np.where(comp1[comp_name].str.lower().str.contains('net claim paid'),'Net Claims Paid',comp1[comp_name])
                    comp1[comp_name] = np.where(comp1[comp_name].str.lower().str.contains('net incurred'),'Net Incurred Claims',comp1[comp_name])
                    comp1[comp_name] = np.where(comp1[comp_name].str.lower().str.contains('add') & comp1[comp_name].str.lower().str.contains('outstanding'),'Add : Claims Outstanding at the end',comp1[comp_name])
                    comp1[comp_name] = np.where(comp1[comp_name].str.lower().str.contains('less') & comp1[comp_name].str.lower().str.contains('outstanding'),'Less : Claims Outstanding at the beginning',comp1[comp_name])

                    final_df = pd.concat([final_df,comp1])

                    #-------------------------nl6--------------------------
                    final_df.loc[len(final_df.index), comp_name] = 'Commission'
                    tables = camelot.read_pdf(co+'_nl-6.pdf',strip_text = '\n', pages = 'all')
                    comp1 = clean_nl(comp_name,'NL6',tables)
                    comp1[comp_name] = np.where(comp1[comp_name].str.lower().str.contains('gross commission'),'Gross Commission',comp1[comp_name])
                    comp1[comp_name] = np.where(comp1[comp_name].str.lower().str.contains('net commission'),'Net Commission',comp1[comp_name])
                    comp1[comp_name] = np.where(comp1[comp_name].str.lower().str.contains('add') & comp1[comp_name].str.lower().str.contains('accepted'),'Add : Commission on Re-insurance accepted',comp1[comp_name])
                    comp1[comp_name] = np.where(comp1[comp_name].str.lower().str.contains('less') & comp1[comp_name].str.lower().str.contains('ceded'),'Less : Commission on Re-insurance ceded',comp1[comp_name])

                    final_df = pd.concat([final_df,comp1])
                    #-----------------------------------------------------
                    final_df.reset_index(drop=True,inplace=True)
                    # final_df[fyq] = final_df[fyq].apply(lambda x: val_spaces(x))
                    final_df[comp_name] = final_df[comp_name].apply(lambda x: spaces(x))
                    final_df[fyq] = np.where(final_df[fyq]=='',np.nan,final_df[fyq])
                    final_df[fyq] = final_df[fyq].apply(float)
                    #--------------------------------------------------------

                    #--------------------------nl7------------------------------
                    final_df.loc[len(final_df.index), comp_name] = 'Operating Expenses'
                    tables = camelot.read_pdf(co+'_nl-7.pdf',strip_text = '\n', pages = 'all')

                    comp = tables[1].df
                    comp = comp[comp[comp.iloc[:,1].str.lower().str.contains('particular')==True].index[0]:].reset_index(drop=True)
                    comp.iloc[:,1] = np.where(comp.iloc[:,1]=='', comp.iloc[:,0], comp.iloc[:,1])
                    ind = []
                    for cols, vals in enumerate(comp.loc[0]):
                        if type(vals) != float:
                            b = 'grand total' in vals.lower()
                    #         c = 'for t' in vals.lower()
                            if (('particulars' in vals.lower()) or (b)):
                                ind.append(cols)
                                if (b==True):
                                    break
                    comp = comp[ind]

                    fyq = 'FY' + comp.iloc[1,1].split('-')[-1]+' ' +comp.iloc[1,1].split(' ')[1]
                    comp.columns = [comp_name,fyq]

                    comp = comp[comp[comp.iloc[:,0].str.lower().str.contains('remuneration',na=False)].index[0]:]
                    comp = comp[comp[comp_name]!=''].reset_index(drop=True)

                    comp[fyq] = comp[fyq].apply(str).apply(lambda x: x.replace(',','')).apply(lambda x: x.replace('-','')).apply(lambda x:'-'+x if '(' in x else x).apply(lambda x: x.replace('(','').replace(')',''))
                    comp = comp[:comp[comp[comp_name].str.lower().str.contains('total')].index[0]+1]
                    main = comp[comp[comp_name].str.lower().str.contains('remuneration') | comp[comp_name].str.lower().str.contains('travel') | comp[comp_name].str.lower().str.contains('training') | comp[comp_name].str.lower().str.contains('rent') | comp[comp_name].str.lower().str.contains('repair') | comp[comp_name].str.lower().str.contains('print') | comp[comp_name].str.lower().str.contains('communication') | comp[comp_name].str.lower().str.contains('professional') | comp[comp_name].str.lower().str.contains('advertisement') | comp[comp_name].str.lower().str.contains('interest') | comp[comp_name].str.lower().str.contains('good') |  comp[comp_name].str.lower().str.contains('total')]
                    main.reset_index(drop=True, inplace=True)
                    other = comp[~(comp[comp_name].str.lower().str.contains('remuneration') | comp[comp_name].str.lower().str.contains('travel') | comp[comp_name].str.lower().str.contains('training') | comp[comp_name].str.lower().str.contains('rent') | comp[comp_name].str.lower().str.contains('repair') | comp[comp_name].str.lower().str.contains('print') | comp[comp_name].str.lower().str.contains('communication') | comp[comp_name].str.lower().str.contains('professional') | comp[comp_name].str.lower().str.contains('advertisement') | comp[comp_name].str.lower().str.contains('interest')  | comp[comp_name].str.lower().str.contains('good') | comp[comp_name].str.lower().str.contains('total'))]

                    other = other[other[fyq]!='']
                    sum_other = other[fyq].apply(float).sum()

                    main.loc[9.5] = ["Others", sum_other]  # adding a row
                    main.index = main.index + 1  # shifting index
                    main = main.sort_index()
                    main.reset_index(drop=True,inplace=True)
                    # main[fyq].loc[main[main[comp_name].str.lower().str.contains('others')].index[0]] = sum_other

                    main[comp_name] = np.where(main[comp_name].str.lower().str.contains('remuneration'),'Employees costs',main[comp_name])
                    main[comp_name] = np.where(main[comp_name].str.lower().str.contains('travel'),'Travel conveyance & vehicle running expenses',main[comp_name])
                    main[comp_name] = np.where(main[comp_name].str.lower().str.contains('training'),'Training expenses',main[comp_name])
                    main[comp_name] = np.where(main[comp_name].str.lower().str.contains('rent'),'Rent, rates and taxes',main[comp_name])
                    main[comp_name] = np.where(main[comp_name].str.lower().str.contains('repair'),'Repairs and maintenance',main[comp_name])
                    main[comp_name] = np.where(main[comp_name].str.lower().str.contains('print'),'Printing and stationery',main[comp_name])
                    main[comp_name] = np.where(main[comp_name].str.lower().str.contains('communication'),'Communication',main[comp_name])
                    main[comp_name] = np.where(main[comp_name].str.lower().str.contains('professional'),'Legal and Professional Charges',main[comp_name])
                    main[comp_name] = np.where(main[comp_name].str.lower().str.contains('advertisement'),'Advertisement and publicity',main[comp_name])
                    main[comp_name] = np.where(main[comp_name].str.lower().str.contains('interest'),'Interest and bank charges',main[comp_name])
                    main[comp_name] = np.where(main[comp_name].str.lower().str.contains('good'),'Service Tax Expenses / GST Expenses',main[comp_name])
                    main[comp_name] = np.where(main[comp_name].str.lower().str.contains('total'),'Total',main[comp_name])

                    main.insert(1,'Segment',None)
                    main.insert(0,'Source','NL7')
                    main[fyq] = np.where(main[fyq]=='',np.nan,main[fyq])
                    main[fyq] = main[fyq].apply(float)

                    chec = final_df[fyq].iat[final_df[final_df[comp_name].str.lower().str.contains('operating expenses')].index[0]] - main[fyq].iat[-1]
                    main = main.append({comp_name : 'Check', fyq : chec},ignore_index=True)

                    # main.columns = final_df.columns
                    final_df = pd.concat([final_df,main])
                    #segment wise
                    final_df.loc[len(final_df.index), comp_name] = 'Operating Expenses - Segment-wise'

                    comp1 = tables[0].df
                    comp2 = tables[1].df

                    comp1 = drop_upto('NL7',comp1)
                    comp2 = drop_upto('NL7',comp2)

                    comp = pd.concat([comp1,comp2])
                    comp[fyq] = comp[fyq].apply(str).apply(lambda x: x.replace(',','')).apply(lambda x: x.replace('-','')).apply(lambda x:'-'+x if '(' in x else x).apply(lambda x: x.replace('(','').replace(')',''))
                    comp['Segment'] = np.where(comp['Segment'].str.lower().str.contains('fire'),'Fire',comp['Segment'])
                    comp['Segment'] = np.where(comp['Segment'].str.lower().str.contains('marine cargo'),'Marine Cargo',comp['Segment'])
                    comp['Segment'] = np.where(comp['Segment'].str.lower().str.contains('total marine'),'Marine Total',comp['Segment'])
                    comp['Segment'] = np.where(comp['Segment'].str.lower().str.contains('total motor'),'Motor Total',comp['Segment'])
                    comp['Segment'] = np.where(comp['Segment'].str.lower().str.contains('total health'),'Health+PA+TI',comp['Segment'])
                    comp['Segment'] = np.where(comp['Segment'].str.lower().str.contains('crop'),'Crop',comp['Segment'])
                    comp['Segment'] = np.where(comp['Segment'].str.lower().str.contains('workmen'),'Workmen’s compensation',comp['Segment'])
                    comp['Segment'] = np.where(comp['Segment'].str.lower().str.contains('credit'),'Credit Insurance',comp['Segment'])
                    comp['Segment'] = np.where(comp['Segment'].str.lower().str.contains('other miscellaneous'),'Other Miscellaneous',comp['Segment'])
                    comp['Segment'] = np.where(comp['Segment'].str.lower().str.contains('total miscellaneous'),'Miscellaneous Total',comp['Segment'])
                    comp['Segment'] = np.where(comp['Segment'].str.lower().str.contains('grand total'),'Total',comp['Segment'])

                    comp[comp_name] = np.where(comp['Segment']== 'Total', 'Total Operating Expenses', comp['Segment'])
                    # comp.insert(1,'Segment',None)
                    comp.insert(0,'Source','NL7')
                    comp[fyq] = np.where(comp[fyq]=='',np.nan,comp[fyq])
                    comp[fyq] = comp[fyq].apply(float)

                    chec = final_df[fyq].iat[final_df[final_df[comp_name].str.lower().str.contains('operating expenses')].index[0]] - comp[fyq].iat[-1]
                    comp = comp.append({comp_name : 'Check','Segment':'Check', fyq : chec},ignore_index=True)

                    final_df = pd.concat([final_df,comp])

                    #---------------------------------nl10-----------------------------
                    final_df.loc[len(final_df.index), comp_name] = 'Reserves & Surplus'

                    tables = camelot.read_pdf(co+'_nl-10.pdf',strip_text = '\n', pages = 'all')
                    comp =  tables[0].df

                    comp = comp[comp[comp.iloc[:,1].str.lower().str.contains('particular')==True].index[0]:].reset_index(drop=True)

                    ind = []
                    for cols, vals in enumerate(comp.loc[0]):
                        if type(vals) != float:
                            b = 'at' in vals.lower()
                            if (('particulars' in vals.lower()) or (b)):
                                ind.append(cols)
                                if (b==True):
                                    break
                    comp = comp[ind]
                    columns = comp.columns.values
                    comp.replace("-","",inplace=True)

                    comp.rename(columns = {columns[0] : comp_name, columns[1] : fyq}, inplace = True)
                    line_items = ["Capital Reserve","Capital Redemption Reserve","Catastrophe Reserve","Share Premium","General Reserves","Other Reserves","Balance of Profit in Profit"]
                    pattern = '|'.join(line_items)
                    comp['item'] = comp[comp_name].apply(lambda x: bool(re.search(pattern, x,re.IGNORECASE)))
                    cb_line_items = ["Share Premium","General Reserves","Other Reserves","Balance of Profit in Profit"]
                    cb_pattern = '|'.join(cb_line_items)

                    flag = 0
                    for i in range(len(comp[comp_name])):
                        if bool(re.search(cb_pattern, comp[comp_name][i],re.IGNORECASE)):
                            if bool(re.search(r'\d+', comp[fyq][i])) == False:
                                if co == 'tata' and "other reserves" in comp[comp_name][i].lower():
                                    comp[fyq][i] = comp[fyq][i+1]
                                else:
                                    closing_bal_index = comp[comp[comp_name].str.contains('closing balance',case=False)].index
                                    comp[fyq][i] = comp[fyq][closing_bal_index[flag]]
                                    flag += 1

                    comp.drop(comp[comp['item'] == False].index, inplace=True)
                    comp.drop(['item'], axis=1, inplace=True)

                    comp[fyq] = comp[fyq].apply(str).apply(lambda x: x.replace(',','')).apply(lambda x: x.replace('-','')).apply(lambda x:'-'+x if '(' in x else x).apply(lambda x: x.replace('(','').replace(')',''))
                    comp.insert(0,'Source','NL10')
                    comp.insert(2, 'Segment', None)
                    comp[fyq] = np.where(comp[fyq]=='',np.nan,comp[fyq])
                    comp[fyq] = comp[fyq].apply(float)

                    fin_sum = comp[fyq].sum()
                    comp = comp.append({comp_name : 'Reserves & Surplus', fyq : fin_sum},ignore_index=True)

                    comp.reset_index(drop=True,inplace=True)
                    chec = final_df[fyq][final_df[comp_name].str.lower().str.contains('reserves & surplus')].iat[0] - comp[fyq].iat[-1]
                    comp = comp.append({comp_name : 'Check', fyq : chec},ignore_index=True)

                    comp[comp_name] = np.where(comp[comp_name].str.lower().str.contains('capital reserve'),'Capital Reserve',comp[comp_name])
                    comp[comp_name] = np.where(comp[comp_name].str.lower().str.contains('capital redemption reserve'),'Capital Redemption Reserve',comp[comp_name])
                    comp[comp_name] = np.where(comp[comp_name].str.lower().str.contains('share premium'),'Share Premium',comp[comp_name])
                    comp[comp_name] = np.where(comp[comp_name].str.lower().str.contains('general reserve'),'General Reserves',comp[comp_name])
                    comp[comp_name] = np.where(comp[comp_name].str.lower().str.contains('catastrophe reserve'),'Catastrophe Reserve',comp[comp_name])
                    comp[comp_name] = np.where(comp[comp_name].str.lower().str.contains('other reserve'),'Other Reserves',comp[comp_name])
                    comp[comp_name] = np.where(comp[comp_name].str.lower().str.contains('balance of profit in profit'),'P&L Balance',comp[comp_name])
                    final_df = pd.concat([final_df,comp])

                    #-----------------------------nl17-----------------
                    final_df.loc[len(final_df.index), comp_name] = 'Current Liabilities & Provisions'
                    tables = camelot.read_pdf(co+'_nl-17.pdf',strip_text = '\n', pages = 'all')
                    comp1 =  tables[0].df
                    # if comp_name == 'ICICI Lombard':
                    comp1.iloc[:,1] = np.where(comp1.iloc[:,1]=='',comp1.iloc[:,0],comp1.iloc[:,1])
                    ind = []
                    for cols, vals in enumerate(comp1.loc[0]):
                        if type(vals) != float:
                            b = 'at' in vals.lower()
                    #         c = 'for t' in vals.lower()
                            if (('particulars' in vals.lower()) or (b)):
                                ind.append(cols)
                                if (b==True):
                                    break
                    comp1 = comp1[ind]
                    comp1.columns = [comp_name,fyq]
                    comp1 = comp1[comp1.iloc[:,0].str.lower().str.contains('agents') | comp1.iloc[:,0].str.lower().str.contains('balances due to other insurance') | comp1.iloc[:,0].str.lower().str.contains('deposits held on re') | comp1.iloc[:,0].str.lower().str.contains('premiums received in advance') | comp1.iloc[:,0].str.lower().str.contains('unallocated premium') | comp1.iloc[:,0].str.lower().str.contains('claims outstanding')]
                    comp1.insert(0, 'Source', 'NL17')

                    #----------------------------nl18 --------------
                    tables = camelot.read_pdf(co+'_nl-18.pdf',strip_text = '\n', pages = 'all')
                    comp2 =  tables[0].df
                    ind = []
                    for cols, vals in enumerate(comp2.loc[0]):
                        if type(vals) != float:
                            b = 'at' in vals.lower()
                    #         c = 'for t' in vals.lower()
                            if (('particulars' in vals.lower()) or (b)):
                                ind.append(cols)
                                if (b==True):
                                    break
                    comp2 = comp2[ind]
                    comp2.columns = [comp_name,fyq]
                    comp2  = comp2[comp2[comp_name].str.lower().str.contains('reserve for unexpired risk')]
                    comp2.insert(0, 'Source', 'NL18')
                    comp = pd.concat([comp1,comp2])

                    comp[fyq] = comp[fyq].apply(str).apply(lambda x: x.replace(',','')).apply(lambda x: x.replace('-','')).apply(lambda x:'-'+x if '(' in x else x).apply(lambda x: x.replace('(','').replace(')',''))
                    comp[fyq] = np.where(comp[fyq]=='',np.nan,comp[fyq])
                    comp[fyq] = comp[fyq].apply(float)

                    comp.insert(2, 'Segment', None)
                    fin_sum = comp[fyq].sum()

                    chec1 = final_df[fyq][final_df[comp_name].str.lower().str.contains('current liabilities & provisions')].iat[0] - fin_sum
                    chec2 = final_df[fyq][final_df[comp_name].str.lower().str.contains('current liabilities & provisions')].iat[0] - (fin_sum+chec1)
                    comp = comp.append({comp_name : 'Others (balancing)', fyq : chec1},ignore_index=True)
                    comp = comp.append({comp_name : 'Total', fyq : fin_sum+chec1},ignore_index=True)
                    comp = comp.append({comp_name : 'Check', fyq : chec2}, ignore_index=True)

                    comp[comp_name] = np.where(comp[comp_name].str.lower().str.contains('agents'),'Agents’ Balances',comp[comp_name])
                    comp[comp_name] = np.where(comp[comp_name].str.lower().str.contains('balances due to other insurance'),'Balances due to other insurance companies',comp[comp_name])
                    comp[comp_name] = np.where(comp[comp_name].str.lower().str.contains('deposits held on re'),'Deposits held on re-insurance ceded',comp[comp_name])
                    comp[comp_name] = np.where(comp[comp_name].str.lower().str.contains('premiums received in advance'),'Premiums received in advance',comp[comp_name])
                    comp[comp_name] = np.where(comp[comp_name].str.lower().str.contains('unallocated premium'),'Unallocated Premium',comp[comp_name])
                    comp[comp_name] = np.where(comp[comp_name].str.lower().str.contains('claims outstanding'),'Claims Outstanding',comp[comp_name])
                    comp[comp_name] = np.where(comp[comp_name].str.lower().str.contains('reserve for unexpired risk'),'Reserve for Unexpired Risk',comp[comp_name])

                    final_df = pd.concat([final_df,comp])

                    #-----------------------------------nl-16--------------------------
                    final_df.loc[len(final_df.index), comp_name] = 'Advances And Other Assets'

                    file = co+'_nl-16.pdf'
                    nl16_df = camelot.read_pdf(file)
                    nl16_df.export(co+'_nl-16.csv', f='csv', compress=True)
                    nl16_df[0].parsing_report
                    nl16_df = pd.DataFrame(nl16_df[0].df)
                    nl16_df.columns = nl16_df.iloc[0]
                    nl16_df = nl16_df.iloc[:,1:3]
                    nl16_df.replace(np. nan,'',regex=True, inplace = True)
                    nl16_df.replace('-','',inplace = True)
                    columns = nl16_df.columns.values

                    nl16_df.rename(columns = {columns[0] : comp_name, columns[1] : fyq}, inplace = True)
                    nl16_df['Source'] = 'NL16'
                    nl16_df['Segment'] = None
                    nl16_df[fyq] = nl16_df[fyq].str.replace(',', '', regex=True)
                    nl16_df[fyq] = nl16_df[fyq].str.replace('(', '', regex=True)
                    nl16_df[fyq] = nl16_df[fyq].str.replace(')', '', regex=True)
                    nl16_df = nl16_df.loc[:,['Source', comp_name,'Segment',fyq]]
                    nl16_df[fyq] = pd.to_numeric(nl16_df[fyq], errors='coerce')
                    for i in range(len(nl16_df[comp_name])):
                        if "less :" in nl16_df[comp_name][i].lower():
                            nl16_df[fyq][i] = nl16_df[fyq][i-1] - nl16_df[fyq][i]
                            nl16_df[comp_name][i] = np.nan
                            nl16_df.ffill(axis=0,inplace=True)
                            nl16_df.drop([i-1],inplace=True)
                    line_items = ["Outstanding Premiums","Agents’ Balances","Due from other entities carrying on insurance","Income accrued on investments"]
                    pattern = '|'.join(line_items)
                    nl16_df['item'] = nl16_df[comp_name].apply(lambda x: bool(re.search(pattern, x)))
                    nl16_df.drop(nl16_df[nl16_df['item'] == False].index, inplace=True)
                    nl16_df.drop(['item'], axis=1, inplace=True)
                    nl16_df[comp_name] = np.where(nl16_df[comp_name].str.lower().str.contains('income accrued'),'Income accrued on investments',nl16_df[comp_name])
                    nl16_df[comp_name] = np.where(nl16_df[comp_name].str.lower().str.contains('outstanding'),'Outstanding Premiums',nl16_df[comp_name])
                    nl16_df[comp_name] = np.where(nl16_df[comp_name].str.lower().str.contains('agent'),'Agents’ Balances',nl16_df[comp_name])
                    nl16_df[comp_name] = np.where(nl16_df[comp_name].str.lower().str.contains('due from'),'Due from other insurance cos',nl16_df[comp_name])
                    nl3_assets = float(final_df[final_df[comp_name]=='Advances and Other Assets'][fyq].item())

                    nl16_df.loc[len(nl16_df.index)] = [np.nan,'Others (balancing)',np.nan, (nl3_assets - nl16_df[fyq].astype(float).sum())]
                    tot_chec =  nl16_df[fyq].sum()
                    nl16_df = nl16_df.append({comp_name : 'Total', fyq : tot_chec},ignore_index=True)
                    nl16_df = nl16_df.append({comp_name : 'Check', fyq : tot_chec -nl3_assets},ignore_index=True)
                    nl16_df.reset_index(drop = True, inplace = True)

                    final_df = pd.concat([final_df,nl16_df])

                    #-------------------------------------------nl-26------------------
                    final_df.loc[len(final_df.index), comp_name] = 'Solvency Margin'
                    file = co+'_nl-26.pdf'
                    tables = camelot.read_pdf(file,strip_text='\n', pages='all')
                    nl_26 = tables[0].df
                    nl_26.columns=['Item', comp_name, 'FYQ']
                    st_index = nl_26[nl_26.iloc[:,1].str.lower().str.contains('available')==True].index[0]
                    nl_26[comp_name].iloc[st_index]='Available Assets in Policyholders’ Funds'
                    nl_26=nl_26.iloc[st_index:,1:]

                    nl_26=nl_26[nl_26['FYQ']!='']
                    pattern = r'\([^)]*\)|\[[^\]]*\]|\{[^}]*\}'
                    nl_26[comp_name] = nl_26[comp_name].str.replace(pattern, '')
                    nl_26[comp_name] = nl_26[comp_name].str.replace('-', '')
                    nl_26[comp_name]=nl_26[comp_name].str.strip()
                    for index, value in nl_26[comp_name].iteritems():
                        if isinstance(value, str):
                            if value.lower() in ('current liabilities as per bs','provisions as per bs','other liabilities'):
                                nl_26.at[index, comp_name] = 'Less: ' + value

                            elif value.lower() in ('solvency ratio'):
                                nl_26.at[index, comp_name] = 'Solvency margin (x)'
                    nl_26[comp_name]=nl_26[comp_name].str.strip()

                    nl_26=nl_26.reset_index(drop=True)
                    nl_26[comp_name] = np.where(nl_26[comp_name].str.lower().str.contains('less: current liabilities'),'Less: Current Liabilities as per B/S',nl_26[comp_name])
                    nl_26[comp_name] = np.where(nl_26[comp_name].str.lower().str.contains('less: provisions'),'Less: Provisions as per B/S',nl_26[comp_name])

                    i=0
                    for find in range(len(nl_26)):
                        if 'Assets' in nl_26[comp_name].iloc[find]:
                            i=find
                    nl_26[comp_name].iloc[i+1]='Less: Other Liabilities'
                    nl_26.columns = [comp_name,fyq]

                    nl_26[fyq] = nl_26[fyq].apply(str).apply(lambda x: x.replace(',','')).apply(lambda x: x.replace('-','')).apply(lambda x:'-'+x if '(' in x else x).apply(lambda x: x.replace('(','').replace(')',''))
                    nl_26[fyq] = np.where(nl_26[fyq]=='',np.nan,nl_26[fyq])
                    nl_26.insert(0, 'Source', 'NL26')
                    nl_26.insert(2, 'Segment', None)

                    final_df = pd.concat([final_df,nl_26])

                    #--------------------------------nl-35------------------
                    final_df.loc[len(final_df.index), comp_name] = 'Policies - Segment-wise'

                    file = co+'_nl-35.pdf'
                    nl35_df = camelot.read_pdf(file)
                    nl35_df.export(co+'_nl-35.csv', f='csv', compress=True)
                    nl35_df[0].parsing_report
                    nl35_df = pd.DataFrame(nl35_df[0].df)
                    nl35_df[1] = nl35_df[1].str.replace('*', '', regex=True)
                    idx = nl35_df[nl35_df[1] == 'Line of Business'].index[0]
                    nl35_df.columns = nl35_df.iloc[idx]
                    start_idx = nl35_df[nl35_df.iloc[:,1] == 'Fire'].index[0]
                    end_idx = nl35_df[nl35_df.iloc[:,1].str.contains('Miscellaneous')].index[0]
                    nl35_df = nl35_df.iloc[start_idx:end_idx+1,1:5]
                    nl35_df.replace(np. nan,None,regex=True, inplace = True)
                    nl35_df.replace('-','',inplace = True)
                    columns = nl35_df.columns.values

                    nl35_df.rename(columns = {columns[0] : comp_name, columns[1] : fyq}, inplace = True)
                    nl35_df['Source'] = 'NL35'
                    nl35_df['Segment'] = None
                    nl35_df[fyq] = nl35_df[fyq].str.replace(',', '', regex=True)
                    nl35_df[fyq] = pd.to_numeric(nl35_df[fyq], errors='coerce')

                    nl35_df = nl35_df.loc[:,['Source', comp_name,'Segment',fyq]]
                    nl35_df = nl35_df[nl35_df[comp_name].str.contains("Other segments") == False]
                    nl35_df[comp_name] = np.where(nl35_df[comp_name].str.lower().str.contains('workmen'),'Workmen’s Compensation',nl35_df[comp_name])
                    nl35_df.loc[end_idx+1] = ['NL35','Total Policies',None,nl35_df[fyq].sum()]
                    marine_policies = nl35_df[nl35_df[comp_name].str.lower().str.contains('marine')][fyq].items()
                    marine_policies = list(zip(*marine_policies))
                    marine_policies = np.array(marine_policies[1]).astype(float).sum()
                    ex = nl35_df[nl35_df.iloc[:,1].str.lower().str.contains('marine other')].index[0]+0.5
                    nl35_df.loc[ex] = ['NL35','Marine Total',None,marine_policies]
                    nl35_df.index = nl35_df.index + 1  # shifting index
                    nl35_df = nl35_df.sort_index()
                    nl35_df.reset_index(drop=True,inplace=True)
                    motor_policies = nl35_df[nl35_df[comp_name].str.lower().str.contains('motor')][fyq].fillna(0).astype(float).items()
                    motor_policies = list(zip(*motor_policies))
                    motor_policies = np.array(motor_policies[1]).astype(float).sum()
                    ex = nl35_df[nl35_df.iloc[:,1].str.lower().str.contains('motor tp')].index[0]+0.5
                    nl35_df.loc[ex] = ['NL35','Motor Total',None,motor_policies]
                    nl35_df.index = nl35_df.index + 1  # shifting index
                    nl35_df = nl35_df.sort_index()

                    health_policies = float(nl35_df[nl35_df[comp_name].str.lower().str.contains('health')][fyq].item())
                    personal_policies = float(nl35_df[nl35_df[comp_name].str.lower().str.contains('personal')][fyq].item())
                    travel_policies = float(nl35_df[nl35_df[comp_name].str.lower().str.contains('travel')][fyq].item())
                    ex = nl35_df[nl35_df.iloc[:,1].str.lower().str.contains('travel')].index[0]+0.5
                    nl35_df.loc[ex] = ['NL35','Health + PA + TI',None,(health_policies+personal_policies+travel_policies)]

                    nl35_df.index = nl35_df.index + 1  # shifting index
                    nl35_df = nl35_df.sort_index()
                    nl35_df.reset_index(drop = True, inplace = True)
                    final_df = pd.concat([final_df,nl35_df])

                    #--------------------------------nl-36---------------
                    final_df.loc[len(final_df.index), comp_name] = 'Business Channel-wise - Policies (nos)'

                    file = co+'_nl-36.pdf'
                    tables = camelot.read_pdf(file,strip_text='\n', pages='all')
                    nl_36_table = tables[0].df
                    st_index = nl_36_table[nl_36_table.iloc[:,1].str.lower().str.contains('individual')==True].index[0]
                    end_index= nl_36_table[nl_36_table.iloc[:,1].str.lower().str.contains('total')==True].index[0]
                    nl_36_table=nl_36_table.iloc[st_index:end_index+1,:4]
                    nl_36_table.columns=['index',comp_name, 'FYQ_policies','FYQ_Premium']
                    nl_36_table=nl_36_table.reset_index(drop=True)
                    nl_36_table=nl_36_table[(nl_36_table[comp_name]!='')]
                    st_index = nl_36_table[nl_36_table.iloc[:,1].str.lower().str.contains('direct')==True].index[0]
                    end_index= nl_36_table[nl_36_table.iloc[:,1].str.lower().str.contains('common')==True].index[0]

                    nl_36_table['FYQ_policies'] = nl_36_table['FYQ_policies'].apply(str).apply(lambda x: x.replace(',','')).apply(lambda x: x.replace('-','')).apply(lambda x:'-'+x if '(' in x else x).apply(lambda x: x.replace('(','').replace(')',''))
                    nl_36_table['FYQ_Premium'] = nl_36_table['FYQ_Premium'].apply(str).apply(lambda x: x.replace(',','')).apply(lambda x: x.replace('-','')).apply(lambda x:'-'+x if '(' in x else x).apply(lambda x: x.replace('(','').replace(')',''))

                    nl_36_table['FYQ_Premium']=nl_36_table['FYQ_Premium'].replace('',np.nan)
                    nl_36_table['FYQ_policies']=nl_36_table['FYQ_policies'].replace('',np.nan)
                    nl_36_table['FYQ_policies']=nl_36_table['FYQ_policies'].astype(float)
                    nl_36_table['FYQ_Premium']=nl_36_table['FYQ_Premium'].astype(float)

                    sum_result_policies = nl_36_table['FYQ_policies'].iloc[st_index:end_index].sum()
                    sum_result_premium = nl_36_table['FYQ_Premium'].iloc[st_index:end_index].sum()

                    nl_36_table=nl_36_table[[comp_name,'FYQ_policies','FYQ_Premium']]
                    nl_36_table = nl_36_table.drop(nl_36_table.index[st_index:end_index])
                    nl_36_table.reset_index(drop=True,inplace=True)
                    ex = nl_36_table[nl_36_table.iloc[:,0].str.lower().str.contains('micro')].index[0]+0.5
                    nl_36_table.loc[ex] = ['Direct Business', sum_result_policies,sum_result_premium]  # adding a row
                    nl_36_table.index = nl_36_table.index + 1  # shifting index
                    nl_36_table = nl_36_table.sort_index()
                    nl_36_table.reset_index(drop=True,inplace=True)


                    nl_36_table=nl_36_table.reset_index(drop=True)
                    indices = nl_36_table[nl_36_table[comp_name].str.lower().str.contains('other')].index
                    last_index = indices[-1]
                    nl_36_table[comp_name].iloc[last_index]='Others'
                    nl_36_policies = nl_36_table.iloc[:, :2]

                    nl_36_premium = nl_36_table.iloc[:, [0,2]]
                    print(comp_name)
                    nl_36_premium.columns = [comp_name,fyq]
                    nl_36_policies.columns = [comp_name,fyq]
                    nl_36_premium[comp_name] = np.where(nl_36_premium[comp_name].str.lower().str.contains('total',na=False),'Total Premium',nl_36_premium[comp_name])
                    nl_36_policies[comp_name] = np.where(nl_36_policies[comp_name].str.lower().str.contains('total',na=False),'Total Policies',nl_36_policies[comp_name])

                    nl_36_premium.insert(1, 'Segment', None)
                    nl_36_premium.insert(0, 'Source', 'NL36')

                    # nl_36_policies.columns = [comp_name,fyq]
                    nl_36_policies.insert(1, 'Segment', None)
                    nl_36_policies.insert(0, 'Source', 'NL36')
                    final_df = pd.concat([final_df,nl_36_policies])
                    final_df.loc[len(final_df.index), comp_name] = 'Business Channel-wise - Total Premium'
                    final_df = pd.concat([final_df,nl_36_premium])

                    final_df.reset_index(drop=True,inplace=True)
                    final_df[fyq] = np.where(final_df[fyq]=='',np.nan,final_df[fyq])
                    # final_df[fyq] = final_df[fyq].apply(float)

                    final_df['Source'] = np.where(final_df['Segment']== 'Check',None,final_df['Source'])
                    final_df.insert(1,'Insurers',comp_name)
                    final_df['FYQ'] = final_df.columns[-1]
                    final_df.rename(columns={comp_name : 'Particulars', fyq:'Values'},inplace=True)

                    final_df['Particulars'] = final_df['Particulars'].apply(lambda x:x.strip())
                    final_df['Particulars'] = final_df['Particulars'].apply(lambda x: spaces(x))

                    final_df['Relevant_Date'] = [month_for_year(i) for i in final_df['FYQ']]
                    final_df['Runtime']=pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
                    #     --------------------*************************----------------------
                    final_df.to_sql(name='GENERAL_INSURANCE_14_NLS_QUARTERLY_DATA',if_exists='append',index=False,con=engine)

                    time.sleep(2)
                    print('Pushed into mysql for ', comp_name, fyq)

                    #S3
                    ACCESS_KEY_ID = adqvest_s3.s3_cred()[0]
                    ACCESS_SECRET_KEY = adqvest_s3.s3_cred()[1]
                    BUCKET_NAME = 'adqvests3bucket'
                    s3 = boto3.resource('s3',
                                        aws_access_key_id=ACCESS_KEY_ID,
                                        aws_secret_access_key=ACCESS_SECRET_KEY,
                                        config=Config(signature_version='s3v4', region_name='ap-south-1')
                                        )
                    for file,name in zip(files_to_remove,title):
                        data_s3 =  open(file, 'rb')
                        s3.Bucket(BUCKET_NAME).put_object(Key=f'NON_LIFE/ICICI_LOMBARD/{fyq}/{name}', Body=data_s3)
                        data_s3.close()
                        os.remove(file)
                    print("Files pushed to S3")


        log.job_end_log(table_name, job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
