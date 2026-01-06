import warnings
warnings.filterwarnings('ignore')
import pandas as pd
import numpy as np
import camelot
import io
import sys
from pypdf import PdfReader
from pytz import timezone
import os
import requests
import re
import datetime 
from playwright.sync_api import sync_playwright
import time
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')    #@TODO: Uncomment in prod
import JobLogNew as log     
import dbfunctions
import adqvest_db                                                 #@TODO: Uncomment in prod
from adqvest_robotstxt import Robots                              #@TODO: Uncomment in prod
robot = Robots(__file__)                                          #@TODO: Uncomment in prod

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'IEX_TAM_ANY_DAY_SINGLE_SIDE_REVERSE_AUCTION_DAILY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        pw = sync_playwright().start()
        browser = pw.chromium.launch(headless = False)
        page = browser.new_page()
        url = 'https://www.iexindia.com/TAM_Anyday.aspx?'
        robot.add_link(url)
        page.goto(url)
        time.sleep(5)
        links_df = pd.read_html(page.content(), extract_links='body')[0]
        links_df = links_df.iloc[:, ~links_df.columns.str.contains('Unnamed')]
        links_df['Auction_Date'], links_df['b'] = links_df.iloc[:, 0].str
        links_df['f'], links_df['File_Links'] = links_df.loc[:, 'Auction Details(PDF file)'].str
        links_df = links_df[['Auction_Date', 'File_Links']]
        pw.stop()
        links_df = links_df[links_df['File_Links'].str.lower().str.contains('after', na=False)]
        links_df['Auction_Date'] = pd.to_datetime(links_df['Auction_Date']).dt.date
        links_df['File_Links'] = 'https://www.iexindia.com' + links_df['File_Links'].str.replace('\.\.', '')
        links_df['Auction_No'] = links_df['File_Links'].apply(lambda x: re.findall('\d{5,}', x)[-1])

        lst_auc_no =pd.read_sql('SELECT Auction_No FROM IEX_TAM_ANY_DAY_SINGLE_SIDE_REVERSE_AUCTION_DAILY_DATA',con=engine)

        lst_auc_no['Auction_No'] = lst_auc_no['Auction_No'].astype(str).str.replace('\.0', '')
        links_df = links_df[~links_df['Auction_No'].isin(lst_auc_no['Auction_No'])]

        if len(links_df) != 0:
            final_df = pd.DataFrame()
            # os.chdir('/home/ubuntu/AdQvestDir/Junk_Files/')
            for link, rel_dt, auc_no in zip(links_df.File_Links, links_df.Auction_Date, links_df.Auction_No):
                r = requests.get(link, timeout = 60)
                filename = auc_no + '_' + str(rel_dt).replace('-', '_') + '.pdf'
                key = '/IEX/Any_Days_Single_Side_Reverse_Auction/'
                with open('C:\\Users\\Administrator\\Junk' + '\\' + filename, 'wb') as f:
                    f.write(r.content)
                    f.close()
                dbfunctions.to_s3bucket('C:\\Users\\Administrator\\Junk' + '\\' + filename, key)
                print(link, '----', rel_dt)
                link = re.sub('\s', '%20', link)
                tables = camelot.read_pdf(link, line_scale=100, pages = 'all')
                
                tb1 = tables[0].df
                tb1 = tb1.applymap(lambda x: x.replace('\xa0', ' '))
                df1 = tb1.T.reset_index(drop=True)
                df1.columns = df1.iloc[0, :]
                df1 = df1.iloc[1:, :]
                df1.columns = [i.replace('-', '').replace('  ', '_').replace(' ', '_').replace('(in_MW)', 'MW').replace('(in_MWH)', 'MW').replace('(cid:415)', '').strip() for i in df1.columns]

                # Allocated Quantity
                tables = camelot.read_pdf(link, line_scale=100, pages = 'all')
                
                if len(tables) > 3:
                    tb2 = tables[-2].df
                    tb2 = tb2.append(tables[-1].df)
                else:
                    tb2 = tables[-1].df
                tb2 = tb2.applymap(lambda x: x.replace('\xa0', ' '))

                try:
                    tb2.columns = ["Col_1", 'Col_2']
                except:
                    tb2.columns = range(tb2.shape[1])

                tb2 = tb2[tb2.Col_2!='']
                tb2 = tb2[tb2.iloc[:, 0].str.lower().str.contains('allocated', na=False)]
                tb2 ['Col_2']= tb2.Col_2.replace('[NAna]', '0', regex=True)

                if tb2.Col_2.str.contains('@.+', na=False).any():
                    tb2['Col_1'] = tb2.Col_1.str.split(', ')
                    tb2['Col_2'] = tb2.Col_2.str.split('@')
                    tb2 = tb2.explode(['Col_1', 'Col_2'])

                elif tb2['Col_2'].str.contains('@', na=False).any():
                    tb2['Col_2'] = tb2['Col_2'].str.replace('@', '').str.strip()

                elif tb2['Col_1'].str.contains('@', regex=True, na=False).any():
                    tb2['Col_1'] = tb2['Col_1'].str.strip().str.split('  ')
                    tb2 = tb2.explode(['Col_1'])
                    tb2 = tb2.reset_index(drop=True)
                    tb2['Col_2'] = np.where(tb2['Col_1'].str.contains('@', na=False), tb2['Col_1'], tb2['Col_2'])
                    tb2['Col_1'] = np.where(tb2['Col_1'].str.contains('@', na=False), '', tb2['Col_1'])
                    tb2['Col_2'] = tb2.Col_2.shift(-1)
                    tb2 = tb2[tb2.Col_1!='']
                    tb2['Col_1'] = tb2.Col_1.str.split(', ')
                    tb2['Col_2'] = tb2.Col_2.str.split('@')
                    tb2 = tb2.explode(['Col_1', 'Col_2'])

                else:
                    tb2['Col_2'] = tb2['Col_2'].apply(lambda x: re.findall('(\d+)', x))
                    tb2 = tb2.explode('Col_2')
                tb2 = tb2[tb2.iloc[:, 0].str.lower().str.contains('allocated', na=False)]
                tb2['Col_2'] = tb2['Col_2'].astype(int)    

                df2 = pd.DataFrame()
                df2 = df2.append(pd.Series(), ignore_index=True)
                df2['Allocated_Qty_MW'] = np.nan
                df2['Allocated_Qty_MU'] = np.nan
                if tb2['Col_1'].str.contains('MW', regex=True, na=False).any() == True:
                    df2['Allocated_Qty_MW'][0] = sum(tb2.Col_2)
                elif tb2['Col_1'].str.contains('MWh', regex=True, na=False).any() == True:
                    df2['Allocated_Qty_MU'][0] = sum(tb2.Col_2)/1000
                
                df2 = df2.reset_index(drop=True)
                df1 = df1.reset_index(drop=True)
                df = pd.concat([df1, df2], axis = 1)

                r = requests.get(link)
                reader = PdfReader(io.BytesIO(r.content))
                corpus = reader.pages[0].extract_text().replace(' ', '').splitlines()
                for i in corpus:
                    if 'initiated' in i.lower():
                        print(i.split('on')[-1].split('th')[-1].split('rd')[-1].split('st')[-1].split('nd')[-1])
                        year = re.findall('\d{4}', i.split('on')[-1])[-1].replace(' ', '')
                        try:
                            month = re.findall('[A-z]+\s[a-z]+', i.split('on')[-1].strip())[-1].replace(' ', '')
                        except:
                            try:
                                month = re.findall('[A-Z][a-z]+', i.split('on')[-1])[0]
                            except:
                                month = re.findall('[A-z]+', i.split('on')[-1].strip())[-1].replace('th', '').replace('rd', '').replace('st', '').replace('nd', '').replace(' ', '')
                        try:
                            day = int(i.split('on')[-1].strip().split('st')[0]) - 1
                            auc_start = pd.to_datetime(f'{day} {month} {year}').date()
                        except:
                            day = re.findall('\d{2}', i.split('on')[-1].strip())[0].replace(' ', '')
                            auc_start = pd.to_datetime(f'{day} {month} {year}').date()
                            
                    elif 'results' in i.lower():
                        print(i.split('on')[-1])
                        year = re.findall('\d{4}', i.split('on')[-1].replace(' ', ''))[-1].replace(' ', '')
                        try:
                            month = re.findall('[A-z]+\s[a-z]+', i.split('on')[-1].strip())[-1].replace(' ', '')
                        except:
                            try:
                                month = re.findall('[A-Z][a-z]+', i.split('on')[-1])[0]
                            except:
                                month = re.findall('[A-z]+', i.split('on')[-1].strip())[-1].replace('th', '').replace('rd', '').replace('st', '').replace('nd', '').replace(' ', '')
                        try:
                            day = int(i.split('on')[-1].strip().split('st')[0]) - 1
                            auc_end = pd.to_datetime(f'{day} {month} {year}').date()
                        except:
                            day = re.findall('\d{2}', i.split('on')[-1].strip())[0].replace(' ', '')
                            auc_end = pd.to_datetime(f'{day} {month} {year}').date()
                        break     
                df['Auction_Start_Date'] = auc_start
                df['Auction_Done_Date'] = auc_end
                df['Link'] = link
                df['Auction_No'] = auc_no
                df['Relevant_Date'] = rel_dt
                print(df)
                final_df = pd.concat([final_df, df])
                os.remove('C:\\Users\\Administrator\\Junk' + '\\' + filename)
            data = final_df.copy()

            data['Delivery_Date_x'] = data.Delivery_Dates.apply(lambda x: x.split('&')[0])
            data['Delivery_Date_y'] = data.Delivery_Dates.apply(lambda x: x.split('&')[1] if len(x.split('&')) > 1 else np.nan)

            data['From_Delivery_Date_1'] = data.Delivery_Date_x.apply(lambda x: pd.to_datetime(x.split('to')[0].strip(), dayfirst=True).date())
            data['To_Delivery_Date_1'] = data.Delivery_Date_x.apply(lambda x: pd.to_datetime(x.split('to')[1].strip()).date())

            data['From_Delivery_Date_2'] = data.Delivery_Date_y.apply(lambda x: pd.to_datetime(x.split('to')[0].strip(), dayfirst=True).date() if type(x) == str else x)
            data['To_Delivery_Date_2'] = data.Delivery_Date_y.apply(lambda x: pd.to_datetime(x.split('to')[1].strip(), dayfirst=True).date() if type(x) == str else x)

            del data['Delivery_Date_x']
            del data['Delivery_Date_y']
            data = data.replace(u'\xa0', u' ', regex=True)
            data['Delivery_Period_x'] = data.Delivery_Period.apply(lambda x: x.split('&')[0].replace('hrs', '').strip())
            data['Delivery_Period_y'] = data.Delivery_Period.apply(lambda x: x.split('&')[1].replace('hrs', '').strip() if len(x.split('&')) > 1 else np.nan)


            data['From_Delivery_Period_1'] = data.Delivery_Period_x.apply(lambda x: x.split('to')[0].strip()+':00' if len(x.split('to')) > 1 else x)
            data['From_Delivery_Period_1'] = data.From_Delivery_Period_1.apply(lambda x: x.split('–')[0].strip()+':00' if len(x.split('–')) > 1 else x)
            data['To_Delivery_Period_1'] = data.Delivery_Period_x.apply(lambda x: x.split('to')[1].strip()+':00' if len(x.split('to')) > 1 else x)
            data['To_Delivery_Period_1'] = data.To_Delivery_Period_1.apply(lambda x: x.split('–')[1].strip()+':00' if len(x.split('–')) > 1 else x)

            data['From_Delivery_Period_1'] = data['From_Delivery_Period_1'].str.replace('As.+', '0', regex=True).str.replace('.', '').str.replace('hr', '')
            data['To_Delivery_Period_1'] = data['To_Delivery_Period_1'].str.replace('As.+', '0', regex=True).str.replace('.', '').str.replace('hr', '').str.replace(' ', '')

            data['From_Delivery_Period_2'] = data.Delivery_Period_y.apply(lambda x: x.split('to')[0].strip()+':00' if type(x) == str else x)
            data['To_Delivery_Period_2'] = data.Delivery_Period_y.apply(lambda x: x.split('to')[1].strip()+':00' if type(x) == str else x)
            try:
                data['From_Delivery_Period_2'] = data['From_Delivery_Period_2'].str.replace('As.+', '0', regex=True).str.replace('.', '').str.replace('hr', '').str.replace(' ', '')
                data['To_Delivery_Period_2'] = data['To_Delivery_Period_2'].str.replace('As.+', '0', regex=True).str.replace('.', '').str.replace('hr', '').str.replace(' ', '')
            except:
                pass

            del data['Delivery_Period_x']
            del data['Delivery_Period_y']

            data['From_Delivery_Period_x1'] = data['From_Delivery_Period_1'].str.replace(':00:00', '').astype(int)
            data['To_Delivery_Period_x1'] = data['To_Delivery_Period_1'].str.replace(':00:00', '').astype(int)
            data['From_Delivery_Period_x2'] = data['From_Delivery_Period_2'].fillna('0').str.replace(':00:00', '').astype(int)
            data['To_Delivery_Period_x2'] = data['To_Delivery_Period_2'].fillna('0').str.replace(':00:00', '').astype(int)

            data['No_Of_Hours_1'] = data['To_Delivery_Period_x1'] - data['From_Delivery_Period_x1']
            data['No_Of_Hours_2'] = data['To_Delivery_Period_x2'] - data['From_Delivery_Period_x2']
            data['No_Of_Hours'] = data['No_Of_Hours_1'] + data['No_Of_Hours_2']

            data['Total_count_of_Delivery_Days'] = data['Total_count_of_Delivery_Days'].astype(float)
            data['Total_Hours'] = data['Total_count_of_Delivery_Days'] * data['No_Of_Hours']
            data['Allocated_Qty_MU'] = data['Allocated_Qty_MU'].fillna(0)
            
            data['Total_MU'] = (data['Allocated_Qty_MW']*data.Total_Hours)/1000 + data['Allocated_Qty_MU']
            data['Auction_Date'] = data.Relevant_Date
            data['Runtime'] = datetime.datetime.now(india_time)
            data = data.rename(columns = {'Total_count_of_Delivery_Days':'Total_No_of_Days'})

            data = data.drop(['From_Delivery_Period_x1', 'To_Delivery_Period_x1', 'From_Delivery_Period_x2', 'To_Delivery_Period_x2'], axis = 1)
            print(data)
            data.to_sql('IEX_TAM_ANY_DAY_SINGLE_SIDE_REVERSE_AUCTION_DAILY_DATA', if_exists='append', index=False, con=engine)
        else:
            print('No New data')
        
        log.job_end_log(table_name,job_start_time, no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')


