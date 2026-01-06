import pandas as  pd
import datetime      
from pytz import timezone
import re
import requests
from bs4 import BeautifulSoup
import warnings
warnings.filterwarnings('ignore')
import sys
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import ClickHouse_db
import JobLogNew as log
engine = adqvest_db.db_conn()

def get_date(headline_col):
    months = ["March", "June", "September", "December"]
    month_dict = {"March": "03", "June": "06", "September": "09", "December": "12"}
    heading = headline_col[0]
    month = [i for i in heading.split() if i in months]
    print(month)
    for m in month:
        mv = month_dict[m]
        print(mv)
    year = re.findall(r"\d+", heading)
    print(year)
    date = str(year[1]) + "-" + mv + "-" + str(year[0])
    print(date)
    return pd.to_datetime(date).date()

def insert_into_db(df, table_name, date,today):
    df['Relevant_Date'] = date
    df['Runtime'] = today
    max_rel_date = pd.read_sql(f'SELECT Max(Relevant_Date) as max_date FROM {table_name}', con=engine)['max_date'][0]
    if date > max_rel_date:
        df.to_sql(table_name, if_exists='append', index=False, con=engine)
        print(f'Inserted into {table_name}\nNo. of Rows: {len(df)}')
    else:
        print('Not yet')    
    
def run_program(run_by='Adqvest_Bot', py_file_name=None):
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'SEBI_AIF_DOM_FOREIGN_INVESTMENT_CUMU_QUARTERLY_DATA,SEBI_AIF_NET_INVESTMENT_IN_SECURITIES_CUMU_QUARTERLY_DATA,SEBI_AIF_NET_INVESTMENT_IN_EQ_DEBT_CUMU_QUARTERLY_DATA,SEBI_AIF_NET_INVESTMENT_IN_STARTUP_MSME_CUMU_QUARTERLY_DATA,SEBI_AIF_NET_INVESTMENT_IN_TOP_10_SECTORS_CUMU_QUARTERLY_DATA'

    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        url = 'https://www.sebi.gov.in/statistics/1392982252002.html'
        r = requests.get(url)  
        print(r)  

        soup = BeautifulSoup(r.content, 'lxml')
        tables = soup.findAll('table',class_='table table-striped table-bordered table-hover background statistics-table')

        headline_col = []
        name = soup.find_all(class_="org-strong")
        for n in name:
            heading = n.text
            heading = heading.replace(",", " ")
            headline_col.append(heading)

        date = get_date(headline_col)
        
        headline = ['domestic investors','listed securities','security receipts','investments made in startup','all investments']
        all_dfs = []
        for head in headline:
            for table in tables:
                if head.lower() in table.get_text().lower():
                    t1 = table
                    data = []
                    for tr in t1.find_all('tr'):
                        row = []
                        for td in tr.find_all('td'):
                            row.append(td.get_text(strip=True))
                        if row:  
                            data.append(row)
                            
                    df = pd.DataFrame(data[1:])  
                    df = df.replace({'-': ''}, regex=True)
                    df = df.replace({'': None, ',': ''}, regex=True)
                    df = df.applymap(lambda x: ' '.join(x.split()) if isinstance(x, str) else x)
                    all_dfs.append(df)

        for head, df in zip(headline, all_dfs): 
            print(head) 
            if head == 'domestic investors':
                df = df.transpose()
                df.iloc[:, 0] = df.iloc[:, 0].fillna(method='ffill')
                df = df.transpose()

                df.iloc[1, [2, 3, 4, 5]] = df.iloc[1, [0, 1, 2, 3]].values
                df.iloc[1, [0, 1]] = [None, None]
                df.iloc[0] = df.apply(lambda x: x[0] + ' ' + x[1] if pd.notna(x[1]) and x[1] != '' else x[0], axis=0)
                df = df.drop(index=1)
                
                df = df.rename(columns=df.iloc[0].str.strip()).drop(0).reset_index(drop=True)
                df = pd.melt(df, id_vars=['Category'], var_name='Funds_Source', value_name='Total_Investment_In_Cr')
                
                df = df[~(df['Category'].str.lower().str.contains('total') & ~df['Category'].str.lower().str.contains('grand total'))]
                
                df = df.rename(columns={'Category': 'Sub_Category'})
                
                df['Category'] = df['Sub_Category'].where(df['Sub_Category'].str.contains(r'Grand Total'), df['Sub_Category'].str.extract(r'^(.*?)\s+AIF\b', expand=False))
                
                df['Sub_Category_Clean'] = df['Sub_Category'].where(df['Sub_Category'].str.contains(r'Grand Total'), df['Sub_Category'].str.extract(r'AIF\s+(.*)', expand=False))
                
                df['Funds_Source_Extract'] = df['Funds_Source'].str.extract(r'from\s+(.*)', expand=False)
                df['Funds_Source_Extract_Type'] = df['Funds_Source_Extract'].str.extract(r'investors\s+(.*)', expand=False)
                df['Funds_Source_Extract'] = df.apply(lambda row: row['Funds_Source_Extract'].replace(row['Funds_Source_Extract_Type'], '').strip() if pd.notna(row['Funds_Source_Extract']) and pd.notna(row['Funds_Source_Extract_Type']) else row['Funds_Source_Extract'], axis=1)

                df.drop(columns=['Funds_Source'], inplace=True)
                df = df.rename(columns={'Funds_Source_Extract': 'Funds_Source'})
                df = df.rename(columns={'Funds_Source_Extract_Type': 'Funds_Source_Type'})
                df['Funds_Source'] = df['Funds_Source'].apply(lambda x: x if len(x.split()) <= 1 else x.title())

                df['Total_Investment_In_Cr'] = pd.to_numeric(df['Total_Investment_In_Cr'])
                df = df[['Category', 'Sub_Category', 'Sub_Category_Clean', 'Funds_Source','Funds_Source_Type','Total_Investment_In_Cr']]
                
                insert_into_db(df, 'SEBI_AIF_DOM_FOREIGN_INVESTMENT_CUMU_QUARTERLY_DATA', date,today)

            elif head == 'listed securities':
                df = df.rename(columns=df.iloc[0].str.strip()).drop(0).reset_index(drop=True)
                
                df = pd.melt(df, id_vars=['Category of AIF'], var_name='Investment_Type', value_name='Total_Investment_In_Cr')
                df = df[~(df['Category of AIF'].str.lower().str.contains('total') & ~df['Category of AIF'].str.lower().str.contains('grand total'))]
                
                df['Category'] = df['Category of AIF'].where(df['Category of AIF'].str.contains(r'Grand Total'), df['Category of AIF'].str.extract(r'^(.*?)\s+AIF\b', expand=False))
                df['Sub_Category_Clean'] = df['Category of AIF'].where(df['Category of AIF'].str.contains(r'Grand Total'), df['Category of AIF'].str.extract(r'AIF\s+(.*)', expand=False))
                
                df['Investment_Type'] = df['Investment_Type'].str.extract(r'Total\s+(.*)')
                df = df.rename(columns={'Category of AIF': 'Sub_Category'})
                df['Total_Investment_In_Cr'] = pd.to_numeric(df['Total_Investment_In_Cr'])
                df = df[['Category', 'Sub_Category', 'Sub_Category_Clean', 'Investment_Type', 'Total_Investment_In_Cr']]
               
                insert_into_db(df, 'SEBI_AIF_NET_INVESTMENT_IN_SECURITIES_CUMU_QUARTERLY_DATA', date,today)

            elif head == 'security receipts':
                df = df.rename(columns=df.iloc[0].str.strip()).drop(0).reset_index(drop=True)
                
                df = pd.melt(df, id_vars=['Category of AIF'], var_name='Investment_Type', value_name='Total_Investment_In_Cr')
                
                df = df[~(df['Category of AIF'].str.lower().str.contains('total') & ~df['Category of AIF'].str.lower().str.contains('grand total'))]
                df['Category'] = df['Category of AIF'].where(df['Category of AIF'].str.contains(r'Grand Total'), df['Category of AIF'].str.extract(r'^(.*?)\s+AIF\b', expand=False))
                df['Sub_Category_Clean'] = df['Category of AIF'].where(df['Category of AIF'].str.contains(r'Grand Total'), df['Category of AIF'].str.extract(r'AIF\s+(.*)', expand=False))
                
                df['Investment_Type'] = df['Investment_Type'].str.replace(r'\s*\(.*\)', '', regex=True)
                
                df = df.rename(columns={'Category of AIF': 'Sub_Category'})

                df['Total_Investment_In_Cr'] = pd.to_numeric(df['Total_Investment_In_Cr'])                
                df = df[['Category', 'Sub_Category', 'Sub_Category_Clean', 'Investment_Type', 'Total_Investment_In_Cr']]
                
                insert_into_db(df, 'SEBI_AIF_NET_INVESTMENT_IN_EQ_DEBT_CUMU_QUARTERLY_DATA', date,today)    

            elif head == 'investments made in startup':
                df = df.rename(columns=df.iloc[0].str.strip()).drop(0).reset_index(drop=True)
                
                df = pd.melt(df, id_vars=['Category of AIF'], var_name='Investment_Type', value_name='Total_Investment_In_Cr')
                df = df[~(df['Category of AIF'].str.lower().str.contains('total') & ~df['Category of AIF'].str.lower().str.contains('grand total'))]
                
                df['Category'] = df['Category of AIF'].where(df['Category of AIF'].str.contains(r'Grand Total'), df['Category of AIF'].str.extract(r'^(.*?)\s+AIF\b', expand=False))
                df['Sub_Category_Clean'] = df['Category of AIF'].where(df['Category of AIF'].str.contains(r'Grand Total'), df['Category of AIF'].str.extract(r'AIF\s+(.*)', expand=False))
                
                df['Investment_Type'] = df['Investment_Type'].str.extract(r'in\s+(.*)')
                df = df.rename(columns={'Category of AIF': 'Sub_Category'})
                df['Total_Investment_In_Cr'] = pd.to_numeric(df['Total_Investment_In_Cr'])
                df = df[['Category', 'Sub_Category', 'Sub_Category_Clean', 'Investment_Type', 'Total_Investment_In_Cr']]
                                
                insert_into_db(df, 'SEBI_AIF_NET_INVESTMENT_IN_STARTUP_MSME_CUMU_QUARTERLY_DATA', date,today)

            elif head == 'all investments':
                df = df.rename(columns=df.iloc[0].str.strip()).drop(0).reset_index(drop=True)
                df = df[~df['Sr. No'].str.lower().str.contains('total')]
                df.drop(columns=['Sr. No'], inplace=True)
                df = df.rename(columns={'Sum of Amount Invested (for all investments including offshore)': 'Total_Investment_In_Cr'})
                df['Total_Investment_In_Cr'] = pd.to_numeric(df['Total_Investment_In_Cr'])
                
                insert_into_db(df, 'SEBI_AIF_NET_INVESTMENT_IN_TOP_10_SECTORS_CUMU_QUARTERLY_DATA', date,today)
                
        #SEBI_tables = ['SEBI_AIF_DOM_FOREIGN_INVESTMENT_CUMU_QUARTERLY_DATA,SEBI_AIF_NET_INVESTMENT_IN_SECURITIES_CUMU_QUARTERLY_DATA,SEBI_AIF_NET_INVESTMENT_IN_EQ_DEBT_CUMU_QUARTERLY_DATA,SEBI_AIF_NET_INVESTMENT_IN_STARTUP_MSME_CUMU_QUARTERLY_DATA,SEBI_AIF_NET_INVESTMENT_IN_TOP_10_SECTORS_CUMU_QUARTERLY_DATA']
        
        SEBI_tables = [ 'SEBI_AIF_DOM_FOREIGN_INVESTMENT_CUMU_QUARTERLY_DATA', 'SEBI_AIF_NET_INVESTMENT_IN_SECURITIES_CUMU_QUARTERLY_DATA',
                        'SEBI_AIF_NET_INVESTMENT_IN_EQ_DEBT_CUMU_QUARTERLY_DATA', 'SEBI_AIF_NET_INVESTMENT_IN_STARTUP_MSME_CUMU_QUARTERLY_DATA',
                        'SEBI_AIF_NET_INVESTMENT_IN_TOP_10_SECTORS_CUMU_QUARTERLY_DATA' ]

        for table in SEBI_tables:
            client1 = ClickHouse_db.db_conn()
            click_max_date = client1.execute(f"select max(Relevant_Date) as Relevant_Date from AdqvestDB.{table}")
            click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])
            query = f'select * from AdqvestDB.{table} where Relevant_Date > "' + click_max_date + '"'
            df = pd.read_sql(query, engine)
            client1.execute(f"INSERT INTO AdqvestDB.{table} VALUES", df.values.tolist())
            print(f'To CH: {len(df)} rows')              
                   
        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)
if(__name__=='__main__'):
    run_program(run_by='manual')   