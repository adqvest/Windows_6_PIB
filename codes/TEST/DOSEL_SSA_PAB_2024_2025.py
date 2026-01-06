import warnings
import datetime
warnings.filterwarnings('ignore')
import camelot
import sys
import os
import pandas as pd
import datetime
from pytz import timezone
import numpy as np
import requests
from bs4 import BeautifulSoup
import camelot
from PyPDF2 import PdfFileReader
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')

import adqvest_db
import adqvest_s3
import ClickHouse_db
engine = adqvest_db.db_conn()
connection = engine.connect()
client = ClickHouse_db.db_conn()
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days

# headers = {
#     'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
#     'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
#     'Cache-Control': 'no-cache',
#     'Connection': 'keep-alive',
#     # 'Cookie': 'SSESS60bba1149e4b7894111a35bbee510e4a=oIQ1-BXo1LmohfszDm0HS6GOMYdaCt-GCEmEH2JELE4; _ga_CKVXR7NHNH=GS1.1.1714120229.1.1.1714120588.0.0.0; _gid=GA1.3.1046420337.1715081532; _ga_91S7T17PZW=GS1.1.1715149843.15.1.1715152932.0.0.0; _ga=GA1.1.644578156.1713948823',
#     'Pragma': 'no-cache',
#     'Sec-Fetch-Dest': 'document',
#     'Sec-Fetch-Mode': 'navigate',
#     'Sec-Fetch-Site': 'none',
#     'Sec-Fetch-User': '?1',
#     'Upgrade-Insecure-Requests': '1',
#     'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
#     'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
#     'sec-ch-ua-mobile': '?0',
#     'sec-ch-ua-platform': '"macOS"',
# }

url = 'https://dsel.education.gov.in/pab-minutes'
r = requests.get(url)
soup = BeautifulSoup(r.content)
year_dict = {i.text.replace(' ', ''):i['value'] for i in soup.find('select', {'id': 'edit-field-financial-year-target-id'}).find_all('option')[1:-1]}
for k in list(year_dict.keys()):
    if int(k.split('-')[0]) < 2016:
        del year_dict[k]
    else:
        pass
years = []
pg_urls = []    
for k, v in year_dict.items():
    pg_urls.append(f'https://dsel.education.gov.in/pab-minutes?field_financial_year_target_id={v}&field_states_target_id=All')
    years.append(k)

pdf_links = []
fy_yrs = []
states = []
for yr, url in zip(years, pg_urls):
    print(yr)
    r = requests.get(url)
    soup = BeautifulSoup(r.content)
    link_ele = soup.find_all('tr', class_ = 'views-table')
    for ele in link_ele:
        print(ele.find('a')['href'])
        print(ele.find('td', class_='views-field views-field-field-states').text)
        pdf_links.append('https://dsel.education.gov.in' + ele.find('a')['href'])
        fy_yrs.append(yr)
        states.append(ele.find('td', class_='views-field views-field-field-states').text)
links_df = pd.DataFrame({'State':states, 'Link':pdf_links, 'Financial_Year':fy_yrs})
links_df = links_df.sort_values(['State'], ascending=False)
links_df = links_df.reset_index(drop = True)

df_act_rel = pd.DataFrame()
df_est_bud = pd.DataFrame()
df_cum_prog = pd.DataFrame()
df_com_brkup = pd.DataFrame()

links = links_df[links_df.Financial_Year.str.lower().str.contains('2024-2025')]
links = links[~(links.State.str.lower().str.contains('uttarakhand|uttar pradesh'))]
links = links.reset_index(drop = True)

for row, vals in links.iloc[:, :].iterrows():
    print(vals['Link'])
    print(vals['State'])
    print(vals['Financial_Year'])
    rel_date = datetime.date(int(vals.Financial_Year.split('-')[-1]), 3, 31)
    runtime = datetime.datetime.now(india_time)
    
    query = f'Insert into DOSEL_SAMAGRA_SHIKSHA_DATA_COLLECTION_STATUS values ("{vals.State.strip()}", null, null, null, null, null, null, null, null, "{vals.Link}","{rel_date}", "{runtime}")'
    connection.execute(query)
    connection.execute('commit')
    tables = []
    try:
        tables = camelot.read_pdf(vals.Link, pages = '1-50', strip_text = '\n', shift_text = [])
    except:
        try:
            tables = camelot.read_pdf(vals.Link, pages = 'all', strip_text = '\n', shift_text = [])
        except Exception as E:
            error = str(E)
            query = f'UPDATE DOSEL_SAMAGRA_SHIKSHA_DATA_COLLECTION_STATUS SET Est_Budget = "{error}", Est_Budget_Txt = "{error}",Act_Release = "{error}",Act_Release_Txt = "{error}",Cumu_Progress = "{error}",Cumu_Progress_Txt = "{error}",Comp_Breakup = "{error}",Comp_Breakup_Txt = "{error}" where State = "{vals.State.strip()}" and Relevant_Date = "{rel_date}"'
            connection.execute(query)
            connection.execute('commit')

    if len(tables) != 0:
        for i in range(len(tables)):
            # print(tables[i].df)
            if 'Head' in tables[i].df.iloc[0, 0]:
                print(i, tables[i].df)
                est_budget = tables[i].df
            # elif 'Head' in tables[i].df.iloc[1, 0]:
            #     print(i, tables[i].df)
            #     est_budget = tables[i].df
            #     # break
            elif tables[i].df.iloc[0, 0] == 'Component':
                #print(i, tables[i].df)
                act_rel = tables[i].df
                # break
            elif tables[i].df.iloc[:, 0].str.contains('Scheme Name').any():
                print(i, tables[i].df)
                cum_prog = tables[i].df

            elif tables[i].df.iloc[:, 0].str.contains('Major Component').any():
                print(i, tables[i].df)
                com_brkup = tables[i].df
                break

        # DOSEL_SAMAGRA_SHIKSHA_TOTAL_ESTIMATED_BUDGET_YEARLY_DATA
        try:  

            est_budget.columns = ['Head','Spill_Over', 'Non_Recurring_Fresh', 'Recurring_Fresh', 'Total_Fresh','Grand_Total']
            est_budget['State'] = vals.State.strip()
            st_idx = est_budget[est_budget.iloc[:, 0].str.lower().str.contains('elementar')].index
            est_budget = est_budget.iloc[st_idx[0]:, :]
            est_budget = est_budget.reset_index(drop = True)
            est_budget['Relevant_Date'] = datetime.date(int(vals.Financial_Year.split('-')[-1]), 3, 31)
            est_budget['Runtime'] = runtime
            est_budget['State'] = vals.State.strip()
            print(est_budget)
            df_est_bud = pd.concat([df_est_bud, est_budget])
            
            try:
                est_budget.to_sql('DOSEL_SAMAGRA_SHIKSHA_TOTAL_ESTIMATED_BUDGET_YEARLY_DATA', if_exists='append', index=False, con=engine)
                query = f'UPDATE DOSEL_SAMAGRA_SHIKSHA_DATA_COLLECTION_STATUS SET Est_Budget = "Succeeded" where State = "{vals.State.strip()}" and Relevant_Date = "{rel_date}"'
                connection.execute(query)
                connection.execute('commit')
                
            except Exception as E:
                error = str(E)
                query_1 = f'UPDATE DOSEL_SAMAGRA_SHIKSHA_DATA_COLLECTION_STATUS SET Est_Budget = "{error}" where State = "{vals.State.strip()}" and Relevant_Date = "{rel_date}"'
                connection.execute(query)
                connection.execute('commit')
                
                try:
                    est_budget.to_sql('DOSEL_SAMAGRA_SHIKSHA_TOTAL_ESTIMATED_BUDGET_YEARLY_DATA_TEXT', if_exists='append', index=False, con=engine)
                    query_2 = f'UPDATE DOSEL_SAMAGRA_SHIKSHA_DATA_COLLECTION_STATUS SET Est_Budget_Txt = "Succeeded" where State = "{vals.State.strip()}" and Relevant_Date = "{rel_date}"'
                    connection.execute(query_2)
                    connection.execute('commit')
                    
                except Exception as e:
                    error = str(e)
                    query_2 = f'UPDATE DOSEL_SAMAGRA_SHIKSHA_DATA_COLLECTION_STATUS SET Est_Budget_Txt = "{error}" where State = "{vals.State.strip()}" and Relevant_Date = "{rel_date}"'
                    connection.execute(query_2)
                    connection.execute('commit')        
            
        except Exception as E:
            error = str(E)
            print(error)
            query = f'UPDATE DOSEL_SAMAGRA_SHIKSHA_DATA_COLLECTION_STATUS SET Est_Budget = "{error}", Est_Budget_Txt = "{error}" where State = "{vals.State.strip()}" and Relevant_Date = "{rel_date}"'
            connection.execute(query)
            connection.execute('commit')
            try:
              del est_budget
            except:
              pass   
          
         # DOSEL_SAMAGRA_SHIKSHA_ACT_RELEASE_BY_GOI_YEARLY_DATA 
        try:
            act_rel.columns = ['Component','Elementary_Education','Secondary_Education','Teacher_Education', 'Total']
            try:
                st_idx = act_rel[act_rel.iloc[:, 0].str.lower().str.contains('recurring')].index
                act_rel = act_rel.iloc[st_idx[0]:, :]
            except:
                act_rel = act_rel[~(act_rel.iloc[:, 0].str.lower().str.contains('component'))]
            act_rel = act_rel.reset_index(drop = True)
            act_rel['State'] = vals.State.strip()
            act_rel = act_rel.reset_index(drop = True)
            act_rel['Relevant_Date'] = datetime.date(int(vals.Financial_Year.split('-')[-1]), 3, 31)
            act_rel['Runtime'] = runtime
            act_rel['State'] = vals.State.strip()
            print(act_rel)
            df_act_rel = pd.concat([df_act_rel, act_rel])
            try:
                act_rel.to_sql('DOSEL_SAMAGRA_SHIKSHA_ACT_RELEASE_BY_GOI_YEARLY_DATA', if_exists='append', index=False, con=engine)
                query = f'UPDATE DOSEL_SAMAGRA_SHIKSHA_DATA_COLLECTION_STATUS SET Act_Release = "Succeeded" where State = "{vals.State.strip()}" and Relevant_Date = "{rel_date}"'
                connection.execute(query)
                connection.execute('commit')
                
            except Exception as E:
                error = str(E)
                query_1 = f'UPDATE DOSEL_SAMAGRA_SHIKSHA_DATA_COLLECTION_STATUS SET Act_Release = "{error}" where State = "{vals.State.strip()}" and Relevant_Date = "{rel_date}"'
                connection.execute(query)
                connection.execute('commit')
                
                try:
                    est_budget.to_sql('DOSEL_SAMAGRA_SHIKSHA_ACT_RELEASE_BY_GOI_YEARLY_DATA_TEXT', if_exists='append', index=False, con=engine)
                    query_2 = f'UPDATE DOSEL_SAMAGRA_SHIKSHA_DATA_COLLECTION_STATUS SET Act_Release_Txt = "Succeeded" where State = "{vals.State.strip()}" and Relevant_Date = "{rel_date}"'
                    connection.execute(query_2)
                    connection.execute('commit')
                    
                except Exception as e:
                    error = str(e)
                    query_2 = f'UPDATE DOSEL_SAMAGRA_SHIKSHA_DATA_COLLECTION_STATUS SET Act_Release_Txt = "{error}" where State = "{vals.State.strip()}" and Relevant_Date = "{rel_date}"'
                    connection.execute(query_2)
                    connection.execute('commit') 
        except Exception as E:
            error = str(E)
            print(error)
            query = f'UPDATE DOSEL_SAMAGRA_SHIKSHA_DATA_COLLECTION_STATUS SET Act_Release = "{error}", Act_Release_Txt = "{error}" where State = "{vals.State.strip()}" and Relevant_Date = "{rel_date}"'
            connection.execute(query)
            connection.execute('commit')
            try:
              del act_rel 
            except:
              pass
          
        # # DOSEL_SAMAGRA_SHIKSHA_CUMULATIVE_PROGRESS_YEARLY_DATA  
        # try:    
        #     cum_prog.columns = ['Scheme','Scheme_Budget_Approved_Cumulative','Cumulative_Progress_Since_Inception', 'Spill_Over']
        #     st_idx = cum_prog[cum_prog.iloc[:, 0].str.contains('Elemen')].index[0]
        #     cum_prog = cum_prog.iloc[st_idx:, :]
        #     cum_prog = cum_prog.apply(pd.to_numeric, errors = 'ignore')
        #     cum_prog['Relevant_Date'] = datetime.date(int(vals.Financial_Year.split('-')[-1]), 3, 31)
        #     com_brkup['Runtime'] = runtime
        #     cum_prog['State'] = vals.State.strip()
        #     df_cum_prog = pd.concat([df_cum_prog, cum_prog])
        #     print(cum_prog)
        #     try:
        #         cum_prog.to_sql('DOSEL_SAMAGRA_SHIKSHA_CUMULATIVE_PROGRESS_YEARLY_DATA', if_exists='append', index=False, con=engine)
        #         query = f'UPDATE DOSEL_SAMAGRA_SHIKSHA_DATA_COLLECTION_STATUS SET Cumu_Progress = "Succeeded" where State = "{vals.State.strip()}" and Relevant_Date = "{rel_date}"'
        #         connection.execute(query)
        #         connection.execute('commit')
                
        #     except Exception as E:
        #         error = str(E)
        #         query_1 = f'UPDATE DOSEL_SAMAGRA_SHIKSHA_DATA_COLLECTION_STATUS SET Cumu_Progress = "{error}" where State = "{vals.State.strip()}" and Relevant_Date = "{rel_date}"'
        #         connection.execute(query)
        #         connection.execute('commit')
                
        #         try:
        #             cum_prog.to_sql('DOSEL_SAMAGRA_SHIKSHA_CUMULATIVE_PROGRESS_YEARLY_DATA_TEXT', if_exists='append', index=False, con=engine)
        #             query_2 = f'UPDATE DOSEL_SAMAGRA_SHIKSHA_DATA_COLLECTION_STATUS SET Cumu_Progress_Txt = "Succeeded" where State = "{vals.State.strip()}" and Relevant_Date = "{rel_date}"'
        #             connection.execute(query_2)
        #             connection.execute('commit')
                    
        #         except Exception as e:
        #             error = str(e)
        #             query_2 = f'UPDATE DOSEL_SAMAGRA_SHIKSHA_DATA_COLLECTION_STATUS SET Cumu_Progress_Txt = "{error}" where State = "{vals.State.strip()}" and Relevant_Date = "{rel_date}"'
        #             connection.execute(query_2)
        #             connection.execute('commit') 
        #     del st_idx
        # except Exception as E:
        #     error = str(E)
        #     print(error)
        #     query = f'UPDATE DOSEL_SAMAGRA_SHIKSHA_DATA_COLLECTION_STATUS SET Cumu_Progress = "{error}", Cumu_Progress_Txt = "{error}" where State = "{vals.State.strip()}" and Relevant_Date = "{rel_date}"'
        #     connection.execute(query)
        #     connection.execute('commit')
        #     try:
        #       del cum_prog
        #     except:
        #       pass
        
        # # DOSEL_SAMAGRA_SHIKSHA_COMP_BREAKUP_YEARLY_DATA
        # try:
        #     com_brkup.columns = ['Component','Scheme_Budget_Approved_Cumulative','Cumulative_Progress_Since_Inception', 'Spill_Over']
        #     try:
        #         st_idx = com_brkup[com_brkup.iloc[:, 0].str.contains('Access')].index[0]
        #     except:
        #         st_idx = com_brkup[com_brkup.iloc[:, 1].str.contains('Access')].index[0]
        #     com_brkup = com_brkup.iloc[st_idx:, :]
        #     com_brkup['Component'] = np.where([com_brkup.iloc[:, 0] == ''],com_brkup.iloc[:, 1].apply(lambda x: re.split('\d', x)[0].strip()),com_brkup.iloc[:, 0])[0]
        #     com_brkup.iloc[:, 1] = com_brkup.iloc[:, 1].apply(lambda x: re.split(' +', x)[-1]).values
        #     com_brkup['Relevant_Date'] = datetime.date(int(vals.Financial_Year.split('-')[-1]), 3, 31)
        #     com_brkup['State'] = vals.State.strip()
        #     df_com_brkup = pd.concat([df_com_brkup, com_brkup])
        #     com_brkup['Runtime'] = runtime
        #     print(com_brkup)
        #     del st_idx
        #     com_brkup.to_sql('DOSEL_SAMAGRA_SHIKSHA_COMP_BREAKUP_YEARLY_DATA', if_exists='append', index=False, con=engine)
        # except:
        #     try:
        #         com_brkup = tables[i].df
        #         com_brkup.columns = ['Component','Scheme_Budget_Approved_Cumulative','Cumulative_Progress_Since_Inception', 'Spill_Over']
        #         try:
        #             st_idx = com_brkup[com_brkup.iloc[:, 0].str.contains('Access')].index[0]
        #         except:
        #             st_idx = com_brkup[com_brkup.iloc[:, 1].str.contains('Access')].index[0]
        #         com_brkup = com_brkup.iloc[st_idx:, :]
        #         com_brkup['Relevant_Date'] = datetime.date(int(vals.Financial_Year.split('-')[-1]), 3, 31)
        #         com_brkup['Runtime'] = runtime
        #         com_brkup['State'] = vals.State.strip()
        #         # df_com_brkup = pd.concat([df_com_brkup, com_brkup])
        #         print(com_brkup)
        #         del st_idx
        #         try:
        #             com_brkup.to_sql('DOSEL_SAMAGRA_SHIKSHA_COMP_BREAKUP_YEARLY_DATA', if_exists='append', index=False, con=engine)
        #             query = f'UPDATE DOSEL_SAMAGRA_SHIKSHA_DATA_COLLECTION_STATUS SET Comp_Breakup = "Succeeded" where State = "{vals.State.strip()}" and Relevant_Date = "{rel_date}"'
        #             connection.execute(query)
        #             connection.execute('commit')
                
        #         except Exception as E:
        #             error = str(E)
        #             query_1 = f'UPDATE DOSEL_SAMAGRA_SHIKSHA_DATA_COLLECTION_STATUS SET Comp_Breakup = "{error}" where State = "{vals.State.strip()}" and Relevant_Date = "{rel_date}"'
        #             connection.execute(query)
        #             connection.execute('commit')
                    
        #             try:
        #                 com_brkup.to_sql('DOSEL_SAMAGRA_SHIKSHA_COMP_BREAKUP_YEARLY_DATA_TEXT', if_exists='append', index=False, con=engine)
        #                 query_2 = f'UPDATE DOSEL_SAMAGRA_SHIKSHA_DATA_COLLECTION_STATUS SET Comp_Breakup_Txt = "Succeeded" where State = "{vals.State.strip()}" and Relevant_Date = "{rel_date}"'
        #                 connection.execute(query_2)
        #                 connection.execute('commit')
                        
        #             except Exception as e:
        #                 error = str(e)
        #                 query_2 = f'UPDATE DOSEL_SAMAGRA_SHIKSHA_DATA_COLLECTION_STATUS SET Comp_Breakup_Txt = "{error}" where State = "{vals.State.strip()}" and Relevant_Date = "{rel_date}"'
        #                 connection.execute(query_2)
        #                 connection.execute('commit') 
        #     except Exception as E:
        #         error = str(E)
        #         print(error)
        #         query = f'UPDATE DOSEL_SAMAGRA_SHIKSHA_DATA_COLLECTION_STATUS SET Comp_Breakup = "{error}", Comp_Breakup_Txt = "{error}" where State = "{vals.State.strip()}" and Relevant_Date = "{rel_date}"'
        #         connection.execute(query)
        #         connection.execute('commit')
        #         try:
        #             del com_brkup
        #         except:
        #             pass
