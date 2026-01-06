from selenium import webdriver
from selenium.webdriver.common.by import By
import time
from bs4 import BeautifulSoup
import pandas as pd
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import datetime
from pytz import timezone
import sys
import re
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
engine = adqvest_db.db_conn()

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    #os.chdir('/home/ubuntu/AdQvestDir')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'DOSEL_NAS_DISTRICT_WISE_PERFORMANCE_YEARLY_DATA_KPMG'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        chrome_driver = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
        download_file_path = r"C:\Users\Administrator\AdQvestDir\codes\One time run"
        # chrome_driver = r"D:\Adqvest\chrome_path\chromedriver.exe"
        # download_file_path = r"D:\Adqvest\Junk"
        prefs = {
            "download.default_directory": download_file_path,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "plugins.always_open_pdf_externally": True
            }

        options = webdriver.ChromeOptions()
        options.add_experimental_option('prefs', prefs)
        driver = webdriver.Chrome(executable_path=chrome_driver, chrome_options=options)

        url = "https://nas.gov.in/report-card/nas-2021"
        driver.get(url)
        html_content = driver.page_source
        soup = BeautifulSoup(html_content, 'html.parser')

        time.sleep(1)
        list_buttons = soup.find_all('a', class_='national_state_list')
        final_df = pd.DataFrame()
        html_content = driver.page_source
        soup = BeautifulSoup(html_content, 'html.parser')
        time.sleep(2)

        district_links = soup.find_all('a', class_='districts')
        print(len(district_links))
        all_district_dataframes = []

        for district in district_links:
            if district.has_attr('onclick'):
                driver.execute_script(district['onclick'])
                time.sleep(1)
                district_name = district.get_text(strip=True)
                print(district_name)
                class_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "class5-tab")))
                class_button.click()
                time.sleep(1)
                
                html_content = driver.page_source
                soup = BeautifulSoup(html_content, 'html.parser')

                learning_tab_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "performance-tab")))
                learning_tab_button.click()
                html_content = driver.page_source
                soup = BeautifulSoup(html_content, 'html.parser')
                span_element = soup.find('span', class_='nav-link t')

                if span_element:
                    text = span_element.get_text(strip=True)
                    state_name = text.split(' > ')[0]
                    html_content = driver.page_source
                    soup = BeautifulSoup(html_content, 'html.parser')

                    all_table_data = [] 
                    table_div = soup.find('div',id = 'performance_class5')
                    table_main = table_div.find('div', class_='per-table')
                    time.sleep(1)
                    html_content = driver.page_source
                    soup = BeautifulSoup(html_content, 'html.parser')
                    if table_main:
                        headers = [th.text.strip() for th in table_main.find('thead').find_all('th')]
                        rows = table_main.find('tbody').find_all('tr')

                        for row in rows:
                            time.sleep(2)
                            html_content = driver.page_source
                            soup = BeautifulSoup(html_content, 'html.parser')
                            row_data = [th.text.strip() for th in row.find_all(['th', 'td'])]
                            row_dict = {
                                'State': state_name,
                                'District': district_name,
                                'Class': 'Class 5',
                                'Performance Level': row_data[0],
                                'Language': row_data[1],
                                'Mathematics': row_data[2],
                                'EVS': row_data[3],
                            }

                            all_table_data.append(row_dict)

                    result_dataframe = pd.DataFrame(all_table_data)
                    all_district_dataframes.append(result_dataframe)

        final_df = pd.concat(all_district_dataframes, ignore_index=True)

        melted_df = pd.melt(final_df, id_vars=['State', 'District', 'Class', 'Performance Level'], 
                            value_vars=['Language', 'Mathematics', 'EVS'], 
                            var_name='Subject', value_name='Percentage')

        performance_mapping = {'BELOW BASIC': 'Below_Basic_Pct', 'BASIC': 'Basic_Pct', 'PROFICIENT': 'Proficient_Pct', 'ADVANCED': 'Advanced_Pct'}
        melted_df['Performance Level'] = melted_df['Performance Level'].map(performance_mapping)
        melted_df['Percentage'] = pd.to_numeric(melted_df['Percentage'], errors='coerce')

        final_df_performance = melted_df.pivot_table(index=['State', 'District', 'Class', 'Subject'], 
                                             columns='Performance Level', 
                                             values='Percentage').reset_index()

        column_order = ['State', 'District', 'Class', 'Subject', 'Below_Basic_Pct', 'Basic_Pct', 'Proficient_Pct', 'Advanced_Pct']
        final_df_performance = final_df_performance[column_order]
        final_df_performance.columns = [f'{col}' for col in final_df_performance.columns]

        india_time = timezone('Asia/Kolkata')
        today      = datetime.datetime.now(india_time)
        final_df_performance['Relevant_Date'] = '2022-03-31'
        final_df_performance['Runtime'] = datetime.datetime.now()

        final_df_performance['Relevant_Date'] = pd.to_datetime(final_df_performance['Relevant_Date']).dt.date
        final_df_performance[['State', 'District', 'Class']] = final_df_performance[['State', 'District', 'Class']].astype(str)
        print(final_df_performance)

        final_df_performance.to_sql(name='DOSEL_NAS_DISTRICT_WISE_PERFORMANCE_YEARLY_DATA_KPMG',con=engine,if_exists='append',index=False)

        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')




