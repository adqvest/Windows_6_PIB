import sqlalchemy
import pandas as pd
import os
import requests
import re
import datetime as datetime
from pytz import timezone
from datetime import timedelta
import calendar
import sys
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import ntpath
import JobLogNew as log
from selenium import webdriver
import time
from selenium.webdriver.common.action_chains import ActionChains
from pytz import timezone
import openpyxl
import dates
import chromedriver
import dbfunctions
import pdftoexcel
import logfunctions
from adqvest_robotstxt import Robots
robot = Robots(__file__)



def data_collection(table_name,table_name2,table_name3):
    file_available = False
    query = f'select max(Relevant_Date) as RD from {table_name};'
    sql_data = dbfunctions.read_sql(query)
    last_added_date = sql_data['RD'][0]

    nhai_roads_query = f'select * from {table_name2} order by Relevant_Date desc limit 1;'
    nhai_roads_data = dbfunctions.read_sql(nhai_roads_query)

    nhai_roads_data_latest = pd.DataFrame(columns = nhai_roads_data.columns)
    

    if (last_added_date.month != 12):
        last_month = last_added_date.month
        month = calendar.month_name[last_month + 1]
        year = last_added_date.year
    else:
        month = calendar.month_name[1]
        year = last_added_date.year + 1

    print("Looking for file dated - ",month,"-",year)


    driver = chromedriver.get_driver(r"C:\Users\Administrator\Junk")

    driver.get('https://nhai.gov.in/#/project-informations-mis')
    driver.set_page_load_timeout(30)
    robot.add_link('https://nhai.gov.in/#/project-informations-mis')
    time.sleep(5)
    driver.find_element("xpath","//a[contains(text(),'English')]").click()
    ActionChains(driver).move_by_offset(10 , 10).click().perform()
    time.sleep(5)
    driver.maximize_window()
    driver.execute_script("window.scrollBy(0, 50)")
    time.sleep(5)
    if len(driver.find_elements("xpath","//a[@class='taggetBlankLink' and @title = 'NHAI performance report for " +month+"-"+str(year)+"']")) != 0:
        link = driver.find_element("xpath","//a[@class='taggetBlankLink' and @title = 'NHAI performance report for " +month+"-"+str(year)+"']")
        file_url = link.get_attribute('href')
        file_name = link.get_attribute('title')
        if file_name != '':
            file_available = True
        response = requests.get(file_url, timeout = 60)
        download_file_path = r"C:\Users\Administrator\Junk"
        open(f'{download_file_path}\{file_name}.pdf', 'wb').write(response.content)
        dbfunctions.to_s3bucket(f'{download_file_path}\{file_name}.pdf','NHAI_PERFORMANCE_REPORT_MONTHLY/PDF/')
    else:
        print("File Not Found")


    if file_available == True:
        print("File available")
        path = pdftoexcel.pdftoexcel(download_file_path,file_name)
        dbfunctions.to_s3bucket(path,'NHAI_PERFORMANCE_REPORT_MONTHLY/EXCEL/')
        wb=openpyxl.load_workbook(path)
        sheets = wb.sheetnames

        df_sheet1 = pd.DataFrame()
        df_sheet1 = pd.read_excel(path, engine='openpyxl',index_col=None,sheet_name = sheets[0])
        df_sheet2 = pd.DataFrame()
        df_sheet2 = pd.read_excel(path, engine='openpyxl',index_col=None,sheet_name = sheets[1])


        Award_Reports = df_sheet1[df_sheet1["Unnamed: 0"].astype(str).str.contains('Report')]
        Award_Report_1 = Award_Reports.iloc[:,0].index[1]
        Award_Report_2 = Award_Reports.iloc[:,0].index[2]
        Award_Report_3 = Award_Reports.iloc[:,0].index[3]


        if len(df_sheet1[df_sheet1["Unnamed: 0"].astype(str).str.contains('Report 1')]) == 1:
            construct_reports = 1 #entire reports on next sheet
        else:
            construct_reports = 0 #half of Report 1 on next sheet

        if construct_reports == 0:
            Construction_Reports = df_sheet1[df_sheet1["Unnamed: 0"].astype(str).str.contains('Report')]
            Construction_Report_1 = Construction_Reports.iloc[:,0].index[5]
            column_name = df_sheet2.columns[0]
            Construction_Reports = df_sheet2[df_sheet2[column_name].astype(str).str.contains('Report')]
            Construction_Report_2 = Construction_Reports.iloc[:,0].index[0]
        else:
            Construction_Reports = df_sheet2[df_sheet2["Unnamed: 0"].astype(str).str.contains('Report')]
            Construction_Report_1 = Construction_Reports.iloc[:,0].index[0]
            Construction_Report_2 = Construction_Reports.iloc[:,0].index[1]

        df_sheet1.iloc[6].ffill(axis=0, inplace=True, limit = 2)
        df_sheet1 = df_sheet1.fillna('')
        df_sheet1.columns = (df_sheet1.iloc[6] + '_' + df_sheet1.iloc[7])


        df_state_wise_award_summary = df_sheet1.iloc[Award_Report_1+1:Award_Report_2,:]
        df_state_wise_award_summary = df_state_wise_award_summary.iloc[2:].reset_index(drop=True)


        df_mode_of_implementation_award = df_sheet1.iloc[Award_Report_2+1:Award_Report_3,:]
        df_mode_of_implementation_award.rename(columns = {'State_':'Mode of Implementation'}, inplace = True)
        df_mode_of_implementation_award = df_mode_of_implementation_award.reset_index(drop=True)
        df_mode_of_implementation_award = df_mode_of_implementation_award.iloc[2:].reset_index(drop=True)

        df_type_of_corr_award = df_sheet1.iloc[Award_Report_3+1:Construction_Report_1-4,:5]
        df_type_of_corr_award.reset_index(drop=True,inplace=True)
        df_type_of_corr_award.iloc[0].ffill(axis=0, inplace=True, limit = 2)
        df_type_of_corr_award.columns = (df_type_of_corr_award.iloc[0] + '_' + df_type_of_corr_award.iloc[1])
        df_type_of_corr_award = df_type_of_corr_award.iloc[2:].reset_index(drop=True)

        if construct_reports == 0:
            report_1_half = df_sheet1.iloc[Construction_Report_1+1:,:6]
            report_1_half = report_1_half.reset_index(drop=True)
            report_1_half.columns = report_1_half.iloc[0]
            report_1_half = report_1_half.iloc[1:].reset_index(drop=True)
            report_2_half = (df_sheet2.T.reset_index().T.reset_index(drop=True))
            report_2_half.columns = report_1_half.columns
            report_2_half = report_2_half.iloc[:Construction_Report_2+1,:]
            frames = [report_1_half, report_2_half]
            df_state_wise_construction = pd.concat(frames)
            df_state_wise_construction = df_state_wise_construction.fillna('')
            df_state_wise_construction['Total']= df_state_wise_construction.iloc[:, 2:5].sum(axis=1)

            df_type_of_corr_construction = df_sheet2.iloc[Construction_Report_2+1:,:3]
            df_type_of_corr_construction = df_type_of_corr_construction.reset_index(drop=True)
            df_type_of_corr_construction.columns = df_type_of_corr_construction.iloc[0]
            df_type_of_corr_construction = df_type_of_corr_construction.iloc[1:].reset_index(drop=True)
            df_type_of_corr_construction = df_type_of_corr_construction.fillna('')

        else:
            df_state_wise_construction = df_sheet2.iloc[Construction_Report_1+1:Construction_Report_2,:6]
            df_state_wise_construction = df_state_wise_construction.reset_index(drop=True)
            df_state_wise_construction.columns = df_state_wise_construction.iloc[0]
            df_state_wise_construction = df_state_wise_construction.iloc[1:].reset_index(drop=True)
            df_state_wise_construction = df_state_wise_construction.fillna('')
            df_state_wise_construction['Total']= df_state_wise_construction.iloc[:, 2:6].sum(axis=1)

            df_type_of_corr_construction = df_sheet2.iloc[Construction_Report_2+1:,:3]
            df_type_of_corr_construction = df_type_of_corr_construction.reset_index(drop=True)
            df_type_of_corr_construction.columns = df_type_of_corr_construction.iloc[0]
            df_type_of_corr_construction = df_type_of_corr_construction.iloc[1:].reset_index(drop=True)
            df_type_of_corr_construction = df_type_of_corr_construction.fillna('')



        #merge & change column names
        merged = [df_state_wise_award_summary.iloc[:-1,1:],df_state_wise_construction.iloc[:-1,2:]]
        df_to_sql = pd.concat(merged, axis=1, ignore_index=False)
        df_to_sql.columns.values[0] = "State"
        df_to_sql.columns.values[1] = "B/R_NHDP_Cumulative_Awarded_No"
        df_to_sql.columns.values[2] = "B/R_NHDP_Cumulative_Awarded_Length_Km"
        df_to_sql.columns.values[3] = "B/R_NHDP_Cumulative_Awarded_Cost_Cr"
        df_to_sql.columns.values[4] = "Other_Scheme_Cumulative_Awarded_NH(O)_No"
        df_to_sql.columns.values[5] = "Other_Scheme_Cumulative_Awarded_NH(O)_Length_Km"
        df_to_sql.columns.values[6] = "Other_Scheme_Cumulative_Awarded_NH(O)_Cost_Cr"
        df_to_sql.columns.values[7] = "Total_Cumulative_Awarded_No"
        df_to_sql.columns.values[8] = "Total_Cumulative_Awarded_Length_Km"
        df_to_sql.columns.values[9] = "Total_Cumulative_Awarded_Cost_Cr"
        df_to_sql.columns.values[10] = "2_Lane_Cumulative_Length_Constructed_Km"
        df_to_sql.columns.values[11] = "4_Lane_Cumulative_Length_Constructed_Km"
        df_to_sql.columns.values[12] = "6_Lane_Cumulative_Length_Constructed_Km"
        df_to_sql.columns.values[13] = "8_Lane_Cumulative_Length_Constructed_Km"
        df_to_sql.columns.values[14] = "Total_Cumulative_Constructed_Length_Km"
        df_to_sql['Relevant_Date'] = dates.last_day_of_month(last_added_date)
        df_to_sql['Runtime'] = datetime.datetime.now()
        
        #drop total columns
        df_to_sql.drop(df_to_sql.columns[[7, 8, 9,14]], axis=1, inplace=True)

        nhai_roads_data_latest.at[0, 'Cumulative_Road_Constructed_KM'] = df_type_of_corr_construction.iloc[-1,-1]
        nhai_roads_data_latest.at[0, 'Cumulative_Road_Awarded_KM'] = df_type_of_corr_award.iloc[-1,-1]
        nhai_roads_data_latest.at[0, 'Relevant_Date'] = dates.last_day_of_month(last_added_date)
        nhai_roads_data_latest.at[0, 'Runtime'] = datetime.datetime.now()
        if(month != 'April'):
            nhai_roads_data_latest.at[0, 'Road_Constructed_KM'] = nhai_roads_data_latest.at[0, 'Cumulative_Road_Constructed_KM'] - nhai_roads_data.at[0, 'Cumulative_Road_Constructed_KM']
            nhai_roads_data_latest.at[0, 'Road_Awarded_KM'] = nhai_roads_data_latest.at[0, 'Cumulative_Road_Awarded_KM']-nhai_roads_data.at[0, 'Cumulative_Road_Awarded_KM']
        else:
            nhai_roads_data_latest.at[0, 'Road_Constructed_KM'] = nhai_roads_data_latest.at[0, 'Cumulative_Road_Constructed_KM']
            nhai_roads_data_latest.at[0, 'Road_Awarded_KM'] = nhai_roads_data_latest.at[0, 'Cumulative_Road_Awarded_KM']

        #write to sql/ch
        dbfunctions.to_sqldb(df_to_sql,table_name)
        dbfunctions.to_sqldb(nhai_roads_data_latest,table_name2)

        #Cumulative to monthly
        if(month != 4):
            relevant_date_query = 'select Relevant_Date FROM {table_name} group by Relevant_Date order by Relevant_Date desc;'
            rel_date = dbfunctions.read_sql(relevant_date_query)
            latest_rel_date = rel_date['Relevant_Date'][0]
            prev_rel_date = rel_date['Relevant_Date'][1]

            query_prev = f'select * from {table_name} where Relevant_Date = "{prev_rel_date}";'
            prev_data = dbfunctions.read_sql(query_prev)

            query_latest = f'select * from {table_name} where Relevant_Date = "{latest_rel_date}";'
            latest_data = dbfunctions.read_sql(query_latest)

            cumu_data = pd.DataFrame(columns = prev_data.columns)
            cumu_data = latest_data.iloc[:,1:-2].subtract(prev_data.iloc[:,1:-2], fill_value=0) 
            cumu_data['State'] = prev_data.State
            cumu_data.insert(0, 'State', cumu_data.pop('State'))
            cumu_data.columns.values[0] = "State"
            cumu_data.columns.values[1] = "B/R_NHDP_Awarded_No"
            cumu_data.columns.values[2] = "B/R_NHDP_Awarded_Length_Km"
            cumu_data.columns.values[3] = "B/R_NHDP_Awarded_Cost_Cr"
            cumu_data.columns.values[4] = "Other_Scheme_Awarded_NH(O)_No"
            cumu_data.columns.values[5] = "Other_Scheme_Awarded_NH(O)_Length_Km"
            cumu_data.columns.values[6] = "Other_Scheme_Awarded_NH(O)_Cost_Cr"
            cumu_data.columns.values[7] = "2_Lane_Length_Km"
            cumu_data.columns.values[8] = "4_Lane_Length_Km"
            cumu_data.columns.values[9] = "6_Lane_Length_Km"
            cumu_data.columns.values[10] = "8_Lane_Length_Km"
            cumu_data['Relevant_Date'] = latest_data['Relevant_Date']
            cumu_data['Runtime'] = latest_data['Runtime']
            dbfunctions.to_sqldb(cumu_data,table_name3)

        else:
            dbfunctions.to_sqldb(df_to_sql,table_name3)
        
    else:
        print("File not found!")


def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir')
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'NHAI_STATEWISE_ROADS_CONSTRUCTED_AND_AWARDED_CUMU_MONTHLY_DATA'
    table_name2 = 'NHAI_ROADS_CONSTRUCTED_AND_AWARDED_MONTHLY_DATA'
    table_name3 = 'NHAI_STATEWISE_ROADS_CONSTRUCTED_AND_AWARDED_MONTHLY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        logfunctions.start_log(run_by,table_name,py_file_name,job_start_time,scheduler)
        session = requests.Session()
        
        data_collection(table_name,table_name2,table_name3)

        log.job_end_log(table_name,job_start_time, no_of_ping)

    except:
        logfunctions.error_log(table_name,job_start_time, no_of_ping)


if(__name__=='__main__'):
    run_program(run_by='manual')
