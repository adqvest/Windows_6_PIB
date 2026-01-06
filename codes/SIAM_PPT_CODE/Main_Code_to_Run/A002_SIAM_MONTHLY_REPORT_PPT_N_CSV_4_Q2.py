
import pandas as pd
from rpy2 import robjects
from rpy2.robjects import pandas2ri
import rpy2.robjects as robjects
#import pandas as pd
from rpy2.robjects.packages import importr
# import feather
# base = importr('base')
# tidyverse = importr('tidyverse')
from rpy2 import robjects
from rpy2.robjects import pandas2ri
#import pandas as pd
from pandas.tseries.offsets import MonthEnd
#import pandas as pd
from rpy2.robjects import pandas2ri
from rpy2.robjects import r
pandas2ri.activate()
#import pandas as pd
from pytz import timezone
import json
import ast
from datetime import datetime, timedelta
import rpy2.robjects as ro
import sys
import os
import re
import time
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')    #@TODO: Uncomment in prod
import JobLogNew as log  
import adqvest_db                                                 

# COMMON FUNCTION
def r_dataframe_to_pandas(r_data_frame):
    data = {}
    for col_name in r_data_frame.colnames:
        print(type(col_name))
        print(r_data_frame)
        if col_name == 'Relevant_Date':
            print('in if block')
            print(type(r_data_frame))
            r_dates = r_data_frame.rx2(str(col_name))
            print(r_dates)
            python_dates = [datetime(1970, 1, 1) + timedelta(days=int(x)) for x in r_dates]
            data[col_name] = python_dates
        else:
            data[col_name] = list(r_data_frame.rx2(str(col_name)))
    print('Trying to print data')
    print(data)
    return pd.DataFrame(data)
def data_clean(data1):
    data1["Relevant_Date"] = pd.to_datetime(data1["Relevant_Date"])
    data1 = data1.sort_values(by=["Relevant_Date"])
    data1["Month"] = data1["Relevant_Date"].dt.to_period("M").dt.to_timestamp()
    data1["value_y_left"] = data1.iloc[:len(data1), 2]
    data1["category"] = data1.iloc[:len(data1), 1]
    data_final = data1[["Relevant_Date", "value_y_left", "Month", "category"]]
    data_final = data_final.apply(lambda x: x.replace([float('inf'), float('-inf')], pd.NA))
    data_final = data_final.sort_values(by=["Relevant_Date"])
    default_start_date = pd.to_datetime("2013-04-01")
    prev_month = (pd.to_datetime("today") - MonthEnd(1)).replace(day=1)
    data_final = data_final[data_final["Relevant_Date"] >= default_start_date]
    return data_final
def niif_excel_cleaning(niif_df):
    niif_df['Slide'] = pd.to_numeric(niif_df['Slide'], errors='coerce')
    return niif_df
def niif_get_clean_variable(widget_ids,column_names,chart_category):
    widget_ids = [list(map(int, s.strip('[]').split(','))) for s in widget_ids]
    print(widget_ids)
    column_names = [ast.literal_eval(s) for s in column_names]
    print(column_names)
    chart_category=list(chart_category)
    return widget_ids,column_names,chart_category
def niif_get_clean_data_frame(result,columns,div):
    data_frame = result.rx2('data_frame')
    print('Inside niif clean data frame')
    print(data_frame)
    new_column_names=[0,1]
    new_column_names[0],new_column_names[1] = columns[0],columns[1]
    print(new_column_names)
    data_frame.colnames = new_column_names
    print(data_frame)
    data_frame = r_dataframe_to_pandas(data_frame)
    relevant_date_col = 'Relevant_Date'
    for col in data_frame.columns:
        if col != relevant_date_col:
            data_frame[col] = data_frame[col] / 10**div
        average=data_frame[col].mean()
    print('In data frame clean function')
    print(data_frame)
    return data_frame,average
def dictionary_to_json(data_final_dict):
    json_data = {}
    for key, df in data_final_dict.items():
        json_data[key] = df.to_json(orient='records')
    r_json_data = ro.ListVector(json_data)
    return r_json_data
def get_clean_chart_variable(my_chart_col,my_legends_col,chart_key_param,no_col_row,chart_pri_axis,chart_source,chart_title,chart_sub_title,function_param,special_case):
    my_chart_col= [ast.literal_eval(s) for s in my_chart_col]
    my_legends_col=[ast.literal_eval(s) for s in my_legends_col]
    chart_key_param=[ast.literal_eval(s) for s in chart_key_param]
    no_col_row=[list(map(int, s.strip('[]').split(','))) for s in no_col_row]
    chart_pri_axis=[list(map(float, s.strip('[]').split(','))) for s in chart_pri_axis]
    chart_source=[str(s) for s in chart_source]
    chart_title=[str(s) for s in chart_title]
    chart_sub_title=[str(s) for s in chart_sub_title]
    function_param=[ast.literal_eval(s) for s in function_param]
    special_case=[str(s) for s in special_case]
    return (my_chart_col,my_legends_col,chart_key_param,no_col_row,chart_pri_axis,chart_source,chart_title,chart_sub_title,function_param,special_case)

def pre_cleaning_niif_excel(niif_df):
    niif_row_merge = niif_df.groupby('Slide').agg(list).reset_index()
    niif_df.reset_index(drop=True, inplace=True)
    niif_df=niif_excel_cleaning(niif_row_merge)
    niif_df = niif_df.infer_objects()
    return niif_df
def format_date(date):
    actual_year = date.year
    next_year = actual_year + 1
    return f'{actual_year}-{next_year % 100}'
def table_preprocessing(data, frequency_normalizer='', period=3, unit='NA',
                               variable='NA', sector='NA', calculate_gr=False,
                               divisor=0, rounder=0, fy_format=False,special_case=False):

    data['Relevant_Date'] = pd.to_datetime(data['Relevant_Date'])
    max_date = data['Relevant_Date'].max()
    min_date_org = data['Relevant_Date'].min()
    print('min_date_org:',min_date_org)
    start_date = min_date_org
    end_date = max_date
    print('end_date:',end_date)
    all_dates = pd.date_range(start=start_date, end=end_date, freq='A' if frequency_normalizer == 'year' else frequency_normalizer)
    dates_df = pd.DataFrame({'Relevant_Date': all_dates})
    print('dates_df:',dates_df)
    if start_date != end_date:
        print('start_date not equal')
        dates_df['Value'] = pd.NA
        dates_df = dates_df[dates_df['Relevant_Date'] < min_date_org]
        data = pd.concat([data, dates_df], ignore_index=True)
        data = data.sort_values('Relevant_Date')
        data['Value'].fillna(0, inplace=True)
        print('start_date not equal',data)

    else:
        print('start_date is equal')
        data = pd.merge(dates_df, data, on='Relevant_Date', how='outer')
        dates_df['Value'] = pd.NA
        data['Value'] += 1
        data = pd.concat([data, dates_df], ignore_index=True)
        data = data.sort_values('Relevant_Date')
        data['Value'].fillna(0, inplace=True)
        data = data.groupby('Relevant_Date')['Value'].sum().reset_index()
        data['Value'] -= 1
        data['Value'].replace(-1, pd.NA, inplace=True)
        print('start date is equal:',data)

    data = data.sort_values('Relevant_Date')
    f1 = lambda x: x[0] + pd.DateOffset(years=1) if x[0].month >= 4 else x[0]
    f2 = lambda x: x[0] - pd.DateOffset(months=3)
    f3 = lambda x: (x[0] + pd.DateOffset(years=1)).strftime("%Y-03-31") if x[0].month >= 4 else x[0].strftime("%Y-03-31")

    if frequency_normalizer == 'quarter':
        data["Fy_year"] = data[['Relevant_Date']].apply(f1, axis=1)
        data["Qtr_mon"] = data[['Relevant_Date']].apply(f2, axis=1)
        data['qtr'] = data['Qtr_mon'].dt.to_period("Q")
        data['year'] = data['Fy_year'].dt.strftime('%y')
        data['Relevant_Date'] = data['qtr'].astype(str) + 'FY' + data['year']
        data = data[['Relevant_Date', 'Value']]

    elif frequency_normalizer == 'month':
        data['Relevant_Date'] = data['Relevant_Date'].dt.strftime('%b-%y')

    elif fy_format:
        data["Fy_year"] = data[['Relevant_Date']].apply(f3, axis=1)
        data = data.groupby('Fy_year')['Value'].sum().reset_index()
        data['year'] = data['Relevant_Date'].dt.strftime('%y')
        data['Relevant_Date'] = 'FY' + data['year'] + ' '
        data = data[['Relevant_Date', 'Value']]

    else:
        data['Relevant_Date'] = pd.to_datetime(data['Relevant_Date'], errors='coerce')
        data['Current_Year'] = data['Relevant_Date'].dt.year
        print('special_case------------------------------->',special_case)
        print(type(special_case))
        if special_case!=True:
            print('---------------INSIDE IF-------------')
            data['Next_Year'] = data['Current_Year'] + 1
            data['Relevant_Date'] = data.apply(lambda row: f"{row['Current_Year']}-{str(row['Next_Year'])[2:]}", axis=1)
        else:
            data['Next_Year'] = data['Current_Year'] + 1
            data['Relevant_Date'] = data.apply(lambda row: f"CY - {str(row['Current_Year'])[2:]}", axis=1)
        data = data.drop(['Current_Year', 'Next_Year'], axis=1)
    data = data.dropna(subset=['Value'])
    print(data)
    data['Value'] = pd.to_numeric(data['Value'], errors='coerce')
    data['Value'] = data['Value'].round(2)
    data['Value'] = data['Value'].astype(float)
    if calculate_gr:
        growth_rate = round(((data['Value'].iloc[-1] / data['Value'].iloc[0]) ** (1 / 5) - 1) * 100, 1)
        data = pd.concat([data, pd.DataFrame({'Relevant_Date': ['Growth (% yoy)'], 'Value': [growth_rate]})])
    print('This is data inside table clean function')
    print(data)
    growth_value = data.loc[data['Relevant_Date'] == 'Growth (% yoy)', 'Value'].values[0]
    data['Value'] = pd.to_numeric(data['Value'], errors='coerce')
    data['Value'] = data['Value'].round(0)
    data['Value'] = data['Value'].astype(float)
    data['Value'] = data['Value'].astype(int).apply(lambda x: format(x, ','))
    
    if unit != 'NA':
        data['Units'] = unit
        data = data[['Units', 'Relevant_Date', 'Value']]

    if variable != 'NA':
        data['Variable'] = variable
        data = data[['Variable', 'Relevant_Date', 'Value']]

    if sector != 'NA':
        data['Sector'] = sector
        data = data[['Sector', 'Relevant_Date', 'Value']]

    data = data.transpose()
    data.columns = data.iloc[0]
    data = data[1:]
    data['Growth (% yoy)']= growth_value
    return data
def get_clean_table_df(siam_df_table):
    siam_df = siam_df_table[siam_df_table['chart_creat_type'].apply(len) > 1]
    print('------------------------Trying to print data inside get clean table df-------------------------------')
    print(siam_df)
    data_final_dict_table = {}
    index=1
    print(siam_df)
    slide_heading_table=[]
    for i in range(siam_df.shape[0]):
        widget_ids,column_names,chart_category=niif_get_clean_variable(siam_df['widget_id'][i],siam_df['Columns'][i],siam_df['chart_category'][i])
        
        table_siam=pd.DataFrame()
        for widget, columns in zip(widget_ids, column_names):
            for j in range(len(widget)):
                print('Running code to get data for widget id inside table function--->',widget[j])
                result = r.data_query_clk_pg(widget[j])
                print(result)
                div=3
                special_case=siam_df['special_case'][i][j]
                print('special_case----------->',special_case)
                data_pg,average=niif_get_clean_data_frame(result,columns,div)
                variable=list(data_pg.columns)[1]
                print('variable:',variable)
                data_pg.columns=['Relevant_Date','Value']
                max_date_format=max(data_pg['Relevant_Date'])
                max_date_format = max_date_format.strftime("%b '%y")
                siam_table=table_preprocessing(data_pg, frequency_normalizer='year', period=5, unit='NA',
                                   variable=variable, sector='Category', calculate_gr=True,
                                   divisor=10^3, rounder=1, fy_format=False,special_case=siam_df['special_case'][i][j])
                print('This is for table for siam')
                siam_table=siam_table.reset_index(drop=True)
                siam_table.columns = siam_table.iloc[0]
                print(siam_table.columns)
                print(siam_table)
                siam_table=siam_table.drop(siam_table.index[0]).reset_index(drop=True)
                siam_table = siam_table.drop(['Growth (% yoy)'], axis=1)
                siam_table.rename(columns={siam_table.columns[-1]: 'CAGR %'}, inplace=True)
                print(siam_table)
                siam_table['Category']=variable
                table_siam=table_siam.append(siam_table)
                print(table_siam)
        slide_heading=siam_df['slide_heading'][i]
        slide_heading_table+=[slide_heading[i]]
        data_final_dict_table[f'table{index}'] = table_siam
        index+=1
        print('This is the index',i)
    return data_final_dict_table,slide_heading_table,max_date_format
def pre_cleaning_niif_excel_table(niif_df):
    grouped_data = niif_df.groupby(['Slide', 'chart']).agg(list)
    
    return grouped_data.reset_index()


# MAIN CODES STARTS FROM HERE
def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.now(india_time)
    days       = timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.now(india_time)
    table_name = 'SIAM_MONTHLY_PPT_REPORT'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = '11_AM_WINDOWS_SERVER_2_SCHEDULER'
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        # Integrate PYTHON AND R
        r = robjects.r
        r.source("C:\\Users\\Administrator\\AdQvestDir\\codes\\SIAM_PPT_CODE\\Common_Function_and_Input_Files\\siam_common_function_quater.R") 

        #ALL FILES AND COMMON VARIABLE
        file="C:/Users/Administrator/AdQvestDir/codes/SIAM_PPT_CODE/Common_Function_and_Input_Files/siam_q2.csv"
        print(file)
        # Integrate PYTHON AND R
        r = robjects.r
        r.source("C:\\Users\\Administrator\\AdQvestDir\\codes\\SIAM_PPT_CODE\\Common_Function_and_Input_Files\\siam_common_function_quater.R") 

        if 'q' in file.lower():
            try:
                siam_df_ex = pd.read_csv(file, encoding='latin-1')
            except UnicodeDecodeError:
                siam_df_ex = pd.read_csv(file, encoding='ISO-8859-1')
            result_df = pd.DataFrame()
            max_relevant_date_df=pd.DataFrame()
            for i in range(siam_df_ex.shape[0]):
                try:
                    if siam_df_ex['chart_creat_type'][i]!='table':
                        column_names = ast.literal_eval(siam_df_ex['Columns'][i])
                        chart_title=siam_df_ex['chart_title'][i]
                        widget=numeric_value = int(siam_df_ex['widget_id'][i].strip('[]'))
                        print('Running code to get data for widget id--->',widget)
                        result = r.data_query_clk_pg(widget)
                        print(result)
                        data_pg,average=niif_get_clean_data_frame(result,column_names,3)
                        max_date=max(data_pg['Relevant_Date'])
                        formatted_month = max_date.strftime("%b '%y")
                        formatted_month_file=max_date.strftime("%b_%y")
                        max_date_data = {'Widget_id': [widget],
                                'Max_Relevant_Date': [max_date]}

                        max_rel_df = pd.DataFrame(max_date_data)
                        data_pg['Relevant_Date'] = data_pg['Relevant_Date'].apply(format_date)
                        data_pg['Variable_Column'] = data_pg.columns[1]
                        print(data_pg)
                        data_pg.columns=['Relevant_Date','Total','Variable_Column']
                        data_pg['Slide_No']=siam_df_ex['Slide'][i]
                        data_pg['Chart_Title']=chart_title+' '+formatted_month
                        result_df = pd.concat([result_df, data_pg[['Slide_No','Chart_Title','Relevant_Date','Total','Variable_Column']]], ignore_index=True)
                        max_relevant_date_df = pd.concat([max_relevant_date_df, max_rel_df], ignore_index=True)
                        print(result_df)
                    else:
                        print('This is table widget ')
                except:
                    print('thanks slide')
        print(result_df)
        df_melted=result_df.pivot_table(index=['Slide_No','Chart_Title','Variable_Column'], columns='Relevant_Date', values='Total').reset_index()
        df_melted['Unit']="In '000"
        value_columns = ['2018-19','2019-20','2020-21','2021-22','2022-23','2023-24']
        df_melted[value_columns] = df_melted[value_columns].apply(lambda x: x.round(2).apply(lambda y: '{:,.2f}'.format(y)), axis=0)
        current_datetime = datetime.now()
        max_date_siam=max(max_relevant_date_df['Max_Relevant_Date'])
        formatted_date = current_datetime.strftime("%d_%m_%Y_%H_%M")

        file_name_csv = "C:/Users/Administrator/AdQvestDir/codes/SIAM_PPT_CODE/Output_Files/SIAM_Q2_"+formatted_month_file+"_" + formatted_date + ".csv"
        file_name_ppt = "C:/Users/Administrator/AdQvestDir/codes/SIAM_PPT_CODE/Output_Files/SIAM_Q2_"+formatted_month_file+"_" + formatted_date + ".pptx"
        df_melted.to_csv(file_name_csv,index=False)
        time.sleep(10)
        
        sys.setrecursionlimit(10000)
        print('DONE CSV FOR FILE---------------->',file_name_csv)
        try:
            siam_df = pd.read_csv(file, encoding='latin-1')
        except UnicodeDecodeError:
            siam_df = pd.read_csv(file, encoding='ISO-8859-1')
        siam_df=pre_cleaning_niif_excel(siam_df)
        date="(Q2 - H1)"
        year_ppt=format_date(max_date_siam)
        val=str(year_ppt)+" " +"Data Overview"
        r.cover_slide(val,date)

        for i in range(siam_df.shape[0]):
            QUATER='False'
            if 'q' in file.lower():
                QUATER='True'
            if any(pd.isnull(siam_df['widget_id'][i])):
                print('null value')
                thanks_chart=r.thanks_slide()
            else:
                widget_ids,column_names,chart_category=niif_get_clean_variable(siam_df['widget_id'][i],siam_df['Columns'][i],siam_df['chart_category'][i])
                data_final_dict = {}
                index=1
                print(siam_df['chart_creat_type'][i][1])
                if siam_df['chart_creat_type'][i][1]=='table':
                    try:
                        siam_df_table = pd.read_csv(file, encoding='latin-1')
                    except UnicodeDecodeError:
                        siam_df_table = pd.read_csv(file, encoding='ISO-8859-1')
                    siam_df_table=pre_cleaning_niif_excel_table(siam_df_table)
                    print(siam_df_table)
                    table_df,slide_heading_table,max_date_format=get_clean_table_df(siam_df_table)
                    r_json_data_table = dictionary_to_json(table_df)
                    chart_title=siam_df['chart_title'][i]
                    chart=r.call_function_table(r_json_data_table,slide_heading_table,chart_title,QUATER
                                                ,max_date_format,file_name_ppt)
                else:
                    for widget, columns in zip(widget_ids, column_names):
                        merge_data=pd.DataFrame()
                        flag=1
                        for j in range(len(widget)):
                            print('Running code to get data for widget id--->',widget[j])
                            result = r.data_query_clk_pg(int(widget[j]))
                            print(result)
                            div=siam_df['Division'][i][j]
                            data_pg,average=niif_get_clean_data_frame(result,columns,div)
                            print(average)
                            if average >2:
                                if j ==0:
                                    merge_data=data_pg
                                else:
                                    merge_data=pd.merge(merge_data,data_pg,on='Relevant_Date')
                            else:
                                flag=0
                        if flag==1:
                            merge_data = pd.melt(merge_data, id_vars=["Relevant_Date"])
                            merge_data = merge_data.dropna()
                            print(merge_data)
                            data_final=data_clean(merge_data)
                            print('DATA FINAL')
                            print(data_final)
                            data_final_dict[f'data{index}'] = data_final
                            index+=1
                            print('-----------Imported data from Postgres-------')

                    r_json_data = dictionary_to_json(data_final_dict)
                    my_chart_col,my_legends_col,chart_key_param,no_col_row,chart_pri_axis,chart_source,chart_title,slide_heading,function_param,special_case=get_clean_chart_variable(siam_df['my_chart_col'][i],siam_df['my_legends_col'][i],
                                                                      siam_df['chart_key_param'][i],siam_df['no_col_row'][i],siam_df['chart_pri_axis'][i],
                                                                    siam_df['chart_source'][i],siam_df['chart_title'][i],
                                                                    siam_df['slide_heading'][i],siam_df['function_param'][i],siam_df['special_case'][i])
                    chart=r.call_function(r_json_data,siam_df['Slide'][i],siam_df['chart_creat_type'][i],
                                          siam_df['chart_type'][i],list(siam_df['chart_category'][i]),
                                          my_chart_col,my_legends_col,list(siam_df['x_axis_interval'][i]),
                                          chart_key_param,no_col_row,chart_pri_axis,
                                          list(siam_df['graph_lim'][i]),chart_source,chart_title,slide_heading,
                                          function_param,QUATER,special_case,file_name_ppt,max_date_format)

        print('DONE FOR THE FILES---------------------------->',file_name_ppt)
        log.job_end_log(table_name,job_start_time, no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
