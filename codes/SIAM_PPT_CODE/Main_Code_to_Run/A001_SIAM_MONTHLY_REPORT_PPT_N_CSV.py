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
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')    #@TODO: Uncomment in prod
import JobLogNew as log  

#common functions
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
def get_clean_chart_variable(my_chart_col,my_legends_col,chart_key_param,no_col_row,chart_pri_axis,chart_source,chart_title,chart_sub_title,function_param):
    my_chart_col= [ast.literal_eval(s) for s in my_chart_col]
    print('1')
    my_legends_col=[ast.literal_eval(s) for s in my_legends_col]
    print('2')
    chart_key_param=[ast.literal_eval(s) for s in chart_key_param]
    print('3')
    no_col_row=[list(map(int, s.strip('[]').split(','))) for s in no_col_row]
    chart_pri_axis=[list(map(float, s.strip('[]').split(','))) for s in chart_pri_axis]
    chart_source=[str(s) for s in chart_source]
    print('4')
    chart_title=[str(s) for s in chart_title]
    chart_sub_title=[str(s) for s in chart_sub_title]
    print('5')
    function_param=[ast.literal_eval(s) for s in function_param]
    return (my_chart_col,my_legends_col,chart_key_param,no_col_row,chart_pri_axis,chart_source,chart_title,chart_sub_title,function_param)

def pre_cleaning_niif_excel(niif_df):
    niif_row_merge = niif_df.groupby('Slide').agg(list).reset_index()
    niif_df.reset_index(drop=True, inplace=True)
    niif_df=niif_excel_cleaning(niif_row_merge)
    niif_df = niif_df.infer_objects()
    return niif_df
def format_date(date):
    if date.month in range(1, 4):
            year_value = date.year
            year_value -= 1
    
    next_year = year_value + 1
    return f'{year_value}-{next_year % 100}'
import adqvest_db                                                 

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


        file_path='C:/Users/Administrator/AdQvestDir/codes/SIAM_PPT_CODE/Common_Function_and_Input_Files/siam_monthly.csv'
        try:
            siam_df = pd.read_csv(file_path, encoding='latin-1')
        except UnicodeDecodeError:
            siam_df = pd.read_csv(file_path, encoding='ISO-8859-1')
        siam_df=pre_cleaning_niif_excel(siam_df)

        # Reading R file
        r = robjects.r
        r.source("C:\\Users\\Administrator\\AdQvestDir\\codes\\SIAM_PPT_CODE\\Common_Function_and_Input_Files\\siam_common_function.R") 


        
        try:
            siam_df_ex = pd.read_csv(file_path, encoding='latin-1')
        except UnicodeDecodeError:
            siam_df_ex = pd.read_csv(file_path, encoding='ISO-8859-1')
        result_df = pd.DataFrame()
        max_relevant_date_df=pd.DataFrame()
        for i in range(siam_df_ex.shape[0]):
            
            column_names = ast.literal_eval(siam_df_ex['Columns'][i])
            chart_title=siam_df_ex['chart_title'][i]
            widget= int(siam_df_ex['widget_id'][i].strip('[]'))
            print('Running code to get data for widget id--->',widget)
            result = r.data_query_clk_pg(widget)
            print(result)
            data_pg,average=niif_get_clean_data_frame(result,column_names,3)
            max_date=max(data_pg['Relevant_Date'])
            formatted_month = max_date.strftime("%b '%y")
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
                    
                
        siam_csv=result_df.pivot_table(index=['Slide_No','Chart_Title','Variable_Column'], columns='Relevant_Date', values='Total').reset_index()
        siam_csv['Unit']="In '000"
        value_columns = list(siam_csv.columns[3:-1])
        print('Value colum---->',value_columns)
        siam_csv[value_columns] = siam_csv[value_columns].apply(lambda x: x.round(2).apply(lambda y: '{:,.2f}'.format(y)), axis=0)

        current_datetime = datetime.now()
        max_date_siam=max(max_relevant_date_df['Max_Relevant_Date'])
        file_date = current_datetime.strftime("%d_%m_%Y_%H_%M")
        file_month_formate=max_date_siam.strftime("%b%y")
        file_name_csv = "C:/Users/Administrator/AdQvestDir/codes/SIAM_PPT_CODE/Output_Files/SIAM_Monthly_Report_"+ file_month_formate+"_" + file_date + ".csv"
        file_name_ppt="C:/Users/Administrator/AdQvestDir/codes/SIAM_PPT_CODE/Output_Files/SIAM_Monthly_Report_" +file_month_formate+"_"+ file_date + ".pptx"
        siam_csv.to_csv(file_name_csv,index=False)
        print('THIS IS THE MAX REL DATE DF')
        print(max_relevant_date_df)
        if (max(max_relevant_date_df['Max_Relevant_Date']))!= (min(max_relevant_date_df['Max_Relevant_Date'])):
            file_name_relevant_date_issue="C:/Users/Administrator/AdQvestDir/codes/SIAM_PPT_CODE/Output_Files/SIAM_Monthly_Report_Relevant_Date_Issue_" + file_date + ".csv"
            max_relevant_date_df.to_csv(file_name_relevant_date_issue,index=False)
        cover_page_date = max_date_siam.strftime("%b '%y")
        val='Monthly Data Overview'
        r.cover_slide(val,cover_page_date)
        for i in range(siam_df.shape[0]):
            QUATER='False'
            if 'q2' in file_path:
                QUATER='True'
            # if any(pd.isnull(siam_df['widget_id'][i])):
            #     print('null value')
            #     thanks_chart=r.thanks_slide()
            
            widget_ids,column_names,chart_category=niif_get_clean_variable(siam_df['widget_id'][i],siam_df['Columns'][i],siam_df['chart_category'][i])
            data_final_dict = {}
            index=1
            for widget, columns in zip(widget_ids, column_names):
                merge_data=pd.DataFrame()
                flag=1
                for j in range(len(widget)):
                    print('Running code to get data for widget id--->',widget[j])
                    result = r.data_query_clk_pg(widget[j])
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
            my_chart_col,my_legends_col,chart_key_param,no_col_row,chart_pri_axis,chart_source,chart_title,slide_heading,function_param=get_clean_chart_variable(siam_df['my_chart_col'][i],siam_df['my_legends_col'][i],
                                                                siam_df['chart_key_param'][i],siam_df['no_col_row'][i],siam_df['chart_pri_axis'][i],
                                                            siam_df['chart_source'][i],siam_df['chart_title'][i],siam_df['slide_heading'][i],siam_df['function_param'][i])
            chart=r.call_function(r_json_data,siam_df['Slide'][i],siam_df['chart_creat_type'][i],
                                    siam_df['chart_type'][i],list(siam_df['chart_category'][i]),
                                    my_chart_col,my_legends_col,list(siam_df['x_axis_interval'][i]),
                                    chart_key_param,no_col_row,chart_pri_axis,
                                    list(siam_df['graph_lim'][i]),chart_source,chart_title,
                                    slide_heading,function_param,QUATER,file_name_ppt)

        print('DONE CREATING PPT AND CSV')
        print(max_relevant_date_df)
        log.job_end_log(table_name,job_start_time, no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')

