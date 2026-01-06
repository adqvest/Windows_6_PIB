import pandas as pd
import datetime as datetime
from pytz import timezone
import warnings
warnings.filterwarnings('ignore')
import os
import re
import time
import io
#os.chdir(r'D:\Adqvest\ncdex')
import sys
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import JobLogNew as log
import adqvest_db
import dbfunctions
import pdftoexcel

from adqvest_robotstxt import Robots

robot = Robots(__file__)

from selenium import webdriver
from selenium.webdriver.common.by import By

def run_program(run_by='Adqvest_Bot', py_file_name=None):

    # os.chdir('C:/Users/Administrator/AdQvestDir/')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    #add table name
    table_name = 'ICRA_DAILY_FILES_RAW_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        repeat=0
        check_df = pd.read_sql("SELECT * FROM ICRA_DAILY_FILES_LINKS where Data_Scrap_Status is null and Comments is null and Download_Status='Yes'",engine)
        if check_df.shape[0]>0:

            while(repeat<5):


                os.chdir(r"C:\Users\Administrator\AdQvestDir\ICRA_DOWNLOAD_FOLDER")

                links = pd.read_sql("SELECT * FROM ICRA_DAILY_FILES_LINKS where Data_Scrap_Status is null and Comments is null and Download_Status='Yes' order by Relevant_Date",engine)
                #links = pd.read_sql("SELECT * FROM ICRA_DAILY_FILES_LINKS where Data_Scrap_Status is null and comments is not null and Relevant_Date>'2020-10-31' and Relevant_Date<'2021-01-01' order by Relevant_Date desc",engine)
                #links = pd.read_sql("SELECT * FROM ICRA_DAILY_FILES_LINKS where Data_Scrap_Status is null and comments is not null and Relevant_Date='2021-02-01' order by Relevant_Date desc",engine)

                os.chdir(r"C:\Users\Administrator\AdQvestDir\ICRA_DOWNLOAD_FOLDER")

                print(links.shape[0])
                for a,values in links.iterrows():
                    try:
                        link=values['Links']
                        name=values['Company_Name']
                        date=values['Relevant_Date']
                        file_name= values['File_Name']

                        path = file_name.replace('pdf','xlsx')

                        os.chdir(r"C:\Users\Administrator\AdQvestDir\ICRA_DOWNLOAD_FOLDER")

                        try:
                            os.remove(file_name)
                            os.remove(path)
                        except:
                            pass

                        #no_of_ping += 1
                        #ro.r('download.file("'+url+'", destfile = "/home/ubuntu/crisil_data/' + row['Rating_File_Name'] + '", mode="wb")')
                        #Added by Nidhi on 20 Dec 2023
                        dbfunctions.from_s3bucket('ICRA',"C:/Users/Administrator/AdQvestDir/ICRA_DOWNLOAD_FOLDER/",file_name) #Addition ends
                        path = "C:/Users/Administrator/AdQvestDir/ICRA_DOWNLOAD_FOLDER/" + file_name
                        path_to_pdf = "C:\\Users\\Administrator\\AdQvestDir\\ICRA_DOWNLOAD_FOLDER"
                        path_to_excel = pdftoexcel.pdftoexcel(path_to_pdf,file_name.replace('.pdf',''))
                        # s3.Bucket('adqvests3bucket').download_file('ICRA/' + file_name, file_name)
                        print(link)
                        os.chdir(r"C:\Users\Administrator\AdQvestDir\ICRA_DOWNLOAD_FOLDER")
                        with open(path_to_excel, "rb") as f:
                            sheet = io.BytesIO(f.read())
                        xls = pd.ExcelFile(sheet,engine='openpyxl')
                        sheet_names = xls.sheet_names
                        df=pd.DataFrame()

                        for i in range(len(sheet_names)):
                            try:
                                with open(path, "rb") as f:
                                        file_io_obj = io.BytesIO(f.read())
                                df = pd.read_excel(file_io_obj, engine='openpyxl', sheet_name=sheet_names[i], header = None)
                            except:
                              df = pd.read_excel(path_to_excel, sheet_name=sheet_names[i], header=None)

                            if df.shape[0]>1:
                                break
                            continue

                        os.remove(file_name)
                        os.remove(path_to_excel)

                        df.dropna(inplace=True,how='all',axis=1)
                        df=df.replace(r'\n',' ',regex=True)
                        df=df.replace(r'#','',regex=True)
                        df=df.replace(r',','',regex=True)
                        df=df.replace(r'\^','',regex=True)
                        df=df.replace(r'\*','',regex=True)
                        df.reset_index(drop=True,inplace=True)



                        for col in df.columns:
                            if df[col].str.lower().str.contains('trust name').any() or df[col].str.lower().str.contains('transaction name').any():
                                req_col_1=col
                                break

                        for col in df.columns:
                            if df[col].str.lower().str.contains('instrument').any():
                                req_col_2=col
                                break

                        try:
                            df=df.loc[:,req_col_1:]
                        except:
                            df=df.loc[:,req_col_2:]

                        try:
                            try:
                                start_index=df[df.loc[:,req_col_1].str.lower().str.contains('trust name',na=False)==True].index[0]
                            except:
                                start_index=df[df.loc[:,req_col_1].str.lower().str.contains('transaction name',na=False)==True].index[0]
                            try:
                                end_index=df[df.loc[:,req_col_1].str.lower().str.contains('instrument details',na=False)==True].index[0]
                            except:
                                end_index=df[df.loc[:,req_col_1].str.contains('Total',na=False)==True].index[0]

                            df=df.loc[:,req_col_1+1:]

                        except:
                            start_index=df[df.loc[:,req_col_2].str.lower().str.contains('instrument',na=False)==True].index[0]
                            try:
                                end_index=df[df.loc[:,req_col_2].str.contains('Total',na=False)==True].index[0]
                            except:
                                end_index=df[df.loc[:,req_col_2].str.lower().str.contains('instrument details',na=False)==True].index[0]

                        df.columns=df.loc[start_index]
                        df=df.loc[start_index+1:end_index-1]
                        df=df.dropna(axis=1,how='all')
                        df=df.dropna(axis=0,how='all')



                        if df.shape[1]==4:
                            df.columns=['Facilities','Previous_Rated_Amount_Cr','Current_Rated_Amount_Cr','Ratings']
                        elif df.shape[1]==3 and 'Previous' not in df.columns:
                            df.columns=['Facilities','Current_Rated_Amount_Cr','Ratings']
                        elif df.shape[1]==5:
                            df.drop(df.columns[2], axis = 1, inplace = True)
                            df.columns=['Facilities','Previous_Rated_Amount_Cr','Current_Rated_Amount_Cr','Ratings']

                        na_index_facilities = df[df['Facilities'].isnull()].index.tolist()
                        na_index_ratings = df[df['Ratings'].isnull()].index.tolist()

                        if na_index_facilities != []:
                            for i in na_index_facilities:
                                df['Facilities'][i] = df['Facilities'][i - 1]
                        else:
                            pass

                        if na_index_ratings != []:
                            for i in na_index_ratings:
                                df['Ratings'][i] = df['Ratings'][i - 1]
                        else:
                            pass

                        final_df=df.copy()
                        final_df['Company']=name
                        final_df=final_df.astype('str')
                        final_df['Relevant_Date']=date
                        final_df['Relevant_Date']=pd.to_datetime(final_df['Relevant_Date'])
                        final_df['Runtime']=datetime.datetime.now()
                        final_df['Links']=link
                        final_df.reset_index(drop=True,inplace=True)
                        #final_df=final_df[['Company','Links','Facilities','Previous_Rated_Amount_Cr','Current_Rated_Amount_Cr','Ratings','Relevant_Date','Runtime']]

                        if final_df.shape[0] == 0:
                            raise Exception()
                        else:
                            pass

                        # driver.find_element(By.CLASS_NAME,"downloader__extra").click()
                        engine = adqvest_db.db_conn()
                        final_df.to_sql(name='ICRA_DAILY_RATINGS_RAW_DATA',con=engine,if_exists='append',index=False)
                        print(final_df)


                        connection.execute("update ICRA_DAILY_FILES_LINKS set Data_Scrap_Status = 'Yes',Comments=null where Links = '"+link+"'")

                        if no_of_ping!=0:
                            no_of_ping-=1


                    except:

                        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
                        error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
                        print(error_msg)

                        # try:
                        #     driver.close()
                        # except:
                        #     pass

                        connection.execute("update ICRA_DAILY_FILES_LINKS set Data_Scrap_Status = 'No',Comments='Error' where Links = '"+link+"'")

                        continue

                # driver.close()
                repeat+=1

        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)



if(__name__=='__main__'):
    run_program(run_by='manual')
