# -*- coding: utf-8 -*-
"""
Created on Fri Aug 28 13:37:30 2020

@author: abhis
"""

import pandas as pd
import requests
import sqlalchemy
import pandas as pd
from pandas.io import sql
import os
import requests
from bs4 import BeautifulSoup
from dateutil import parser
import re
import datetime as datetime
import requests
import io
import numpy as np
import PyPDF2
from pytz import timezone
import sys
import warnings
import codecs
warnings.filterwarnings('ignore')
import numpy as np
import csv
import calendar
import pdb
import json
import calendar
import time
import os
from dateutil import parser
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import JobLogNew as log
import adqvest_db

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
    table_name = 'CAG_SCHOOLS_VILLAGE_LEVEL_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)


        st_1 = ["PASHCHIM CHAMPARAN","PURBA CHAMPARAN","SITAMARHI"]
        st_2 = ["CHATRA","PALAMU","PASHCHIMI SINGHBHUM"]
        st_3 = ["KORAPUT","MAYURBHANJ","NABARANGPUR"]
        st_4 = ["ALLAHABAD","GONDA","PRATAPGARH"]
        url1 = "http://src.udiseplus.gov.in/locateSchool/state/6"
        url2 = "http://src.udiseplus.gov.in/locateSchool/getDistrict"
        url3 = "http://src.udiseplus.gov.in/locateSchool/getBlock?"
        main = []
        main.append(st_1)
        main.append(st_2)
        main.append(st_3)
        main.append(st_4)
        r = requests.get(url1)
        res = r.text.strip('][').split(', ')
        src = u"[%s]" % res[0]
        states = json.loads(src)
        states = [{k: states[k] for k in ('stateName','stateId', 'yearId')} for states in states]
        reqd1 = []
        for st in states:
            for key, value in st.items():
                if ((value == 'Bihar') or (value == 'Jharkhand') or (value == 'Odisha') or (value == 'Uttar Pradesh')):
                    reqd1.append(st)
        dist_level = []
        for vals in reqd1:
            #for st in reqd1:
            r = requests.post(url2,data=vals)
            res = r.text.strip('][').split(', ')
            src = u"[%s]" % res[0]
            district = json.loads(src)
            district = [{k: dist[k] for k in ('districtName','districtId', 'yearId')} for dist in district]
            dist_level.append(district)

        reqd2 = []
        for i in range(len(main)):
            for vals in dist_level:
              sub_reqd_2 = []
              for a in vals:
                for key, value in a.items():
                    print(value)
                    if ((value == main[i][0]) or (value == main[i][1]) or (value == main[i][2])):
                        sub_reqd_2.append(a)
              reqd2.append(sub_reqd_2)


        reqd2 = [x for x in reqd2 if x != []]

        reqd3 = []
        for ents in reqd2:
            sub_reqd_3 = []
            for values in ents:
                try:
                    print(values['districtId'],values['yearId'])
                    main_url = "http://src.udiseplus.gov.in/locateSchool/getBlock?districtId="+str(values['districtId'])+"&yearId="+str(values['yearId'])+""
                    print(main_url)
                    time.sleep(1)
                    r = requests.get(main_url)
                    no_of_ping += 1
                    res = r.text.strip('][').split(', ')
                    src = u"[%s]" % res[0]
                    block = json.loads(src)
                    block = [{k: blck[k] for k in ('eduBlockId', 'yearId')} for blck in block]
                    sub_reqd_3.append(block)
                    time.sleep(1)
                except:
                    pass
            reqd3.append(sub_reqd_3)

        for i in range(len(reqd1)):
            for values1 in reqd2[i]:
               values1.update(reqd1[i])

        for i in range(len(reqd2)):
            for l in range(len(reqd2[i])):
                for r in range(len(reqd3[i][l])):
                    print(reqd2[i][l],reqd3[i][l][r])
                    reqd3[i][l][r].update(reqd2[i][l])


        #do it for remaining places
        final = []
        for value in reqd3:
            for i in range(len(value)):
                for vals in value[i]:
                    print(vals)
                    sample = {"year": vals["yearId"],
                    "stateName": vals['stateId'],
                    "districtName": vals['districtId'],
                    "blockName": vals['eduBlockId'],
                    "villageName":"" ,
                    "clusterName": "",
                    "categoryName": 0,
                    "managementName": 0,
                    "Search": "search"}
                    final.append(sample)
        data1 = []
        for vals in final:
            time.sleep(1)
            result_url = "http://src.udiseplus.gov.in/locateSchool/searchSchool"
            r = requests.post(result_url,data=vals)
            no_of_ping += 1
            print(r.status_code)
            #r = requests.get(main_url)
            res = r.text.strip('][')
        #    src = u"[%s]" % res[0]
            result = json.loads(res)
            result = [{k: rslt[k] for k in ('schoolId', 'schoolName','villageName','districtName','blockName', 'stateName')} for rslt in result['list']]
            data1.append(result)

        url = "http://src.udiseplus.gov.in/NewReportCard/PdfReportSchId"
        header = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36'}

        school_name = []
        village_name = []
        district_name = []
        block_name = []
        state_name = []
        elec1 = []
        for vals in data1:
            print(vals)
            school_name = []
            village_name = []
            district_name = []
            block_name = []
            state_name = []
            elec1 = []
            elec2 = []
            total = []
            for value in vals:
                print(value)
                time.sleep(1)

                try:
                    r = requests.post(url, headers=header, data={'schoolId':str(value['schoolId'])})
                    no_of_ping += 1
                except:
                    try:
                        time.sleep(10)
                        r = requests.post(url, headers=header, data={'schoolId':str(value['schoolId'])})
                        no_of_ping += 1
                    except:
                        raise Exception("Site Not Reachable")
                try:
                    print(r.status_code,value['districtName'],value[ 'blockName'])
                    if r.status_code == 500 or r.status_code == 404 or r.status_code == 405 or r.status_code == 400:
                        school_name = value['schoolName']
                        village_name = value['villageName']
                        district_name = value[ 'districtName']
                        block_name = value[ 'blockName']
                        state_name = value['stateName']
                        avail = 0

                    else:
                        f = io.BytesIO(codecs.decode(r.content, "base64"))
                        reader = PyPDF2.PdfFileReader(f)
                        contents1 = reader.getPage(0).extractText().split('\n')
                        contents2 = reader.getPage(1).extractText().split('\n')
                        elec = contents1[0].split()
                        school_name = value['schoolName']
                        village_name = value['villageName']
                        district_name = value['districtName']
                        block_name = value['blockName']
                        try:
                            avail = re.findall(r'[1-9]{1}[0-9]{5}|[1-9]{1}[0-9]{3}\\s[0-9]{3}',[s for s in elec if "Pincode" in s][0])[0]
                        except:
                            avail = 0 

                    avail = int(avail)
                    print(avail)
    #                output= pd.DataFrame(list(school_name, village_name,district_name,block_name,state_name,elec2,total),
    #                       columns =['School_Name', 'Village_Name','District_Name','Block_Name','State_Name','Electricity_Availability','Total_Students'])
                    output = pd.DataFrame({'School_Name':school_name, 'Village_Name':village_name,'District_Name':district_name,'Block_Name':block_name,'State_Name':state_name,'Pincode':avail}, index=[0])
                    #output = pd.DataFrame({'School_Name':school_name, 'Village_Name':village_name,'District_Name':district_name,'Block_Name':block_name,'State_Name':state_name,'Pincode':}, index=[0])

            #        output = pd.concat([output,df])
                    output['Relevant_Date'] = datetime.date(2020,3,31)
                    output['Runtime'] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
                    print(len(output))

                    output = output[['School_Name','State_Name', 'District_Name', 'Block_Name','Village_Name', 'Electricity_Availability','Total_Students',"Relevant_Date","Runtime"]]

                    print(output.head())

                    output.to_sql(name='CAG_SCHOOLS_VILLAGE_LEVEL_PINCODE_DATA',con=engine,if_exists='append',index=False)

                    del output
                except:
                    pass
        log.job_end_log(table_name,job_start_time, no_of_ping)

    except:
        try:
            connection.close()
        except:
            pass
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])
        print(error_msg)

        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
