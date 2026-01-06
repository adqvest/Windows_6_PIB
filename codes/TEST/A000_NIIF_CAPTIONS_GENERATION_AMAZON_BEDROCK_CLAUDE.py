
#Created By Hetesh
import sys
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')

import datetime as datetime
import pandas as pd
import numpy as np
import requests
import warnings
import openai
import base64
import boto3
import json
import time
import os
import re

warnings.filterwarnings('ignore')


from dateutil.relativedelta import relativedelta
from pytz import timezone

pd.options.display.max_columns = None
pd.options.display.max_rows = None
warnings.filterwarnings('ignore')

import adqvest_db
import JobLogNew as log

# #### AMAZON BEDROCK SET UP

def run_program(run_by='Adqvest_Bot', py_file_name=None):

    engine = adqvest_db.db_conn()
    connection = engine.connect()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days
    
    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'ALL_SCHEDULER_LINKS_WEEKLY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0
    
    engine = adqvest_db.db_conn()
    
    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        # #### FUNCTION FOR ANTHROPIC THROUGH BEDROCK

        def encoded(sample_string):
            sample_string_bytes = sample_string.encode("ascii")
            base64_bytes = base64.b64encode(sample_string_bytes)
            base64_string = base64_bytes.decode("ascii")
            base64_string
            
            return base64_string

        def run_multi_modal_prompt(bedrock, model_id, messages, max_tokens, system_prompt):

            body = json.dumps(
                {
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": max_tokens,
                    "system": system_prompt,
                     "messages": messages
                }
            )

            response = bedrock.invoke_model(
                body=body, modelId=model_id)
            response_body = json.loads(response.get('body').read())

            return response_body

        def run_multi_modal_prompt_cohere(bedrock, model_id, max_tokens, system_prompt):

            body = json.dumps(
                {
                    "message": system_prompt,
                    "max_tokens": max_tokens,
                    "temperature": 0
                }
            )

            response = bedrock.invoke_model(
                body=body, modelId=model_id)
            response_body = json.loads(response.get('body').read())

            return response_body


        def rewrite_caption(data_entries):
            """
            Re-write a caption if it exceeds 70 characters.
            
            Parameters:
            data_entries (list): List of dictionaries containing widget data.

            Returns:
            str: Re-written caption.

            """
            model_id = 'anthropic.claude-3-haiku-20240307-v1:0'
            # model_id = 'anthropic.claude-3-sonnet-20240229-v1:0'
            max_tokens = 2000

            system_prompt = """
            You are a Macro Economist creating a single statement title.
            Follow the steps and give the output.
                
            <input-context>
                The input given to you is a title of a chart.
            </input-context>
            
            <steps-to-follow>
                <first>
                    Rewrite the title in less than 70 characters.
                </first>
                <second>
                    Return only the final output.
                </second>
            <\steps-to-follow>
            """

            messages = [{"role": "user", "content": [{"type": "text", "text": f"<title> {data_entries} </title>"}]}]
            
            output = run_multi_modal_prompt(bedrock, model_id, messages, max_tokens, system_prompt)
            return output.get('content')[0].get('text')

        if today.day==5 or today.day==7 or today.day==9:

            os.environ["AWS_ACCESS_KEY_ID"] = "AKIAXYKJVGGG67O7TD7Z"
            os.environ["AWS_SECRET_ACCESS_KEY"] = "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"

            region_name = 'us-west-2'
            bedrock = boto3.client(service_name='bedrock-runtime', region_name=region_name)


            email_id = 's.heteshkumar@adqvest.com'
            password = 'Masthailife2!' # Test
            # password = 'welcome@2024' # Prod

            # provide the username and password

            user = '{"email":"'+email_id+'", "password":"'+password+'"}'
            user_id = encoded(user)

            auth_url = 'https://test.thurro.com/api/getAPIToken'

            data1 = {"user":user_id}

            #Post request will return the Authorization token value
            r = requests.post(url=auth_url, data=data1)
            token =r.json()
            token = token['token']

            getwidget_url = 'https://test.thurro.com/api/getWidgetData'
            # getwidget_url = 'https://app.thurro.com/api/getWidgetData'

            # pass the token value in headers
            headers = {"Authorization": token}


            df = pd.read_excel("C:/Users/Administrator/AdQvestDir/codes/NIIF_TITLE_AND_DESCRIPTION_GENERATION/niif_updated_widgtes_description.xlsx")

            df = df[["Slide","chart","widget_id"]]
            df.reset_index(inplace = True, drop = True)

            df["widget_id"] = ["["+str(x)+"]" for x in df["widget_id"]]
            df = df.groupby(['Slide','chart'])['widget_id'].apply(list).reset_index()

            df.reset_index(inplace = True, drop = True)

            chart_list = df["widget_id"].tolist()

            overall_output = []
            overall_output_once = []

            count = 0    
            for x in chart_list:
                
                title = []
                metric = []
                latest_data = []
                unit = []
                growth_rate = []

                for y in x:
                    
                    # provide widget_id for which details are required
                    widget_id = y

                    widget_encode = encoded(widget_id)
                    data2 = {"widgets":widget_encode}

                    #Post request will return the details of widget ids
                    r = requests.post(url=getwidget_url, headers=headers, data=data2)

                    j_dict = (r.json())
                    
                    try :
            #             title.append(j_dict['result'][0]['title'].replace('|',''))
                        title.append(j_dict['result'][0]['title'])
                    except :
                        continue
                        
                    metric.append(j_dict['result'][0]['metric'])
                    
                    data = j_dict['result'][0]['data']
                    
                    if data == []:
                        continue
                        
                    if 'units' in j_dict['result'][0]:
                        unit.append(j_dict['result'][0]['units'])
                    else :
                        unit.append('')
                
                    # Calculating Growth

                    for entry in data:
                        try :
                            entry['Relevant_Date'] = datetime.datetime.strptime(entry['Relevant_Date'], '%Y-%m-%d')
                        except :
                            entry['Relevant_Date'] = datetime.datetime.strptime(entry['Relevant_Date'], '%Y-%m-%d %H:%M:%S')

                    latest_date = max(entry['Relevant_Date'] for entry in data)
                    one_year_ago = latest_date - relativedelta(months=12)
                    
                    latest_date_act = max(entry['Relevant_Date'] for entry in data)
                    start_of_month_date = datetime.date.today().replace(day=1)
                    
                    if latest_date.date() > start_of_month_date:
                        
                        max_date = max(entry['Relevant_Date'] for entry in data)
                        previous_entries = [entry['Relevant_Date'] for entry in data if entry['Relevant_Date'] < max_date]
                        latest_date = max(previous_entries) if previous_entries else None
                        one_year_ago = latest_date - relativedelta(months=12)

                    value_keys = set(entry.keys()) - {'Relevant_Date','Unit','Variable','Sub_Type'}
                    
                    segment_list = []
                    
                    for value_key in value_keys:
                        
                        if len(set(entry.keys())) >= 3:
                            segment_list.append(value_key)
                            
                        # Find the values for the latest date and one year ago
                        latest_value = next(entry[value_key] for entry in data if entry['Relevant_Date'] == latest_date)

                        try :
                            one_year_ago_value = next(entry[value_key] for entry in data if entry['Relevant_Date'].year == one_year_ago.year and entry['Relevant_Date'].month == one_year_ago.month)
                        except :
                            one_year_ago_value = 0

                        # Calculate the growth rate
                        if 'growth' not in j_dict['result'][0]['metric'].lower().split(' '):
                            if 'growth' not in value_key.lower().split('_'):
                                try :
                                    growth_rate.append(round(((latest_value - one_year_ago_value) / one_year_ago_value) * 100,1))
                                except :
                                    growth_rate.append('')
                            else :
                                growth_rate.append('')
                        else :
                            growth_rate.append('')
                        
                        if latest_date_act.date() > start_of_month_date:
                            latest_data.append(data[-2])
                        else :
                            latest_data.append(data[-1])
                
                # Getting The Key Words 
                keywords = "'TITLE': " + str(title) + ", 'METRIC': " + str(metric) + ", 'GROWTH': " + str(growth_rate) + ", 'LATEST DATA': " + str(latest_data) + ", 'UNIT': " + str(unit)
                keywords = keywords.replace('(For NIIF)','').replace('for NIIF','').replace('For NIIF','')
                
                # Feeding The Key Words Into Amazon Bedrock
                
                if title == []:
                    for output_count in range(len(x)):
                        overall_output.append("NO OUTPUT") 
                    overall_output_once.append("NO OUTPUT")
                    continue
                

                model_id = 'cohere.command-r-plus-v1:0'
                max_tokens = 1000

                system_prompt = f'''
                
                PERSONA : You are an Analyst creating a single statement title for each chart of a PPT presentation.
                    
                INPUT CONTEXT : The input given to you is the title, descirpion, growth, unit and the latest data point of a 
                time series data in the chart. Some of the charts are multi charts as in they have many data points for each 
                relevant date. 
                
                INPUT : {keywords}
                    
                OUTPUT STRUCTURE : The output should only include the single statement title which can just be copied and pasted
                into the PPT document. Using he word robust in the output is strictly prohibited.
                
                INSTRUCTIONS :
                
                - Understand the keywords and then start generating your title.
                - Your answer must only include the single statement title in sentence case within 70 characters.
                - The date should be in the format of short month and short year, example : Jan 23.
                - Round off all values to 1 decimal point and its corresponding numerical system, 
                    example : 12300000 should be mentioned as 12.3 Million.
                    example : 2430000000 should be mentioned as 2.4 Billion.
                - Using he word robust in the single statement title is strictly prohibited.
                - While talking about financial quarters use India's Financial quarters.
                    
                EXAMPLE OUTPUTS :
                
                Sharp pickup in demand for commercial vehicles
                Strong growth in electricity demand in Nov 23
                Fertilizer sales picked up by 16% yoy to 5.4 million tonnes in Oct 23
                Wholesale inflation moderated to 8.4% yoy in Oct 23
                Low volatility in Indian equity markets
                REITs generated average returns of 1.2% compared to 6.5% on InvITs
                Net portfolio outflows from India in Oct 23 at INR 31.4 billion
                Net FDI flows in Sep 23 remain strong at USD 20.2 billion
                Net investments by AIFs remain strong at INR 273 billion
                Ports' cargo traffic stable at 62 million tonnes
                
                '''


                output = run_multi_modal_prompt_cohere(
                    bedrock, model_id, max_tokens, system_prompt)

                output = output.get('chat_history')[1].get('message')
                
                countb = 0
                while len(output) > 70:
                    output = rewrite_caption(output)
                    if countb > 5:
                        break
                    
                    countb+=1
                    
                overall_output_once.append(output)
                
                for output_count in range(len(x)):
                    overall_output.append(output)

                count+=1
                print("COMPLETED CHART : ",count)

            print("ALL DONE")


            # Changing somethings mannually
            overall_output = [s.replace('&','and') for s in overall_output]

            for x in overall_output_once:
                if len(x) > 70:
                    print(x)

            overall_output = pd.DataFrame(overall_output,columns = ['THURRO_CHART_TITLE'])

            df1 = df
            df1["THURRO_CHART_TITLE"] = overall_output_once
            df1 = df1.groupby(['Slide'])['THURRO_CHART_TITLE'].apply(list).reset_index()

            df = pd.read_excel("C:/Users/Administrator/AdQvestDir/codes/NIIF_TITLE_AND_DESCRIPTION_GENERATION/niif_updated_widgtes_description.xlsx")

            df = df[["Slide","chart","widget_id"]]
            df.reset_index(inplace = True, drop = True)

            slide_counts = df['Slide'].value_counts().sort_index()

            count = 0
            slide_overall = []
            for x in df1["THURRO_CHART_TITLE"]:

                # Getting The Key Words 
                keywords = "'ALL TITLEs': " + str(x)
                
                keywords = keywords.replace("['NO OUTPUT']", "")
                
                if keywords == "'ALL TITLEs': ":
                    slide_overall.append("NO OUTPUT")
                    continue

                # Feeding The Key Words Into Claude
                
                
                model_id = 'anthropic.claude-3-sonnet-20240229-v1:0'
                max_tokens = 1000

                system_prompt = f'''
                
                PERSONA : You are an Analyst creating a title for each slide of a PPT presentation.
                    
                INPUT CONTEXT : The input given to you are the titles of each chart in the slide. 
                    
                OUTPUT STRUCTURE : The output should only include the single statement title which can just be copied and pasted
                into the PPT document. Using the word robust in the output is strictly prohibited.
                
                INSTRUCTIONS :
                
                - Your answer must only include the single statement title in sentence case within 90 characters.
                - The date should be in the format of short month and short year, example : Jan 23.
                - You must understand the context of the titles and give the the single statement title.
                - Using he word robust in the single statement title is strictly prohibited.
                    
                EXAMPLE OUTPUTS :
                
                Higher domestic demand and investments, and rebound in services sector supported growth
                Core infrastructure growth slowed as cement and coal production weakened in Oct 23
                Logistics movement remained high; manufacturing and services activity maintained momentum
                Higher demand for passenger vehicles and two-wheelers in the last few months
                Sharp pick up in demand for three-wheelers and commercial vehicles in Nov 23
                Rural demand supported by stable tractor registrations and strong fertilizer sales
                Strong credit growth may put pressure on banks to leverage its excess SLR holdings
                Digital payments including credit cards showed growth; currency in circulation remain high
                Wholesale and retail inflation moderated in Nov 23
                Labor force participation remained low; moderation seen in hiring activity
                Goods trade deficit remained wide with elevated imports and slower growth in exports
                RBI had limited liquid reserves left to manage rupee volatility and a healthy import cover
                Foreign investments remained volatile, domestic investors continue to support the market
                Strong credit take-off supported by growth in retail and NBFC
                            
                '''

                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": f'''KEYWORDS: {keywords}'''
                            }
                        ]
                    }
                ]


                output = run_multi_modal_prompt(
                    bedrock, model_id, messages, max_tokens, system_prompt)

                output = output.get('content')[0].get('text')
                
                countb = 0
                while len(output) > 70:
                    output = rewrite_caption(output)
                    if countb > 5:
                        break
                    countb += 1
                    
                slide_overall.append(output)

                count+=1
            #     if count == 5:
            #         break
                print("COMPLETED SLIDE : ",count)


            expanded_list = []

            for count, element in zip(slide_counts, slide_overall):
                expanded_list.extend([element] * count)


            expanded_list = pd.DataFrame(expanded_list,columns = ['THURRO_SLIDE_TITLE'])
            # expanded_list.to_excel("/Users/aezn/Downloads/NIIF_Chat_Amazon_Bedrock_Slide_Title_"+str(today.date())+"_2.xlsx")


            for x in slide_overall:
                if len(x) > 100:
                    print(x)


            df_final = pd.DataFrame()
            df = pd.read_excel("C:/Users/Administrator/AdQvestDir/codes/NIIF_TITLE_AND_DESCRIPTION_GENERATION/niif_updated_widgtes_description.xlsx")

            df_final["Slide"] = df["Slide"]
            df_final["chart"] = df["chart"]
            df_final["widget_id"] = df["widget_id"]
            df_final["chart_source"] = df["chart_source"]
            df_final["chart_title"] = overall_output["THURRO_CHART_TITLE"]
            df_final["slide_heading"] = expanded_list["THURRO_SLIDE_TITLE"]
            df_final["slide_sub_heading"] = df["slide_sub_heading"]

            df_final.to_excel("C:/Users/Administrator/AdQvestDir/codes/NIIF_TITLE_AND_DESCRIPTION_GENERATION/niif_updated_widgtes_description_"+str(today.date())+".xlsx")


        log.job_end_log(table_name,job_start_time,no_of_ping)
        
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        error_msg = str(sys.exc_info()[1]) + " line " + str(exc_tb.tb_lineno)
        print(error_msg)

        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)
    
        
if(__name__=='__main__'):
    run_program(run_by='manual')
