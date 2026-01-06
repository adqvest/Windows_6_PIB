#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 13 12:36:03 2023

@author: nidhigoel
"""

import sys
import pandas as pd
import json
import requests
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
from adqvest_robotstxt import Robots
robot = Robots(__file__)
import warnings
warnings.filterwarnings('ignore')

def find_state(pincode,address):
    state = ''
    with open(r'C:\Users\Administrator\AdQvestDir\State_Function\pincodes.json') as f:
        data = json.load(f)
        
    if pincode != '':
        data['data'] = sorted(data['data'], key=lambda item: item[2])
        df_pincode = pd.DataFrame(data['data'], columns=data['columns'])
        generalised_pin = pincode[:3] + '000'
        df_pincode = df_pincode[df_pincode['Pincode'] >= int(generalised_pin)]
        columns_json = df_pincode.columns.tolist()
        data_json = df_pincode.values.tolist()
        data = {
            "columns": columns_json,
            "data": data_json
        }

        for item in data['data']:
            if item[2] == pincode:
                state = item[0].title()
                print(f'{state} found from pin in json file')
                return state
            elif int(str(item[2])[:3]) == int(str(pincode)[:3]):
                state = item[0].title()
                print(f'{state} found from pin in json file')
                return state
            else:
                pass
            
        if state == '':
            try:
                r = requests.get(f'https://api.postalpincode.in/pincode/{pincode}',timeout=60)
                if r.status_code != 200:
                    pass
                else:
                    json_data = json.loads(r.content)
                    if json_data[0]['Message'] == 'No records found':
                        pass
                    else:
                        state = json_data[0]['PostOffice'][0]['State'].title()
                        print(f'{state} found from api')
                        return state
            except:
                pass
            
            
    if state == '':
        for x in data['data']:
            if x[0] in address.upper():
                state =  x[0].title()
                print(f'{state} found from json file match with state')
                return state
            
    if state == '':
        for x in data['data']:
            if x[1].upper() in address.upper():
                state =  x[0].title()
                print(f'{state} found from json file match with district')
                return state
               
    return state

def find_district(pincode,address):
    district = ''
    with open(r'C:\Users\Administrator\AdQvestDir\State_Function\pincodes.json') as f:
        data = json.load(f)
        
    data['data'] = sorted(data['data'], key=lambda item: item[2])

    if pincode != '':
        df_pincode = pd.DataFrame(data['data'], columns=data['columns'])
        generalised_pin = pincode[:3] + '000'
        df_pincode = df_pincode[df_pincode['Pincode'] >= int(generalised_pin)]
        columns_json = df_pincode.columns.tolist()
        data_json = df_pincode.values.tolist()
        data = {
            "columns": columns_json,
            "data": data_json
        }
        for item in data['data']:
            if item[2] == pincode:
                district = item[1].title()
                print(f'{district} found from pin in json file 1')
                return district
            elif int(str(item[2])[:3]) == int(str(pincode)[:3]):
                district = item[1].title()
                print(f'{district} found from pin in json file 2')
                return district
            else:
                pass
            
        if district == '':
            r = requests.get(f'https://api.postalpincode.in/pincode/{pincode}',timeout=60)
            if r.status_code != 200:
                pass
            else:
                json_data = json.loads(r.content)
                if json_data[0]['Message'] == 'No records found':
                    pass
                else:
                    district = json_data[0]['PostOffice'][0]['District'].title()
                    print(f'{district} found from api')
                    return district
            
    if district == '':
        for x in data['data']:
            if x[1].upper() in address.upper():
                district =  x[1].title()
                print(f'{district} found from json file match with district')
                return district
               
    return district