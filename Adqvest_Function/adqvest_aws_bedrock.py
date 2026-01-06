# -*- coding: utf-8 -*-
"""
Created on Tue Mar 25 18:10:47 2025

@author: Santonu
"""

import pandas as pd
import os


def bedrock_cred():
    os.chdir('C:/Users/Administrator/AdQvestDir/Adqvest_Function')
    s3=pd.read_csv("AWS_Bedrock_Credentials.txt",delim_whitespace=True)
    ACCESS_KEY_ID=s3[s3['Env']=='Acess_Key_ID']['Detail'][0]
    ACCESS_SECRET_KEY=s3[s3['Env']=='Access_Secret_Key']['Detail'][1]
    return ACCESS_KEY_ID,ACCESS_SECRET_KEY


