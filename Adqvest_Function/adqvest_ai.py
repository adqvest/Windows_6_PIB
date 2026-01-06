# -*- coding: utf-8 -*-
"""
Created on Sun Jun 22 11:24:41 2025

@author: Santonu
"""

import os
import sys
import boto3
import json
import re
# sys.path.insert(0, '/home/ubuntu/AdQvestDir/Adqvest_Function')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
# sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Windows_3/Adqvest_Function')


import adqvest_aws_bedrock as aws_bedrock_cred

KEY_ID=aws_bedrock_cred.bedrock_cred()[0]
ACCESS_KEY=aws_bedrock_cred.bedrock_cred()[1]

os.environ["AWS_ACCESS_KEY_ID"] =KEY_ID
os.environ["AWS_SECRET_ACCESS_KEY"] = ACCESS_KEY
region_name = 'us-west-2'
bedrock = boto3.client(service_name='bedrock-runtime', region_name=region_name)


def run_multi_modal_prompt(bedrock, model_id, messages, max_tokens):
    body = json.dumps(
        {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": max_tokens,
            "messages": messages,
            "temperature": 0,
            "top_p": 0,
            "top_k": 1
        }
    )
    # print("Invoking Model")
    response = bedrock.invoke_model(
        body=body, modelId=model_id)
    response_body = json.loads(response.get('body').read())
    return response_body

def generate_answer(content,Instructions,sample_output):
    model_id = 'anthropic.claude-3-5-haiku-20241022-v1:0'
    max_tokens = 4000
  
    messages=[
        {
            "role": "user",
            "content": [
                {"type": "text", "text": f'''
                
                <document>
                    {content}
                </document>
                
                {Instructions}
                <sample-output>
                   {sample_output}
               <sample-output> 
               
                 '''}
            ]
        }
    ]
    # print("Hitting Multi Modal Prompt")
    output = run_multi_modal_prompt(
        bedrock, model_id, messages, max_tokens)
    return output.get('content')[0].get('text')


def extract_json_block(text):
    json_match = re.search(r'\{[^{}]+\}', text, re.DOTALL)
    
    if json_match:
        json_str = json_match.group(0)
        try:
            parsed = json.loads(json_str.replace("null", "null"))
            return parsed
        except json.JSONDecodeError as e:
            return f"Found JSON-like block, but it couldn't be parsed: {e}"
    else:
        return "No JSON block found."