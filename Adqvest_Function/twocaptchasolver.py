#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 11 17:06:51 2024

@author: nidhigoel
"""
import os
from twocaptcha import solver
import requests

key = '368c8fa1c31062b71eab0e93f3e41d01'

def check_balance():
    r =  requests.get(f'https://2captcha.com/res.php?key={key}&action=getbalance')
    bal = float(r.content)
    if(bal < 0.01):
        return 0
    return bal

def solve_captcha(filepath,filename,numeric=0,calc=0):
    balance = check_balance()
    if balance > 0.01:
        api_key = os.getenv('APIKEY_2CAPTCHA', key)
        solver_inst = solver.TwoCaptcha(api_key)
        text = solver_inst.normal(filepath+f"/{filename}",numeric=numeric,calc=calc)
        solved_captcha = text['code'].upper()
        captcha_id = text['captchaId'].upper()
        return captcha_id,solved_captcha
    else:
        raise Exception('Insufficient balance in 2captcha solver!')

def report_bad_captcha(captcha_id):
    requests.get(f'https://2captcha.com/res.php?key={key}&action=reportbad&id={captcha_id}')


def report_good_captcha(captcha_id):
    requests.get(f'https://2captcha.com/res.php?key={key}&action=reportgood&id={captcha_id}')