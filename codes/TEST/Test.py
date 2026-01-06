# -*- coding: utf-8 -*-
"""
Created on Fri Jun 13 19:10:02 2025

@author: Santonu
"""
import sys
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import MySql_To_Clickhouse as MySql_CH
 
 
#insert into CH
 
table_name = "MCX_SPOT_PRICE_DAILY_DATA"
 
MySql_CH.ch_truncate_and_insert(table_name)