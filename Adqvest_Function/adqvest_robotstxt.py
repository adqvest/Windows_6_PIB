# -*- coding: utf-8 -*-
"""
Created on Tue Jan 10 14:45:14 2023

@author: Abdulmuizz
"""

from urllib.parse import urlparse
from sqlalchemy import text
from protego import Protego
import requests
import pandas as pd
import tldextract
import datetime
from pytz import timezone
import sys
import re
import adqvest_db
import stopit


class Robots:
    
    user_agent = '*'
    request_headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'}    
    link_pool = []
    
    def __init__(self, py_file_name: str):
        
        self.py_file_name = py_file_name.split('\\')[-1].split('/')[-1].split('.')[0]
        
        if self.py_file_name == '':
            raise ValueError('Please Give a Valid File Name')
    
    def add_link(self, link: str):
        
        try:
            urlparse(link)
            assert link.startswith('http')
        except:
            raise ValueError("Please give a valid URL for robots.txt compliance")

        if link not in self.link_pool:

            self.link_pool.append(link)
            
            result = dict()
            result['Python_File_Name'] = self.py_file_name
            result['Links'] = link
            result['Scheme'] = None
            result['Domain'] = None
            result['Path'] = None
            result['User_Agent'] = None
            result['Can_Fetch'] = None
            result['Robots_Available'] = None
            result['Robotstxt_url'] = None
            result['Comments'] = None
            
            final_df = pd.DataFrame([result])
            final_df['Relevant_Date'] = datetime.datetime.now(timezone('Asia/Kolkata')).date()
            final_df['Runtime'] = datetime.datetime.now(timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S")
            
            engine = adqvest_db.db_conn()
            final_df.to_sql(name = "ROBOTS_TXT_COMP_TABLE_DAILY_DATA",con = engine,if_exists = 'append',index = False)
    
    def check_compliance(self, link, response):
    
        if 'text/plain' in response.headers["content-type"] and response.status_code == 200:
            rp = Protego.parse(response.text)
            
            return rp.can_fetch(link, self.user_agent)
        else:
            raise Exception("Robots Failed")
    
    def robotstxt(self, link, relevant_date, runtime):
        
        url = urlparse(link)
        
        domain = url.netloc
    
        robotstxt_url = url.scheme + '://' + domain + '/robots.txt'
        
        try: 
            robots_open = requests.get(robotstxt_url, headers = self.request_headers, timeout = 60)
            fetch_result = self.check_compliance(link,robots_open)
            code = robots_open.status_code
        except:
            try:
                robotstxt_url = url.scheme + '://' + 'www.' + domain + '/robots.txt'
                robots_open = requests.get(robotstxt_url, headers = self.request_headers, timeout = 60)
                fetch_result = self.check_compliance(link,robots_open)
                code = robots_open.status_code
                domain = 'www.' + domain
            except: 
                try:
                    tld = tldextract.extract(domain)
                    robotstxt_url = url.scheme + '://' + 'www.' + tld.domain + '.' + tld.suffix + '/robots.txt'
                    robots_open = requests.get(robotstxt_url, headers = self.request_headers, timeout = 60)
                    fetch_result = self.check_compliance(link,robots_open)
                    code = robots_open.status_code
                    domain = 'www.' + tld.domain + '.' + tld.suffix
                except:
                    code = 404
    
        result = dict()
        result['Python_File_Name'] = self.py_file_name
        result['Links'] = link
        result['Scheme'] = url.scheme
        result['Domain'] = domain
        result['Path'] = url.path
        result['User_Agent'] = self.user_agent
        if code == 200:
            result['Can_Fetch'] = fetch_result
            result['Robots_Available'] = True
            result['Robotstxt_url'] = robotstxt_url
        else:
            result['Can_Fetch'] = True
            result['Robots_Available'] = False
            result['Robotstxt_url'] = None
        result['Comments'] = None
        result['Relevant_Date'] = relevant_date
        result['Runtime'] = runtime
        
        query = f"""UPDATE 
                        ROBOTS_TXT_COMP_TABLE_DAILY_DATA 
                    SET
                        Scheme = "{result['Scheme']}",
                        Domain = "{result['Domain']}",
                        Path = "{result['Path']}",
                        User_Agent = "{result['User_Agent']}",
                        Can_Fetch =  {int(result['Can_Fetch'])},
                        Robots_Available = {int(result['Robots_Available'])},
                        Robotstxt_url = {'null' if result['Robotstxt_url'] == None else "'" + result['Robotstxt_url'] + "'"},
                        Comments = null
                    WHERE 
                        Python_File_Name = "{result['Python_File_Name']}" and 
                        Links = "{result['Links']}" and 
                        Relevant_Date = "{result['Relevant_Date']}" and 
                        Runtime = "{result['Runtime']}"
                """

        engine = adqvest_db.db_conn()
        connection = engine.connect()
        connection.execute(text(query))
        connection.execute('commit')
        connection.close()
    
    def exception_func(self, link, relevant_date, runtime, error):
        
        url = urlparse(link)
                
        result = dict()
        result['Python_File_Name'] = self.py_file_name
        result['Links'] = link
        result['Scheme'] = url.scheme
        result['Domain'] = url.netloc
        result['Path'] = url.path
        result['User_Agent'] = self.user_agent
        result['Can_Fetch'] = None
        result['Robots_Available'] = None
        result['Robotstxt_url'] = None
        result['Comments'] = error
        result['Relevant_Date'] = relevant_date
        result['Runtime'] = runtime
        
        query = f"""UPDATE 
                        ROBOTS_TXT_COMP_TABLE_DAILY_DATA 
                    SET
                        Scheme = "{result['Scheme']}",
                        Domain = "{result['Domain']}",
                        Path = "{result['Path']}",
                        User_Agent = "{result['User_Agent']}",
                        Can_Fetch =  null,
                        Robots_Available = null,
                        Robotstxt_url = null,
                        Comments = "{result['Comments']}"
                    WHERE 
                        Python_File_Name = "{result['Python_File_Name']}" and 
                        Links = "{result['Links']}" and 
                        Relevant_Date = "{result['Relevant_Date']}" and 
                        Runtime = "{result['Runtime']}"
                """

        engine = adqvest_db.db_conn()
        connection = engine.connect()
        connection.execute(text(query))
        connection.execute('commit')
        connection.close()
    
    def check_robotstxt(self, link, relevant_date, runtime):
        
        try:
            with stopit.ThreadingTimeout(120) as context_manager:
                self.robotstxt(link, relevant_date, runtime)
    
            if context_manager.state == context_manager.EXECUTED:
                pass
            elif context_manager.state == context_manager.TIMED_OUT:
                raise TimeoutError("robots.txt timed out, probably no robots.txt available for the domain")
        except:
            error = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
            self.exception_func(link, relevant_date, runtime, error)
            
            
                














        