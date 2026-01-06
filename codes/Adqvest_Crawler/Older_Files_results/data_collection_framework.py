
import pandas as pd
import os
import asyncio
import re
from bs4 import BeautifulSoup
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass
import logging
import json
import sys
from pathlib import Path
import importlib
import inspect
from datetime import timedelta  
from  datetime import datetime
from pytz import timezone
#------------------------------------------------------------------------------------------------------------
india_time = timezone('Asia/Kolkata')
today      = datetime.now()
days       = timedelta(1)
yesterday = today - days

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import ClickHouse_db
# working_dir=r"C:\Users\Santonu\Adqvest_Crawler"
working_dir=r"C:\Users\Administrator\AdQvestDir\codes\Adqvest_Crawler"

from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from crawlee import Request
logging.getLogger('crawlee').setLevel(logging.WARNING)
from Store_Locator_Class import CascadingDataCollector,SiteConfigLoader

#--------------------------------------------------------------------------------------------------------------
async def collect_custom_site_data(site_config, today: datetime = None) -> pd.DataFrame:
    """Collect data from any custom site"""
    collector = CascadingDataCollector(site_config)
    collected_data=await collector._collect_data(today)
    await collector.Upload_Data(site_config.brand_name, collected_data)

    
    print('--------------------------------------------COLLECTION INFO---------------------------------------------------------------')
    # df_info=await collector._get_data_info(site_config.brand_name)
    # print(df_info)

    filename = f"{site_config.brand_name}_Stores_{today.strftime('%Y%m%d')}.xlsx"
    full_path = os.path.join(working_dir, filename)
    await asyncio.to_thread(collected_data.to_excel, full_path)
    print(f"{site_config.brand_name}: {len(collected_data)} stores collected and saved")
    return f"{site_config.brand_name}: {len(collected_data)} stores collected and saved"


#---------------------------------------------------------------------------------------------------------------
# Main execution function
async def run_program():

    today = datetime.now()
    site_configs=SiteConfigLoader()
    all_configs=site_configs._load_configs()
    # all_configs=[i for i in all_configs.values()]
    all_configs=[all_configs['McDonalds_North_and_East_India']]

    # print(all_configs)
    #----------------------------------------------------------------------------------------------------------------------------------    
    results = await asyncio.gather(*[collect_custom_site_data(config, today) for config in all_configs],
                                   return_exceptions=True)

    print('-----------------------------------------------------ALL DONE--------------------------------------------------------------')

# Run the framework
if __name__ == "__main__":
    asyncio.run(run_program())