
import pandas as pd
import os
import asyncio
import logging
import sys
import argparse
from datetime import timedelta, datetime
from pytz import timezone

# ------------------------------------------------------------------------------------------------------------
india_time = timezone('Asia/Kolkata')
today = datetime.now()
days = timedelta(1)
yesterday = today - days

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
# sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Windows_Main/Adqvest_Function')
# sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Windows_3/Adqvest_Function')

import adqvest_db
import ClickHouse_db

# working_dir = r"C:\Users\Santonu\Adqvest_Crawler"
working_dir=r"C:\Users\Administrator\AdQvestDir\codes\Adqvest_Crawler"
# working_dir=r"C:\Users\Administrator\AdQvestDir\Windows_Main\codes\Adqvest_Crawler"
# working_dir=r"C:\Users\Administrator\AdQvestDir\Windows_3\codes\Adqvest_Crawler"
os.chdir(working_dir)

logging.getLogger('crawlee').setLevel(logging.WARNING)
from store_collector import CascadingDataCollector
from site_config import SiteConfigLoader
from search_data_provider import InputDataSource
from custom_extractors import dedup_similar_address

# --------------------------------------------------------------------------------------------------------------
async def collect_custom_site_data(site_config, today: datetime = None) -> pd.DataFrame:
    """Collect data from any custom site"""
    try:
        collector = CascadingDataCollector(site_config)
        collected_data = await collector._collect_data(today)

        if len(collected_data)>0:
            if site_config.Sub_Category_1:
                  collected_data=collected_data[collected_data['Sub_Category_1']==site_config.Sub_Category_1]
                  
            if site_config.Sub_Category_2:
                  collected_data=collected_data[collected_data['Sub_Category_2']==site_config.Sub_Category_2]

            if site_config.brand_name == 'shoppers_stop':
                collected_data = dedup_similar_address(collected_data)

            df_info=await InputDataSource.get_data_info(site_config.brand_name,Sub_Category_1=site_config.Sub_Category_1,Sub_Category_2=site_config.Sub_Category_2)
            print(df_info)
            if (len(df_info)==0):
                await InputDataSource.Upload_Data(site_config.brand_name, collected_data,Sub_Category_1=site_config.Sub_Category_1,Sub_Category_2=site_config.Sub_Category_2)
                print(f"Collected data for {site_config.brand_name}")
                
            elif (len(collected_data)>(df_info.iloc[0]['Current_Week_Count']*0.90)) or (df_info.iloc[0]['Current_Week_Count']==None):
               await InputDataSource.Upload_Data(site_config.brand_name, collected_data,Sub_Category_1=site_config.Sub_Category_1,Sub_Category_2=site_config.Sub_Category_2)
               print(f"Collected data for {site_config.brand_name}")
            else:
                print(f"Collected data is Not Correct:: {site_config.brand_name}")
            # filename = f"{site_config.brand_name}_Stores_{today.strftime('%Y%m%d')}.xlsx"
            # full_path = os.path.join(working_dir, filename)
            # await asyncio.to_thread(collected_data.to_excel, full_path)
            return collected_data
        else:
            pd.DataFrame()
            
    except Exception as e:
        InputDataSource._log_strategy_error('collect_custom_site_data')
        print(f"Error in {site_config.brand_name}: {e}")
        
#-------------------------------THIS IS AN EXPERIMENT--------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------
async def run_single_site(site_name: str):
    """Run crawler for one specific site"""
    today = datetime.now()
    site_configs = SiteConfigLoader()._load_configs()
    # print(site_configs[site_name])
    if site_name not in site_configs:
        print(f"Site '{site_name}' not found in configs")
        return
    await collect_custom_site_data(site_configs[site_name], today)

async def run_program():
    """Run all crawlers (multi-site mode)"""
    today = datetime.now()
    site_configs = SiteConfigLoader()._load_configs()
    # print(site_configs)
    all_configs = list(site_configs.values())   # <-- run all
    # print(all_configs)
    await asyncio.gather(*[collect_custom_site_data(cfg, today) for cfg in all_configs],return_exceptions=True)
    print('------------------------ALL DONE----------------------------')

# ---------------------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    ##Manual Run
    # asyncio.run(run_single_site('shoppers_stop'))

    parser = argparse.ArgumentParser()
    parser.add_argument("--site", type=str, help="Run crawler for a specific site")
    args = parser.parse_args()

    if args.site:
        asyncio.run(run_single_site(args.site))
    else:
        asyncio.run(run_program())
