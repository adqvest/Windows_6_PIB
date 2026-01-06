
import pandas as pd
import os
import asyncio
import logging
import sys

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
working_dir=r"C:\Users\Santonu\Adqvest_Crawler"
# working_dir=r"C:\Users\Administrator\AdQvestDir\codes\Adqvest_Crawler"
os.chdir(working_dir)

logging.getLogger('crawlee').setLevel(logging.WARNING)
from store_collector import CascadingDataCollector
from site_config import SiteConfigLoader
from search_data_provider  import InputDataSource
from custom_extractors import dedup_similar_address

#--------------------------------------------------------------------------------------------------------------
async def collect_custom_site_data(site_config, today: datetime = None) -> pd.DataFrame:
    """Collect data from any custom site"""
    try:
        collector = CascadingDataCollector(site_config)
        collected_data=await collector._collect_data(today)
        if site_config.brand_name=='shoppers_stop':
            collected_data=dedup_similar_address(collected_data)

        # await InputDataSource.Upload_Data(site_config.brand_name, collected_data)

        print('--------------------------------------------COLLECTION INFO---------------------------------------------------------------')
        # df_info=await InputDataSource.get_data_info(site_config.brand_name)
        # print(df_info)

        filename = f"{site_config.brand_name}_Stores_{today.strftime('%Y%m%d')}.xlsx"
        full_path = os.path.join(working_dir, filename)
        # await asyncio.to_thread(collected_data.to_excel, full_path)
        # print(f"{site_config.brand_name}: {len(collected_data)} stores collected and saved")
        # return f"{site_config.brand_name}: {len(collected_data)} stores collected and saved"
        return collected_data
    except Exception as e:
        print(e)
    
#---------------------------------------------------------------------------------------------------------------
# Main execution function
async def run_program():

    today = datetime.now()
    site_configs=SiteConfigLoader()
    all_configs=site_configs._load_configs()   
    # work=['H&M','Asics','Nike','Decathlon','V2','pizza_hut','Puma','Zara','uniqlo','go_colors','lifestyle','styleup']   


    # all_configs=[v for k,v in all_configs.items() if k in work]
    all_configs=[all_configs['melorra']]
    # print(all_configs)
    
    
    # for i in work:
    #     all_configs1=[v for k,v in all_configs.items() if k in [i]]
    #     results = await collect_custom_site_data(all_configs1[0], today)


    #----------------------------------------------------------------------------------------------------------------------------------    
    results = await asyncio.gather(*[collect_custom_site_data(config, today) for config in all_configs],return_exceptions=True)

    print('------------------------ALL DONE----------------------------')

# Run the framework
if __name__ == "__main__":
    asyncio.run(run_program())