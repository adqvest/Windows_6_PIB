
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

india_time = timezone('Asia/Kolkata')
today      = datetime.now()
days       = timedelta(1)
yesterday = today - days

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import ClickHouse_db
# working_dir=r"C:\Users\Santonu\Adqvest_Crawler"
working_dir=r"C:\Users\Administrator\AdQvestDir\codes\Adqvest_Crawler"

from camoufox import AsyncNewBrowser
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from crawlee import Request
from crawlee.fingerprint_suite import (DefaultFingerprintGenerator,HeaderGeneratorOptions,ScreenOptions)
from typing_extensions import override
from crawlee.browsers import (BrowserPool,PlaywrightBrowserController,PlaywrightBrowserPlugin)

from store_locator_class_helper_functions import ExtractorLoader,CamoufoxPlugin,DropdownHandler,PopupHandler
logging.getLogger('crawlee').setLevel(logging.WARNING)

@dataclass
class Connections:
    mysql: any = None
    clickhouse: any = None
    
    def __post_init__(self):
        if self.mysql is None:
            self.mysql = adqvest_db.db_conn()
        if self.clickhouse is None:
            self.clickhouse = ClickHouse_db.db_conn()

@dataclass
class SiteConfig:
    # Required fields (no defaults)
    url: str
    company_name: str
    brand_name: str
    country: str
    data_container_selector: str
    address_selector: str

    # Optional fields (with defaults)
    site_id: Optional[str] = None
    state_options_selector: Optional[str] = None
    city_options_selector: Optional[str] = None
    
    skip_state_selector: bool = None
    skip_city_selector: bool = None
    skip_locality_selector: bool = None
    click_option_before_select: bool = None
    
    state_selector:  Optional[str] = None
    search_selector: Optional[str] = None
    city_selector: Optional[str] = None
    locality_selector: Optional[str] = None
    input_selector: Optional[str] = None
    input_option_selector: Optional[str] = None
    store_type_selector: Optional[str] = None
    links_selector: Optional[str] = None
    data_tab_selector: Optional[str] = None
    country_tab_selector: Optional[str] = None
    country_option_selector: Optional[str] = None
    pagination_selector: Optional[str] = None
    next_page_selector: Optional[str] = None
    
    back_button_selector: Optional[str] = None
    paginate_selector: Optional[str] = None
    suggested_option_selector: Optional[str] = None

    phone_selector: Optional[str] = None
    email_selector: Optional[str] = None
    hours_selector: Optional[str] = None

    filter_flag: Optional[List[str]] = None
    input_type: Optional[List[str]] = None
    
    cascade_wait_time: int = 3
    page_load_timeout: int = 60000
    overall_request: int = 300
    search_limit:int = None
    suggestion_search_limit:int = None
    
    cookie_reject_selector: Optional[str] = None
    popup_close_selectors: Optional[List[str]] = None
    custom_data_extractor: Optional[Callable] = None
    
    def __post_init__(self):
        if self.popup_close_selectors is None:
           self.popup_close_selectors = []

class SiteConfigLoader:
    def __init__(self,config_file: str ="Store_Locators_Site_Config.json"):

        self.config_file = Path(config_file)

    def _load_configs(self):
        """Load standard JSON (removes comment fields)"""
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                 data = json.load(f)

            sites=data.get('sites', [])

            configs={}
            for site in sites:
                if not site.get('enabled',True):
                   continue

                configs[site["site_id"]]=self._create_config_from_json(site)

            return configs

        except:
            print(f"Error processing: {self._log_strategy_error('_load_json')}")

    def _create_config_from_json(self,site_data:Dict[str, any]) -> SiteConfig:

        try:
            extractor_name = site_data.get('options', {}).get('custom_data_extractor')
            extractor_loader=ExtractorLoader()
            _custom_extractor=None
            if extractor_name:
                _custom_extractor = extractor_loader._get_extractor(extractor_name)

            else:
                _custom_extractor = extractor_loader._get_extractor('_universal_custom_extractor')

            config = SiteConfig(

                site_id=site_data.get('site_id'),
                url=site_data['url'],
                company_name=site_data['company_name'],
                brand_name=site_data['brand_name'],
                country=site_data['country'],

                #--------------------------SELECTOR INFO----------------------------------------------------------
                state_selector=site_data.get("selectors",{}).get('state_selector', None),
                city_selector=site_data.get("selectors",{}).get("city_selector",None),
                locality_selector=site_data.get("selectors",{}).get("locality_selector",None),

                state_options_selector=site_data.get("selectors",{}).get('state_options_selector',None),
                city_options_selector=site_data.get("selectors",{}).get('city_options_selector',None),

                data_container_selector=site_data.get("selectors",{})['data_container_selector'],
                address_selector=site_data.get("selectors",{})['address_selector'],
                
                search_selector=site_data.get("selectors",{}).get("search_selector",None),
                input_selector=site_data.get("selectors",{}).get("input_selector",None),
                input_option_selector=site_data.get("selectors",{}).get("input_option_selector",None),

                back_button_selector=site_data.get("selectors",{}).get("back_button_selector",None),
                paginate_selector=site_data.get("selectors",{}).get("paginate_selector",None),
                input_type=site_data.get("selectors",{}).get('input_type', None),
                suggested_option_selector=site_data.get("selectors",{}).get('suggested_option_selector', None),
                store_type_selector=site_data.get("selectors",{}).get('store_type_selector', None),
                links_selector=site_data.get("selectors",{}).get('links_selector', None),
                data_tab_selector=site_data.get("selectors",{}).get('data_tab_selector', None),
                country_tab_selector=site_data.get("selectors",{}).get('country_tab_selector', None),
                country_option_selector=site_data.get("selectors",{}).get('country_option_selector', None),
                pagination_selector=site_data.get("selectors",{}).get('pagination_selector', None),
                next_page_selector=site_data.get("selectors",{}).get('next_page_selector', None),
                #-------------------------------------------------------------------------------------------------
                filter_flag=site_data.get("options",{}).get('filter_flag', None),
                skip_state_selector=site_data.get("options",{}).get('skip_state_selector', False),
                skip_city_selector=site_data.get("options",{}).get('skip_city_selector', False),
                skip_locality_selector=site_data.get("options",{}).get('skip_locality_selector', False),
                click_option_before_select=site_data.get("options",{}).get('click_option_before_select', True),

                search_limit=site_data.get("options",{}).get('search_limit', None),
                suggestion_search_limit=site_data.get("options",{}).get('suggestion_search_limit', None),
                custom_data_extractor=_custom_extractor)

            return config

        except Exception as e:
            print(f"Error processing: {self._log_strategy_error('_create_config_from_json')}")

    def _log_strategy_error(self,strategy_name:str):
        exc_type, exc_obj, exc_tb = sys.exc_info()
        error_msg = str(exc_obj)
        line_no = exc_tb.tb_lineno if exc_tb else 'unknown'
        print(f"Error in strategy '{strategy_name}':\nError Message: {error_msg}\nLine: {line_no}\n")

class CascadingDataCollector:
    """Universal framework for collecting data from sites with cascading dropdowns"""
    
    def __init__(self, site_config: SiteConfig):
        self.config = site_config
        self.data = pd.DataFrame(columns=['Company', 'Brand','Sub_Brand','Address', 'State', 'City', 'Locality', 'Pincode','Latitude', 'Longitude', 'Country', 'Relevant_Date', 'Runtime'])
        self.crawler = None
        self.collection_date = datetime.now()
        # self.stealth_instance = stealth()
        self.dropdown_handler = DropdownHandler(site_config)
        self.popup_handler=PopupHandler(site_config)

        self.base_record={'Company': self.config.company_name,
                          'Brand': self.config.brand_name,
                          'Latitude': None,
                          'Longitude': None,
                          'Sub_Brand':None,
                          'Country': self.config.country,
                          'Relevant_Date': self.collection_date.date(),
                          'Runtime': self.collection_date}
                   
                    

        self._dropdown_type_cache = {}
        self._strategy_success_count = {}
        self._back_to_orginal_link=True
        # self.working_dir=r"C:\Users\Santonu\Adqvest_Crawler"
        self.working_dir=r"C:\Users\Administrator\AdQvestDir\codes\Adqvest_Crawler"


    async def _collect_data(self, today: datetime = None) -> pd.DataFrame:
        """Main data collection method"""
        if today is None:
            today = datetime.now()
        
        fingerprint_generator = DefaultFingerprintGenerator(
                                header_options=HeaderGeneratorOptions(browsers=['chromium']),
                                screen_options=ScreenOptions(min_width=1200, min_height=800),
                            )

        browser_new_context_options={
            # Security
            "permissions": [],  # Block all permissions
            "ignore_https_errors": True,
            "bypass_csp": True,
            
            # Stealth
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            # "viewport": {"width": 1920, "height": 1080},
            "locale": "en-US",
            "timezone_id": "America/New_York",
            
            # Performance
            "java_script_enabled": True,
            "accept_downloads": False,
            # 'args': ['--max-old-space-size=8192',  # Increase V8 heap size to 8GB (default is ~1.4GB)
            #          '--memory-pressure-off',       # Disable memory pressure notifications
            #         ],
        }

        self.crawler = PlaywrightCrawler(
                            request_handler=self._handle_request,
                            headless=False,            # Set to True for production
                            max_requests_per_crawl=20,  # We only need one page
                            browser_type='chromium',      # or 'chromium', 'firefox','webkit',

                            request_handler_timeout=timedelta(minutes=300),
                            fingerprint_generator=fingerprint_generator,
                            browser_new_context_options=browser_new_context_options

                            )


        # Add the target URL to the request queue
        await self.crawler.add_requests([self.config.url])
        try:
            # Start crawling
            # df_info=await self._get_data_info(self.config.brand_name)
            # Latest_Date=max(df_info['Relevant_Date']) if df_info.empty==False else today.date()
            Latest_Date=today.date()

            if (((today.date() - Latest_Date) >= timedelta(7)) | (Latest_Date==today.date())):

                await self.crawler.run([Request.from_url(self.config.url)])
                
                # Finalize and return data
                return self._finalize_data()
            else:
                print('------------------------------------------------------------------------')
                print(f"Brand:{self.config.brand_name}\nLast_Collected:{Latest_Date}")
            
        except Exception as e:
            print(f"Data collection failed: {e}")
            raise e

    async def _handle_request(self, context: PlaywrightCrawlingContext) -> None:
        """Main request handler"""
        print(f"Processing: {context.request.url}")
        
        try:
            # Enhanced navigation and popup handling
            await self.popup_handler._navigate_and_prepare_with_context(context)
            
            print('-------------------------------------Strategy INFO-------------------------------------------')
            #Option Selector Different means its a div dropdown 
            if self.config.state_options_selector:
                print("Data Collection type\n:Cascading Div Based")
                await self._collect_cascading_data_div_dropdowns(context)

            elif self.config.input_selector:
                print("Data Collection type\n:Input Based")
                await self._collect_input_based_data(context)

            elif self.config.paginate_selector:
                print("Data Collection type\n:Pagination")
                await self._collect_paginated_data(context)

            elif self.config.site_id=="kalyan_jewellers":
                print("Data Collection type\n:kalyan_jewellers")
                await self._collect_kalyan_data(context)

            elif self.config.links_selector:
                print("Data Collection type\n:Navigated Links")
                extracted_links = await context.extract_links(selector='div.india_mp a',label='CATEGORY')
                for link in extracted_links:
                    print(link.url)
                    await self._collect_nevigated_links_data(context,link)
                
            elif (not self.config.paginate_selector and
                  not self.config.input_selector and
                  not self.config.state_options_selector and
                  not self.config.state_selector and 
                  not self.config.links_selector):

                print("Data Collection type\n:One Short")
                await self._collect_oneshort_data(context)

            else:
                # Start cascading data collection
                print("Data Collection type\n:Simple Cascading")
                await self._collect_cascading_data(context)

        except Exception as e:
            print(f"Error processing: {self._log_strategy_error('_handle_request')}")
        

    #---------------------------COLLECTION FUNCTIONS---------------------------------------------------------------------------
    async def _collect_cascading_data(self, context: PlaywrightCrawlingContext):
        try:
            #----------------------------------------IF WE HAVE STORE CATEGORY|Specific to FirstCry--------------------------------------------------
            if self.config.store_type_selector:
               await context.page.wait_for_selector(self.config.store_type_selector, timeout=self.config.page_load_timeout)
               sub_brands = await self.dropdown_handler._get_native_options(context.page,selector=self.config.store_type_selector)
               sub_brands=[i for i in sub_brands if not re.search(r"select|all| select city",i,flags=re.IGNORECASE)]
               print(sub_brands)
               
               sub_brands=['FirstCry']
               for store_type in sub_brands:
                    print(f"Processing store type: {store_type}")

                    #SELECT CATEGORY
                    await self.dropdown_handler._select_native_option(context.page, self.config.store_type_selector, store_type)
                    await self._wait_for_cascade_load(context.page)

                    avl_states = await self.dropdown_handler._get_native_options(context.page, selector=self.config.state_selector)
                    # avl_states=avl_states[:2]

                    for state_index, state in enumerate(avl_states):
                        await self.dropdown_handler._select_native_option(context.page, self.config.store_type_selector, store_type)
                        await self.dropdown_handler._select_native_option(context.page, self.config.state_selector, state)

                        avl_city = await self.dropdown_handler._get_native_options(context.page,selector= self.config.city_selector)
                        # avl_city=avl_city[:1]

                        for city_index, city in enumerate(avl_city):
                            await self.dropdown_handler._click_option_selector(context.page, self.config.store_type_selector)
                            await self.dropdown_handler._select_native_option(context.page, self.config.store_type_selector, store_type)

                            await self.dropdown_handler._click_option_selector(context.page, self.config.state_selector)
                            await self.dropdown_handler._select_native_option(context.page, self.config.state_selector, state)

                            await self.dropdown_handler._click_option_selector(context.page, self.config.city_selector)
                            await self.dropdown_handler._select_native_option(context.page, self.config.city_selector, city)

                            if (self.config.search_selector):
                                await self._open_by_javascript(context.page, self.config.search_selector)

                            await self._extract_and_store_data(context.page, state, city, None,store_type)
                            await context.page.goto(self.config.url, wait_until='networkidle')   
            

            #-------------------NORMAL CONDITION WORKS FOR 99% Cases--------------------------------------------
            else:
                await self._collect_simple_state_city_locality_cascading_data(context.page)

        except Exception as e:
            print(f"Error processing: {self._log_strategy_error('_collect_cascading_data')}")
                
    async def _collect_simple_state_city_locality_cascading_data(self, page):
    
        #example Puma Stores
        if self.config.data_tab_selector:
            await self._open_by_javascript(page, self.config.data_tab_selector)

        # Get all states
        #-------------------------------------------------------------------------------------------------------------------------
        states = await self.dropdown_handler._get_native_options(page,selector=self.config.state_selector)
        print(states)
        if len(states)>1:
           states=[i for i in states if not re.search(r"select|all| select city",i,flags=re.IGNORECASE)]
           states=[re.sub(r'Location\s*', '', i).strip() for i in states]
        
           
        print(states)
        print(f"Found {len(states)} states")
        #Santonu
        # states=states[0:2]
        for state_index, state in enumerate(states):
            print(f"\nProcessing state {state_index + 1}/{len(states)}: {state}")

            #------------------------IF WE HAVE ALL IN ONE PAGE ------------------------------------
            if self.config.search_limit:
                if state_index==self.config.search_limit:
                   break
            
            try:
                # Select state|Skip if Required
                if not self.config.skip_state_selector:
                    self._back_to_orginal_link=True
                    try:
                        await page.wait_for_selector(self.config.state_selector, timeout=self.config.page_load_timeout)
                    except Exception as e:
                        print(self._log_strategy_error('_collect_cascading_data'))

                    await self.dropdown_handler._select_native_option(page, options_selector=self.config.state_selector, option=state)
                    try:
                        await self._wait_for_cascade_load(context.page)
                    except:
                        pass
                   
                # Moved to City|Skip if Required
                if self.config.city_selector:
                    # Get cities for this state
                    self._back_to_orginal_link=False
                    cities = await self.dropdown_handler._get_native_options(page,selector= self.config.city_selector)
                    cities=[i for i in cities if not re.search(r"select|all|select city",i,flags=re.IGNORECASE)]   
                    print(cities)
                    print(f"Found {len(cities)} cities in {state}")
                    #Santonu
                    # cities=cities[:2]
                    
                    for city_index, city in enumerate(cities):
                        print(f"Processing city {city_index + 1}/{len(cities)}: {city}")
                        
                        try:
                            if self.config.data_tab_selector:
                               await self._open_by_javascript(page, self.config.data_tab_selector)

                            # Select city|Skip if Required
                            if not self.config.skip_city_selector:
                                await self.dropdown_handler._select_native_option(page, options_selector=self.config.city_selector, option=city)
                                await self._wait_for_cascade_load(page)

                            if (not self.config.locality_selector and self.config.search_selector):
                                print('----------------------HERE-')
                                click_success = await self._open_by_javascript(page, self.config.search_selector)

                            # Handle locality if configured
                            if self.config.locality_selector:
                                await self._process_localities(page, state, city)
                            else:
                                # Extract data directly
                                await self._extract_and_store_data(page, state, city, None)
                                if self.config.pagination_selector:
                                   await self._get_panination_info(page,self.config.pagination_selector,state,city)

                                
                        except Exception as e:
                            print(f"Error processing city {city}: {self._log_strategy_error('_collect_simple_state_city_locality_cascading_data')}")
                            continue
                else:
                    if self.config.search_selector:
                        await self._open_by_javascript(page, self.config.search_selector)

                    # Extract data directly
                    await self._extract_and_store_data(page, state, None, None)


            except Exception as e:
                print(f"Error processing state {state}: {self._log_strategy_error('_collect_simple_state_city_locality_cascading_data')}")
                continue
    
    async def _get_panination_info(self,page,selector,state=None, city=None):
        page_counter = 1
        max_attempts = 5
        while page_counter <= max_attempts:
            await self._open_by_javascript(page, selector)
            await self._extract_and_store_data(page, state, city, None)
            page_counter += 1

    async def _collect_cascading_data_div_dropdowns(self, context: PlaywrightCrawlingContext):
        print("Starting cascading data collection with div dropdowns...")
        
        try:
            #kalyan
            if self.config.country_tab_selector:
               country=await self.dropdown_handler._get_dropdown_options(context.page, self.config.country_tab_selector,self.config.country_option_selector)
               country=[i for i in country if  re.search(r"India",i,flags=re.IGNORECASE)] 
               print(country)
               await self.dropdown_handler._select_dropdown_option(context.page,self.config.country_tab_selector, country[0],self.config.country_option_selector)

            #example Puma Stores
            if self.config.data_tab_selector:
                await self._open_by_javascript(context.page, self.config.data_tab_selector)

            
            context.page.set_default_navigation_timeout(self.config.page_load_timeout*0.5)
            # Get all states
            states = await self.dropdown_handler._get_dropdown_options(context.page,self.config.state_selector, self.config.state_options_selector)# New config parameter
            states=[i for i in states if not re.search(r"select|all|india|usa",i,flags=re.IGNORECASE)] 
            print(states)   


            print(f"Found {len(states)} states")
            #Santonu
            # states = states[6:8]  # Limit for testing
            
            for state_index, state in enumerate(states):
                print(f"\nProcessing state {state_index + 1}/{len(states)}: {state}")
                if self.config.search_limit:
                    if state_index==self.config.search_limit:
                       break

                try:
                    if not self.config.skip_state_selector:
                        await self.dropdown_handler._select_dropdown_option(context.page, self.config.state_selector, state,self.config.state_options_selector)
                        await self._wait_for_cascade_load(context.page)
                    
                    # Handle City if configured
                    if self.config.city_selector:
                        await context.page.wait_for_selector(self.config.city_selector, timeout=self.config.page_load_timeout)
                        cities = await self.dropdown_handler._get_dropdown_options(context.page, self.config.city_selector, self.config.city_options_selector)
                        cities=[i for i in cities if not re.search(r"select|all",i,flags=re.IGNORECASE)]     
                        print(f"Found {len(cities)} cities in {state}")
                        #Santonu
                        # cities = cities[:2]  # Limit for testing
                        
                        for city_index, city in enumerate(cities):
                            print(f"Processing city {city_index + 1}/{len(cities)}: {city}")
                            
                            try:
                                if not self.config.skip_city_selector:
                                    await self._wait_for_cascade_load(context.page)
                                    await self.dropdown_handler._select_dropdown_option(context.page, self.config.city_selector, city,self.config.city_options_selector)
                                    
                                
                                if (self.config.locality_selector==None and self.config.search_selector):
                                    await self._open_by_javascript(context.page, self.config.search_selector)
                                    print('-----------------------------------------------------------------')
            
                                # Handle locality if configured
                                if self.config.locality_selector:
                                    await self._process_localities_div(context.page, state, city)
                                else:
                                    # Extract data directly
                                    await self._extract_and_store_data(context.page, state, city, None)
                                    
                            except Exception as e:
                                print(f"Error processing city {city}: {self._log_strategy_error('_collect_cascading_data_div_dropdowns')}")
                                continue
                    else:
                        if self.config.search_selector:
                            await self._open_by_javascript(context.page, self.config.search_selector)
                            await self._wait_for_cascade_load(context.page)

                        await self._extract_and_store_data(context.page, state,None, None)

                                
                except Exception as e:
                    print(f"Error processing state {state}: {self._log_strategy_error('_collect_cascading_data_div_dropdowns')}")

                    continue
        except:
            print(f"Error processing: {self._log_strategy_error('_collect_cascading_data_div_dropdowns')}")

    async def _collect_input_based_data(self,context: PlaywrightCrawlingContext):
        
        required_inputs=await self._get_input_data(self.config.input_type)
        # required_inputs=required_inputs[:2]

        for index,txt_input in enumerate(required_inputs):

            print(f"Processing {self.config.input_type[0]} {index + 1}/{len(required_inputs)}: {txt_input}")
            if self.config.search_limit:
                if index==self.config.search_limit:
                   break
                   
            try:
                await self._js_clear_and_input(context.page,self.config.input_selector,str(txt_input))

                if self.config.suggested_option_selector:
                    options = await self.dropdown_handler._get_custom_options(context.page,dropdown_selector=self.config.input_selector,options_selector=self.config.suggested_option_selector)
                    print(options)
                    for option_index,option in enumerate(options):
                        print(f"---------------------------{option}")
                        # await self._wait_for_cascade_load(context.page)
                        if option_index==self.config.suggestion_search_limit:
                            break
                        #--------------------------------------------------------------------------------------------------
                        # For sites where we need to click option selector to activate other options
                        if option_index>0:
                           await self.dropdown_handler._click_option_selector(context.page, self.config.input_selector)

                        await self.dropdown_handler._select_custom_option(context.page, options_selector=self.config.suggested_option_selector, option=option,dropdown_selector=self.config.input_selector)
                        
                        #------------Customize for Sketchers-----------------------------------------------------------
                        if self.config.input_option_selector:
                           await self.dropdown_handler._select_native_option(context.page,option="300",options_selector=self.config.input_option_selector)


                        if (self.config.search_selector):
                            print("----------HERE IN CLICK-------------")
                            await self.dropdown_handler._click_option_selector(context.page, self.config.search_selector)
                            await self._wait_for_cascade_load(context.page) 

                        await self._extract_and_store_data(context.page, None,None, None) 

                else:
                    if self.config.search_selector:
                        try:
                            await self._open_by_javascript(context.page, self.config.search_selector)
                            await self._wait_for_cascade_load(context.page)
                        except:
                            await self.dropdown_handler._click_option_selector(context.page, self.config.search_selector)
                            await self._wait_for_cascade_load(context.page)

                    await self._extract_and_store_data(context.page, None,None, None) 

            except Exception as e:
               print(f"Error processing {self.config.input_type[0]}: {self._log_strategy_error('_collect_input_based_data')}")

    async def _collect_paginated_data(self,context: PlaywrightCrawlingContext):
        
        processed_pages = set()
        page_counter = 1
        max_attempts = 10

        while page_counter <= max_attempts:
            try:
                available_pages=await self._get_available_pages(context.page,self.config.paginate_selector)
                new_pages = [p for p in available_pages if p not in processed_pages]
                print(new_pages)
                if not new_pages:
                    break

                for page_index,page_num in enumerate(new_pages):
                    print(f"Processing Page {page_num}/{len(new_pages)}: {page_num}")
                    await self._navigate_to_page(context.page, f"li[data-lp='{page_num}'] a")
                    await self._extract_and_store_data(context.page, None, None, None)
                    processed_pages.add(page_num)
                   
                next_button = await context.page.query_selector(self.config.next_page_selector)
                if next_button:
                    await next_button.click()
                    await self._wait_for_cascade_load(context.page)
                    print("Successfully clicked next button")
                    available_pages=await self._get_available_pages(context.page,self.config.next_page_selector)
                    print(available_pages)

                page_counter += 1

            except Exception as e:
                print(f"Error processing page: {self._log_strategy_error('_collect_paginated_data')}")


    async def _collect_oneshort_data(self,context: PlaywrightCrawlingContext):
        
        await context.page.route("**/*", lambda route: (
        route.abort() if route.request.redirected_from else route.continue_()
    ))

        await self._extract_and_store_data(context.page, None, None, None)
    

    async def _collect_nevigated_links_data(self,context: PlaywrightCrawlingContext,link):
        new_page = await context.page.context.new_page()
        await new_page.goto(link.url)
        await new_page.wait_for_load_state('networkidle')
            
        # Extract data
        await self._extract_and_store_data(new_page, None, None, None)
        await new_page.close()
    
    async def _collect_kalyan_data(self,context: PlaywrightCrawlingContext):
        try:
            self._back_to_orginal_link=False
            await self._wait_for_cascade_load(context.page)
            await self._open_by_javascript(context.page,self.config.search_selector)
            city_links = await context.extract_links(selector=self.config.links_selector,label='KALYAN_MAJOR_CITY')
            for link in city_links[:1]:
                print(link.url)
                await context.page.goto(link.url)
                await context.page.wait_for_load_state('networkidle')
                await self._wait_for_cascade_load(context.page)
                await self._open_by_javascript(context.page, "div.text-center.mt-5 button.rounded-lg")
    
                # Now context.extract_links will work because it uses context.page
                locality_links = await context.extract_links(selector="div.addreddDiv div.ant-row.css-ut69n1 a", label='KALYAN_LOCALITY')
                
                for link2 in locality_links:
                    print(link2.url)
                    await context.page.goto(link2.url)
                    await context.page.wait_for_load_state('networkidle')
                    await self._wait_for_cascade_load(context.page)
                    
                    stores_links = await context.extract_links(selector="div.cardCityName div.ant-col a", label='KALYAN_TOWN')
                    for link3 in stores_links:
                        await self._collect_nevigated_links_data(context,link3)

        except Exception as e:
            print(f"Error processing: {self._log_strategy_error('_collect_kalyan_data')}")
    #--------------------------------------------------------------------------------------------
    async def _get_available_pages(self, page_web,selector):
        try:
            pages=await self.dropdown_handler._get_native_options(page_web,selector=selector)
            pages=[i for i in pages if not re.search(r"«|all|»",i,flags=re.IGNORECASE)]  
            return pages

        except Exception as e:
                print(f"Error processing\n: {self._log_strategy_error('_get_available_pages')}")
                return []

    async def _navigate_to_page(self, page_web, page_number):
        """Navigate to specific page number"""
        try:
            try:
               await page_web.wait_for_selector(self.config.data_container_selector, timeout=5000)
            except:
                print(f"Error in:{self.config.data_container_selector}")

            # Select the page number
            await self.dropdown_handler._click_option_selector(page_web,page_number)
            return True

        except Exception as e:
            print(f"Error processing page: {self._log_strategy_error('_navigate_to_page')}")
            return False
    #------------------------------------------------------------------------------------------------------
    async def _js_clear_and_input(self,page, selector, text):
        try:
            await page.wait_for_selector(selector, state='visible', timeout=self.config.page_load_timeout*0.60)
        except:
            pass

        
        element = page.locator(selector)
        await element.clear()
        await element.fill(text)
        await asyncio.sleep(10) 
        print('-------Cleared Input fileld-----------')
    #------------------------------------------------------------------------------------------------------
    async def _open_by_javascript(self, page, selector):
        """Open dropdown by JavaScript click with force"""
        try:
            result = await page.evaluate(f"""
                () => {{
                    const element = document.querySelector('{selector}');
                    if (!element) return false;
                    
                    try {{
                        element.click();
                        return true;
                    }} catch (e) {{
                        console.log('JS click failed:', e);
                        return false;
                    }}
                }}
            """)
            
            if not result:
                return False
            
        except Exception as error:
            print(f"Error processing: {self._log_strategy_error('_open_by_javascript')}")
            return False

    async def _process_localities(self, page, state: str, city: str):
        """Process localities if site has state → city → locality cascade"""
        try:
            await self._wait_for_cascade_load(context.page)
        except:
            pass  

        print('----------------------------------------LOCALITIES----------------------------------------------')
        localities = await self.dropdown_handler._get_native_options(page,selector=self.config.locality_selector)
        print(f"Found {len(localities)} localities in {city}")
        
        for locality_index, locality in enumerate(localities):
            print(f"Processing locality {locality_index + 1}/{len(localities)}: {locality}")
            
            try:
                if not self.config.skip_locality_selector:
                       await self.dropdown_handler._select_native_option(page, options_selector=self.config.locality_selector, option=locality)
                       await self._wait_for_cascade_load(page)

            
                if self.config.search_selector:
                        await self._open_by_javascript(page, self.config.search_selector)
                        await self._wait_for_cascade_load(page)
                        
                # Extract data
                await self._extract_and_store_data(page, state, city, locality)
                
            except Exception as e:
                print(f"Error processing locality {locality}: {self._log_strategy_error('_process_localities')}")
                continue

    #------------------------------------------------------------------------------------------------------
    async def _wait_for_cascade_load(self, page):
        """Wait for cascade loading to complete"""
        try:
            await page.wait_for_load_state('networkidle', timeout=10000)
        except:
            pass
        
        # Additional wait time
        await asyncio.sleep(self.config.cascade_wait_time)
    #-----------------------------------------------------------------------------------------------------
    async def _extract_and_store_data(self, page, state: Optional[str], city:  Optional[str], locality: Optional[str],sub_brand: Optional[str]=None):
        """Extract data from the current page and store it"""
        
        try:
            print('-----------------------------------------------------------DATA COLLECTION-----------------------------------------------------------------')
            # original_url=page.url
            print(page.url)

            #-----------------------------------For ASICS------------------------------------------
            if self.config.site_id=='Asics':
                # page = await page.new_page()
                await page.goto(self.config.url)
                await page.wait_for_load_state('networkidle')

            # Get page content
            await self._wait_for_cascade_load(page)
            try:
                await page.wait_for_selector(self.config.data_container_selector, timeout=self.config.page_load_timeout)
            except:
                pass

            html = await page.content()
            soup = BeautifulSoup(html, 'html.parser')

            # Find data containers
            containers = soup.select(self.config.data_container_selector)
            # print(containers)
            
            if not containers:
                if city:
                   print(f"No data found for {city}" + (f" - {locality}" if locality else ""))
                return
            
            print(f"Found {len(containers)} records")
            for container in containers:
                # Use custom extractor if provided
                if self.config.custom_data_extractor:
                    record = self.config.custom_data_extractor(container, state, city, locality,self.config.address_selector,self.config.filter_flag,self.base_record,self.config.site_id)
                
                
                if record and isinstance(record,list):
                    # record=[self.base_record.update(i) for i in record]
                    self.data=pd.concat([self.data,pd.DataFrame(record)])
                    if sub_brand:
                        self.data['Sub_Brand']=sub_brand

            
            if ((page.url != self.config.url) and (not self.config.data_tab_selector) and (self._back_to_orginal_link)):
               print('Page url changed-----------Going Back to Orginal')
               await page.goto(self.config.url, wait_until='networkidle')    

            if self.config.back_button_selector:
                  await self._open_by_javascript(page, self.config.back_button_selector)
                    
        except Exception as e:
            print(f"Error extracting data: {self._log_strategy_error('_extract_and_store_data')}")
    
    def _finalize_data(self) -> pd.DataFrame:
        """Clean and finalize the collected data"""
        print(f"\nData collection complete! Found {len(self.data)} records") 

        # Remove duplicates
        initial_count = len(self.data)
        self.data = self.data.drop_duplicates(subset='Address', keep='last')
        final_count = len(self.data)
        
        if initial_count != final_count:
            print(f"Removed {initial_count - final_count} duplicate records")
        
        return self.data

    async def _get_input_data(self,input_type):
        try:
            conn = Connections()
            if len(input_type)==1:
                if re.search(r"pincode",input_type[0],flags=re.IGNORECASE):
                   req_col=('Pincode','Pincode_Rank')

                elif re.search(r"city",input_type[0],flags=re.IGNORECASE):
                   req_col=('City','City_Rank')

                input_df=pd.read_sql(f"select Distinct {req_col[0]} from STORE_LOCATOR_WEEKLY_DATA_INPUT_TABLE_STATIC group by {req_col[1]} order by {req_col[1]} asc;",conn.mysql)
                os.chdir(self.working_dir)
                return input_df[req_col[0]].to_list()


            if len(input_type)>1:
                column_string = ", ".join(input_type)
                query = f"select Distinct {column_string} from STORE_LOCATOR_WEEKLY_DATA_INPUT_TABLE_STATIC where State is not NULl or City is not Null group by Pincode_Rank order by Pincode_Rank asc;"
                input_df=pd.read_sql(query,conn.mysql)
                input_df['Serach']=input_df[input_type[0]]+' '+input_df[input_type[1]]+' '+input_df[input_type[2]]
                os.chdir(self.working_dir)
                return input_df['Serach'].to_list()

            
        except Exception as e:
            print(f"Error processing: {self._log_strategy_error('_get_input_data')}")

    @staticmethod
    async def Upload_Data(brand: str,data):
        conn = Connections()
        db_max_date = pd.read_sql(f"select max(Relevant_Date) as Max from STORE_LOCATOR_WEEKLY_DATA where Brand = '{brand}';",conn.mysql)["Max"][0]
        if db_max_date==None:
           db_max_date=yesterday.date()

        data=data.loc[data['Relevant_Date'] > db_max_date]
        data.to_sql('STORE_LOCATOR_WEEKLY_DATA', con=conn.mysql, if_exists='append', index=False)
        print(data.info())

        click_max_date = conn.clickhouse.execute(f"select max(Relevant_Date) from STORE_LOCATOR_WEEKLY_DATA where Brand = '{brand}'")
        click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])

        query =f"select * from AdqvestDB.STORE_LOCATOR_WEEKLY_DATA where Brand ='{brand}' and Relevant_Date >'{click_max_date}'"
        df = pd.read_sql(query, conn.mysql)
        conn.clickhouse.execute("INSERT INTO AdqvestDB.STORE_LOCATOR_WEEKLY_DATA VALUES", df.values.tolist())
        print(f'To CH: {len(df)} rows')
        print(f"Data uploaded to SQL & Clickhouse------>{brand}")

    async def _get_data_info(self, brand: str):
        try:
            conn = Connections()
            query="""SELECT Brand, Relevant_Date,count(*) as Week_Count,Rank
                    FROM (
                        SELECT 
                            Brand,
                            Relevant_Date,
                            DENSE_RANK() OVER (PARTITION BY Brand ORDER BY Relevant_Date DESC) as Rank
                        FROM AdqvestDB.STORE_LOCATOR_WEEKLY_DATA
                        WHERE Brand IS NOT NULL AND Brand != ''
                    ) ranked
                    group by Brand, Relevant_Date,Rank
                    ORDER BY Brand,Relevant_Date DESC;
                            """

            data = conn.clickhouse.execute(query)
            df = pd.DataFrame(data, columns=["Brand", "Relevant_Date", "Week_Count", "Rank"])

            df_prev_col=df[df['Rank']==2]
            df_cur=df[df['Rank']==1]
            df_cur=df_cur.merge(df_prev_col,on='Brand')
            df_cur['Pct_Change_Count']=round(((df_cur['Week_Count_x']/df_cur['Week_Count_y'])-1)*100)

            df_cur=df_cur[['Brand', 'Relevant_Date_x', 'Week_Count_x','Week_Count_y', 'Pct_Change_Count']]
            df_cur.columns=['Brand','Relevant_Date','Current_Week_Count','Prev_Week_Count','Pct_Change_Count']
            df_cur=df_cur[df_cur['Brand']==brand]
            os.chdir(self.working_dir)
            return df_cur
        
        except:
            print(f"Error processing: {self._log_strategy_error('_get_data_info')}")

    def _log_strategy_error(self,strategy_name:str):
        exc_type, exc_obj, exc_tb = sys.exc_info()
        error_msg = str(exc_obj)
        line_no = exc_tb.tb_lineno if exc_tb else 'unknown'
        print(f"Error in strategy '{strategy_name}':\nError Message: {error_msg}\nLine: {line_no}\n")
   #------------------------------------------------------------------------------------------------------


