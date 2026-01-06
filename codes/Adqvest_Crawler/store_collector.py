# -*- coding: utf-8 -*-
"""
Created on Mon Aug  4 17:34:08 2025
@author: Santonu
"""
import json
import pandas as pd
import os
import asyncio
import re
from bs4 import BeautifulSoup
from typing import Dict, List, Optional, Callable,Union,Any  
from dataclasses import dataclass, asdict
import aiohttp
import logging
import sys

from datetime import timedelta  
from  datetime import datetime
from pytz import timezone

india_time = timezone('Asia/Kolkata')
today      = datetime.now()
days       = timedelta(1)
yesterday = today - days

from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from crawlee import Request
from crawlee.fingerprint_suite import (DefaultFingerprintGenerator,HeaderGeneratorOptions,ScreenOptions)
from abc import ABC, abstractmethod

from search_data_provider  import InputDataSource
from store_locator_helpers import CamoufoxPlugin,DropdownHandler,PopupHandler,BrowserManager,PageNavigator,RefreshManager
logging.getLogger('crawlee').setLevel(logging.WARNING)

# ================================================================================================
@dataclass
class CollectionContext:
    crawlee_context: PlaywrightCrawlingContext
    state: Optional[str] = None
    city: Optional[str] = None
    pincode: Optional[str] = None
    locality: Optional[str] = None
    sub_brand: Optional[str] = None
    Sub_Category_1: Optional[str] = None
    _back_to_original_link: bool = True
    extracted_links_lyr1:Optional[list] = None
    click_addional_view=True #For Apollo_Pharmacy
    not_collcted_links = []
    request_params: Optional[Dict[str, any]] = None
    request_url:Optional[str] = None
    
    @property
    def page(self):
        """Direct access to page"""

        return self.crawlee_context.page
    
    @property 
    def request(self):
        """Direct access to request"""
        return self.crawlee_context.request
    
    async def extract_links(self, *args, **kwargs):
        """Maintain extract_links functionality"""
        return await self.crawlee_context.extract_links(*args, **kwargs)
    
    async def enqueue_links(self, *args, **kwargs):
        """Maintain enqueue_links functionality"""
        return await self.crawlee_context.enqueue_links(*args, **kwargs)
    
    def __str__(self):
        """Human-readable representation for logging"""
        location_parts = []
        if self.state: location_parts.append(f"State: {self.state}")
        if self.city: location_parts.append(f"City: {self.city}")
        if self.locality: location_parts.append(f"Locality: {self.locality}")
        
        location = " | ".join(location_parts) if location_parts else "No location set"
        return f"CollectionContext({location})"

@dataclass
class ProgressReport:
    current_strategy: Optional[str] = None
    current_state: Optional[str] = None
    current_city: Optional[str] = None
    current_locality: Optional[str] = None
    current_pincode: Optional[str] = None
    current_sub_brand: Optional[str] = None
    
    # Progress tracking
    states_completed: List[str] = None
    states_fully_completed: List[str] = None
    locality_fully_completed: List[str] = None
    cities_completed: Dict[str, List[str]] = None
    cities_fully_completed: List[str] = None
    localities_completed: Dict[str, Dict[str, List[str]]] = None
    inputs_completed: List[str] = None
    links_completed: List[str] = None
    latitude_completed: List[str] = None
    
    # Pagination tracking
    current_page: int = 1
    
    # Timestamps
    last_refresh_time: float = 0
    # session_start_time: float = 0
    
    def __post_init__(self):
        if self.states_completed is None:
            self.states_completed = []
        if self.states_fully_completed is None:
            self.states_fully_completed = []
        if self.locality_fully_completed is None:
            self.locality_fully_completed = []
        if self.cities_fully_completed is None:
            self.cities_fully_completed = []            
        if self.cities_completed is None:
            self.cities_completed = {}
        if self.localities_completed is None:
            self.localities_completed = {}
        if self.inputs_completed is None:
            self.inputs_completed = []
        if self.links_completed is None:
            self.links_completed = []
        if self.latitude_completed is None:
            self.latitude_completed = []
       
@dataclass
class CollectionResult:
    """Result of a collection operation"""
    success: bool
    records_collected: int
    data: pd.DataFrame
    errors: List[str]
    context: CollectionContext
# ================================================================================================
# PROGRESS MANAGER
# ================================================================================================
class ProgressManager:

    def __init__(self, brand_name: str, progress_info: Optional[ProgressReport] = None,  state_file_path: str = None):
        self.brand_name = brand_name
        self.state_file_path = state_file_path or f"Json_Data_Backup/{brand_name}_{today.strftime('%Y-%m-%d')}_scraping_state.json"
        self.backup_file_path = f"Json_Data_Backup/{brand_name}_{today.strftime('%Y-%m-%d')}_scraping_state_backup.json"
        
        if progress_info:
           self.progress_info=progress_info
   
    def save_progress(self, progress_info: ProgressReport):
        try:
            if os.path.exists(self.state_file_path):
                if os.path.exists(self.backup_file_path):
                    os.remove(self.backup_file_path)
                os.rename(self.state_file_path, self.backup_file_path)
            
            # Save new state
            with open(self.state_file_path, 'w') as f:
                state_dict = asdict(progress_info)
                json.dump(state_dict, f, indent=2, default=str)
            
            print(f"State saved: {progress_info.current_state}/{progress_info.current_city}")
        except Exception as e:
            print(f"Error saving state: {e}")
    
    @classmethod
    def load_progress(self, brand_name:str) -> Optional[ProgressReport]:
        """Load existing scraping state"""
        try:
            state_file_path = f"Json_Data_Backup/{brand_name}_{today.strftime('%Y-%m-%d')}_scraping_state.json"
            if os.path.exists(state_file_path):
                with open(state_file_path, 'r') as f:
                    state_dict = json.load(f)
                    return ProgressReport(**state_dict)
        except Exception as e:
            print(f"Error loading state: {e}")
            # Try backup file
            backup_file_path = f"Json_Data_Backup/{brand_name}_{today.strftime('%Y-%m-%d')}_scraping_state_backup.json"
            try:
                if os.path.exists(backup_file_path):
                    with open(backup_file_path, 'r') as f:
                        state_dict = json.load(f)
                        return ProgressReport(**state_dict)
            except Exception as e2:
                print(f"Error loading backup state: {e2}")
        
        return None

    @classmethod
    def load_backup_data(self, brand_name:str) -> pd.DataFrame:
        """Load existing scraping state"""
        backup_data_file_path = f"Json_Data_Backup/{brand_name}_timed_backup_{today.strftime('%Y-%m-%d')}.xlsx"
        try:
            if os.path.exists(backup_data_file_path):
                df=pd.read_excel(backup_data_file_path)
            else:
                df=pd.DataFrame(columns=['Category','Company','Sub_Category_1','Sub_Category_2', 'Brand', 'Sub_Brand', 'Dealer_Name', 'Address', 'State', 'City', 'Locality', 'Pincode', 'Latitude', 'Longitude','Crawler', 'Country', 'Relevant_Date', 'Runtime'])

        except Exception as e:
                print(f"Error In Loading Backup data: {e}")
                df=pd.DataFrame(columns=['Category','Company','Sub_Category_1','Sub_Category_2', 'Brand', 'Sub_Brand', 'Dealer_Name', 'Address', 'State', 'City', 'Locality', 'Pincode', 'Latitude', 'Longitude','Crawler', 'Country', 'Relevant_Date', 'Runtime'])
        return df

    @classmethod
    def save_backup_data(self, brand_name:str,df):
        """Load existing scraping state"""
        backup_data_file_path = f"Json_Data_Backup/{brand_name}_timed_backup_{today.strftime('%Y-%m-%d')}.xlsx"
        try:
            df.to_excel(backup_data_file_path, index=False)
        except Exception as e:
            print(f"Error In Saving Backup Data: {e}")
            
    def clear_progress(self):
        """Clear saved state files"""
        for file_path in [self.state_file_path, self.backup_file_path]:
            if os.path.exists(file_path):
                os.remove(file_path)
    
    def update(self, level: str, item: str, parent: str = None, subparent: str = None):

        if level == "state":
            self.progress_info.current_state = item
            if item not in self.progress_info.states_completed:
                self.progress_info.states_completed.append(item)

        elif level == "city" and parent:
            self.progress_info.current_city = item
            self.progress_info.cities_completed.setdefault(parent, [])
            if item not in self.progress_info.cities_completed[parent]:
                self.progress_info.cities_completed[parent].append(item)

        elif level == "locality" and parent and subparent:
            self.progress_info.current_locality = item
            self.progress_info.localities_completed.setdefault(parent, {})
            self.progress_info.localities_completed[parent].setdefault(subparent, [])

            if item not in self.progress_info.localities_completed[parent][subparent]:
                self.progress_info.localities_completed[parent][subparent].append(item)

        elif level == "custom_input":
            self.progress_info.inputs_completed.append(item)

        elif level == "custom_links":
            self.progress_info.links_completed.append(item)

        elif level == "latitude":
            self.progress_info.latitude_completed.append(item)
            
        self.save_progress(self.progress_info)
# ================================================================================================
# DATA MANAGEMENT
# ================================================================================================
class DataManager:    
    def __init__(self, config,BrowserManager, base_record,progress_manager: ProgressManager):
        self.progress_manager = progress_manager
        self.config = config
        self.base_record = base_record
        self.page_navigator = PageNavigator(config)
        self._log_strategy_error=InputDataSource._log_strategy_error
        self.make_custom_request=BrowserManager.make_custom_request
        
        if len(self.progress_manager.load_backup_data(self.config.site_id))>0:
            self.data =self.progress_manager.load_backup_data(self.config.site_id)
        else:
            self.data = pd.DataFrame(columns=['Category','Company','Sub_Category_1','Sub_Category_2', 'Brand', 'Sub_Brand', 'Dealer_Name', 'Address', 'State', 'City', 'Locality', 'Pincode', 'Latitude', 'Longitude','Crawler', 'Country', 'Relevant_Date', 'Runtime'])
    
    async def extract_page_data(self, context: CollectionContext) -> CollectionResult:
        try:
            print('-----------------------------------------------------------DATA COLLECTION-----------------------------------------------------------------')
            print(context.page.url)
            if self.config.site_id in ['Asics','manyavar_mohey'] :
                await context.page.goto(self.config.url)
                await self.page_navigator.wait_for_cascade_load(context.page)
            
            # Get page content
            await self.page_navigator.wait_for_cascade_load(context.page)
            try:
                await context.page.wait_for_selector(self.config.data_container_selector, timeout=self.config.page_load_timeout)
            except:
                pass
            
            html = await context.page.content()
            soup = BeautifulSoup(html, 'html.parser')
            if re.search(r"ERROR CODE: 404|page you're looking for |no longer available", str(soup), flags=re.IGNORECASE):
                containers = []
            else:
                containers = soup.select(self.config.data_container_selector)

            if ((self.config.links_selector) and (len(containers)==0)):
                context.not_collcted_links.append(context.page.url)

            if self.config.site_id=='hdfc':
                new_page = await context.page.context.new_page()
                # Create temporary context for new page
                temp_context = CollectionContext(crawlee_context=new_page,state=None,city=None,locality=None,sub_brand=None)

                # Override page reference
                temp_context.crawlee_context.page = new_page
                extracted_links_lyr1 = await temp_context.extract_links(selector='a', label='STATE')

            if not containers:
                if context.city:
                    print(f"No data found for {context.city}" + 
                         (f" - {context.locality}" if context.locality else ""))
                return CollectionResult(True, 0, pd.DataFrame(), [], context)
            
            print(f"Found {len(containers)} records")
            records_added = 0
            # Extract data from containers
            for container in containers:
                
                if self.config.custom_data_extractor:
                    if not self.config.skip_custom_extractor:
                        record = self.config.custom_data_extractor(container, context.state, context.city, context.locality,context.pincode,
                                                                   self.config.address_selector, self.config.filter_flag,
                                                                   self.base_record, self.config.site_id)

                    
                    #-------------------------------------------------------------------------------------------------------------
                    if self.config.site_id in ['bata','hush_puppies']:
                        output = json.loads(soup.find('pre').text)
                        df = pd.DataFrame(output['stores'])
                        if self.config.site_id in ['hush_puppies']:
                            df=df[df['brandDV'].str.lower().str.contains('puppies',na=False)]

                        df['State'] = df['sCode'].apply(lambda x: x.upper())
                        df['Locality'] = df['name'].apply(lambda x : x.split(" ", 1)[1])

                        record=[]
                        for i,row in df.iterrows():
                            record.append({**self.base_record,**{'State':row['State'],'City':row['city']},**{'Latitude':row['lat'],'Longitude':row['long']},'Address': row['address'],'Locality':row['Locality'],'Pincode': row['pCode']})
                    
                    elif self.config.site_id in ['Tanishq']:
                        data = json.loads(soup.find('pre').text)
                        record=[]
                        for row in data['result']:
                            record.append({**self.base_record,**{'State':row['storeState'],'City':row['storeCity']},**{'Latitude':row['storeLatitude'],'Longitude':row['storeLongitude']},'Address': row['storeAddress'],'Locality':'','Pincode': row['storeZipCode']})
                    
                    elif self.config.site_id in ['vishal_megamart']:
                        
                        data=soup.select("[id='latlongadd']")
                        data1=[i['value'] for i in data]
                        data2 = json.loads(data1[0])
                        record=[]
                        for row in data2:
                            record.append({**self.base_record,**{'State':context.state,'City':context.city},**{'Latitude':row['latlng']['lat'],'Longitude':row['latlng']['lng']},'Address': row['address'],'Locality':row['locality']})


                    elif self.config.site_id in ['lenskart_2']:
                        data=soup.select("script[id='__NEXT_DATA__']")
                        data = json.loads(data[0].string)
                        try:
                           data=data['props']['pageProps']['props']['result']
                           data=data['data']
                           record=[]
                           for row in data:
                             record.append({**self.base_record,**{'State':row['address_state'],'City':row['address_city']},**{'Latitude':row['latitude'],'Longitude':row['longitude']},'Address': row['address_full'],'Locality':'','Pincode': row['address_pin_code']})
                    
                        except:
                           row=data['props']['pageProps']['props']['storeDetails']
                           record=[]
                           record.append({**self.base_record,**{'State':row['address_state'],'City':row['address_city']},**{'Latitude':row['latitude'],'Longitude':row['longitude']},'Address': row['address_full'],'Locality':'','Pincode': row['address_pin_code']})
                    

                        
                    elif self.config.brand_name in ['Hyundai']:
                        data= await self.make_custom_request(self.config.request_url,params=context.request_params,headers=context.header,return_json=False)
                        data_list = json.loads(data)
                        data1=pd.DataFrame(data_list)
                        print(len(data1))
                        record=[]
                        for i,row in data1.iterrows():
                            record.append({**self.base_record,**{'State':row['state'],'City':row['city']},**{'Latitude':row['latitude'],'Longitude':row['longitude']},'Address': row['address1'],'Locality':row['location'],'Pincode': row['postCode'],"Dealer_Name":row['dealerName']})
                    
                    elif self.config.brand_name in ['Kajaria Ceramics']:
                        data= await self.make_custom_request(self.config.request_url,params=context.request_params,headers=context.header)
                        data_list = data['data']
                        data1=pd.DataFrame(data_list)
                        record=[]
                        for i,row in data1.iterrows():
                            record.append({**self.base_record,**{'State':row['state'],'City':row['city']},**{'Latitude':row['latitude'],'Longitude':row['longitude']},'Address': str(row['address1'])+' '+str(row['address2']),'Locality':row['suburb'],'Pincode': row['pincode'],"Dealer_Name":row['name']})
                    
                    elif self.config.brand_name in ['Manyavar']:
                        data= await self.make_custom_request(self.config.request_url,params=context.request_params,headers=context.header)
                        data_list = data['stores']
                        data1=pd.DataFrame(data_list)
                        print(len(data1))
                        record=[]
                        for i,row in data1.iterrows():
                            record.append({**self.base_record,**{'State':row['stateCode'],'City':row['city']},**{'Latitude':row['latitude'],'Longitude':row['longitude']},'Address': str(row['address1']),'Locality':'','Pincode': row['postalCode']})
                    
                    elif self.config.brand_name in ['Joylukkas']:
                        data= await self.make_custom_request(self.config.request_url,params=self.config.request_params,headers=self.config.request_headers)
                        data_list = data['data']['checkstores_availability']
                        data1=pd.DataFrame(data_list)
                        print(len(data1))
                        record=[]
                        for i,row in data1.iterrows():
                            record.append({**self.base_record,**{'State':'','City':row['store_city']},**{'Latitude':row['latitude'],'Longitude':row['longitude']},'Address': str(row['store_address']),'Locality':'','Pincode': row['store_pincode']})
                    
                    elif self.config.brand_name in ['Fab India']:
                        data= await self.make_custom_request(context.request_url,params=self.config.request_params,headers=self.config.request_headers)
                        data_list = data['pointOfServices']
                        data1=pd.DataFrame(data_list)
                        print(len(data1))
                        record=[]
                        for i,row in data1.iterrows():
                            record.append({**self.base_record,**{'State':'','City':row['city']},**{'Latitude':row['latitude'],'Longitude':row['longitude']},'Address': str(row['address']),'Locality':'','Pincode': row['pincode']})
                    
                    elif self.config.brand_name in ['Havells']:
                        print(context.request_params)
                        data = await self.make_custom_request(self.config.request_url,method='POST',data=context.request_params,headers=context.header,)
                        data1=pd.DataFrame(data)
                        print(len(data1))
                        record=[]
                        for i,row in data1.iterrows():
                            record.append({**self.base_record,**{'State':row['state'],'City':row['city']},**{'Latitude':row['lattitude'],'Longitude':row['longitude']},'Address': str(row['address1'])+' '+str(row['address2']),'Locality':'','Pincode': row['pincode']})
                    
                    
                    elif self.config.brand_name in ['Nexa Maruti Suzuki','Maruti Suzuki']:
                        print(context.request_url)
                        if self.config.brand_name=='Maruti Suzuki':
                           data = await self.make_custom_request(context.request_url,headers=self.config.request_headers)
                        else:
                           data = await self.make_custom_request(context.request_url,headers=self.config.request_headers,params=context.request_params)

                        data1=pd.DataFrame(data['data'])
                        print(len(data1))
                        record=[]
                        for i,row in data1.iterrows():
                            record.append({**self.base_record,**{'State':row['stateDesc'],'City':row['cityDesc']},**{'Latitude':row['latitude'],'Longitude':row['longitude']},'Address': str(row['addr1'])+' '+str(row['addr2'])+' '+str(row['addr3']),'Locality':'','Pincode': row['pin'],"Dealer_Name":row['dealerDisplayName']})

                    #-------------------------------------------------------------------------------------------------------------------------------------------------------------------

                    if record and isinstance(record, list):
                        new_data = pd.DataFrame(record)
                        if context.sub_brand:
                            new_data['Sub_Brand'] = context.sub_brand

                        if self.config.Sub_Category_1:
                            new_data['Sub_Category_1'] = self.config.Sub_Category_1

                        if self.config.Sub_Category_2:
                            new_data['Sub_Category_2'] = self.config.Sub_Category_2
                        
                        self.data = pd.concat([self.data, new_data], ignore_index=True)
                        records_added += len(record)
            #-----------------------------------------------------------------------------------------------------------------------
            # Handle back navigation
            if ((context.page.url != self.config.url) and (not self.config.data_tab_selector) and (context._back_to_original_link)):
                print(f'Page url changed-----------Going Back to Original{context._back_to_original_link}')
                await context.page.goto(self.config.url, wait_until='networkidle')
            
            if self.config.back_button_selector:
                await self.page_navigator.click_by_javascript(context.page, self.config.back_button_selector)

            #--------------------------------------UPDATE PROGRESS-------------------------------------------------------------------
            if ((context.locality) and (self.config.locality_selector)):
                self.progress_manager.update("locality", context.locality,parent=context.state, subparent=context.city)

            if ((context.city) and (self.config.city_selector) and (not self.config.locality_selector)):
                self.progress_manager.update("city", context.city, parent=context.state)

            if ((context.state) and (self.config.state_selector) and (not self.config.city_selector)):
                self.progress_manager.progress_info.states_fully_completed.append(context.state)
                self.progress_manager.update("state", context.state)

            if not self.data.empty:
               self.progress_manager.save_backup_data(self.config.site_id,self.data)
               # print(f"Data backup saved: {backup_filename} ({len(self.data)} records)")

            #-----------------------------------------------------------------------------------------------------------------
            return CollectionResult(True, records_added, self.data, [], context)
        
        except Exception as e:
            self._log_strategy_error('DataManager.extract_page_data')
            return CollectionResult(False, 0, pd.DataFrame(), [e], context)
    
    def finalize_data(self) -> pd.DataFrame:
        try:
            print(f"\nData collection complete! Found {len(self.data)} records")
            initial_count = len(self.data)
            self.data['City'] = self.data['City'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)

            df_duplicates=self.data[self.data.duplicated(subset='Address', keep='last')]

            self.data = self.data.drop_duplicates(subset='Address', keep='last')
            final_count = len(self.data)
            filename = f"{self.config.brand_name}_Duplicate_Stores_{today.strftime('%Y%m%d')}.xlsx"
            # df_duplicates.to_excel(filename)

            if initial_count != final_count:
                print(f"Removed {initial_count - final_count} duplicate records")

            return self.data
        except Exception as e:
            self._log_strategy_error('DataManager.finalize_data')
# ================================================================================================
# MAIN CLASS
class BaseCollectionStrategy(ABC):
    """Base class for collection strategies"""
    def __init__(self, config,BrowserManager, dropdown_handler, page_navigator, data_manager, InputDataSource,progress_manager: ProgressManager):
        self.config = config
        self.dropdown_handler = dropdown_handler
        self.page_navigator = page_navigator
        self.data_manager = data_manager
        self.InputDataSource = InputDataSource
        self._back_to_original_link = True
        self._log_strategy_error=InputDataSource._log_strategy_error
        self.progress_manager=progress_manager
        self.make_custom_request=BrowserManager.make_custom_request
    
    @abstractmethod
    async def execute(self, context: CollectionContext) -> CollectionResult:
        """Execute the collection strategy"""
        pass
    
    def get_strategy_name(self) -> str:
        """Get strategy name for logging"""
        return self.__class__.__name__
    
    async def _navigate_to_new_page(self,context: CollectionContext, page_url):
        try:
            if page_url in self.progress_manager.progress_info.inputs_completed:
               return None 

            await self._popup_watcher(context.page)
            new_page = await context.page.context.new_page()
            await new_page.goto(page_url)
            await self.page_navigator.wait_for_cascade_load(new_page)

            if len(self.config.popup_close_selectors)>0:
                try:
                    await self.page_navigator.click_by_javascript(context.page, self.config.popup_close_selectors[0])
                except:
                    pass
            
            # await self._scroll_to_load_all_stores(context.page)
            # Create temporary context for new page
            temp_context = CollectionContext(crawlee_context=new_page,state=None,city=None,locality=None,sub_brand=None) 
            # Override page reference
            temp_context.crawlee_context.page = new_page
            
            
            result = await self.data_manager.extract_page_data(temp_context)
            self.progress_manager.update("custom_links", str(page_url))
            await new_page.close()

        except:
            self.InputDataSource._log_strategy_error('BaseCollectionStrategy._navigate_to_new_page')
        
        finally:
            try:
                if new_page and not new_page.is_closed():
                    await new_page.close()
            except Exception as e:
                self.InputDataSource._log_strategy_error('BaseCollectionStrategy._navigate_to_new_page')

    async def _block_unwanted_routes(self,context: CollectionContext):
        # Block heavy, unnecessary resources
        # await context.page.route("**/*.{png,gif,webp,ico}", lambda route: route.abort())
        # await context.page.route("**/*.{woff,woff2,ttf,eot}", lambda route: route.abort())
        # await context.page.route("**/analytics.js", lambda route: route.abort())
        # await context.page.route("**/gtag/**", lambda route: route.abort())   
        print("--------------------------------BLOCKED----------------------------------------------------")   

    async def check_element_exists(self, page, selector: str) -> bool:
        """Check if element exists without waiting"""
        try:
            element = await page.query_selector(selector)
            return element is not None
        except Exception as e:
            self.InputDataSource._log_strategy_error('BaseCollectionStrategy.check_element_exists')

    async def _popup_watcher(self,page):
        # print('------SANTONU DEBNATH--------')
        try:
            if self.config.popup_close_selectors:
                for ele in self.config.popup_close_selectors:
                    element = await page.query_selector(ele)
                    if element and await element.is_visible():
                        await self.page_navigator.click_by_javascript(page,ele)
                        await element.click()
                        await asyncio.sleep(1)
        except Exception as e:
            self.InputDataSource._log_strategy_error('BaseCollectionStrategy._popup_watcher')
            pass
        try:
            html = await page.content()
            soup = BeautifulSoup(html, 'html.parser')
            if re.search(r"ERROR CODE: 404|page you're looking for |no longer available", str(soup), flags=re.IGNORECASE):
                await page.goto(self.config.url, wait_until='networkidle')
        except Exception as e:
            self.InputDataSource._log_strategy_error('BaseCollectionStrategy._popup_watcher')
            pass
        try:
            await page.keyboard.press('Escape')
        except Exception as e:
            self.InputDataSource._log_strategy_error('BaseCollectionStrategy._popup_watcher')
            pass   

    async def _scroll_to_load_all_stores(self, page,max_scroll=20):
        try:
            last_height = await page.evaluate("() => document.body.scrollHeight")
            stagnant = 0 
            max_scrolls=max_scroll
            for i in range(max_scrolls):
                # Scroll down by 800 pixels
                await page.evaluate("() => window.scrollBy(0, 800)")
                
                # Wait for content to load
                await page.wait_for_timeout(2000)  # 2 seconds
                
                # Check new page height
                new_height = await page.evaluate("() => document.body.scrollHeight")
                
                if new_height == last_height:
                    stagnant += 1
                else:
                    stagnant = 0
                    last_height = new_height
                
                # Exit if no changes for 4 consecutive scrolls
                if stagnant >= 4:
                    break
            
            # Wait for final lazy-loaded elements
            await page.wait_for_timeout(3000)  # 3 seconds
            
            # Return all store card elements
            # return await page
        except:
            self._log_strategy_error('InputBasedStrategy._scroll_to_load_all_stores')

# ================================================================================================
class SimpleDropdownStrategy(BaseCollectionStrategy):
    async def execute(self, context: CollectionContext) -> CollectionResult:
        
        await self._block_unwanted_routes(context)
        try:
            # Store type handling
            if self.config.store_type_selector:
                return await self._collect_with_store_types(context)
            else:
                return await self._collect_simple_cascade(context)
        
        except Exception as e:
            self._log_strategy_error('SimpleDropdownStrategy')
            return CollectionResult(False, 0, pd.DataFrame(), [e], context)
    
    async def _collect_with_store_types(self, context: CollectionContext) -> CollectionResult:
        """Collect using--------state->city->locality"""
        try:
            total_records = 0
            errors = []
            try:
                await context.page.wait_for_selector(self.config.store_type_selector, timeout=self.config.page_load_timeout)
            except:
                pass

            sub_brands = await self.dropdown_handler._get_native_options(context.page,selector=self.config.store_type_selector)
            sub_brands=[i for i in sub_brands if not re.search(r"select|all| select city",i,flags=re.IGNORECASE)]
            # print(sub_brands)

            if self.config.site_id=='firstcry':
                sub_brands=['FirstCry']
                for store_type in sub_brands:
                    print(f"Processing store type: {store_type}")

                    #SELECT CATEGORY
                    await self.dropdown_handler._select_native_option(context.page, self.config.store_type_selector, store_type)
                    await self.page_navigator.wait_for_cascade_load(context.page)

                    avl_states = await self.dropdown_handler._get_native_options(context.page, selector=self.config.state_selector)
                    

                    for state_index, state in enumerate(avl_states):
                        context.state=state
                        
                        await self.dropdown_handler._select_native_option(context.page, self.config.store_type_selector, store_type)
                        await self.dropdown_handler._select_native_option(context.page, self.config.state_selector, state)

                        avl_city = await self.dropdown_handler._get_native_options(context.page,selector= self.config.city_selector)
                        # avl_city=avl_city[:1]

                        for city_index, city in enumerate(avl_city):
                            context.city=city
                            self.progress_manager.current_city=city
                            await self.dropdown_handler._click_option_selector(context.page, self.config.store_type_selector)
                            await self.dropdown_handler._select_native_option(context.page, self.config.store_type_selector, store_type)

                            await self.dropdown_handler._click_option_selector(context.page, self.config.state_selector)
                            await self.dropdown_handler._select_native_option(context.page, self.config.state_selector, state)

                            await self.dropdown_handler._click_option_selector(context.page, self.config.city_selector)
                            await self.dropdown_handler._select_native_option(context.page, self.config.city_selector, city)

                            if (self.config.search_selector):
                                await self.page_navigator.click_by_javascript(context.page, self.config.search_selector)
                            
                            await self.data_manager.extract_page_data(context)
                            await context.page.goto(self.config.url, wait_until='networkidle')
                            # self.progress_info.cities_completed=city
                             
                return CollectionResult(True, total_records, self.data_manager.data, errors, context)
            else:
                if self.config.Sub_Category_1:
                   sub_category=self.config.Sub_Category_1
                if self.config.Sub_Category_2:
                   sub_category=self.config.Sub_Category_2

                await self.dropdown_handler._select_native_option(context.page, self.config.store_type_selector, sub_category)
                await self.page_navigator.wait_for_cascade_load(context.page)
                await self._popup_watcher(context.page)
                await self._collect_simple_cascade(context)
                return CollectionResult(True, total_records, self.data_manager.data, errors, context)

        except Exception as e:
            self._log_strategy_error('SimpleDropdownStrategy._collect_simple_cascade')
    
    async def _collect_simple_cascade(self, context: CollectionContext) -> CollectionResult:
        """Collect using--------state->city->locality"""
        try:
            total_records = 0
            errors = []

            # Handle data tab---|eg. Puma
            if self.config.data_tab_selector:
               await self.page_navigator.click_by_javascript(context.page, self.config.data_tab_selector)
            
            # Get and process states
            states = await self._get_State_City_Locality_Options(context,selector=self.config.state_selector,level='state')
            print(states)
            print(f"Found {len(states)} states")
            #Santonu
            # states=['karnataka']
            
            #Loop through States
            for state_index, state in enumerate(states):
                if self.config.search_limit:
                    if state_index==self.config.search_limit:
                       break

                print(f"\nProcessing state {state_index + 1}/{len(states)}: {state}")
                context.state = state
                try:
                    result = await self._process_state(context)
                    total_records += result.records_collected
                    errors.extend(result.errors)
                except Exception as e:
                    errors.append(f"Error processing state {state}: {e}")
            
            return CollectionResult(True, total_records, self.data_manager.data, errors, context)
        except Exception as e:
            self._log_strategy_error('SimpleDropdownStrategy._collect_simple_cascade')
       
    async def _process_state(self, context: CollectionContext) -> CollectionResult:
        """Process a single state"""
        try:
            await self._popup_watcher(context.page)
            if len(self.config.popup_close_selectors)>0:
                try:
                    await self.page_navigator.click_by_javascript(context.page, self.config.popup_close_selectors[0])
                except:
                    pass

            # Select state if not skipped
            if not self.config.skip_state_selector:
                context._back_to_original_link = True
                try:
                    await context.page.wait_for_selector(self.config.state_selector, timeout=self.config.page_load_timeout)
                except Exception as e:
                    print(e)
                
                await self.dropdown_handler._select_native_option(context.page, options_selector=self.config.state_selector, option=context.state,custom_timeout=3500)
                await self.page_navigator.wait_for_cascade_load(context.page)
            
            
            #--------------------------------------------------------------------------------------------------------------------------------
            # Process cities or extract data directly

            if self.config.city_selector:
                return await self._process_cities(context)
            else:
                if self.config.search_selector:
                    await self.page_navigator.click_by_javascript(context.page, self.config.search_selector)
                
                result = await self.data_manager.extract_page_data(context)
                # Handle pagination if configured
                await self.page_navigator.wait_for_cascade_load(context.page)
                if (self.config.pagination_selector and await self.check_element_exists(context.page, self.config.pagination_selector)):               
                    result=await self._handle_pagination(context)

                self.progress_manager.update("state", context.state)
                return result
            #--------------------------------------------------------------------------------------------------------------------------------
        except Exception as e:
            self._log_strategy_error('SimpleDropdownStrategy._process_state')
            return CollectionResult(False, 0, pd.DataFrame(), [e], context)
    
    async def _process_cities(self, context: CollectionContext) -> CollectionResult:
        """Process cities for current state"""
        try:
            context._back_to_original_link = False
            cities = await self._get_State_City_Locality_Options(context, selector=self.config.city_selector,level='city')
            print(f"Found {len(cities)} cities in {context.state}")
            #Santonu
            # cities=['Bengaluru']
            
            total_records = 0
            errors = []
            #--------------------------------------------------------------------------------------------------------------------------------
            for city_index, city in enumerate(cities):
                print(f"Processing city {city_index + 1}/{len(cities)}: {city}")
                context.city = city
                
                try:
                    result = await self._process_city(context)
                    total_records += result.records_collected
                    errors.extend(result.errors)
                    self.progress_manager.progress_info.cities_fully_completed.append(context.city)
                except Exception as e:
                    errors.append(f"Error processing city {city}: {e}")

            self.progress_manager.progress_info.states_fully_completed.append(context.state)
            return CollectionResult(True, total_records, self.data_manager.data, errors, context)
            #--------------------------------------------------------------------------------------------------------------------------------
        except Exception as e:
            self._log_strategy_error('SimpleDropdownStrategy._process_cities')
        
    async def _process_city(self, context: CollectionContext) -> CollectionResult:
        """Process a single city"""
        try:
            await self._popup_watcher(context.page)
            if self.config.data_tab_selector:
                await self.page_navigator.click_by_javascript(context.page, self.config.data_tab_selector)
            
            # Select city if not skipped
            if not self.config.skip_city_selector:
                await self.dropdown_handler._select_native_option(context.page, options_selector=self.config.city_selector, option=context.city,custom_timeout=3000)
                await self.page_navigator.wait_for_cascade_load(context.page)
            
            if self.config.radius_selector:
                await context.page.fill("#radius", "100")

            # Handle search button or localities
            if not self.config.locality_selector and self.config.search_selector:
                await self.page_navigator.click_by_javascript(context.page, self.config.search_selector)
            
            #--------------------------------------------------------------------------------------------------------------------------------
            # Process localities or extract data directly
            if self.config.locality_selector:
                return await self._process_localities(context)
            else:
                result = await self.data_manager.extract_page_data(context)
                # Handle pagination if configured
                await self.page_navigator.wait_for_cascade_load(context.page)
                if (self.config.pagination_selector and await self.check_element_exists(context.page, self.config.pagination_selector)): 
                    context._back_to_original_link = False
                    result=await self._handle_pagination(context)
                
                return result
            #--------------------------------------------------------------------------------------------------------------------------------
        
        except Exception as e:
            self._log_strategy_error('SimpleDropdownStrategy._process_city')
            return CollectionResult(False, 0, pd.DataFrame(), [e], context)
    
    async def _process_localities(self, context: CollectionContext) -> CollectionResult:
        """Process localities for current city"""
        try:
            try:
                await self.page_navigator.wait_for_cascade_load(context.page)
            except:
                pass
        
            print('----------------------------------------LOCALITIES----------------------------------------------')
            localities = await self._get_State_City_Locality_Options(context, selector=self.config.locality_selector,level='locality')
            localities = [s for s in localities if not re.search(r"locality", s, flags=re.IGNORECASE)]
            print(f"Found {len(localities)} localities in {context.city} | {context.state}")
            
            total_records = 0
            errors = []
            #--------------------------------------------------------------------------------------------------------------------------------
            await self._popup_watcher(context.page)
            for locality_index, locality in enumerate(localities):
                print(f"Processing locality {locality_index + 1}/{len(localities)}: {locality}")
                context.locality = locality
                
                try:
                    if not self.config.skip_locality_selector:
                        await self.dropdown_handler._select_native_option(context.page, options_selector=self.config.locality_selector, option=locality, custom_timeout=3500)
                        await self.page_navigator.wait_for_cascade_load(context.page)
                    
                    if self.config.search_selector:
                        await self.page_navigator.click_by_javascript(context.page, self.config.search_selector)
                        await self.page_navigator.wait_for_cascade_load(context.page)
                    
                    result = await self.data_manager.extract_page_data(context)
                    total_records += result.records_collected
                    errors.extend(result.errors)
                    self.progress_manager.progress_info.locality_fully_completed.append(context.locality)
                except Exception as e:
                    errors.append(f"Error processing locality {locality}: {e}")

            self.progress_manager.progress_info.cities_fully_completed.append(context.city)
            #--------------------------------------------------------------------------------------------------------------------------------
        except Exception as e:
            self._log_strategy_error('SimpleDropdownStrategy._process_localities')
            return CollectionResult(True, total_records, self.data_manager.data, e, context)

    async def _get_State_City_Locality_Options(self, context,selector,level) -> List[str]:
        #---------------Get and filter list of states | City | Locality-----------------------------------
        try:
            states = await self.dropdown_handler._get_native_options(context.page, selector=selector)
            if len(states) > 1:
                states = [s for s in states if not re.search(r"select|all|select city", s, flags=re.IGNORECASE)]
                states = [re.sub(r'Location\s*', '', s).strip() for s in states]
            
            #--------------------------------------------------------------------------------------------------------------------------------
            if ((level=="locality") and (self.progress_manager.progress_info.locality_fully_completed)):
                # states=[i for i in states if not i in self.progress_manager.progress_info.localities_completed.get(context.state, {}).get(context.city, [])]
                states=[i for i in states if not i in self.progress_manager.progress_info.locality_fully_completed]


            if ((level=="city") and (self.progress_manager.progress_info.cities_fully_completed)):
               states=[i for i in states if not i in self.progress_manager.progress_info.cities_completed.get(context.state, [])]
               states=[i for i in states if not i in self.progress_manager.progress_info.cities_fully_completed]

            
            completed_states = set(self.progress_manager.progress_info.states_completed or []) | set(self.progress_manager.progress_info.states_fully_completed or [])
            if ((level=="state") and (self.progress_manager.progress_info.states_fully_completed)):
               states=[i for i in states if not i in self.progress_manager.progress_info.states_fully_completed]
            #--------------------------------------------------------------------------------------------------------------------------------
            return states

        except Exception as e:
            self._log_strategy_error('SimpleDropdownStrategy._get_State_City_Locality_Options')
            return []

    async def _handle_pagination(self, context: CollectionContext):
        """Handle pagination for current page"""
        try:
            context._back_to_original_link = False 
            # print("--------------HERE _handle_pagination-------------------")
            page_counter = 1
            if self.config.max_nevigated_page:
                max_attempts = self.config.max_nevigated_page
            else:
                max_attempts = 10

            while page_counter <= max_attempts:
                await self.page_navigator.click_by_javascript(context.page, self.config.pagination_selector)

                if self.config.page_load:
                    if page_counter==max_attempts:
                        await self.data_manager.extract_page_data(context)
                else:
                    await self.data_manager.extract_page_data(context)
                
                if not (await self.check_element_exists(context.page, self.config.pagination_selector)): 
                   print("---NEXT PAGE NOT EXIST------Closing")
                   break

                page_counter += 1

        except Exception as e:
            self._log_strategy_error('SimpleDropdownStrategy._handle_pagination',context=context)

class DivDropdownStrategy(BaseCollectionStrategy):
    async def execute(self, context: CollectionContext) -> CollectionResult:
        await self._block_unwanted_routes(context)
        #Example: Thomas_Cook
        if (context.page.url != self.config.url):
            print('Page url changed-----------Going Back to Original')
            await context.page.goto(self.config.url, wait_until='networkidle')

        try:
            # Handle data tab
            if self.config.data_tab_selector:
                await self.page_navigator.click_by_javascript(context.page, self.config.data_tab_selector)
            
            context.page.set_default_navigation_timeout(self.config.page_load_timeout*0.5)
            return await self._collect_states_and_cities(context)
        
        except Exception as e:
            self._log_strategy_error('DivDropdownStrategy')
            return CollectionResult(False, 0, pd.DataFrame(), [e], context)
    
    async def _collect_states_and_cities(self, context: CollectionContext) -> CollectionResult:
        try:
            states = await self._get_div_State_City_Locality_Options(context, self.config.state_selector, self.config.state_options_selector,level="state")        
            print(states)
            print(f"Found {len(states)} states")
            
            #Santonu
            # states=states[:1]
            total_records = 0
            errors = []
            
            #--------------------------------------------------------------------------------------------------------------------------------
            # Apply search limit
            if self.config.search_limit:
                states = states[:self.config.search_limit]
            
            for state_index, state in enumerate(states):
                print(f"\nProcessing state {state_index + 1}/{len(states)}: {state}")
                context.state = state
                
                try:
                    result = await self._process_state_div(context)
                    total_records += result.records_collected
                    errors.extend(result.errors)
                except Exception as e:
                    errors.append(f"Error processing state {state}: {e}")

            return CollectionResult(True, total_records, self.data_manager.data, errors, context)
            #--------------------------------------------------------------------------------------------------------------------------------
        except Exception as e:
            self._log_strategy_error('DivDropdownStrategy._collect_states_and_cities')

    async def _process_state_div(self, context: CollectionContext) -> CollectionResult:
        try:
            if len(self.config.popup_close_selectors)>0:
                try:
                    await self.page_navigator.click_by_javascript(context.page, self.config.popup_close_selectors[0])
                except:
                    pass

            context._back_to_original_link = True
            if not self.config.skip_state_selector:
                await self.dropdown_handler._select_dropdown_option(context.page, self.config.state_selector, context.state, self.config.state_options_selector)
                await self.page_navigator.wait_for_cascade_load(context.page)
            
            #--------------------------------------------------------------------------------------------------------------------------------
            # Handle cities
            if self.config.city_selector:
                return await self._process_cities_div(context)
            else:
                if self.config.search_selector:
                    await self.page_navigator.click_by_javascript(context.page, self.config.search_selector)
                    await self.page_navigator.wait_for_cascade_load(context.page)
                result=await self.data_manager.extract_page_data(context)

                self.progress_manager.update("state", context.state)
                return result
            #--------------------------------------------------------------------------------------------------------------------------------
        
        except Exception as e:
            self._log_strategy_error('DivDropdownStrategy._process_state_div')
            return CollectionResult(False, 0, pd.DataFrame(), [e], context)
    
    async def _process_cities_div(self, context: CollectionContext) -> CollectionResult:
        try:
            context._back_to_original_link = False
            cities = await self._get_div_State_City_Locality_Options(context, self.config.city_selector, self.config.city_options_selector,level="city")        
            print(f"Found {len(cities)} cities in {context.state}")
            
            total_records = 0
            errors = []
            
            #--------------------------------------------------------------------------------------------------------------------------------
            for city_index, city in enumerate(cities):
                print(f"Processing city {city_index + 1}/{len(cities)}: {city}")
                context.city = city
                try:
                    if not self.config.skip_city_selector:
                        await self.page_navigator.wait_for_cascade_load(context.page)
                        await self.dropdown_handler._select_dropdown_option(context.page, self.config.city_selector, city, self.config.city_options_selector,custom_timeout=1000)

                    if not self.config.locality_selector and self.config.search_selector:
                        await self.page_navigator.click_by_javascript(context.page, self.config.search_selector)
                       
                    
                    # Handle locality if configured
                    if self.config.locality_selector:
                        return await self._process_localities(context)
                    else:
                        result = await self.data_manager.extract_page_data(context)
                        total_records += result.records_collected
                        errors.extend(result.errors)
                        self.progress_manager.update("city", context.city, parent=context.state)
                
                except Exception as e:
                    errors.append(f"Error processing city {city}: {e}")
            
            self.progress_manager.progress_info.states_fully_completed.append(context.state)
            return CollectionResult(True, total_records, self.data_manager.data, errors, context)
            #--------------------------------------------------------------------------------------------------------------------------------
        except Exception as e:
            self._log_strategy_error('DivDropdownStrategy._process_cities_div')
    
    async def _process_localities(self, context: CollectionContext) -> CollectionResult:
        """Process localities for current city"""
        try:
            try:
                await self.page_navigator.wait_for_cascade_load(context.page)
            except:
                pass
        
            print('----------------------------------------LOCALITIES----------------------------------------------')
            localities = await self._get_div_State_City_Locality_Options(context, self.config.locality_selector, self.config.locality_options_selector,level="locality")        
            localities = [s for s in localities if not re.search(r"locality", s, flags=re.IGNORECASE)]
            print(f"Found {len(localities)} localities in {context.city} | {context.state}")
            
            total_records = 0
            errors = []
            #--------------------------------------------------------------------------------------------------------------------------------
            for locality_index, locality in enumerate(localities):
                print(f"Processing locality {locality_index + 1}/{len(localities)}: {locality}")
                context.locality = locality
                try:
                    if not self.config.skip_city_selector:
                        await self.page_navigator.wait_for_cascade_load(context.page)
                        await self.dropdown_handler._select_dropdown_option(context.page, self.config.locality_selector,locality, self.config.locality_options_selector,custom_timeout=self.config.page_load_timeout*0.25)

                    if not self.config.locality_selector and self.config.search_selector:
                        await self.page_navigator.click_by_javascript(context.page, self.config.search_selector)
                        await self.page_navigator.wait_for_cascade_load(context.page)
                    
                    result = await self.data_manager.extract_page_data(context)
                    total_records += result.records_collected
                    errors.extend(result.errors)
                except Exception as e:
                    errors.append(f"Error processing locality {locality}: {e}")

            self.progress_manager.progress_info.cities_fully_completed.append(context.city)
            #--------------------------------------------------------------------------------------------------------------------------------
        except Exception as e:
            self._log_strategy_error('SimpleDropdownStrategy._process_localities')
            return CollectionResult(True, total_records, self.data_manager.data, e, context)

    async def _get_div_State_City_Locality_Options(self, context,main_selector,option_selector,level) -> List[str]:
        try:
            #---------------Get and filter list of states | City | Locality-----------------------------------
            try:
                await context.page.wait_for_selector(main_selector, timeout=self.config.page_load_timeout)
            except:
                pass
            
            states = await self.dropdown_handler._get_dropdown_options(context.page, main_selector, option_selector)
            states = [s for s in states if not re.search(r"select|all|india|usa|city", s, flags=re.IGNORECASE)]
            
            #--------------------------------------------------------------------------------------------------------------------------------
            if ((level=="locality") and (self.progress_manager.progress_info.locality_fully_completed)):
                # states=[i for i in states if not i in self.progress_manager.progress_info.localities_completed.get(context.state, {}).get(context.city, [])]
                states=[i for i in states if not i in self.progress_manager.progress_info.locality_fully_completed]


            if ((level=="city") and (self.progress_manager.progress_info.cities_fully_completed)):
               states=[i for i in states if not i in self.progress_manager.progress_info.cities_completed.get(context.state, [])]
               states=[i for i in states if not i in self.progress_manager.progress_info.cities_fully_completed]

            
            completed_states = set(self.progress_manager.progress_info.states_completed or []) | set(self.progress_manager.progress_info.states_fully_completed or [])
            if ((level=="state") and (self.progress_manager.progress_info.states_fully_completed)):
               states=[i for i in states if not i in self.progress_manager.progress_info.states_fully_completed]

            #--------------------------------------------------------------------------------------------------------------------------------
            return states
        except Exception as e:
            self._log_strategy_error('DivDropdownStrategy._get_div_State_City_Locality_Options')
            return []

class InputBasedStrategy(BaseCollectionStrategy):
    async def execute(self, context: CollectionContext) -> CollectionResult:
        await self._block_unwanted_routes(context)
        try:
            required_inputs = await self.InputDataSource.get_input_data(self.config.input_type,self.config.brand_name)
            required_inputs=[i for i in required_inputs if i not in self.progress_manager.progress_info.inputs_completed]

            # required_inputs=["201301"]
            total_records = 0
            errors = []

            if self.config.store_type_selector:
               sub_brands = await self.dropdown_handler._get_native_options(context.page,selector=self.config.store_type_selector)
               sub_brands=[i for i in sub_brands if not re.search(r"select|all| select city",i,flags=re.IGNORECASE)]
               # print(sub_brands)

            required_inputs_gen=iter(required_inputs)
            total_input=len(required_inputs)
            del required_inputs

            for index, txt_input in enumerate(required_inputs_gen):
                print(f"Processing {self.config.input_type[0]} {index + 1}/{total_input}: {txt_input}")

                if re.search(r"pincode",self.config.input_type[0],flags=re.IGNORECASE):
                    context.pincode=txt_input

                elif re.search(r"city",self.config.input_type[0],flags=re.IGNORECASE):
                     context.city=txt_input

                if self.config.search_limit:
                    if index==self.config.search_limit:
                       break
               
                try:
                    result = await self._process_input(context, str(txt_input))
                    total_records += result.records_collected
                    errors.extend(result.errors)
                    self.progress_manager.update("custom_input", str(txt_input))
                except Exception as e:
                    errors.append(f"Error processing input {txt_input}: {e}")
            
            return CollectionResult(True, total_records, self.data_manager.data, errors, context)
        
        except Exception as e:
            self._log_strategy_error('InputBasedStrategy')
            return CollectionResult(False, 0, pd.DataFrame(), [e], context)
    
    async def _process_input(self, context: CollectionContext, txt_input: str) -> CollectionResult:
        context._back_to_original_link=False
        try:
            if self.config.suggested_option_selector:
                await self.page_navigator.clear_and_input(context.page, self.config.input_selector, txt_input)
                return await self._handle_suggestions(context)
            else:
                # return await self._handle_direct_search(context)
                return await self._handle_force_input(context,txt_input)

        except Exception as e:
            self._log_strategy_error('InputBasedStrategy._process_input')
            return CollectionResult(False, 0, self.data_manager.data, [], context)
    
    async def _handle_force_input(self, context: CollectionContext, txt_input: str) -> CollectionResult:

        try:
            #For Hyundai
            try:
                await context.page.click(self.config.popup_close_selectors[0], timeout=2000)
            except:
                pass

            if self.config.input_selector.startswith("input"):
                element = context.page.locator(self.config.input_selector)
                await element.clear(force=True)
                await element.fill(txt_input)

            else:
                await context.page.get_by_placeholder(self.config.input_selector).nth(1).fill(txt_input)

            if self.config.search_selector:
                await self.dropdown_handler._click_option_selector(context.page, self.config.search_selector)
                await self.page_navigator.wait_for_cascade_load(context.page)
            else:
                await context.page.keyboard.press('Enter')

            await asyncio.sleep(1)

            if ((self.config.all_view_option_selector) and (context.click_addional_view)):
               tab_options = await self.dropdown_handler._get_dropdown_options(context.page, self.config.all_view_option_selector, self.config.additional_view_option_selector)
               await self.dropdown_handler._select_dropdown_option(context.page, self.config.all_view_option_selector, tab_options[0], self.config.additional_view_option_selector)
               context.click_addional_view=False
            
            await self._scroll_to_load_all_stores(context.page)   
            # context.pincode=txt_input
            await self.data_manager.extract_page_data(context)
            return CollectionResult(True, 0, self.data_manager.data, [], context)
        except:
            self._log_strategy_error('InputBasedStrategy._handle_force_input')
            return CollectionResult(False, 0, self.data_manager.data, [], context)

    async def _handle_suggestions(self, context: CollectionContext) -> CollectionResult:
        try:
            # options = await self.dropdown_handler._get_native_options(context.page,selector=self.config.suggested_option_selector)
            if self.config.site_id in ['baja_auto']:
                maintain_spaces=True
            else:
                maintain_spaces=False

            options = await self.dropdown_handler._get_custom_options(context.page, 
                                                                  dropdown_selector=self.config.input_selector,
                                                                  options_selector=self.config.suggested_option_selector,
                                                                  maintain_spaces=maintain_spaces)
            

            # print(options)
            # print(options)
            total_records = 0
            errors = []
            
            options_gen=iter(options)
            total_input=len(options)
            del options

            for option_index, option in enumerate(options_gen):
                print(f"---------------------------{option}")

                if self.config.suggestion_search_limit:
                    if option_index==self.config.suggestion_search_limit:
                       break
                #For Hyundai
                try:
                    await context.page.click(self.config.popup_close_selectors[0], timeout=2000)
                except:
                    pass

                try:
                    # For sites where we need to click option selector to activate other options
                    if option_index > 0:
                        await self.dropdown_handler._click_option_selector(context.page, self.config.input_selector)
                    

                    # Customize for Sketchers 
                    if self.config.input_option_selector:
                        if self.config.site_id=='Sketchers':
                            await self.dropdown_handler._select_native_option(context.page, option="300", options_selector=self.config.input_option_selector)
                        
                    
                    #For Hyundai
                    try:
                        await context.page.click(self.config.popup_close_selectors[0], timeout=2000)
                    except:
                        pass

                    #Put that options in Input box |Eg. Medplus
                    await self.page_navigator.clear_and_input(context.page, self.config.input_selector, option)
                    await self.dropdown_handler._select_custom_option(context.page, options_selector=self.config.suggested_option_selector,
                                                                      option=option, dropdown_selector=self.config.input_selector)
                        
                    if self.config.search_selector:
                        await self.dropdown_handler._click_option_selector(context.page, self.config.search_selector)
                        await self.page_navigator.wait_for_cascade_load(context.page)
                    else:
                        await context.page.keyboard.press('Enter')
                    
                    result = await self.data_manager.extract_page_data(context)
                    total_records += result.records_collected
                    errors.extend(result.errors)
                
                except Exception as e:
                    errors.append(f"Error processing suggestion {option}: {e}")
            
            return CollectionResult(True, 0, None, [], context)
        except Exception as e:
            self._log_strategy_error('InputBasedStrategy._handle_suggestions')
            return CollectionResult(False, 0, self.data_manager.data, [], context)

    async def _handle_direct_search(self, context: CollectionContext) -> CollectionResult:
        try:
            if self.config.search_selector:
                try:
                    await self.page_navigator.click_by_javascript(context.page, self.config.search_selector)
                    await self.page_navigator.wait_for_cascade_load(context.page)
                except:
                    await self.dropdown_handler._click_option_selector(context.page, self.config.search_selector)
                    await self.page_navigator.wait_for_cascade_load(context.page)
        
            await self.data_manager.extract_page_data(context)
            return CollectionResult(True, 0, self.data_manager.data, [], context)
        except:
            self._log_strategy_error('InputBasedStrategy._handle_direct_search')
            return CollectionResult(False, 0, self.data_manager.data, [], context)

    async def _scroll_to_load_all_stores(self, page):
        try:
            last_height = await page.evaluate("() => document.body.scrollHeight")
            print(last_height)
            stagnant = 0 
            max_scrolls=50
            for i in range(max_scrolls):
                # Scroll down by 800 pixels
                await page.evaluate("() => window.scrollBy(0, 800)")
                
                # Wait for content to load
                await page.wait_for_timeout(2000)  # 2 seconds
                
                # Check new page height
                new_height = await page.evaluate("() => document.body.scrollHeight")
                
                if new_height == last_height:
                    stagnant += 1
                else:
                    stagnant = 0
                    last_height = new_height
                
                # Exit if no changes for 4 consecutive scrolls
                if stagnant >= 4:
                    break
            
            # Wait for final lazy-loaded elements
            await page.wait_for_timeout(3000)  # 3 seconds

            # Return all store card elements
            return await page.locator("li:has-text('card-body'), li[class*='card-body']").all()
        except:
            self._log_strategy_error('InputBasedStrategy._scroll_to_load_all_stores')
            
class PaginationStrategy(BaseCollectionStrategy):
    async def execute(self, context: CollectionContext) -> CollectionResult:
        await self._block_unwanted_routes(context)
        try:
            if self.config.paginate_selector=='Url_Pages':
                return await self.navigate_through_url_pages(context)

            elif self.config.paginate_selector=='Custom_Pages':
                return await self.navigate_through_custom_pages(context)

            else:
                return await self.navigate_through_ajax_pages(context)

        except Exception as e:
            self._log_strategy_error('PaginationStrategy')
            return CollectionResult(False, 0, pd.DataFrame(), [e], context)
    
    async def _get_available_pages(self, page) -> List[str]:
        try:
            pages = await self.dropdown_handler._get_native_options(page, selector=self.config.paginate_selector)
            return [p for p in pages if not re.search(r"«|all|»", p, flags=re.IGNORECASE)]
        except Exception as e:
            self._log_strategy_error('_get_available_pages')
            return []
    
    async def _navigate_to_page(self, page, page_number):
        """Navigate to specific page number"""
        try:
            try:
                await page.wait_for_selector(self.config.data_container_selector)
            except:
                print(f"Error in:{self.config.data_container_selector}")
            
            # Select the page number
            await self.dropdown_handler._click_option_selector(page, page_number)
            return True
        
        except Exception as e:
            self._log_strategy_error('_navigate_to_page')
            return False

    async def navigate_through_url_pages(self,context: CollectionContext):
        try:
            # total_pages = await self.page_navigator.get_total_pages(context.page,self.config.next_page_selector)
            # print(f"TOTAL PAGES:  {total_pages}")
            total_pages = self.config.max_nevigated_page


            for page_num in range(1, total_pages + 1):
                print(f"\n--- Page {page_num} ---")
                
                # Build page URL
                if page_num == 1:
                    page_url = self.config.url
                else:
                    page_url = f"{self.config.url}?page={page_num}"
                
                print(page_url)
                await self._navigate_to_new_page(context,page_url)
                await asyncio.sleep(10)  # Keep original timing

            return CollectionResult(True, 0, self.data_manager.data, [], context)
        except:
            self._log_strategy_error('PageNavigator.navigate_through_url_pages')

    async def navigate_through_custom_pages(self,context: CollectionContext):
        try:
            html = await context.page.content()
            soup = BeautifulSoup(html,'lxml')
            cities = [x.text for x in soup.find('div',class_='other-cities-list').find('ul').findAll('li')]
            print(cities)
            
            if self.config.search_limit:
                cities = cities[:self.config.search_limit]

            for city_index, city in enumerate(cities):
                print(f"Processing city {city_index + 1}/{len(cities)}: {city}")
                context.city = city
                page_url = f"{self.config.url}/{city}"
                print(page_url)
                await self._navigate_to_new_page(context,page_url)
                await asyncio.sleep(10)  # Keep original timing

            return CollectionResult(True, 0, self.data_manager.data, [], context)
        except:
            self._log_strategy_error('PageNavigator.navigate_through_url_pages')
    
    async def navigate_through_ajax_pages(self,context: CollectionContext):
        try:
            page_counter = 1
            max_attempts = self.config.max_nevigated_page
            total_records = 0
            errors = []

            while page_counter <= max_attempts:
                print(f'Page No:{page_counter}')
                # Get all available page numbers
                page_numbers = await self._get_available_pages(context.page)
                print(f"Available pages: {page_numbers}")

                if page_counter == 1:
                    # First page is already loaded
                    await self.data_manager.extract_page_data(context)
                else:
                    # Click on the page
                    success = await self.page_navigator.click_ajax_page(context.page, page_counter)
                    
                    if success:
                        # Wait for AJAX content to load
                        await self.page_navigator.wait_for_ajax_content(context.page)
                        
                        # Extract data
                        await self.data_manager.extract_page_data(context)
                    else:
                        print(f"Failed to navigate to page {page_counter}")
                        next_button = await context.page.query_selector(self.config.next_page_selector)
                        await next_button.click()
                        success = await self.page_navigator.click_ajax_page(context.page, page_counter)
                        await self.page_navigator.wait_for_ajax_content(context.page)
                        await self.data_manager.extract_page_data(context)

                
                # Small delay between pages
                await context.page.wait_for_timeout(500)
                page_counter += 1
            return CollectionResult(True, total_records, self.data_manager.data, errors, context)
            
        except:
            self._log_strategy_error('PageNavigator.navigate_through_ajax_pages')

class OneShotStrategy(BaseCollectionStrategy):    
    async def execute(self, context: CollectionContext) -> CollectionResult:
        await self._block_unwanted_routes(context)
        """Execute one-shot strategy - from original _collect_oneshort_data"""
        try:
            return await self.data_manager.extract_page_data(context)
        
        except Exception as e:
            self._log_strategy_error('OneShotStrategy')
            return CollectionResult(False, 0, pd.DataFrame(), [e], context)

class LinkNavigationStrategy(BaseCollectionStrategy):
    async def execute(self, context: CollectionContext) -> CollectionResult:
        await self._block_unwanted_routes(context)
        try:
            total_records = 0
            errors = []
            context._back_to_original_link = False
            if self.config.site_id=='hero_links':
               if ((context.page.url != self.config.url)):
                    print(f'Page url changed-----------Going Back to Original')
                    await context.page.goto(self.config.url, wait_until='networkidle')

            
            if self.config.search_selector:
               await self.page_navigator.click_by_javascript(context.page, self.config.search_selector)
            else:
                await self.page_navigator.wait_for_cascade_load(context.page)
            # Extract links
            await self._scroll_to_load_all_stores(context.page,max_scroll=6)
            extracted_links_lyr1 = await context.extract_links(selector=f'{self.config.links_selector} a', label='STATE')
            # print(extracted_links_lyr1)
            context.extracted_links_lyr1=extracted_links_lyr1

            if self.config.links_selector_lyr2:
                for index1,link in enumerate(context.extracted_links_lyr1):
                    print(f"\nProcessing Link Layer1:: {index1 + 1}/{len(context.extracted_links_lyr1)}: {link.url}")
                    await context.page.goto(link.url)
                    await self.page_navigator.wait_for_cascade_load(context.page)
                    await self._scroll_to_load_all_stores(context.page,max_scroll=6)

                    if self.config.site_id=='lenskart_2':
                       self.progress_manager.update("custom_links", str(link.url))


                    extracted_links_lyr2 = await context.extract_links(selector=f'{self.config.links_selector_lyr2} a', label='CITY')
                    context.extracted_links_lyr2=extracted_links_lyr2
                    
                    context.extracted_links_lyr2=[i for i in extracted_links_lyr2 if i.url not in self.progress_manager.progress_info.links_completed]

                    if self.config.links_selector_lyr3:
                        for index2,link2 in enumerate(context.extracted_links_lyr2):
                            print(f"\nProcessing Link Layer2:: {index2 + 1}/{len(context.extracted_links_lyr2)}: {link2.url}")
                            await context.page.goto(link2.url)
                            await self.page_navigator.wait_for_cascade_load(context.page)
                            await self._scroll_to_load_all_stores(context.page,max_scroll=6)

                        
                            extracted_links_lyr3 = await context.extract_links(selector=f'{self.config.links_selector_lyr3} a', label='LOCALITY')
                            
                            if self.config.site_id=='lenskart_2':
                               extracted_links_lyr3=[i for i in extracted_links_lyr3 if i.url not in [j.url for j in extracted_links_lyr1]]
                               # print(extracted_links_lyr3)
                            if self.config.site_id == "lenskart_2" and len(extracted_links_lyr3) == 0:
                               await self._navigate_to_new_page(context, link2.url)

                            else:
                                context.extracted_links_lyr3=extracted_links_lyr3
                                await self._collect_navigated_links_data(context)
                                
                    else:
                        await self._collect_navigated_links_data(context)

            else:
                await self._collect_navigated_links_data(context)

            
            if (len(context.not_collcted_links)>0):
                print('Working on Failed Links')
                print(f'Total Links Failed:{len(context.not_collcted_links)}')
                for url in context.not_collcted_links:
                    await self._navigate_to_new_page(context,url)

            return CollectionResult(True, 0, self.data_manager.data, [], context)

        except Exception as e:
            self._log_strategy_error('LinkNavigationStrategy')
            return CollectionResult(False, 0, pd.DataFrame(), [e], context)
    
    async def _collect_navigated_links_data(self, context: CollectionContext):
        try:
            if self.config.links_selector_lyr3 and len(context.extracted_links_lyr3)>0:
                await self._collect_list_links_data(context,context.extracted_links_lyr3)

            elif self.config.links_selector_lyr2:
                await self._collect_list_links_data(context,context.extracted_links_lyr2)

            else:
                await self._collect_list_links_data(context,context.extracted_links_lyr1)
        except:
            self._log_strategy_error('LinkNavigationStrategy._collect_navigated_links_data')

    
    async def _collect_list_links_data(self, context: CollectionContext, extracted_links):
        try:
            if self.config.search_limit:
                extracted_links = extracted_links[:self.config.search_limit]
            
            extracted_links=[i for i in extracted_links if i.url not in self.progress_manager.progress_info.links_completed]
            for index1,link in enumerate(extracted_links):
                print(f"\nProcessing Link Final Layer:: {index1 + 1}/{len(extracted_links)}: {link.url}")

            # for link in extracted_links:
                try:
                    await self._navigate_to_new_page(context, link.url)
                except Exception as e:
                    print(f"Error processing link {link.url}: {e}")
                        
        except:
            self._log_strategy_error('LinkNavigationStrategy._collect_list_links_data')

class RequestNavigationStrategy(BaseCollectionStrategy):
    async def execute(self, context: CollectionContext) -> CollectionResult:
        await self._block_unwanted_routes(context)
        await self.page_navigator.wait_for_cascade_load(context.page)

        try:
            if self.config.data_tab_selector:
               await self.page_navigator.click_by_javascript(context.page, self.config.data_tab_selector)
            total_records = 0
            errors = []
            context._back_to_original_link = False
            if self.config.site_id=='escorts_kubota_request':
                html = await context.page.content()
                soup = BeautifulSoup(html, 'html.parser')

                # brand_select = soup.select_one("#inputBrand")
                # brand_info = { opt["value"]:opt["data-slug"] for opt in brand_select.find_all("option") if opt.get("value") and opt["value"] != ""}
                brand_info={'63':'kubota'}
                state_select = soup.select_one("#inputState")
                state_info = {opt["value"]:opt["data-slug"] for opt in state_select.find_all("option") if opt.get("value") and opt["value"] != ""}
            
                for brand_id in brand_info.keys():
                    for state_id in state_info.keys():
                        req_dist_data= await self.make_custom_request(self.config.request_url,
                                                                 params={'state_id': str(state_id),'brand_id': str(brand_id),'module': '2'},
                                                                 headers=self.config.request_headers)

                        for indx,row in pd.DataFrame().from_dict(req_dist_data).iterrows():
                            dist_url=f"https://www.tractorjunction.com/find-tractor-dealers/{brand_info[brand_id]}/{row['dist_slug']}/"
                            if dist_url not in self.progress_manager.progress_info.links_completed:
                               await self._navigate_to_new_page(context,dist_url)

            elif self.config.brand_name in ['Hyundai','Manyavar']:
                required_inputs = await self.InputDataSource.get_input_data(self.config.input_type,self.config.brand_name)
                required_inputs=[i for i in required_inputs if i not in self.progress_manager.progress_info.inputs_completed or i!=None]

                required_inputs_gen=iter(required_inputs)
                total_input=len(required_inputs)
                del required_inputs
                for index, txt_input in enumerate(required_inputs_gen):
                    print(f"Processing {self.config.input_type[0]} {index + 1}/{total_input}: {txt_input}")
                    if self.config.search_limit:
                        if index==self.config.search_limit:
                           break
                    
                    if self.config.brand_name== 'Hyundai': 
                        headers = {
                                    'accept': '*/*',
                                    'accept-language': 'en-US,en;q=0.9',
                                    'authorization': 'bearer edeb5482-db71-4a83-8ffb-8e5bc5621c42',
                                    'origin': 'https://www.hyundai.com',
                                    'priority': 'u=1, i',
                                    'referer': 'https://www.hyundai.com/',
                                    'sec-ch-ua': '"Microsoft Edge";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
                                    'sec-ch-ua-mobile': '?0',
                                    'sec-ch-ua-platform': '"Windows"',
                                    'sec-fetch-dest': 'empty',
                                    'sec-fetch-mode': 'cors',
                                    'sec-fetch-site': 'cross-site',
                                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0',
                                }


                        lat_long=await self.make_custom_request(f"https://explore.mappls.com/apis/O2O/entity/{txt_input}",params={'loc': 'IN','lan': 'en',}, headers=headers)
                        print(lat_long)
                        context.request_params={'dealerCategoryId': '1','latitude': lat_long['latitude'],'longitude': lat_long['longitude'],'distance': '10',
                                                'modelId': '','loc': 'IN','lan': 'en',}

                        context.header=self.config.request_headers
                        await self.data_manager.extract_page_data(context)
                        self.progress_manager.update("custom_input", str(txt_input))

                    elif self.config.brand_name== 'Manyavar':  
                        context.header=self.config.request_headers
                        context.request_params = {'showMap': 'true','radius': '15.0','storeType': 'all','postalCode':str(txt_input),}
                        await self.data_manager.extract_page_data(context)
                        self.progress_manager.update("custom_input", str(txt_input))

            elif self.config.brand_name=='Metropolis Labs':
                html = await context.page.content()
                soup = BeautifulSoup(html, 'html.parser')
                state_select = soup.select_one("select[id='state']")
                # print(state_select)
                state_info = {opt.text:opt["value"] for opt in state_select.find_all("option") if opt.get("value") and opt.text != "All"}
                # print(state_info)
                for state_id in state_info.keys():
                    headers = {
                        'accept': 'application/json, text/javascript, */*; q=0.01',
                        'accept-language': 'en-US,en;q=0.9',
                        'priority': 'u=1, i',
                        'referer': 'https://www.metropolisindia.com/labs',
                        'sec-ch-ua': '"Microsoft Edge";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
                        'sec-ch-ua-mobile': '?0',
                        'sec-ch-ua-platform': '"Windows"',
                        'sec-fetch-dest': 'empty',
                        'sec-fetch-mode': 'cors',
                        'sec-fetch-site': 'same-origin',
                        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0',
                        'x-requested-with': 'XMLHttpRequest',
                    }

                    state_url=self.config.request_url+state_info[state_id]
                    req_city_data= await self.make_custom_request(state_url,headers=headers)
                    # print(req_city_data)
                    for city in req_city_data:
                        city_url='https://www.metropolisindia.com/get-localities/'+city['city']
                        req_locality_data= await self.make_custom_request(city_url,headers=headers)
                        # print(req_locality_data)
                        for locality in req_locality_data:
                            final_url=f"https://www.metropolisindia.com/labs/{state_info[state_id]}/{city['city'].replace(' ','-')}/diagnostic-centre-in-{locality['locality'].replace(' ','-')}"
                            await self._navigate_to_new_page(context,final_url)

            elif self.config.brand_name=='Kajaria Ceramics':
                context.header = {
                        'Accept': 'application/json, text/plain, */*',
                        'Accept-Language': 'en-US,en;q=0.9',
                        'Connection': 'keep-alive',
                        'Sec-Fetch-Dest': 'empty',
                        'Sec-Fetch-Mode': 'cors',
                        'Sec-Fetch-Site': 'same-origin',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0',
                        'X-XSRF-TOKEN': 'eyJpdiI6Ilp2SDJvdkpqVjhwWndIQmI3amQ0ZkE9PSIsInZhbHVlIjoiSXF0MkZmL1JJNjdkOERlL1E3NHJEYUZJcFlyYjlEUTNSNERWSnR4TVlLSXI3UVpCblVzaUlUNkwvSlNYb1ZzVDNyVGV6OWJCTlZMTzVwUW9jMmllUnZkdVY0ektPdU9pYVdPM3BSWm83UTRzTG13WC84R3pNMWJvQzNCelZ1eXgiLCJtYWMiOiI0ODU0ODlkZGE3MmUyMjFiNzY4YThjOGY0MmNjOGJmOWYyNjU0ZGNmNjlhNDNlZDk3MGZlNWI2ZTk1OWZjMzYwIiwidGFnIjoiIn0=',
                        'sec-ch-ua': '"Microsoft Edge";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
                        'sec-ch-ua-mobile': '?0',
                        'sec-ch-ua-platform': '"Windows"',
                    }

                html = await context.page.content()
                soup = BeautifulSoup(html, 'html.parser')
                category = soup.select_one("select[id='storeCategory']")
                category_info = {opt.text:opt["value"] for opt in category.find_all("option") if opt.get("value") and opt.text != "All Categories"}
                category_info={'Kajaria_Ceramics_gt':'3','Kajaria_Ceramics_pvt':'4','Kajaria_Ceramics_gvt':'2'}
                for cat_id in category_info.keys():
                    params = {'country': 'India','storeCategory': category_info[self.config.site_id],}
                    states = await self.make_custom_request('https://www.kajariaceramics.com/api/getStates', params=params, headers=context.header)
                    for state in states:
                        print(state)
                        context.request_params = { 'search': '','storeCategory': category_info[self.config.site_id],'country': 'India','state': state,'city': '','pincode': '','page': '1','perPage': '300',}
                        await self.data_manager.extract_page_data(context)
                        self.progress_manager.update("custom_input", str(state))

            elif self.config.brand_name== 'Fab India': 
                context.header=self.config.request_headers
                context.request_params = self.config.request_params
                headers = {
                            'accept': 'application/json, text/plain, */*',
                            'accept-language': 'en-US,en;q=0.9',
                            'if-none-match': '"0e31c35c39d8651cf8c48e0b9df16e914-gzip"',
                            'origin': 'https://www.fabindia.com',
                            'priority': 'u=1, i',
                            'referer': 'https://www.fabindia.com/',
                            'sec-ch-ua': '"Microsoft Edge";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
                            'sec-ch-ua-mobile': '?0',
                            'sec-ch-ua-platform': '"Windows"',
                            'sec-fetch-dest': 'empty',
                            'sec-fetch-mode': 'cors',
                            'sec-fetch-site': 'same-site',
                            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0',
                            'x-anonymous-consents': '%5B%5D',
                            }

                params = {'isFabOne': 'false','lang': 'en','curr': 'INR',}
                city_info = await self.make_custom_request('https://apisap.fabindia.com/occ/v2/fabindiab2c/stores/storescounts/domestic/',
                                                         params={'isFabOne': 'false','lang': 'en','curr': 'INR',}, headers=headers)

                city_info=[i['name'] for i in city_info['popularCityList']]+[i['name'] for i in city_info['otherCityList']]
                print(city_info)
                required_inputs_gen=iter(city_info)
                total_input=len(city_info)
                del city_info
                for index, txt_input in enumerate(required_inputs_gen):
                    print(f"Processing {self.config.input_type[0]} {index + 1}/{total_input}: {txt_input}")
                    context.request_url= self.config.request_url+str(txt_input)
                    await self.data_manager.extract_page_data(context)
                    self.progress_manager.update("custom_input", str(txt_input))                
            
            elif self.config.brand_name== 'Havells': 
                category_info={"Dealer Locator":'dealer',"Exclusive Brand Stores":"galaxy","Utsav Stores":'utsav',"Branches":"branches","Experience Centre":"experience_centre"}
                # context.header=self.config.request_headers
                context.header = headers = {
                    'accept': 'application/json, text/javascript, */*; q=0.01',
                    'accept-language': 'en-US,en;q=0.9',
                    'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
                    'newrelic': 'eyJ2IjpbMCwxXSwiZCI6eyJ0eSI6IkJyb3dzZXIiLCJhYyI6IjM4MzM1MDQiLCJhcCI6IjExMjAyODE4MTkiLCJpZCI6IjQ3NDJkZjEwNzA2OTBkNzUiLCJ0ciI6ImY1YTUwMjhjODAyN2UzZGRmMmJmODlhNjUzN2QyYzgxIiwidGkiOjE3NjA1NTIyOTEwNjIsInRrIjoiMTMyMjg0MCJ9fQ==',
                    'origin': 'https://havells.com',
                    'priority': 'u=0, i',
                    'referer': 'https://havells.com/store-locator',
                    'sec-ch-ua': '"Microsoft Edge";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                    'sec-fetch-dest': 'empty',
                    'sec-fetch-mode': 'cors',
                    'sec-fetch-site': 'same-origin',
                    'traceparent': '00-f5a5028c8027e3ddf2bf89a6537d2c81-4742df1070690d75-01',
                    'tracestate': '1322840@nr=0-1-3833504-1120281819-4742df1070690d75----1760552291062',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0',
                    'x-newrelic-id': 'Vw4EUlNTDBABVFlWDwgFUVAF',
                    'x-requested-with': 'XMLHttpRequest',
                }
                # print(category_info[self.config.Sub_Category_1])
                st_info = await self.make_custom_request('https://havells.com/storelocator/index/getstate',method='POST',
                                                        data={'action': 'get_state','selectWebsite': 'Havells','selectType': category_info[self.config.Sub_Category_1],}, 
                                                        headers=context.header,)
                st_info=json.loads(st_info)
                for index,st in enumerate(st_info):
                    print(f"Processing {index} {index + 1}/{len(st_info)}: {st}")
                    context.request_params ={'action': 'get_storelocatorlist','selectWebsite': 'Havells','selectType':category_info[self.config.Sub_Category_1],'selectState': str(st),'selectCity': '','selectCategory': '',}
                    await self.data_manager.extract_page_data(context)

            elif self.config.brand_name in ['Nexa Maruti Suzuki','Maruti Suzuki']:

                df_dealer_details = await self.make_custom_request(self.config.request_url,params=self.config.request_params, headers=self.config.request_headers,)
                df_dealer_details=df_dealer_details['data']
                df_dist_details=pd.DataFrame().from_dict(df_dealer_details)
                df_dist_details=df_dist_details[~df_dist_details['latitude'].isin(self.progress_manager.progress_info.latitude_completed)]

                for index,dlr in df_dist_details.iterrows():
                    if self.config.search_limit:
                        if index==self.config.search_limit:
                           break

                    print(f"Processing::{index}/{len(df_dist_details)}")
                    if dlr['latitude']:
                        context.header = self.config.request_headers
                        if self.config.brand_name=='Maruti Suzuki':
                            context.request_url=f"https://www.marutisuzuki.com/dms/v2/api/common/msil/dms/nearest-dealers?latitude={dlr['latitude']}&longitude={dlr['longitude']}&distance=50000&channel={self.config.request_params['channel']}&dealerType=S,3S&modelCode=All+cars"

                        else:
                            params={'isActive': 'true','latitude': dlr['latitude'],'longitude': dlr['longitude'],'distance': '500000','dealerType': 'S,3S','channel': 'EXC'}
                            context.request_params  = params
                            context.request_url="https://www.nexaexperience.com/dms/v2/api/common/msil/dms/nearest-dealers"

                        await self.data_manager.extract_page_data(context)
                        self.progress_manager.update("latitude", str(dlr['latitude']))

            else:
                await self.data_manager.extract_page_data(context)


            return CollectionResult(True, 0, self.data_manager.data, [], context)

        except Exception as e:
            self._log_strategy_error('LinkNavigationStrategy')
            return CollectionResult(False, 0, pd.DataFrame(), [e], context)
# ================================================================================================
class CollectionStrategyFactory:
    @staticmethod
    def create_strategy(config,BrowserManager, dropdown_handler, popup_handler, InputDataSource, page_navigator, data_manager,progress_manager: ProgressManager):
        
        # Original strategy selection logic
        if config.state_options_selector:
            print("Data Collection type: Cascading Div Based")
            return DivDropdownStrategy(config,BrowserManager, dropdown_handler, page_navigator, data_manager,InputDataSource,progress_manager)
        
        elif config.input_selector:
            print("Data Collection type: Input Based")
            return InputBasedStrategy(config,BrowserManager, dropdown_handler, page_navigator, data_manager, InputDataSource,progress_manager)
        
        elif config.paginate_selector:
            print("Data Collection type: Pagination")
            return PaginationStrategy(config,BrowserManager, dropdown_handler, page_navigator, data_manager,InputDataSource,progress_manager)
        
        # elif config.site_id == "kalyan_jewellers":
        #     print("Data Collection type: kalyan_jewellers")
        #     return KalyanJewellersStrategy(config, dropdown_handler, page_navigator, data_manager,InputDataSource)
        
        elif config.links_selector:
            print("Data Collection type: Navigated Links")
            return LinkNavigationStrategy(config,BrowserManager, dropdown_handler, page_navigator, data_manager,InputDataSource,progress_manager)
        
        elif config.request_url:
            print("Data Collection type: Navigated Links")
            return RequestNavigationStrategy(config,BrowserManager, dropdown_handler, page_navigator, data_manager,InputDataSource,progress_manager)

        elif (not config.paginate_selector and not config.input_selector and 
              not config.state_options_selector and not config.state_selector and 
              not config.links_selector):
            print("Data Collection type: One Shot")
            return OneShotStrategy(config,BrowserManager, dropdown_handler, page_navigator, data_manager,InputDataSource,progress_manager)
        
        else:
            print("Data Collection type: Simple Cascading")
            return SimpleDropdownStrategy(config,BrowserManager, dropdown_handler, page_navigator, data_manager, InputDataSource,progress_manager)

class CascadingDataCollector:

    def __init__(self, site_config):
        # Original initialization
        self.config = site_config
        self.collection_date = datetime.now()
        
        # Create base record
        self.base_record = {
            'Category': self.config.category,
            'Company': self.config.company_name,
            'Brand': self.config.brand_name,
            'Latitude': None,
            'Longitude': None,
            'Crawler':'Crawlee',
            'Sub_Brand': None,
            'Country': self.config.country,
            'Relevant_Date': self.collection_date.date(),
            'Runtime': self.collection_date
        }
        
         #Get the Progress report if You have
        self.progress_report=ProgressManager.load_progress(self.config.site_id)
        if not self.progress_report:
           self.progress_report=ProgressReport()

        self.progress_manager=ProgressManager(self.config.site_id,progress_info=self.progress_report)

        # Initialize components
        self.browser_manager = BrowserManager(site_config)
        self.page_navigator = PageNavigator(site_config)
        self.data_manager = DataManager(site_config,self.browser_manager, self.base_record, self.progress_manager)
       
        self.dropdown_handler = DropdownHandler(site_config)
        self.popup_handler = PopupHandler(site_config)
        self.InputDataSource = InputDataSource()
        self.refresh_manager = RefreshManager(refresh_interval=120, memory_threshold=80.0)

        # Original cache variables
        self._back_to_orginal_link = True 
        self.session_start_time = datetime.now()
        self._restart_needed = False


    async def _collect_data(self, today: datetime = None) -> pd.DataFrame:
        if today is None:
            today = datetime.now()
        
        crawler = self.browser_manager.create_crawler(self._handle_request)
        try:
            # Latest_Date = today.date()
            try:
                df_info=await self.InputDataSource.get_data_info(self.config.brand_name,Sub_Category_1=self.config.Sub_Category_1,Sub_Category_2=self.config.Sub_Category_2)
                print(df_info)
                Latest_Date=max(df_info['Relevant_Date'])
            except:
                Latest_Date = today.date()


            if (((today.date() - Latest_Date) >= timedelta(7)) | (Latest_Date == today.date())):
                await crawler.run([Request.from_url(self.config.url)])
                final_data = self.data_manager.finalize_data()
                return final_data
            else:
                print('------------------------------------------------------------------------')
                print(f"Brand:{self.config.brand_name}\nLast_Collected:{Latest_Date}")
                return pd.DataFrame()
            
        except Exception as e:
            self.InputDataSource._log_strategy_error('CascadingDataCollector._collect_data')
            # raise e
    
    async def _handle_request(self, context: PlaywrightCrawlingContext) -> None:
        print(f"Processing: {context.request.url}")
        try:
            # Original popup handling
            await self.popup_handler._navigate_and_prepare_with_context(context)
            print('-------------------------------------Strategy INFO-------------------------------------------') 
            # Create enhanced context that wraps PlaywrightCrawlingContext
            collection_context = CollectionContext(crawlee_context=context)
            
            # Create and execute appropriate strategy
            strategy = CollectionStrategyFactory.create_strategy(self.config, self.browser_manager,self.dropdown_handler, self.popup_handler,
                self.InputDataSource, self.page_navigator, self.data_manager,self.progress_manager)
            
            print(f"Executing strategy: {strategy.get_strategy_name()}")
            try:
                result = await strategy.execute(collection_context)
            except:
                result = await strategy.execute(collection_context)
            
            if result.success:
                print(f"Strategy completed successfully. Found {result.records_collected} records.")
                self.data = self.data_manager.data
            else:
                print(f"Strategy failed with errors: {result.errors}")
        except:
            self.InputDataSource._log_strategy_error("CascadingDataCollector._handle_request")


    # async def _create_fresh_browser_session(self, context: PlaywrightCrawlingContext):
    #     try:
    #         # Store browser reference before closing
    #         browser = context.page.context.browser
            
    #         # Close current page and context
    #         await context.page.close()
    #         await context.page.context.close()
            
    #         # Create completely new browser context
    #         new_context = await browser.new_context(**self.browser_manager.get_browser_config())
    #         new_page = await new_context.new_page()
            
    #         # Navigate to original URL with fresh session
    #         await new_page.goto(self.config.url, wait_until='networkidle')
    #         await self.page_navigator.wait_for_cascade_load(new_page)
            
    #         # Update context with new page
    #         context.page = new_page
            
    #         # Handle initial popups in fresh session
    #         await self.popup_handler._navigate_and_prepare_with_context(context)
            
    #         print("Fresh browser session created successfully")
        
    #     except Exception as e:
    #         print(f"Error creating fresh browser session: {e}")





     