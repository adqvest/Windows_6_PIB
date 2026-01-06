# -*- coding: utf-8 -*-
"""
Created on Mon Aug  4 17:34:08 2025
@author: Santonu
"""
import pandas as pd
import os
import asyncio
import re
from bs4 import BeautifulSoup
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass
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
from storel_locator_helpers import CamoufoxPlugin,DropdownHandler,PopupHandler,BrowserManager,PageNavigator
logging.getLogger('crawlee').setLevel(logging.WARNING)

# ================================================================================================
@dataclass
class CollectionContext:
    crawlee_context: PlaywrightCrawlingContext
    state: Optional[str] = None
    city: Optional[str] = None
    locality: Optional[str] = None
    sub_brand: Optional[str] = None
    _back_to_original_link=True
    
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
class CollectionResult:
    """Result of a collection operation"""
    success: bool
    records_collected: int
    data: pd.DataFrame
    errors: List[str]
    context: CollectionContext

# ================================================================================================
# DATA MANAGEMENT
# ================================================================================================
class DataManager:    
    def __init__(self, config, base_record):
        self.config = config
        self.base_record = base_record
        self.page_navigator = PageNavigator(config)
        self._log_strategy_error=InputDataSource._log_strategy_error
        self.data = pd.DataFrame(columns=['Company', 'Brand', 'Sub_Brand', 'Address', 'State', 'City', 'Locality', 'Pincode', 'Latitude', 'Longitude', 'Country', 'Relevant_Date', 'Runtime'])
    
    async def extract_page_data(self, context: CollectionContext) -> CollectionResult:
        try:
            print('-----------------------------------------------------------DATA COLLECTION-----------------------------------------------------------------')
            print(context.page.url)
            
            # Handle special site requirements
            if self.config.site_id == 'Asics':
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
            containers = soup.select(self.config.data_container_selector)
            
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
                    record = self.config.custom_data_extractor(container, context.state, context.city, context.locality,
                                                               self.config.address_selector, self.config.filter_flag,
                                                               self.base_record, self.config.site_id)
                        
                    
                    if record and isinstance(record, list):
                        new_data = pd.DataFrame(record)
                        if context.sub_brand:
                            new_data['Sub_Brand'] = context.sub_brand
                        
                        self.data = pd.concat([self.data, new_data], ignore_index=True)
                        records_added += len(record)
            
            # Handle back navigation
            if ((context.page.url != self.config.url) and (not self.config.data_tab_selector) and (context._back_to_original_link)):
                print('Page url changed-----------Going Back to Original')
                await context.page.goto(self.config.url, wait_until='networkidle')
            
            if self.config.back_button_selector:
                await self.page_navigator.click_by_javascript(context.page, self.config.back_button_selector)
            
            return CollectionResult(True, records_added, self.data, [], context)
        
        except Exception as e:
            self._log_strategy_error('DataManager.extract_page_data')
            return CollectionResult(False, 0, pd.DataFrame(), [e], context)
    
    def finalize_data(self) -> pd.DataFrame:
        try:
            print(f"\nData collection complete! Found {len(self.data)} records")
            # Remove duplicates
            initial_count = len(self.data)
            df_duplicates=self.data[self.data.duplicated(subset='Address', keep='last')]

            self.data = self.data.drop_duplicates(subset='Address', keep='last')
            final_count = len(self.data)

            filename = f"{self.config.brand_name}_Duplicate_Stores_{today.strftime('%Y%m%d')}.xlsx"
            df_duplicates.to_excel(filename)

            if initial_count != final_count:
                print(f"Removed {initial_count - final_count} duplicate records")
            
            return self.data
        except Exception as e:
            self._log_strategy_error('DataManager.finalize_data')
# ================================================================================================
# MAIN CLASS
class BaseCollectionStrategy(ABC):
    """Base class for collection strategies"""
    def __init__(self, config, dropdown_handler, page_navigator, data_manager, InputDataSource):
        self.config = config
        self.dropdown_handler = dropdown_handler
        self.page_navigator = page_navigator
        self.data_manager = data_manager
        self.InputDataSource = InputDataSource
        self._back_to_original_link = True
        self._log_strategy_error=InputDataSource._log_strategy_error
    
    @abstractmethod
    async def execute(self, context: CollectionContext) -> CollectionResult:
        """Execute the collection strategy"""
        pass
    
    def get_strategy_name(self) -> str:
        """Get strategy name for logging"""
        return self.__class__.__name__
    
    async def _navigate_to_new_page(self,context: CollectionContext, page_url):
        try:
            new_page = await context.page.context.new_page()
            await new_page.goto(page_url)
            await self.page_navigator.wait_for_cascade_load(new_page)
            
            # Create temporary context for new page
            temp_context = CollectionContext(crawlee_context=new_page,state=None,city=None,locality=None,sub_brand=None)

            # Override page reference
            temp_context.crawlee_context.page = new_page
            
            result = await self.data_manager.extract_page_data(temp_context)
            await new_page.close()
        except:
            self.InputDataSource._log_strategy_error('BaseCollectionStrategy._navigate_to_new_page')
# ================================================================================================
class SimpleDropdownStrategy(BaseCollectionStrategy):
    async def execute(self, context: CollectionContext) -> CollectionResult:
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
            print(sub_brands)
               
            sub_brands=['FirstCry']
            for store_type in sub_brands:
                print(f"Processing store type: {store_type}")

                #SELECT CATEGORY
                await self.dropdown_handler._select_native_option(context.page, self.config.store_type_selector, store_type)
                await self.page_navigator.wait_for_cascade_load(context.page)

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
                            await self.page_navigator.click_by_javascript(context.page, self.config.search_selector)
                        
                        await self.data_manager.extract_page_data(context)
                        await context.page.goto(self.config.url, wait_until='networkidle') 
        
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
            states = await self._get_State_City_Locality_Options(context.page,selector=self.config.state_selector)
            print(states)
            print(f"Found {len(states)} states")
            #Santonu
            # states=states[14:15]
            
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
            # Select state if not skipped
            if not self.config.skip_state_selector:
                context._back_to_original_link = True
                try:
                    await context.page.wait_for_selector(self.config.state_selector, timeout=self.config.page_load_timeout)
                except Exception as e:
                    print(e)
                
                await self.dropdown_handler._select_native_option(context.page, options_selector=self.config.state_selector, option=context.state)
                await self.page_navigator.wait_for_cascade_load(context.page)
            
            # Process cities or extract data directly
            if self.config.city_selector:
                return await self._process_cities(context)
            else:
                if self.config.search_selector:
                    await self.page_navigator.click_by_javascript(context.page, self.config.search_selector)
                return await self.data_manager.extract_page_data(context)
        
        except Exception as e:
            self._log_strategy_error('SimpleDropdownStrategy._process_state')
            return CollectionResult(False, 0, pd.DataFrame(), [e], context)
    
    async def _process_cities(self, context: CollectionContext) -> CollectionResult:
        """Process cities for current state"""
        try:
            context._back_to_original_link = False
            cities = await self._get_State_City_Locality_Options(context.page, selector=self.config.city_selector)
            print(f"Found {len(cities)} cities in {context.state}")
            #Santonu
            # cities=cities[2:3]
            
            total_records = 0
            errors = []
            
            for city_index, city in enumerate(cities):
                print(f"Processing city {city_index + 1}/{len(cities)}: {city}")
                context.city = city
                
                try:
                    result = await self._process_city(context)
                    total_records += result.records_collected
                    errors.extend(result.errors)
                except Exception as e:
                    errors.append(f"Error processing city {city}: {e}")
            
            return CollectionResult(True, total_records, self.data_manager.data, errors, context)
        except Exception as e:
            self._log_strategy_error('SimpleDropdownStrategy._process_cities')
        
    async def _process_city(self, context: CollectionContext) -> CollectionResult:
        """Process a single city"""
        try:
            if self.config.data_tab_selector:
                await self.page_navigator.click_by_javascript(context.page, self.config.data_tab_selector)
            
            # Select city if not skipped
            if not self.config.skip_city_selector:
                await self.dropdown_handler._select_native_option(context.page, options_selector=self.config.city_selector, option=context.city)
                await self.page_navigator.wait_for_cascade_load(context.page)
            
            # Handle search button or localities
            if not self.config.locality_selector and self.config.search_selector:
                await self.page_navigator.click_by_javascript(context.page, self.config.search_selector)
            
            # Process localities or extract data directly
            if self.config.locality_selector:
                return await self._process_localities(context)
            else:
                result = await self.data_manager.extract_page_data(context)
                
                # Handle pagination if configured
                if self.config.pagination_selector:
                    result=await self._handle_pagination(context)

                return result
        
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
            localities = await self._get_State_City_Locality_Options(context.page, selector=self.config.locality_selector)
            print(f"Found {len(localities)} localities in {context.city} | {context.state}")
            
            total_records = 0
            errors = []
            
            for locality_index, locality in enumerate(localities):
                print(f"Processing locality {locality_index + 1}/{len(localities)}: {locality}")
                context.locality = locality
                
                try:
                    if not self.config.skip_locality_selector:
                        await self.dropdown_handler._select_native_option(context.page, options_selector=self.config.locality_selector, option=locality)
                        await self.page_navigator.wait_for_cascade_load(context.page)
                    
                    if self.config.search_selector:
                        await self.page_navigator.click_by_javascript(context.page, self.config.search_selector)
                        await self.page_navigator.wait_for_cascade_load(context.page)
                    
                    result = await self.data_manager.extract_page_data(context)
                    total_records += result.records_collected
                    errors.extend(result.errors)
                except Exception as e:
                    errors.append(f"Error processing locality {locality}: {e}")

        except Exception as e:
            self._log_strategy_error('SimpleDropdownStrategy._process_localities')
            return CollectionResult(True, total_records, self.data_manager.data, e, context)

    async def _get_State_City_Locality_Options(self, page,selector) -> List[str]:
        #---------------Get and filter list of states | City | Locality-----------------------------------
        try:
            states = await self.dropdown_handler._get_native_options(page, selector=selector)
            if len(states) > 1:
                states = [s for s in states if not re.search(r"select|all|select city", s, flags=re.IGNORECASE)]
                states = [re.sub(r'Location\s*', '', s).strip() for s in states]
         
            return states
        except Exception as e:
            self._log_strategy_error('SimpleDropdownStrategy._get_State_City_Locality_Options')
            return []

    async def _handle_pagination(self, context: CollectionContext):
        """Handle pagination for current page"""
        try:
            page_counter = 1
            max_attempts = 10
            
            while page_counter <= max_attempts:
                await self.page_navigator.click_by_javascript(context.page, self.config.pagination_selector)
                await self.data_manager.extract_page_data(context)
                page_counter += 1

        except Exception as e:
            self._log_strategy_error('SimpleDropdownStrategy._handle_pagination',context=context)

class DivDropdownStrategy(BaseCollectionStrategy):
    async def execute(self, context: CollectionContext) -> CollectionResult:
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
            states = await self._get_div_State_City_Locality_Options(context.page, self.config.state_selector, self.config.state_options_selector)        
            print(f"Found {len(states)} states")
            #Santonu
            # states=states[15:16]
            total_records = 0
            errors = []
            
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
        except Exception as e:
            self._log_strategy_error('DivDropdownStrategy._collect_states_and_cities')

    async def _process_state_div(self, context: CollectionContext) -> CollectionResult:
        try:
            if not self.config.skip_state_selector:
                await self.dropdown_handler._select_dropdown_option(context.page, self.config.state_selector, 
                                                                    context.state, self.config.state_options_selector)
                    
                await self.page_navigator.wait_for_cascade_load(context.page)
            
            # Handle cities
            if self.config.city_selector:
                return await self._process_cities_div(context)
            else:
                if self.config.search_selector:
                    await self.page_navigator.click_by_javascript(context.page, self.config.search_selector)
                    await self.page_navigator.wait_for_cascade_load(context.page)
                
                return await self.data_manager.extract_page_data(context)
        
        except Exception as e:
            self._log_strategy_error('DivDropdownStrategy._process_state_div')
            return CollectionResult(False, 0, pd.DataFrame(), [e], context)
    
    async def _process_cities_div(self, context: CollectionContext) -> CollectionResult:
        try:
            cities = await self._get_div_State_City_Locality_Options(context.page, self.config.city_selector, self.config.city_options_selector)        
            print(f"Found {len(cities)} cities in {context.state}")
            #Santonu
            # cities=cities[3:4]
            total_records = 0
            errors = []
            
            for city_index, city in enumerate(cities):
                print(f"Processing city {city_index + 1}/{len(cities)}: {city}")
                context.city = city
                try:
                    if not self.config.skip_city_selector:
                        await self.page_navigator.wait_for_cascade_load(context.page)
                        await self.dropdown_handler._select_dropdown_option(context.page, self.config.city_selector, 
                                                                            city, self.config.city_options_selector)

                    if not self.config.locality_selector and self.config.search_selector:
                        await self.page_navigator.click_by_javascript(context.page, self.config.search_selector)
                        print('-----------------------------------------------------------------')
                    
                    # Handle locality if configured
                    if self.config.locality_selector:
                        pass
                    else:
                        result = await self.data_manager.extract_page_data(context)
                        total_records += result.records_collected
                        errors.extend(result.errors)
                
                except Exception as e:
                    errors.append(f"Error processing city {city}: {e}")
            
            return CollectionResult(True, total_records, self.data_manager.data, errors, context)
        except Exception as e:
            self._log_strategy_error('DivDropdownStrategy._process_cities_div')

    async def _get_div_State_City_Locality_Options(self, page,main_selector,option_selector) -> List[str]:
        try:
            #---------------Get and filter list of states | City | Locality-----------------------------------
            try:
                await page.wait_for_selector(main_selector, timeout=self.config.page_load_timeout)
            except:
                pass
            
            states = await self.dropdown_handler._get_dropdown_options(page, main_selector, option_selector)
            states = [s for s in states if not re.search(r"select|all|india|usa", s, flags=re.IGNORECASE)]
            return states
        except Exception as e:
            self._log_strategy_error('DivDropdownStrategy._get_div_State_City_Locality_Options')
            return []
    
class InputBasedStrategy(BaseCollectionStrategy):

    # def __init__(self, config, dropdown_handler, page_navigator, data_manager, InputDataSource):
    #     super().__init__(config, dropdown_handler, page_navigator, data_manager)
    #     self.InputDataSource = InputDataSource
    
    async def execute(self, context: CollectionContext) -> CollectionResult:

        try:
            required_inputs = await self.InputDataSource.get_input_data(self.config.input_type)
            # required_inputs=["201301"]
            total_records = 0
            errors = []
            
            for index, txt_input in enumerate(required_inputs):
                print(f"Processing {self.config.input_type[0]} {index + 1}/{len(required_inputs)}: {txt_input}")
                
                if self.config.search_limit:
                    if index==self.config.search_limit:
                       break
                
                try:
                    result = await self._process_input(context, str(txt_input))
                    total_records += result.records_collected
                    errors.extend(result.errors)
                except Exception as e:
                    errors.append(f"Error processing input {txt_input}: {e}")
            
            return CollectionResult(True, total_records, self.data_manager.data, errors, context)
        
        except Exception as e:
            self._log_strategy_error('InputBasedStrategy')
            return CollectionResult(False, 0, pd.DataFrame(), [e], context)
    
    async def _process_input(self, context: CollectionContext, txt_input: str) -> CollectionResult:
        context._back_to_original_link=False
        try:
            if not re.search(r"div|id|select",self.config.input_selector,flags=re.IGNORECASE):
                await context.page.get_by_placeholder("Enter state, city, street or pin code").nth(1).fill(txt_input)
                await context.page.keyboard.press('Enter')
                await asyncio.sleep(1)
                await self.data_manager.extract_page_data(context)
                return CollectionResult(True, 0, self.data_manager.data, [], context)
            else:
                await self.page_navigator.clear_and_input(context.page, self.config.input_selector, txt_input)
                if self.config.suggested_option_selector:
                    return await self._handle_suggestions(context)
                else:
                    return await self._handle_direct_search(context)

        except Exception as e:
            self._log_strategy_error('InputBasedStrategy._process_input')
            return CollectionResult(False, 0, self.data_manager.data, [], context)
        
    async def _handle_suggestions(self, context: CollectionContext) -> CollectionResult:
        try:
            options = await self.dropdown_handler._get_custom_options(context.page, 
                                                                  dropdown_selector=self.config.input_selector,
                                                                  options_selector=self.config.suggested_option_selector)
            
            print(options)
            total_records = 0
            errors = []

            for option_index, option in enumerate(options):
                print(f"---------------------------{option}")
                if self.config.suggestion_search_limit:
                    if option_index==self.config.suggestion_search_limit:
                       break

                try:
                    # For sites where we need to click option selector to activate other options
                    if option_index > 0:
                        await self.dropdown_handler._click_option_selector(context.page, self.config.input_selector)
                    
                    await self.dropdown_handler._select_custom_option(context.page, options_selector=self.config.suggested_option_selector,
                                                                      option=option, dropdown_selector=self.config.input_selector)
                        
                    
                    # Customize for Sketchers 
                    if self.config.input_option_selector:
                        await self.dropdown_handler._select_native_option(context.page, option="300", options_selector=self.config.input_option_selector)
                    
                    if self.config.search_selector:
                        await self.dropdown_handler._click_option_selector(context.page, self.config.search_selector)
                        await self.page_navigator.wait_for_cascade_load(context.page)
                    
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

class PaginationStrategy(BaseCollectionStrategy):

    async def execute(self, context: CollectionContext) -> CollectionResult:
        try:
            if self.config.paginate_selector=='Url_Pages':
                return await self.navigate_through_url_pages(context)
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
                await page.wait_for_selector(self.config.data_container_selector, timeout=5000)
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
        """Execute one-shot strategy - from original _collect_oneshort_data"""
        try:
            # Block redirects 
            await context.page.route("**/*", lambda route: (
                route.abort() if route.request.redirected_from else route.continue_()
            ))
            
            return await self.data_manager.extract_page_data(context)
        
        except Exception as e:
            self._log_strategy_error('OneShotStrategy')
            return CollectionResult(False, 0, pd.DataFrame(), [e], context)

class LinkNavigationStrategy(BaseCollectionStrategy):

    async def execute(self, context: CollectionContext) -> CollectionResult:
        try:
            total_records = 0
            errors = []
            
            # Extract links
            extracted_links = await context.extract_links(selector=f'{self.config.links_selector} a', label='CATEGORY')
            
            for link in extracted_links:
                print(link.url)
                try:
                    result = await self._collect_navigated_links_data(context, link)
                    total_records += result.records_collected
                    errors.extend(result.errors)
                except Exception as e:
                    errors.append(f"Error processing link {link.url}: {e}")
            
            return CollectionResult(True, total_records, self.data_manager.data, errors, context)
        
        except Exception as e:
            self._log_strategy_error('LinkNavigationStrategy')
            return CollectionResult(False, 0, pd.DataFrame(), [e], context)
    
    async def _collect_navigated_links_data(self, context: CollectionContext, link):
        try:
            new_page = await context.page.context.new_page()
            await new_page.goto(link.url)
            await new_page.wait_for_load_state('networkidle')
            
            # Create temporary context for new page
            temp_context = CollectionContext(crawlee_context=new_page,state=None,city=None,locality=None,sub_brand=None)

            # Override page reference
            temp_context.crawlee_context.page = new_page
            
            result = await self.data_manager.extract_page_data(temp_context)
            await new_page.close()
            return result
        except:
            self._log_strategy_error('LinkNavigationStrategy._collect_navigated_links_data')
        
class KalyanJewellersStrategy(BaseCollectionStrategy):
    
    async def execute(self, context: CollectionContext) -> CollectionResult:
        """Execute Kalyan strategy - from original _collect_kalyan_data"""
        try:
            self._back_to_original_link = False
            await self.page_navigator.wait_for_cascade_load(context.page)
            await self.page_navigator.click_by_javascript(context.page, self.config.search_selector)
            
            total_records = 0
            errors = []
            
            city_links = await context.extract_links(selector=self.config.links_selector, label='KALYAN_MAJOR_CITY')
            #Santonu
            # city_links=city_links[:1]

            for link in city_links:
                print(link.url)
                await context.page.goto(link.url)
                await self.page_navigator.wait_for_cascade_load(context.page)
                await self.page_navigator.click_by_javascript(context.page, "div.text-center.mt-5 button.rounded-lg")
                
                locality_links = await context.extract_links(selector="div.addreddDiv div.ant-row.css-ut69n1 a", label='KALYAN_LOCALITY')
                
                for link2 in locality_links:
                    print(link2.url)
                    await context.page.goto(link2.url)
                    await self.page_navigator.wait_for_cascade_load(context.page)
                    
                    stores_links = await context.extract_links(selector="div.cardCityName div.ant-col a", label='KALYAN_TOWN')
                    for link3 in stores_links:
                        result = await self._collect_navigated_links_data(context, link3)
                        total_records += result.records_collected
                        errors.extend(result.errors)
            
            return CollectionResult(True, total_records, self.data_manager.data, errors, context)
        
        except Exception as e:
            self._log_strategy_error('KalyanJewellersStrategy')
            return CollectionResult(False, 0, pd.DataFrame(), [e], context)
    
    async def _collect_navigated_links_data(self, context: CollectionContext, link):
        try:
            new_page = await context.page.context.new_page()
            await new_page.goto(link.url)
            await self.page_navigator.wait_for_cascade_load(new_page)
            
            # Create temporary context for new page
            temp_context = CollectionContext(crawlee_context=new_page,state=None,city=None,locality=None,sub_brand=None)

            # Override page reference
            temp_context.crawlee_context.page = new_page
            
            result = await self.data_manager.extract_page_data(temp_context)
            await new_page.close()
            return result
        except:
            self._log_strategy_error('KalyanJewellersStrategy._collect_navigated_links_data')
       
# ================================================================================================
class CollectionStrategyFactory:
    @staticmethod
    def create_strategy(config, dropdown_handler, popup_handler, InputDataSource, page_navigator, data_manager):
        
        # Original strategy selection logic
        if config.state_options_selector:
            print("Data Collection type: Cascading Div Based")
            return DivDropdownStrategy(config, dropdown_handler, page_navigator, data_manager,InputDataSource)
        
        elif config.input_selector:
            print("Data Collection type: Input Based")
            return InputBasedStrategy(config, dropdown_handler, page_navigator, data_manager, InputDataSource)
        
        elif config.paginate_selector:
            print("Data Collection type: Pagination")
            return PaginationStrategy(config, dropdown_handler, page_navigator, data_manager,InputDataSource)
        
        elif config.site_id == "kalyan_jewellers":
            print("Data Collection type: kalyan_jewellers")
            return KalyanJewellersStrategy(config, dropdown_handler, page_navigator, data_manager,InputDataSource)
        
        elif config.links_selector:
            print("Data Collection type: Navigated Links")
            return LinkNavigationStrategy(config, dropdown_handler, page_navigator, data_manager,InputDataSource)
        
        elif (not config.paginate_selector and not config.input_selector and 
              not config.state_options_selector and not config.state_selector and 
              not config.links_selector):
            print("Data Collection type: One Shot")
            return OneShotStrategy(config, dropdown_handler, page_navigator, data_manager,InputDataSource)
        
        else:
            print("Data Collection type: Simple Cascading")
            return SimpleDropdownStrategy(config, dropdown_handler, page_navigator, data_manager, InputDataSource)

class CascadingDataCollector:

    def __init__(self, site_config):
        # Original initialization
        self.config = site_config
        self.collection_date = datetime.now()
        
        # Create base record
        self.base_record = {
            'Company': self.config.company_name,
            'Brand': self.config.brand_name,
            'Latitude': None,
            'Longitude': None,
            'Sub_Brand': None,
            'Country': self.config.country,
            'Relevant_Date': self.collection_date.date(),
            'Runtime': self.collection_date
        }

        # Initialize components
        self.browser_manager = BrowserManager(site_config)
        self.page_navigator = PageNavigator(site_config)
        self.data_manager = DataManager(site_config, self.base_record)
        
        self.dropdown_handler = DropdownHandler(site_config)
        self.popup_handler = PopupHandler(site_config)
        self.InputDataSource = InputDataSource()

        # Original cache variables
        self._dropdown_type_cache = {}
        self._strategy_success_count = {}
        self._back_to_orginal_link = True
        # self._log_strategy_error=InputDataSource._log_strategy_error

    async def _collect_data(self, today: datetime = None) -> pd.DataFrame:
        if today is None:
            today = datetime.now()
        
        crawler = self.browser_manager.create_crawler(self._handle_request)
        try:
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
            # self.InputDataSource._log_strategy_error('_collect_data')
            raise e
    
    async def _handle_request(self, context: PlaywrightCrawlingContext) -> None:
        print(f"Processing: {context.request.url}")
        # try:
        # Original popup handling
        await self.popup_handler._navigate_and_prepare_with_context(context)

        print('-------------------------------------Strategy INFO-------------------------------------------') 
        # Create enhanced context that wraps PlaywrightCrawlingContext
        collection_context = CollectionContext(crawlee_context=context)
        
        # Create and execute appropriate strategy
        strategy = CollectionStrategyFactory.create_strategy(self.config, self.dropdown_handler, self.popup_handler,
            self.InputDataSource, self.page_navigator, self.data_manager)
        
        print(f"Executing strategy: {strategy.get_strategy_name()}")
        result = await strategy.execute(collection_context)
        
        if result.success:
            print(f"Strategy completed successfully. Found {result.records_collected} records.")
            self.data = self.data_manager.data
        else:
            print(f"Strategy failed with errors: {result.errors}")
    
        # except Exception as e:
        #     self.InputDataSource._log_strategy_error('_handle_request')
    
