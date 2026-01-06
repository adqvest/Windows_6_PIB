# -*- coding: utf-8 -*-
"""
Created on Mon Aug  4 12:19:50 2025

@author: Santonu
"""
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass
from pathlib import Path
import importlib
import inspect
import sys
import os
import asyncio
import json

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
    max_nevigated_page: Optional[int] = 1
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
                max_nevigated_page=site_data.get("options",{}).get('max_nevigated_page', 1),
                custom_data_extractor=_custom_extractor)

            return config

        except Exception as e:
            print(f"Error processing: {self._log_strategy_error('_create_config_from_json')}")

    def _log_strategy_error(self,strategy_name:str):
        exc_type, exc_obj, exc_tb = sys.exc_info()
        error_msg = str(exc_obj)
        line_no = exc_tb.tb_lineno if exc_tb else 'unknown'
        print(f"Error in strategy '{strategy_name}':\nError Message: {error_msg}\nLine: {line_no}\n")



class ExtractorLoader:
    def __init__(self, extractor_file: str = "custom_extractors.py"):
        """Initialize with path to Python file containing extractors"""
        self.extractor_file = extractor_file
        self.extractors: Dict[str, Callable] = {}
        self.load_extractors()
    
    def load_extractors(self):
        """Load all extractor functions from the file"""
        try:
            file_path = Path(self.extractor_file)
            
            if not file_path.exists():
                print(f"Extractor file not found: {self.extractor_file}")
                return
            
            # Import the module
            module_name = file_path.stem
            spec = importlib.util.spec_from_file_location(module_name, file_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            # Find all extractor functions
            extractor_count = 0
            for name, func in inspect.getmembers(module, inspect.isfunction):
                if self._is_extractor_function(name):
                    self.extractors[name] = func
                    extractor_count += 1

        except Exception as e:
            print(f"Error loading extractors: {e}")
    
    def _is_extractor_function(self, function_name: str) -> bool:
        """Check if function name matches extractor pattern"""
        return (function_name.endswith('_extractor') or 
                function_name.endswith('_custom_extractor'))
    
    
    def _get_extractor(self, function_name: str='_universal_custom_extractor') -> Optional[Callable]:
        """Get extractor function by name"""
        if function_name in self.extractors:
            return self.extractors[function_name]
        
        print(f"Extractor '{function_name}' not found")
        # print(f"Available extractors: {list(self.extractors.keys())}")
        return None
    
    def list_extractors(self) -> list:
        return list(self.extractors.keys())
    
    def reload(self):
        self.extractors.clear()
        self.load_extractors()
    
    def validate_extractor(self, function_name: str) -> bool:
        """Check if extractor exists and is callable"""
        extractor = self._get_extractor(function_name)
        return extractor is not None and callable(extractor)