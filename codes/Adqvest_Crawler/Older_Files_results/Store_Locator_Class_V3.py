
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
working_dir=r"C:\Users\Santonu\Adqvest_Crawler"
# working_dir=r"C:\Users\Administrator\AdQvestDir\codes\Adqvest_Crawler"

from camoufox import AsyncNewBrowser
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from crawlee import Request
from crawlee.fingerprint_suite import (
    DefaultFingerprintGenerator,
    HeaderGeneratorOptions,
    ScreenOptions,
)

from typing_extensions import override
from crawlee.browsers import (
    BrowserPool,
    PlaywrightBrowserController,
    PlaywrightBrowserPlugin,
)


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
    """Configuration for a specific site's data collection"""
    # Required fields (no defaults)
    url: str
    company_name: str
    brand_name: str
    country: str
    data_container_selector: str
    address_selector: str

    site_id: Optional[str] = None
    # Optional fields (with defaults)
    state_options_selector: Optional[str] = None
    city_options_selector: Optional[str] = None
    
    skip_state_selector: bool = None
    skip_city_selector: bool = None
    skip_locality_selector: bool = None
    

    state_selector:  Optional[str] = None
    search_selector: Optional[str] = None
    city_selector: Optional[str] = None
    locality_selector: Optional[str] = None
    input_selector: Optional[str] = None
    input_option_selector: Optional[str] = None
    store_type_selector: Optional[str] = None
    links_selector: Optional[str] = None
    data_tab_selector: Optional[str] = None
    

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




class ExtractorLoader:
    """
    Simple loader for custom extractor functions.
    
    Usage:
        loader = ExtractorLoader("custom_extractors.py")
        extractor = loader.get_extractor("_zudio_custom_extractor")
    """
    
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
                    # print(f"Loaded: {name}")
            
            # print(f"Total extractors loaded: {extractor_count}")
            
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

                #-------------------------------------------------------------------------------------------------
                filter_flag=site_data.get("options",{}).get('filter_flag', None),
                skip_state_selector=site_data.get("options",{}).get('skip_state_selector', False),
                skip_city_selector=site_data.get("options",{}).get('skip_city_selector', False),
                skip_locality_selector=site_data.get("options",{}).get('skip_locality_selector', False),

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


class CamoufoxPlugin(PlaywrightBrowserPlugin):
    """Example browser plugin that uses Camoufox browser,
    but otherwise keeps the functionality of PlaywrightBrowserPlugin.
    """

    @override
    async def new_browser(self) -> PlaywrightBrowserController:
        if not self._playwright:
            raise RuntimeError('Playwright browser plugin is not initialized.')
        
        browser_options = {**self._browser_launch_options, 'headless': False}
        return PlaywrightBrowserController(
            browser=await AsyncNewBrowser(
                self._playwright, **browser_options
            ),
            # headless=False,       
            # Increase, if camoufox can handle it in your use case.
            max_open_pages_per_browser=1,
            # This turns off the crawlee header_generation. Camoufox has its own.
            header_generator=None,
        )


class GeoEnabledPlaywrightCrawler(PlaywrightCrawler):

    @override
    async def _create_browser_context(self, browser):
        # Override the method to set geolocation and permissions
        context = await browser.new_context(
            permissions=[],
            geolocation=None,  # New Delhi
            locale='en-US'
           
        )
        return context

class CascadingDataCollector:
    """Universal framework for collecting data from sites with cascading dropdowns"""
    
    def __init__(self, site_config: SiteConfig):
        self.config = site_config
        self.data = pd.DataFrame(columns=['Company', 'Brand','Sub_Brand','Address', 'State', 'City', 'Locality', 'Pincode','Latitude', 'Longitude', 'Country', 'Relevant_Date', 'Runtime'])
        self.crawler = None
        self.collection_date = datetime.now()
        # self.stealth_instance = stealth()

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

        self.crawler = PlaywrightCrawler(
                            request_handler=self._handle_request,
                            headless=False,            # Set to True for production
                            max_requests_per_crawl=20,  # We only need one page
                            browser_type='chromium',      # or 'chromium', 'firefox','webkit',

                            request_handler_timeout=timedelta(minutes=300),
                            fingerprint_generator=fingerprint_generator,
                            browser_new_context_options={
                                "permissions": []
                            }

                            )

        # self.crawler.pre_navigation_hook(self._freeze_after_load_hook())

        # self.crawler = GeoEnabledPlaywrightCrawler(
        #                     request_handler=self._handle_request,
        #                     headless=False,            # Set to True for production
        #                     max_requests_per_crawl=1,  # We only need one page
        #                     browser_type='chromium',      # or 'chromium', 'firefox','webkit',

        #                     request_handler_timeout=timedelta(minutes=300),
        #                     fingerprint_generator=fingerprint_generator,

        #                     )

        # self.crawler = PlaywrightCrawler(
        #                     request_handler=self._handle_request,
        #                     request_handler_timeout=timedelta(minutes=300),
        #                     browser_pool=BrowserPool(plugins=[CamoufoxPlugin()]),
                            
        #                     )

        # browser_new_context_options={
        #     # Security
        #     "permissions": [],  # Block all permissions
        #     "ignore_https_errors": True,
        #     "bypass_csp": True,
            
        #     # Stealth
        #     "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        #     "viewport": {"width": 1920, "height": 1080},
        #     "locale": "en-US",
        #     "timezone_id": "America/New_York",
            
        #     # Performance
        #     "java_script_enabled": True,
        #     "accept_downloads": False,
            
        #     # Debugging (optional)
        #     "record_har_path": "./network_logs.har" if DEBUG else None,
        # }


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

    async def _apply_stealth_hook(self, context, page):
        """Apply stealth before each navigation"""
        await self.stealth_instance.apply_stealth_async(page)

    async def _handle_request(self, context: PlaywrightCrawlingContext) -> None:
        """Main request handler"""
        print(f"Processing: {context.request.url}")
        
        try:
            # Enhanced navigation and popup handling
            await self._navigate_and_prepare_with_context(context)
            
             
            #Option Selector Different means its a div dropdown 
            if self.config.state_options_selector:
                await self._collect_cascading_data_div_dropdowns(context)

            elif self.config.input_selector:
                await self._collect_input_based_data(context)

            elif self.config.paginate_selector:
                await self._collect_paginated_data(context)

            elif self.config.links_selector:
                # extracted_links = await context.extract_links(selector='div.india_mp a',label='CATEGORY')
                extracted_links = await context.extract_links(selector='div.india_mp a',label='CATEGORY')
                # extracted_links=extracted_links[:1]
                for link in extracted_links:
                    print(link.url)
                    await self._collect_nevigated_links_data(context,link)
                
            
            
            elif (not self.config.paginate_selector and
                  not self.config.input_selector and
                  not self.config.state_options_selector and
                  not self.config.state_selector and 
                  not self.config.links_selector):

                
                await self._collect_oneshort_data(context)


            else:
                # Start cascading data collection
                await self._collect_cascading_data(context)

        except Exception as e:
            print(f"Error processing: {self._log_strategy_error('_handle_request')}")
        
            
    #-------------------------------------------------------------------------------------------------------
    async def _navigate_and_prepare_with_context(self, context: PlaywrightCrawlingContext):
        """Navigate and handle initial setup with comprehensive timeout and popup handling"""
        print(f"Page already loaded by Crawlee: {context.request.url}")
        
        # Wait for page to be fully ready
        await self._wait_for_page_ready_with_context(context)

        # Since Crawlee already navigated, we focus on popup handling and page preparation
        await self._comprehensive_popup_handling_with_context(context)
    
    async def _comprehensive_popup_handling_with_context(self, context: PlaywrightCrawlingContext):
        """Handle popups with enhanced detection using PlaywrightCrawlingContext"""
        
        print("Starting comprehensive popup handling...")
        
        # Wait for potential popups to appear
        await asyncio.sleep(2)
        
        # Multiple rounds of popup detection
        for round_num in range(3):
            print(f"Popup detection round {round_num + 1}")
            
            popups_found = False
            
            # Handle configured cookie selectors
            if await self._handle_cookie_popups_with_context(context):
                popups_found = True
            
            # Handle configured popup selectors
            if await self._handle_configured_popups_with_context(context):
                popups_found = True
            
            # Auto-detect common popups
            if await self._auto_detect_popups_with_context(context):
                popups_found = True
            
            # Handle overlay/modal popups
            if await self._handle_overlay_popups_with_context(context):
                popups_found = True
            
            # If no popups found in this round, we're likely done
            if not popups_found:
                print(f"No popups found in round {round_num + 1}")
                break
            
            # Wait between rounds for new popups to potentially appear
            await asyncio.sleep(1)
        
        print("Popup handling completed")
    
    async def _handle_cookie_popups_with_context(self, context: PlaywrightCrawlingContext) -> bool:
        """Handle cookie-specific popups using context"""
        
        if not self.config.cookie_reject_selector:
            return False
        
        cookie_selectors = [
            self.config.cookie_reject_selector,
            # Additional common cookie selectors
            '#onetrust-reject-all-handler',
            '#onetrust-accept-btn-handler', 
            '.cookie-reject',
            '.cookie-decline',
            '[data-testid="cookie-reject"]',
            'button[aria-label*="reject" i]',
            'button[aria-label*="decline" i]',
            'button[data-action="block"]',
            'button:contains("Block")',
            '[onclick*="block"]'

        ]
        
        popups_handled = False
        
        for selector in cookie_selectors:
            try:
                # Check if element exists and is visible
                element = await context.page.query_selector(selector)
                if element and await element.is_visible():
                    await element.click(timeout=self.config.page_load_timeout)
                    print(f"Clicked cookie popup: {selector}")
                    popups_handled = True
                    await asyncio.sleep(0.5)  # Wait for popup to close
                    
            except Exception as e:
                continue
        
        return popups_handled
    
    async def _handle_configured_popups_with_context(self, context: PlaywrightCrawlingContext) -> bool:
        """Handle popups defined in site configuration using context"""
        
        popups_handled = False
        
        for popup_selector in self.config.popup_close_selectors:
            try:
                # Wait briefly for popup to appear
                await context.page.wait_for_selector(popup_selector, timeout=2000)
                
                # Click with multiple strategies
                click_success = await self._try_multiple_click_strategies_with_context(context, popup_selector)
                
                if click_success:
                    print(f"Closed configured popup: {popup_selector}")
                    popups_handled = True
                    await asyncio.sleep(0.5)
                    
            except Exception as e:
                continue
        
        return popups_handled
    
    async def _auto_detect_popups_with_context(self, context: PlaywrightCrawlingContext) -> bool:
        """Auto-detect and close common popup patterns using context"""
        
        # Common popup patterns
        common_popup_selectors = [
            # Modal/dialog closers
            '.modal .close', '.modal [aria-label*="close" i]',
            '.dialog .close', '.dialog [aria-label*="close" i]',
            '.popup .close', '.popup [aria-label*="close" i]',
            'div.contact-popup-overlay.visible button.contact-popup-close',
            'button.dl-label-btn[data-v-07a881c9]',
            
            # Overlay closers
            '.overlay .close', '.overlay [data-dismiss]',
            '.backdrop .close', '.backdrop [data-dismiss]',
            
            # Common close button patterns
            'button[aria-label*="close" i]',
            'button[title*="close" i]',
            'button[data-dismiss="modal"]',
            'button[data-dismiss="popup"]',
            
            # X buttons
            '.close-btn', '.close-button', '.btn-close',
            '[class*="close"]', '[id*="close"]',
            
            # Newsletter/subscription popups
            '.newsletter-popup .close',
            '.subscription-popup .close',
            '.email-popup .close',
            
            # Age verification/location popups
            '.age-verification button',
            '.location-popup button',
            '.region-selector button'
        ]
        
        popups_handled = False
        
        for selector in common_popup_selectors:
            try:
                elements = await context.page.query_selector_all(selector)
                
                for element in elements:
                    if await element.is_visible():
                        try:
                            await element.click(timeout=2000)
                            print(f"Auto-closed popup: {selector}")
                            popups_handled = True
                            await asyncio.sleep(0.3)
                        except:
                            continue
                            
            except Exception as e:
                continue
        
        return popups_handled
    
    async def _handle_overlay_popups_with_context(self, context: PlaywrightCrawlingContext) -> bool:
        """Handle overlay/backdrop style popups using context"""
        popups_handled = False
        
        try:
            # Look for modal/overlay containers
            overlay_containers = await context.page.query_selector_all(
                '.modal, .dialog, .popup, .overlay, [role="dialog"], [aria-modal="true"]'
            )
            
            for container in overlay_containers:
                if await container.is_visible():
                    # Try to find close button within container
                    close_buttons = await container.query_selector_all(
                        '.close, [aria-label*="close" i], [data-dismiss], .btn-close'
                    )
                    
                    for close_btn in close_buttons:
                        try:
                            await close_btn.click(timeout=2000)
                            print(f"Closed overlay popup")
                            popups_handled = True
                            await asyncio.sleep(0.5)
                            break
                        except:
                            continue
                    
                    # If no close button found, try pressing Escape
                    if popups_handled:
                        break
                    
                    try:
                        await context.page.keyboard.press('Escape')
                        print(f"Closed popup with Escape key")
                        popups_handled = True
                        await asyncio.sleep(0.5)
                    except:
                        pass
                        
        except Exception as e:
            pass
        
        return popups_handled
    
    async def _try_multiple_click_strategies_with_context(self, page, selector: str) -> bool:
        """Try multiple strategies to click an element using context"""
        
        strategies = [
            # Strategy 1: Normal click
            lambda: page.click(selector, timeout=self.config.page_load_timeout*0.2),

            # Strategy 2: Focus + Enter key
            lambda: self._focus_and_enter(page, selector),
            
            # Strategy 2: Force click
            lambda: page.click(selector, force=True, timeout=self.config.page_load_timeout*0.2),
            
            # Strategy 3: JavaScript click
            lambda: page.evaluate(f"""
                () => {{
                    const element = document.querySelector('{selector}');
                    if (element) {{
                        element.click();
                        return true;
                    }}
                    return false;
                }}
            """),
            
            # Strategy 4: Dispatch click event
            lambda: page.evaluate(f"""
                () => {{
                    const element = document.querySelector('{selector}');
                    if (element) {{
                        element.dispatchEvent(new MouseEvent('click', {{bubbles: true}}));
                        return true;
                    }}
                    return false;
                }}
            """)
        ]
        
        for strategy in strategies:
            try:
                result = await strategy()
                if result is not False:  # JavaScript methods return False on failure
                    return True
            except Exception as e:
                print(f"Error in _try_multiple_click_strategies_with_context:{e}")
                continue
        
        return False

    async def _focus_and_enter(self,page, selector):
        await page.focus(selector)
        await page.keyboard.press("Enter")
        return True

    async def _wait_for_page_ready_with_context(self, context: PlaywrightCrawlingContext):
        """Wait for page to be fully ready with multiple checks using context"""
        
        print("Waiting for page to be ready...")
        
        # Strategy 1: Wait for network idle
        try:
            await context.page.wait_for_load_state('networkidle', timeout=10000)
            print("Network idle achieved")
        except:
            print("Network idle timeout, continuing...")
        
        # Strategy 2: Wait for DOM content loaded
        try:
            await context.page.wait_for_load_state('domcontentloaded', timeout=5000)
            print("DOM content loaded")
        except:
            print("DOM content loaded timeout, continuing...")
        
        # Strategy 3: Wait for specific elements that indicate page is ready
        readiness_indicators = [
            self.config.state_selector,  # Main dropdown should be present
            'body',  # Basic page structure
            'head'   # HTML head should be loaded
        ]
        
        for indicator in readiness_indicators:
            try:
                await context.page.wait_for_selector(indicator, timeout=5000)
                print(f"Found readiness indicator: {indicator}")
                break
            except:
                continue
        
        # Strategy 4: Execute JavaScript to check if frameworks are ready
        try:
            await context.page.evaluate("""
                () => {
                    return new Promise((resolve) => {
                        // Wait for jQuery if present
                        if (typeof jQuery !== 'undefined') {
                            jQuery(document).ready(() => resolve());
                        }
                        // Wait for common frameworks
                        else if (document.readyState === 'complete') {
                            resolve();
                        }
                        else {
                            window.addEventListener('load', () => resolve());
                        }
                    });
                }
            """)
            print("JavaScript framework ready")
        except:
            print("JavaScript readiness check timeout")
        
        # Final wait to ensure everything is settled
        await asyncio.sleep(2)
        print("Page preparation completed")
    
    async def _handle_popups(self, context: PlaywrightCrawlingContext):
        """Handle cookie banners and popups using context (legacy method for compatibility)"""
        
        # Handle cookie reject button
        if self.config.cookie_reject_selector:
            try:
                await context.page.wait_for_selector(self.config.cookie_reject_selector, timeout=5000)
                await context.page.click(self.config.cookie_reject_selector)
                print("Rejected cookies")
            except:
                print("No cookie prompt found")
        
        # Handle other popups
        for popup_selector in self.config.popup_close_selectors:
            try:
                await context.page.wait_for_selector(popup_selector, timeout=self.config.page_load_timeout)
                await context.page.click(popup_selector)
                print(f"Closed popup: {popup_selector}")
            except:
                continue

    #---------------------------COLLECTION FUNCTIONS---------------------------------------------------------------------------
    async def _collect_cascading_data(self, context: PlaywrightCrawlingContext):
        try:
            #----------------------------------------IF WE HAVE STORE CATEGORY|Specific to First Cry--------------------------------------------------
            if self.config.store_type_selector:
               await context.page.wait_for_selector(self.config.store_type_selector, timeout=self.config.page_load_timeout)
               sub_brands = await self._get_dropdown_options(context.page, self.config.store_type_selector)
               sub_brands=[i for i in sub_brands if not re.search(r"select|all| select city",i,flags=re.IGNORECASE)]
               print(sub_brands)
               
               sub_brands=['FirstCry']
               for store_type in sub_brands:
                    print(f"Processing store type: {store_type}")

                    #SELECT CATEGORY
                    await self._select_dropdown_option(context.page, self.config.store_type_selector, store_type)
                    await self._wait_for_cascade_load(context.page)

                    avl_states = await self._get_dropdown_options(context.page, self.config.state_selector)
                    # avl_states=avl_states[:2]


                    for state_index, state in enumerate(avl_states):
                        await self._select_dropdown_option(context.page, self.config.store_type_selector, store_type)
                        await self._select_dropdown_option(context.page, self.config.state_selector, state)

                        avl_city = await self._get_dropdown_options(context.page, self.config.city_selector)
                        # avl_city=avl_city[:1]


                        for city_index, city in enumerate(avl_city):
                            await self.click_option_selector(context.page, self.config.store_type_selector)
                            await self._select_dropdown_option(context.page, self.config.store_type_selector, store_type)

                            await self.click_option_selector(context.page, self.config.state_selector)
                            await self._select_dropdown_option(context.page, self.config.state_selector, state)

                            await self.click_option_selector(context.page, self.config.city_selector)
                            await self._select_dropdown_option(context.page, self.config.city_selector, city)

                            if (self.config.search_selector):
                                await self._try_multiple_click_strategies_with_context(context.page, self.config.search_selector)

                            await self._extract_and_store_data(context.page, state, city, None,store_type)
                            await context.page.goto(self.config.url, wait_until='networkidle')   

            #-------------------NORMAL CONDITION WORKS FOR 99% Cases--------------------------------------------
            else:
                await self._collect_simple_state_city_locality_cascading_data(context.page)

        except Exception as e:
            print(f"Error processing: {self._log_strategy_error('_collect_cascading_data')}")
                
    async def _collect_simple_state_city_locality_cascading_data(self, page):
        print("Starting cascading data collection...")
        
        #example Puma Stores
        if self.config.data_tab_selector:
            await self._open_by_javascript(page, self.config.data_tab_selector)

        # Get all states
        # await context.page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
        #-------------------------------------------------------------------------------------------------------------------------
        states = await self._get_dropdown_options(page, self.config.state_selector)
        # print(states)
        if len(states)>1:
           states=[i for i in states if not re.search(r"select|all| select city",i,flags=re.IGNORECASE)]
        
           
        print(states)
        print(f"Found {len(states)} states")
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
                     
                    try:
                        await page.wait_for_selector(self.config.state_selector, timeout=self.config.page_load_timeout)
                    except Exception as e:
                        print(self._log_strategy_error('_collect_cascading_data'))

                    await self._select_dropdown_option(page, self.config.state_selector, state)
                    try:
                        await self._wait_for_cascade_load(context.page)
                    except:
                        pass
                   
                # Moved to City|Skip if Required
                if self.config.city_selector:
                    # Get cities for this state
                    cities = await self._get_dropdown_options(page, self.config.city_selector)
                    cities=[i for i in cities if not re.search(r"select|all|select city",i,flags=re.IGNORECASE)]   
                    print(cities)
                    print(f"Found {len(cities)} cities in {state}")
                    # cities=cities[:2]
                    
                    for city_index, city in enumerate(cities):
                        print(f"Processing city {city_index + 1}/{len(cities)}: {city}")
                        
                        try:
                            if self.config.data_tab_selector:
                               await self._open_by_javascript(page, self.config.data_tab_selector)

                            # Select city|Skip if Required
                            if not self.config.skip_city_selector:
                                await self._select_dropdown_option(page, self.config.city_selector, city)
                                await self._wait_for_cascade_load(page)

                    
                            if (self.config.locality_selector==None and self.config.search_selector):
                                print('-----------------------')
                                click_success = await self._open_by_javascript(page, self.config.search_selector)
                                # click_success = await self._try_multiple_click_strategies_with_context(context.page, self.config.search_selector)

                            # Handle locality if configured
                            if self.config.locality_selector:
                                await self._process_localities(page, state, city)
                            else:
                                # Extract data directly
                                await self._extract_and_store_data(page, state, city, None)
                                
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

    async def _collect_cascading_data_div_dropdowns(self, context: PlaywrightCrawlingContext):
        """Collect data using div-based cascading dropdowns"""
        print("Starting cascading data collection with div dropdowns...")
        #example Puma Stores
        if self.config.data_tab_selector:
            await self._open_by_javascript(context.page, self.config.data_tab_selector)

        
        context.page.set_default_navigation_timeout(self.config.page_load_timeout*0.5)
        # Get all states
        states = await self._get_div_dropdown_options(context.page,self.config.state_selector, self.config.state_options_selector)# New config parameter
        states=[i for i in states if not re.search(r"select|all",i,flags=re.IGNORECASE)] 
        print(states)   


        print(f"Found {len(states)} states")
        # states = states[14:15]  # Limit for testing
        
        for state_index, state in enumerate(states):
            print(f"\nProcessing state {state_index + 1}/{len(states)}: {state}")
            if self.config.search_limit:
                if state_index==self.config.search_limit:
                   break

            
            try:
                if not self.config.skip_state_selector:
                    await self._select_div_dropdown_option(context.page, self.config.state_selector, state,self.config.state_options_selector)
                    await self._wait_for_cascade_load(context.page)
                    # await context.page.wait_for_selector(self.config.city_selector, timeout=self.config.page_load_timeout)
                
                # Handle City if configured
                if self.config.city_selector:
                    await context.page.wait_for_selector(self.config.city_selector, timeout=self.config.page_load_timeout)
                    cities = await self._get_div_dropdown_options(context.page, self.config.city_selector, self.config.city_options_selector)
                    cities=[i for i in cities if not re.search(r"select|all",i,flags=re.IGNORECASE)]     
                    print(f"Found {len(cities)} cities in {state}")
                    # cities = cities[:2]  # Limit for testing
                    
                    for city_index, city in enumerate(cities):
                        print(f"Processing city {city_index + 1}/{len(cities)}: {city}")
                        
                        try:
                            if not self.config.skip_city_selector:
                                await self._wait_for_cascade_load(context.page)
                                await self._select_div_dropdown_option(context.page, self.config.city_selector, city,self.config.city_options_selector)
                                
                            
                            if (self.config.locality_selector==None and self.config.search_selector):
                                await self._open_by_javascript(context.page, self.config.search_selector)
                                # click_success = await self._try_multiple_click_strategies_with_context(context.page, self.config.search_selector)
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
                        await self._try_multiple_click_strategies_with_context(context.page, self.config.search_selector)
                        await self._wait_for_cascade_load(context.page)

                    await self._extract_and_store_data(context.page, state,None, None)

                            
            except Exception as e:
                print(f"Error processing state {state}: {self._log_strategy_error('_collect_cascading_data_div_dropdowns')}")

                continue

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
                # await self._js_clear_and_input(context.page,self.config.input_selector,str("201301"))
                

                if self.config.suggested_option_selector:
                    options = await self._get_dropdown_options(context.page, self.config.suggested_option_selector)
                    print(options)
                    for option_index,option in enumerate(options):
                        print(f"---------------------------{option}")
                        # await self._wait_for_cascade_load(context.page)
                        if option_index==self.config.suggestion_search_limit:
                            break
                        #--------------------------------------------------------------------------------------------------
                        # For sites where we need to click option selector to activate other options
                        if option_index>0:
                           await self.click_option_selector(context.page, self.config.input_selector)

                        await self._select_dropdown_option(context.page, self.config.suggested_option_selector, option)
                        
                        #------------Customize for Sketchers-----------------------------------------------------------
                        if self.config.input_option_selector:
                           await self.click_option_selector(context.page, self.config.input_option_selector)
                           await self._select_dropdown_option(context.page, self.config.input_option_selector, "300")


                        if (self.config.search_selector):
                            print("----------HERE IN CLICK-------------")
                            await self.click_option_selector(context.page, self.config.search_selector)
                            await self._wait_for_cascade_load(context.page) 

                        await self._extract_and_store_data(context.page, None,None, None) 

                else:
                    if self.config.search_selector:
                        try:
                            await self._open_by_javascript(context.page, self.config.search_selector)
                            await self._wait_for_cascade_load(context.page)
                        except:
                            await self.click_option_selector(context.page, self.config.search_selector)
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
                available_pages=await self._get_available_pages(context.page)
                new_pages = [p for p in available_pages if p not in processed_pages]
                print(new_pages)
                if not new_pages:
                    break

                for page_index,page_num in enumerate(new_pages):
                    print(f"Processing Page {page_num}/{len(new_pages)}: {page_num}")
                    print(f"li[data-lp='{page_num}'] a")
                    await self._navigate_to_page(context.page, f"li[data-lp='{page_num}'] a")
                    await self._extract_and_store_data(context.page, None, None, None)
                    processed_pages.add(page_num)
                    if page_index+1==len(new_pages):
                        # await self.click_option_selector(context.page,click_option_selector)
                        available_pages=await self._get_available_pages(context.page)
                        await self._navigate_to_page(context.page, f"li[data-lp='{int(page_num)+1}'] a")
                      
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
  
    
    #--------------------------------------------------------------------------------------------

    async def _get_available_pages(self, page_web):
        try:
            pages=await self._get_dropdown_options(page_web, self.config.paginate_selector)
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
            await self.click_option_selector(page_web,page_number)
            # await self._select_dropdown_option(page_web, self.config.paginate_selector, page_number)
            return True

        except Exception as e:
            print(f"Error processing page: {self._log_strategy_error('_navigate_to_page')}")
            return False

    async def _freeze_after_load_hook(context):
        # Let the initial page load
        await context.page.wait_for_load_state("networkidle")
        
        # Then block all further navigation
        await context.page.route("**/*", lambda route: (
            route.abort() if route.request.is_navigation_request() else route.continue_()
        ))
        
        # Also block JavaScript-based redirects
        await context.page.add_init_script("""
            // Override location methods that cause redirects
            Object.defineProperty(window.location, 'href', {
                set: function(url) { console.log('Blocked redirect to:', url); },
                get: function() { return window.location.href; }
            });
            
            // Block replace and assign
            window.location.replace = function(url) { console.log('Blocked replace to:', url); };
            window.location.assign = function(url) { console.log('Blocked assign to:', url); };
            
            // Block meta refresh
            const observer = new MutationObserver(mutations => {
                mutations.forEach(mutation => {
                    mutation.addedNodes.forEach(node => {
                        if (node.tagName === 'META' && node.getAttribute('http-equiv') === 'refresh') {
                            node.remove();
                            console.log('Blocked meta refresh');
                        }
                    });
                });
            });
            observer.observe(document.head, { childList: true });
        """)


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


        # """Clear and input using JavaScript"""
        # await page.evaluate("""
        #         ({selector, text}) => {
        #             const input = document.querySelector(selector);
        #             if (input) {
        #                 input.focus();
        #                 input.value = '';
        #                 input.value = text;
        #                 input.dispatchEvent(new Event('input', {bubbles: true}));
        #             }
        #         }
        #     """, {"selector": selector, "text": text})
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
        
        print('----------------------------------------LOCALITIES-------------------------------------------------')
        localities = await self._get_dropdown_options(page, self.config.locality_selector)
        print(f"Found {len(localities)} localities in {city}")
        
        for locality_index, locality in enumerate(localities):
            print(f"Processing locality {locality_index + 1}/{len(localities)}: {locality}")
            
            try:
                if not self.config.skip_locality_selector:
                       await self._select_dropdown_option(page, self.config.locality_selector, locality)
                       await self._wait_for_cascade_load(page)

            
                if self.config.search_selector:
                        await self._try_multiple_click_strategies_with_context(page, self.config.search_selector)
                        await asyncio.sleep(30)
                        # await context.page.wait_for_selector(self.config.locality_selector, timeout=self.config.page_load_timeout)
                        
                
                # Extract data
                await self._extract_and_store_data(page, state, city, locality)
                
            except Exception as e:
                print(f"Error processing locality {locality}: {self._log_strategy_error('_process_localities')}")
                continue
    
    async def _get_dropdown_options(self, page, selector: str) -> List[str]:
        """Get all options from a dropdown"""
        try:
            await page.wait_for_selector(selector, timeout=self.config.page_load_timeout*0.8)
        except:  
            pass      
        option_patterns = [
                        selector,
                        f"{selector} option",  # For fake selects with option elements
                        f"{selector} li",
                        f"{selector} [role='option']",
                        f"{selector} .option",
                        f"{selector} .dropdown-item",
                        f"{selector} .select-item",
                        f"{selector} [class*='option']",
                        f"{selector} [class*='item']"
                        
                    ]

        try:
            try:
                await self.click_option_selector(page, selector)
            except:
                pass

            options = await page.evaluate(f"""
                (selector) => {{
                    const dropdown = document.querySelector(selector);
                    if (!dropdown) return [];
                    
                    const options = Array.from(dropdown.querySelectorAll('option'));
                    return options
                        .filter(opt => opt.value && opt.textContent.trim() && 
                                      !opt.textContent.toLowerCase().includes('select'))
                        .map(opt => opt.textContent.trim());
                }}
            """, selector)
            
            if not options:
                for pattern in option_patterns:
                    # Get options after dropdown is opened
                    # print(pattern)
                    options = await page.query_selector_all(pattern)
                    locator = page.locator(pattern)
                    options = await locator.all_text_contents()
                    await asyncio.sleep(1) 
                    print('Trying Patterns')
                    if options:
                        break

            return options
            
        except Exception as e:
            print(f"Error getting options from {selector}: {e}")
            return []

    async def _get_div_dropdown_options(self, page, dropdown_selector: str, options_selector: str = None) -> List[str]:
        """Get all options from a div-based dropdown"""
        try:
            await self.click_option_selector(page,dropdown_selector)
            # Get options after dropdown is opened
            option_patterns = [
                        options_selector,
                        f"{options_selector} option",  # For fake selects with option elements
                        f"{options_selector} li",
                        f"{options_selector} [role='option']",
                        f"{options_selector} .option",
                        f"{options_selector} .dropdown-item",
                        f"{options_selector} .select-item",
                        f"{options_selector} [class*='option']",
                        f"{options_selector} [class*='item']",
                        f"{options_selector} h3"
                        
                    ]


            for pattern in option_patterns:
                    # Get options after dropdown is opened
                    # options = await page.query_selector_all(pattern)
                    locator = page.locator(pattern)
                    options = await locator.all_text_contents()
                    await asyncio.sleep(1) 
                    print('Trying Patterns')
                    if options:
                        break

            return options
            
        except Exception as e:
            self._log_strategy_error('_get_div_dropdown_options')
            return []

    async def click_option_selector(self, page, dropdown_selector: str):

        if re.search(r"mob",dropdown_selector,flags=re.IGNORECASE):
            await page.set_viewport_size({"width": 375, "height": 667})
            await page.wait_for_timeout(self.config.page_load_timeout)
    
        await asyncio.sleep(self.config.cascade_wait_time)
        try:
            await page.wait_for_selector(dropdown_selector, state='visible', timeout=self.config.page_load_timeout)
        except:
            pass

        await asyncio.sleep(10) 

        # First, click the dropdown to open it
        await page.click(dropdown_selector, timeout=self.config.page_load_timeout)
        await asyncio.sleep(5)  # Wait for dropdown to open

    async def _select_dropdown_option(self, page, selector: str, option: str):
        """Select an option in a dropdown"""
        try:
            # Try multiple selection methods
            methods = [
                lambda: page.locator(selector).select_option(option),
                lambda: page.locator(selector).select_option(option.lower()),
                lambda: page.select_option(selector, label=option),
                lambda: page.select_option(selector, value=option),
                lambda: page.click(f"text={option}", timeout=self.config.page_load_timeout*0.5)
            ]
            
            for method in methods:
                try:
                    try:
                        await self.click_option_selector(page, selector)
                    except:
                        pass
                    
                    await method()
                    return True
                except:
                    continue
            #-----------------------------------------------------------------------------------------------

            return await self._select_regular_option(page, selector,option)
            #-----------------------------------------------------------------------------------------------
            # Fallback: JavaScript selection
            await page.evaluate(f"""
                (selector, optionText) => {{
                    const dropdown = document.querySelector(selector);
                    if (dropdown) {{
                        const option = Array.from(dropdown.options)
                            .find(opt => opt.textContent.trim() === optionText);
                        if (option) {{
                            dropdown.value = option.value;
                            dropdown.dispatchEvent(new Event('change', {{bubbles: true}}));
                        }}
                    }}
                }}
            """, selector, option)
            
        except Exception as e:
            print(f"Error selecting option '{option}' in {selector}: {e}")
            self._log_strategy_error('_select_dropdown_option')
            raise e

    async def _select_div_dropdown_option(self, page, dropdown_selector: str, option: str, options_selector: str = None):
        """Select an option in a div-based dropdown (supports both regular and JCF dropdowns)"""
        
        try:

            # Step 1: Detect dropdown type
            if dropdown_selector not in self._dropdown_type_cache:
                dropdown_type = await self._detect_dropdown_type(page, dropdown_selector)
                self._dropdown_type_cache[dropdown_selector] = dropdown_type
                print(f"Detected and cached dropdown type: {dropdown_type} for {dropdown_selector}")
            else:
                dropdown_type = self._dropdown_type_cache[dropdown_selector]
                print(f"Using cached dropdown type: {dropdown_type}")

            if dropdown_type == "jcf":
                return await self._select_jcf_option(page, dropdown_selector, option,options_selector)
            else:
                return await self._select_regular_option(page, dropdown_selector,option)
            
        except Exception as e:
            print(f"Error selecting dropdown option '{option}': {e}")
            return False

    async def _detect_dropdown_type(self, page, dropdown_selector: str):
        """Detect if dropdown is JCF or regular div dropdown"""
        try:
            # Check for JCF indicators
            jcf_indicators = [
                f"{dropdown_selector} .jcf-select",
                f"{dropdown_selector} .jcf-select-opener", 
                f"{dropdown_selector} select.jcf-hidden",
                f"{dropdown_selector} .jcf-option",
                f"{dropdown_selector} .form-control city jcf-hidden"
            ]
            
            for indicator in jcf_indicators:
                try:
                    element = await page.query_selector(indicator)
                    if element:
                        return "jcf"
                except:
                    continue
                    
            return "regular"
            
        except:
            return "regular"

    async def _open_jcf_dropdown(self, page, dropdown_selector: str):
        """Open JCF dropdown"""
        try:
            jcf_opener_selectors = [
                f"{dropdown_selector} .jcf-select-opener",
                f"{dropdown_selector} .jcf-select",
                f"{dropdown_selector} span.jcf-select"
            ]
            
            for opener_selector in jcf_opener_selectors:
                try:
                    await page.click(opener_selector, timeout=self.config.page_load_timeout)
                    print(f"Opened JCF dropdown using: {opener_selector}")
                    return True
                except:
                    continue
            return False
            
        except:
            return False

    async def _select_jcf_option(self, page, dropdown_selector: str, option: str, options_selector: str):
        """Select option in JCF dropdown"""
        try:
            # JCF-specific strategies
            jcf_strategies = [
            # Strategy 1
            # Strategy 3
            (
                lambda: self.click_option_selector(page, dropdown_selector),
                lambda: page.click(f"span.jcf-option:text('{option}')", timeout=self.config.page_load_timeout)
            ),
            # Strategy 2

            (
                lambda: self.click_option_selector(page, dropdown_selector),
                lambda: page.click(f'.jcf-option:has-text("{option}")')
            ),
            
            # Strategy 4
            (
                lambda: self.click_option_selector(page, dropdown_selector),
                lambda: page.click(f"{dropdown_selector} span.jcf-option:text('{option}')", timeout=self.config.page_load_timeout)
            ),
            # Strategy 5
            (
                lambda: self.click_option_selector(page, dropdown_selector),
                lambda: page.click(f"xpath=//span[@class='jcf-option' and contains(text(), '{option}')]", timeout=self.config.page_load_timeout)
            ),

            (
                lambda: self.click_option_selector(page, dropdown_selector),
                lambda: self._click_jcf_by_data_index(page, option, dropdown_selector)
            )
        ]

        
            # Try each JCF strategy
            for i, (click_dropdown, click_option) in enumerate(jcf_strategies, 1):
                try:
                    await click_dropdown()
                    await click_option()
                    print(f"Successfully selected '{option}' using JCF strategy {i}")
                    return True
                    
                except Exception as e:
                    print(f"JCF strategy {i} failed: {str(e)}")
                    continue
            
            # JCF Fallback: Manual search
            try:
                print("Trying JCF fallback with manual search...")
                option_elements = await page.query_selector_all("span.jcf-option")
                for element in option_elements:
                    try:
                        text_content = await element.text_content()
                        if text_content and text_content.strip().upper() == option.upper():
                            await element.scroll_into_view_if_needed()
                            await element.click(timeout=2000)
                            print(f"Successfully selected '{option}' with JCF fallback")
                            return True
                    except:
                        continue
            except Exception as e:
                print(f"JCF fallback failed: {e}")
            
            return False
            
        except Exception as e:
            print(f"Error in JCF option selection: {e}")
            return False
    
    async def _reorder_strategies_by_success(self, strategies):
        """Reorder strategies based on success history"""
        return sorted(strategies, key=lambda x: self._strategy_success_count.get(x[2], 0), reverse=True)
    
    async def _select_regular_option(self, page, dropdown_selector: str, option: str, options_selector: str = None):
        """Select option in regular div dropdown"""
        try:
            # Regular dropdown strategies
            regular_strategies = [

             # Strategy 1: Use provided options_selector if available
            (
                lambda: self.click_option_selector(page, dropdown_selector),
                lambda: page.click(f"{options_selector}:text('{option}')", timeout=self.config.page_load_timeout) if options_selector else None,
                "Strategy 1"
            ),
            # Strategy 2: Try exact text match
            (
                lambda: self.click_option_selector(page, dropdown_selector),
                lambda: page.click(f"text={option}", timeout=self.config.page_load_timeout),
                "Strategy 2"
            ),
            # Strategy 4: Use evaluate to trigger the onclick
            ( lambda: self.click_option_selector(page, dropdown_selector),
              lambda: page.evaluate(f"document.getElementById('{option}').click()"),
              "Strategy 3"
            ),
            # Strategy 3: Try with common dropdown option selectors
            (
                lambda: self.click_option_selector(page, dropdown_selector),
                lambda: page.click(f"div.dropdown-item:text('{option}')", timeout=self.config.page_load_timeout),
                "Strategy 4"
            ),
            
            (
                lambda: self.click_option_selector(page, dropdown_selector),
                lambda: page.click(f"li:text('{option}')", timeout=self.config.page_load_timeout*0.5),
                "Strategy 5"
            ),
            
            (
                lambda: self.click_option_selector(page, dropdown_selector),
                lambda: page.click(f"[role='option']:text('{option}')", timeout=self.config.page_load_timeout*0.5),
                "Strategy 6"
            ),
            (
                lambda: self.click_option_selector(page, dropdown_selector),
                lambda: page.click(f"li[role='option'] span:text('{option}')", timeout=self.config.page_load_timeout),
                "Strategy 11"
            ),
            
            # Strategy 4: Try with case-insensitive matching
            (
                lambda: self.click_option_selector(page, dropdown_selector),
                lambda: page.click(f"text=/{option}/i", timeout=self.config.page_load_timeout),
                "Strategy 6"
            ),
            
            # Strategy 5: Try finding by data attributes
            (
                lambda: self.click_option_selector(page, dropdown_selector),
                lambda: page.click(f"[data-value='{option}']", timeout=self.config.page_load_timeout),
                "Strategy 7"
            ),
            
            (
                lambda: self.click_option_selector(page, dropdown_selector),
                lambda: page.click(f"[value='{option}']", timeout=self.config.page_load_timeout),
                "Strategy 8"
            ),
            
            # Strategy 6: Try with xpath
            (
                lambda: self.click_option_selector(page, dropdown_selector),
                lambda: page.click(f"xpath=//div[contains(text(), '{option}')]", timeout=self.config.page_load_timeout),
                "Strategy 9"
            ),
            
            (
                lambda: self.click_option_selector(page, dropdown_selector),
                lambda: page.click(f"xpath=//li[contains(text(), '{option}')]", timeout=self.config.page_load_timeout),
                "Strategy 10"
            ),

            (
                lambda: self.click_option_selector(page, dropdown_selector),
                lambda: page.click(f"li[role='option'] span:text('{option}')", timeout=self.config.page_load_timeout),
                "Strategy 11"
            ),

            ]
            
            regular_strategies =await  self._reorder_strategies_by_success(regular_strategies)

            # Try each regular strategy
            for i, (click_dropdown, click_option,strategy_name) in enumerate(regular_strategies, 1):
                try:
                    await click_dropdown()
                    await page.locator(dropdown_selector).scroll_into_view_if_needed()
                    # await page.wait_for_timeout(10)
                    await click_option()
                    print(f"Successfully selected '{option}' using Regular strategy {i}")
                    self._strategy_success_count[strategy_name] = self._strategy_success_count.get(strategy_name, 0) + 1
                    return True
                        
                except Exception as e:
                    print(f"Regular strategy {i} failed: {str(e)}")
                    continue
            
            # Regular fallback with scrolling
            try:
                print("Trying regular fallback with scrolling...")
                option_elements = await page.query_selector_all("div, li, [role='option']")
                
                for element in option_elements:
                    try:
                        text_content = await element.text_content()
                        if text_content and option.lower() in text_content.lower():
                            await element.scroll_into_view_if_needed()
                            await element.click(timeout=2000)
                            print(f"Successfully selected '{option}' with regular fallback")
                            return True
                    except:
                        continue
            except Exception as e:
                print(f"Regular fallback failed: {e}")
            
            return False
            
        except Exception as e:
            print(f"Error in regular option selection: {e}")
            return False

    async def _click_jcf_by_data_index(self, page, option_text: str, dropdown_selector: str):
        """Helper method to click JCF option by data-index"""
        try:
            option_elements = await page.query_selector_all(f"{dropdown_selector} span.jcf-option")
            
            for element in option_elements:
                text = await element.text_content()
                if text and text.strip().upper() == option_text.upper():
                    data_index = await element.get_attribute('data-index')
                    if data_index:
                        await page.click(f"span.jcf-option[data-index='{data_index}']")
                        return True
            return False
        except:
            return False

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
                else:
                    record = self._default_data_extractor(container, state, city, locality)
                
                if record and isinstance(record,list):
                    # record=[self.base_record.update(i) for i in record]
                    self.data=pd.concat([self.data,pd.DataFrame(record)])
                    if sub_brand:
                        self.data['Sub_Brand']=sub_brand

            
            if ((page.url != self.config.url) and (not self.config.data_tab_selector)):
               print('Page url changed-----------Going Back to Orginal')
               await page.goto(self.config.url, wait_until='networkidle')    

            if self.config.back_button_selector:
                  await self._open_by_javascript(page, self.config.back_button_selector)
                    
        except Exception as e:
            print(f"Error extracting data: {self._log_strategy_error('_extract_and_store_data')}")
    

    def _default_data_extractor(self, container, state: str, city: str, locality: Optional[str]):
        """Custom data extractor for Zudio with specific business logic"""
        try:
            all_address=[]
            address_elem = container.select_one(self.config.address_selector)

            if not address_elem:
                    return None

            for adr in address_elem:
                print(adr)
                address = adr.text.replace('\n', '').replace('  ', '').strip()
                
                # Custom pincode extraction for Zudio
                pincode_match = re.findall(r'\b[0-9]{6}\b', address)
                pincode = pincode_match[0].strip() if pincode_match else ''


                phone = self._extract_optional_field(container, self.config.phone_selector)
                email = self._extract_optional_field(container, self.config.email_selector)
                hours = self._extract_optional_field(container, self.config.hours_selector)
                
                # Custom validation
                if len(address) < 5:  # Skip very short addresses
                    return None

                all_address.append({
                    'Company': self.config.company_name,
                    'Brand': self.config.brand_name,
                    'Address': address,
                    'State': state,
                    'City': city,
                    'Locality': locality or '',
                    'Pincode': pincode,
                    'Latitude': None,
                    'Longitude': None,
                    'Country': self.config.country,
                    'Relevant_Date': self.collection_date.date(),
                    'Runtime': self.collection_date
                })

            return all_address
        except Exception as e:
                print(f'Error in ------ yousta_custom_extractor\n:{e}')
                return None


    def _extract_optional_field(self, container, selector: Optional[str]) -> str:
        """Extract optional field if selector is provided"""
        if not selector:
            return ''
        
        try:
            elem = container.select_one(selector)
            return elem.get_text().strip() if elem else ''
        except:
            return ''
    
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


