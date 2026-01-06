
import pandas as pd
import os
import asyncio
import time
import re
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass
import logging
import asyncio
import json
import sys

from datetime import timedelta  
from  datetime import datetime
from pytz import timezone



from urllib.parse import urljoin, urlparse
from typing import List, Dict, Any


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



@dataclass
class Connections:
    mysql: Any = None
    clickhouse: Any = None
    
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
    state_selector: str
    data_container_selector: str
    address_selector: str
    
    # Optional fields (with defaults)
    state_options_selector: Optional[str] = None
    city_options_selector: Optional[str] = None

    skip_city_selector: bool = None
    search_button: Optional[str] = None
    city_selector: Optional[str] = None
    locality_selector: Optional[str] = None
    phone_selector: Optional[str] = None
    email_selector: Optional[str] = None
    hours_selector: Optional[str] = None
    cascade_wait_time: int = 3
    page_load_timeout: int = 60000
    overall_request: int = 300
    cookie_reject_selector: Optional[str] = None
    popup_close_selectors: Optional[List[str]] = None
    custom_data_extractor: Optional[Callable] = None
    
    def __post_init__(self):
        if self.popup_close_selectors is None:
           self.popup_close_selectors = []

class CascadingDataCollector:
    """Universal framework for collecting data from sites with cascading dropdowns"""
    
    def __init__(self, site_config: SiteConfig):
        self.config = site_config
        self.data = pd.DataFrame(columns=['Company', 'Brand', 'Address', 'State', 'City', 'Locality', 'Pincode','Latitude', 'Longitude', 'Country', 'Relevant_Date', 'Runtime'])
        self.crawler = None
        self.collection_date = None

        # self.engine = connections.mysql
        # self.client1= connections.clickhouse
  
        
    async def _collect_data(self, today: datetime = None) -> pd.DataFrame:
        """Main data collection method"""
        if today is None:
            today = datetime.now()

        self.collection_date = today
        self.crawler = PlaywrightCrawler(
                            request_handler=self._handle_request,
                            headless=False,            # Set to True for production
                            max_requests_per_crawl=1,  # We only need one page
                            browser_type='chromium',      # or 'chromium', 'firefox','webkit',

                            request_handler_timeout=timedelta(minutes=300)

                           
                            
                        )

        # Add the target URL to the request queue
        await self.crawler.add_requests([self.config.url])

        try:
            # Start crawling
            await self.crawler.run([Request.from_url(self.config.url)])
            
            # Finalize and return data
            return self._finalize_data()
            
        except Exception as e:
            print(f"Data collection failed: {e}")
            raise e
        # finally:
        #     await self._cleanup()
    
    async def _handle_request(self, context: PlaywrightCrawlingContext) -> None:
        """Main request handler"""
        
        print(f"Processing: {context.request.url}")

        # Enhanced navigation and popup handling
        await self._navigate_and_prepare_with_context(context)
        
        #Option Selector Different means its a div dropdown 
        if self.config.state_options_selector:
            await self._collect_cascading_data_div_dropdowns(context)

        else:
            # Start cascading data collection
            await self._collect_cascading_data(context)
            
       


    #----------------------------------------------------------------
    async def _navigate_and_prepare_with_context(self, context: PlaywrightCrawlingContext):
        """Navigate and handle initial setup with comprehensive timeout and popup handling"""
        print(f"Page already loaded by Crawlee: {context.request.url}")
        
        # Since Crawlee already navigated, we focus on popup handling and page preparation
        await self._comprehensive_popup_handling_with_context(context)
        
        # Wait for page to be fully ready
        await self._wait_for_page_ready_with_context(context)
    
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
            'button[aria-label*="decline" i]'
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
    
    async def _try_multiple_click_strategies_with_context(self, context: PlaywrightCrawlingContext, selector: str) -> bool:
        """Try multiple strategies to click an element using context"""
        
        strategies = [
            # Strategy 1: Normal click
            lambda: context.page.click(selector, timeout=self.config.page_load_timeout),
            
            # Strategy 2: Force click
            lambda: context.page.click(selector, force=True, timeout=self.config.page_load_timeout),
            
            # Strategy 3: JavaScript click
            lambda: context.page.evaluate(f"""
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
            lambda: context.page.evaluate(f"""
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
                print("Error in _try_multiple_click_strategies_with_context")
                continue
        
        return False
    
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
                print("ℹ️ No cookie prompt found")
        
        # Handle other popups
        for popup_selector in self.config.popup_close_selectors:
            try:
                await context.page.wait_for_selector(popup_selector, timeout=self.config.page_load_timeout)
                await context.page.click(popup_selector)
                print(f"Closed popup: {popup_selector}")
            except:
                continue

    #------------------------------------------------------------------------------------------------------
    async def _collect_cascading_data(self, context: PlaywrightCrawlingContext):
        """Collect data using cascading dropdown logic with Crawlee"""
        print("Starting cascading data collection...")
        
        # Get all states
        states = await self._get_dropdown_options(context.page, self.config.state_selector)
        print(f"Found {len(states)} states")
        # states=states[0:2]
        for state_index, state in enumerate(states):
            print(f"\nProcessing state {state_index + 1}/{len(states)}: {state}")
            
            try:
                # Select state
                await self._select_dropdown_option(context.page, self.config.state_selector, state)
                await self._wait_for_cascade_load(context.page)
                
                # print(self.config.city_selector)
                # Handle City if configured
                if self.config.city_selector:
                    # Get cities for this state
                    cities = await self._get_dropdown_options(context.page, self.config.city_selector)
                    print(f"Found {len(cities)} cities in {state}")
                    # cities=cities[:2]
                    
                    for city_index, city in enumerate(cities):
                        print(f"Processing city {city_index + 1}/{len(cities)}: {city}")
                        
                        try:
                            # Select city
                            await self._select_dropdown_option(context.page, self.config.city_selector, city)
                            await self._wait_for_cascade_load(context.page)

                            # if self.config.search_button:
                            #     click_success = await self._try_multiple_click_strategies_with_context(context.page, self.config.search_button)
                            #     if click_success:
                            #         print('---------------------')

                             
                            
                            # Handle locality if configured
                            if self.config.locality_selector:
                                await self._process_localities(context.page, state, city)
                            else:
                                # Extract data directly
                                await self._extract_and_store_data(context.page, state, city, None)
                                
                        except Exception as e:
                            print(f"Error processing city {city}: {e}")
                            continue

                # elif self.config.search_button:
                #         print('Here--------------------------------------------------------------------------')
                #         click_success = await self._open_by_javascript(context.page, self.config.search_button)
                        # if click_success:
                        #     print('---------------------')

                        
            except Exception as e:
                print(f"Error processing state {state}: {e}")
                continue

    async def _collect_cascading_data_div_dropdowns(self, context: PlaywrightCrawlingContext):
        """Collect data using div-based cascading dropdowns"""
        print("Starting cascading data collection with div dropdowns...")
        
        # Get all states
        states = await self._get_div_dropdown_options(context.page,self.config.state_selector, self.config.state_options_selector  # New config parameter
            )
        states=[i for i in states if not re.search(r"select",i,flags=re.IGNORECASE)]                                              

        print(f"Found {len(states)} states")
        states = states[0:2]  # Limit for testing
        
        for state_index, state in enumerate(states):
            print(f"\nProcessing state {state_index + 1}/{len(states)}: {state}")
            
            try:
                # Select state
                success = await self._select_div_dropdown_option(context.page, self.config.state_selector, state,self.config.state_options_selector)

                
                if not success:
                    print(f"Failed to select state '{state}', skipping...")
                    continue
                
                await self._wait_for_cascade_load(context.page)
                # Handle City if configured
                if self.config.city_selector:
                    
                    # await context.page.wait_for_selector(f"{self.config.city_options_selector}", timeout=self.config.page_load_timeout)
                    # await context.page.wait_for_timeout(1000)

                    cities = await self._get_div_dropdown_options(context.page, self.config.city_selector, self.config.city_options_selector)
                    cities=[i for i in cities if not re.search(r"select",i,flags=re.IGNORECASE)]     
                    print(f"Found {len(cities)} cities in {state}")
                    cities = cities[:2]  # Limit for testing
                    
                    for city_index, city in enumerate(cities):
                        print(f"Processing city {city_index + 1}/{len(cities)}: {city}")
                        
                        try:
                            # Select city
                            if not self.config.skip_city_selector:
                                    success = await self._select_div_dropdown_option(context.page, self.config.city_selector, city,self.config.city_options_selector)

                            if not success:
                                print(f"Failed to select city '{city}', skipping...")
                                continue
                            
                            await self._wait_for_cascade_load(context.page)
                            
                            # Handle locality if configured
                            if self.config.locality_selector:
                                await self._process_localities_div(context.page, state, city)
                            else:
                                # Extract data directly
                                await self._extract_and_store_data(context.page, state, city, None)
                                
                        except Exception as e:
                            print(f"Error processing city {city}: {e}")
                            continue
                else:
                    await self._extract_and_store_data(context.page, state,None, None)

                            
            except Exception as e:
                print(f"Error processing state {state}: {e}")
                continue
    
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
            print(f"------------------failed: {error}")
            return False

    async def _process_localities(self, page, state: str, city: str):
        """Process localities if site has state → city → locality cascade"""
        
        localities = await self._get_dropdown_options(page, self.config.locality_selector)
        print(f"Found {len(localities)} localities in {city}")
        
        for locality_index, locality in enumerate(localities):
            print(f"Processing locality {locality_index + 1}/{len(localities)}: {locality}")
            
            try:
                await self._select_dropdown_option(page, self.config.locality_selector, locality)
                await self._wait_for_cascade_load(page)
                
                # Extract data
                await self._extract_and_store_data(page, state, city, locality)
                
            except Exception as e:
                print(f"Error processing locality {locality}: {e}")
                continue
    
    async def _get_dropdown_options(self, page, selector: str) -> List[str]:
        """Get all options from a dropdown"""
        try:
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
            
            return options
            
        except Exception as e:
            print(f"Error getting options from {selector}: {e}")
            return []

    async def _get_div_dropdown_options(self, page, dropdown_selector: str, options_selector: str = None) -> List[str]:
        """Get all options from a div-based dropdown"""
        try:
            try:

                await page.wait_for_selector(dropdown_selector, state='visible', timeout=3000)
                await asyncio.sleep(1) 

                # First, click the dropdown to open it
                await page.click(dropdown_selector, timeout=3000)
                await asyncio.sleep(5)  # Wait for dropdown to open
            except:
                pass
            # Default options selector if not provided
            # if not options_selector:
            #     options_selector = f"{dropdown_selector} div, {dropdown_selector} li, {dropdown_selector} span"
            
            # Get options after dropdown is opened
            options = await page.query_selector_all(options_selector)
            locator = page.locator(options_selector)
            options = await locator.all_text_contents()
            # print(options)
            
            return options
            
        except Exception as e:
            print(f"Error getting div dropdown options from {dropdown_selector}: {e}")
            return []
    
    async def _select_dropdown_option(self, page, selector: str, option: str):
        """Select an option in a dropdown"""
        
        try:
            # Try multiple selection methods
            methods = [
                lambda: page.locator(selector).select_option(option),
                lambda: page.select_option(selector, label=option),
                lambda: page.select_option(selector, value=option),
            ]
            
            for method in methods:
                try:
                    await method()
                    return True
                except:
                    continue
            
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
            raise e

    # async def _select_div_dropdown_option(self, page, dropdown_selector: str, option: str, options_selector: str = None):
    #     """Select an option in a div-based dropdown"""
        
    #     try:
    #         # Step 1: Click dropdown to open it
    #         await page.click(dropdown_selector, timeout=5000)
    #         await asyncio.sleep(1)  # Wait for dropdown to open

            
    #         # Try multiple strategies to select the option
    #         success = False

    #         # Strategy 4: Try clicking by visible text using Playwright
    #         try:
    #             await page.click(f"text={option}", timeout=self.config.page_load_timeout)
    #             print(f"Successfully selected '{option}' using Playwright text selector")
    #             return True
    #         except:
    #             pass
            
    #         # Strategy 5: Try with contains
    #         try:
    #             await page.click(f"text*={option}", timeout=self.config.page_load_timeout)
    #             print(f"Successfully selected '{option}' using Playwright contains selector")
    #             return True
    #         except:
    #             pass
            
    #         print(f"Failed to select option '{option}' - option not found")
    #         return False
            
    #     except Exception as e:
    #         print(f"Error selecting div dropdown option '{option}': {e}")
    #         return False


    async def _select_div_dropdown_option(self, page, dropdown_selector: str, option: str, options_selector: str = None):
        """Select an option in a div-based dropdown (supports both regular and JCF dropdowns)"""
        
        try:

            # Step 1: Detect dropdown type
            dropdown_type = await self._detect_dropdown_type(page, dropdown_selector)
            print(f"Detected dropdown type: {dropdown_type}")
        
            # Step 3: Wait for dropdown to be visible
            await page.wait_for_timeout(100)
            
            # Step 4: Try to select option based on dropdown type
            if dropdown_type == "jcf":
                return await self._select_jcf_option(page, dropdown_selector, option,options_selector)
            else:
                return await self._select_regular_option(page, dropdown_selector, option, options_selector)
            
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
                f"{dropdown_selector} .jcf-option"
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

    async def _open_regular_dropdown(self, page, dropdown_selector: str):
        """Open regular div dropdown"""
        try:
            await page.click(dropdown_selector, timeout=2000)
            print(f"Opened regular dropdown using: {dropdown_selector}")
            return True
        except:
            return False

    async def _select_jcf_option(self, page, dropdown_selector: str, option: str, options_selector: str):
        """Select option in JCF dropdown"""
        try:
            # JCF-specific strategies
            jcf_strategies = [
                # Strategy 1: Direct JCF option selector
                lambda: page.click(f"span.jcf-option:text('{option}')", timeout=self.config.page_load_timeout),
                
                # Strategy 2: With dropdown context
                lambda: page.click(f"{dropdown_selector} span.jcf-option:text('{option}')", timeout=self.config.page_load_timeout),

                # Strategy 5: XPath approach
                lambda: page.click(f"xpath=//span[@class='jcf-option' and contains(text(), '{option}')]", timeout=self.config.page_load_timeout),
                
                # Strategy 6: Find by data-index
                lambda: self._click_jcf_by_data_index(page, option, dropdown_selector),
            ]
            
            # Try each JCF strategy
            for i, strategy in enumerate(jcf_strategies, 1):
                try:

                    await strategy()
                    print(f"Successfully selected '{option}' using JCF strategy {i}")
                    
                    # # Verify selection
                    # await page.wait_for_timeout(1)
                    # try:
                    #     dropdown_visible = await page.is_visible(f"{dropdown_selector} .jcf-select-drop", timeout=1000)
                    #     if not dropdown_visible:
                    #         print(f"JCF dropdown closed - selection confirmed")
                    # except:
                    #     pass
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

    async def _select_regular_option(self, page, dropdown_selector: str, option: str, options_selector: str = None):
        """Select option in regular div dropdown"""
        try:
            # Regular dropdown strategies
            regular_strategies = [

                # Strategy 1: Use provided options_selector if available
                lambda: page.click(f"{options_selector}:text('{option}')", timeout=self.config.page_load_timeout) if options_selector else None,
                
                # Strategy 2: Try exact text match
                lambda: page.click(f"text={option}", timeout=self.config.page_load_timeout),
                
                # Strategy 4: Try with common dropdown option selectors
                lambda: page.click(f"div.dropdown-item:text('{option}')", timeout=self.config.page_load_timeout),
                lambda: page.click(f"li:text('{option}')", timeout=self.config.page_load_timeout),
                lambda: page.click(f"[role='option']:text('{option}')", timeout=self.config.page_load_timeout),
                
                # Strategy 5: Try with case-insensitive matching
                lambda: page.click(f"text=/{option}/i", timeout=self.config.page_load_timeout),
                
                # Strategy 6: Try finding by data attributes
                lambda: page.click(f"[data-value='{option}']", timeout=self.config.page_load_timeout),
                lambda: page.click(f"[value='{option}']", timeout=self.config.page_load_timeout),
                
                # Strategy 7: Try with xpath
                lambda: page.click(f"xpath=//div[contains(text(), '{option}')]", timeout=self.config.page_load_timeout),
                lambda: page.click(f"xpath=//li[contains(text(), '{option}')]", timeout=self.config.page_load_timeout),
            ]
            
            # Try each regular strategy
            for i, strategy in enumerate(regular_strategies, 1):
                if strategy is None:  # Skip if options_selector not provided
                    continue
                    
                try:
                    await strategy()
                    print(f"Successfully selected '{option}' using regular strategy {i}")
                    
                    # Verify selection
                    await page.wait_for_timeout(300)
                    try:
                        dropdown_still_open = await page.is_visible(f"{dropdown_selector}[aria-expanded='true']", timeout=1000)
                        if not dropdown_still_open:
                            print(f"Regular dropdown closed - selection confirmed")
                    except:
                        pass
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
        # Wait for network idle
        try:
            await page.wait_for_load_state('networkidle', timeout=5000)
        except:
            pass
        
        # Additional wait time
        await asyncio.sleep(self.config.cascade_wait_time)
    #-----------------------------------------------------------------------------------------------------
    
    async def _extract_and_store_data(self, page, state: str, city:  Optional[str], locality: Optional[str]):
        """Extract data from the current page and store it"""
        
        try:
            print('-------------------------DATA COLLECTION-----------------------------------------------------------------')
            # Get page content
            html = await page.content()
            soup = BeautifulSoup(html, 'html.parser')
            # print(soup)
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
                    record = self.config.custom_data_extractor(container, state, city, locality, self.collection_date)
                else:
                    record = self._default_data_extractor(container, state, city, locality)
                
                if record and isinstance(record,list):
                    self.data=pd.concat([self.data,pd.DataFrame(record)])
                    
                    
        except Exception as e:
            print(f"Error extracting data: {e}")
    

    # def _default_data_extractor(self, container, state: str, city: str, locality: Optional[str]) -> Dict:
    #     """Default data extraction logic"""
        
    #     try:
    #         # Extract address
    #         address_elem = container.select_one(self.config.address_selector)
    #         if not address_elem:
    #             return None
            
    #         address = address_elem.get_text().replace('\n', '').replace('  ', '').strip()
    #         print(address)
            
    #         # Extract pincode
    #         pincode_match = re.search(r'\b[0-9]{6}\b', address)
    #         pincode = pincode_match.group(0) if pincode_match else ''
            
    #         # Extract optional fields
    #         phone = self._extract_optional_field(container, self.config.phone_selector)
    #         email = self._extract_optional_field(container, self.config.email_selector)
    #         hours = self._extract_optional_field(container, self.config.hours_selector)
            
    #         return {
    #             'Company': self.config.company_name,
    #             'Brand': self.config.brand_name,
    #             'Address': address,
    #             'State': state,
    #             'City': city,
    #             'Locality': locality or '',
    #             'Pincode': pincode,
    #             'Phone': phone,
    #             'Email': email,
    #             'Hours': hours,
    #             'Country': self.config.country,
    #             'Relevant_Date': self.collection_date.date(),
    #             'Runtime': self.collection_date
    #         }
            
    #     except Exception as e:
    #         print(f"Error extracting record: {e}")
    #         return None
    
    def _default_data_extractor(self, container, state: str, city: str, locality: Optional[str]):
        """Custom data extractor for Zudio with specific business logic"""
        try:
            all_address=[]
            address_elem = container.select_one(self.config.address_selector)
            # address_elem= [i.find('p') for i in elements]

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
   #------------------------------------------------------------------------------------------------------

async def collect_custom_site_data(site_config: SiteConfig, today: datetime = None) -> pd.DataFrame:
    """Collect data from any custom site"""
    collector = CascadingDataCollector(site_config)
    collected_data=await collector._collect_data(today)
    await collector.Upload_Data(site_config.brand_name, collected_data)
    return collected_data


# Custom data extractor example
def _zudio_custom_extractor(container, state, city, locality, today):
    """Custom data extractor for Zudio with specific business logic"""
    try:
        address_elem = container.find('p')
        if not address_elem:
            return None
        
        address = address_elem.text.replace('\n', '').replace('  ', '').strip()
        
        # Custom pincode extraction for Zudio
        pincode_match = re.findall(r'\b[0-9]{6}\b', address)
        pincode = pincode_match[0].strip() if pincode_match else ''
        
        # Custom validation
        if len(address) < 10:  # Skip very short addresses
            return None
        
        return {
            'Company': 'Trent Ltd',
            'Brand': 'Zudio',
            'Address': address,
            'State': state,
            'City': city,
            'Locality': locality or '',
            'Pincode': pincode,
            'Latitude': None,
            'Longitude': None,
            'Country': 'India',
            'Relevant_Date': today.date(),
            'Runtime': today
        }
    except:
        return None


def _yousta_custom_extractor(container, state, city, locality, today):
    """Custom data extractor for Zudio with specific business logic"""
    try:
        all_address=[]
        elements = container.select(f"[data-city='{city}'][data-state='{state}']")
        address_elem= [i.find('p') for i in elements]

        if not address_elem:
                return None

        for adr in address_elem:
            print(adr)
            address = adr.text.replace('\n', '').replace('  ', '').strip()
            
            # Custom pincode extraction for Zudio
            pincode_match = re.findall(r'\b[0-9]{6}\b', address)
            pincode = pincode_match[0].strip() if pincode_match else ''
            
            # Custom validation
            if len(address) < 5:  # Skip very short addresses
                return None

            all_address.append({
                            'Company': 'Reliance Retail',
                            'Brand': 'Yousta',
                            'Address': address,
                            'State': state,
                            'City': city,
                            'Locality': locality or '',
                            'Pincode': pincode,
                            'Latitude': None,
                            'Longitude': None,
                            'Country': 'India',
                            'Relevant_Date': today.date(),
                            'Runtime': today
                        })

        return all_address
    except Exception as e:
            print(f'Error in ------ yousta_custom_extractor\n:{e}')
            return None
    

def _pngadgil_custom_extractor(container, state, city, locality, today):
    """Custom data extractor for Zudio with specific business logic"""
    try:
        # print(container)
        all_address=[]
        address_elem = container.select("div.column_attr.mfn-inline-editor.clearfix.align_center")
        # print(pngadgil_config.address_selector)
        address_elem= [i.find('p') for i in address_elem]
        print(address_elem)

        if not address_elem:
                return None

        for adr in address_elem:
            print("---------------------------------------------------------------------------------------------------")
            # adr=adr.get_text(separator=' ', strip=True)
            print(adr)
            address = adr.text.replace('\n', '').replace('  ', '').strip()
            if city.lower() not in address.lower():
                continue
            
            # Custom pincode extraction for Zudio
            pincode_match = re.findall(r'\b[0-9]{6}\b', address)
            pincode = pincode_match[0].strip() if pincode_match else ''
            
            # Custom validation
            if len(address) < 5:  # Skip very short addresses
                return None

            all_address.append({
                            'Company': 'P N Gadgil & Sons Ltd',
                            'Brand': 'P N Gadgil',
                            'Address': address,
                            'State': state,
                            'City': city,
                            'Locality': locality or '',
                            'Pincode': pincode,
                            'Latitude': None,
                            'Longitude': None,
                            'Country': 'India',
                            'Relevant_Date': today.date(),
                            'Runtime': today
                        })

        return all_address
    except Exception as e:
            print(f'Error in ------ _pngadgil_custom_extractor\n:{e}')
            return None


def _popeyes_custom_extractor(container, state, city, locality, today):
    """Custom data extractor for POPEYES with specific business logic"""
    try:
        print(container)
        all_address=[]
        address_elem = container.find('p', class_=lambda x: x != 'content-key')
        # print(pngadgil_config.address_selector)
        # address_elem= [i.find('p') for i in address_elem]
        print(address_elem)

        if not address_elem:
                return None

        for adr in address_elem:
            print("---------------------------------------------------------------------------------------------------")
            # adr=adr.get_text(separator=' ', strip=True)
            print(adr)
            address = adr.text.replace('\n', '').replace('  ', '').strip()
            print(address)
            if state.lower() not in address.lower():
                continue
            
            # Custom pincode extraction for Zudio
            pincode_match = re.findall(r'\b[0-9]{6}\b', address)
            pincode = pincode_match[0].strip() if pincode_match else ''
            
            # Custom validation
            if len(address) < 5:  # Skip very short addresses
                return None

            all_address.append({
                            'Company': 'Jubilant FoodWorks Limited ',
                            'Brand': 'Popeyes',
                            'Address': address,
                            'State': None,
                            'City': state,
                            'Locality': locality or '',
                            'Pincode': pincode,
                            'Latitude': None,
                            'Longitude': None,
                            'Country': 'India',
                            'Relevant_Date': today.date(),
                            'Runtime': today
                        })

        return all_address
    except Exception as e:
            print(f'Error in ------ _popeyes_custom_extractor\n:{e}')
            return None

def _melorra_custom_extractor(container, state, city, locality, today):
    """Custom data extractor for POPEYES with specific business logic"""
    try:
        print(container)
        all_address=[]
        address_elem = container.find('p', class_=lambda x: x == 'store_cityDesc')
        # print(pngadgil_config.address_selector)
        # address_elem= [i.find('p') for i in address_elem]
        print(address_elem)

        if not address_elem:
                return None

        for adr in address_elem:
            print("---------------------------------------------------------------------------------------------------")
            # adr=adr.get_text(separator=' ', strip=True)
            print(adr)
            address = adr.text.replace('\n', '').replace('  ', '').strip()
            print(address)
            if state.lower() not in address.lower():
                continue
            
            # Custom pincode extraction for Zudio
            pincode_match = re.findall(r'\b[0-9]{6}\b', address)
            pincode = pincode_match[0].strip() if pincode_match else ''
            
            # Custom validation
            if len(address) < 5:  # Skip very short addresses
                return None

            all_address.append({
                            'Company': 'Melorra',
                            'Brand': 'Melorra',
                            'Address': address,
                            'State': None,
                            'City': state,
                            'Locality': locality or '',
                            'Pincode': pincode,
                            'Latitude': None,
                            'Longitude': None,
                            'Country': 'India',
                            'Relevant_Date': today.date(),
                            'Runtime': today
                        })

        return all_address
    except Exception as e:
            print(f'Error in ------ _melorra_custom_extractor\n:{e}')
            return None

def _maxfashion_custom_extractor(container, state, city, locality, today):
    """Custom data extractor for POPEYES with specific business logic"""
    try:
        print(container)
        all_address=[]
        address_elem = container.select('address')
        # print(pngadgil_config.address_selector)
        # address_elem= [i.find('p') for i in address_elem]
        # print(address_elem)

        if not address_elem:
                return None

        for adr in address_elem:
            print("---------------------------------------------------------------------------------------------------")
            # adr=adr.get_text(separator=' ', strip=True)
            print(adr)
            address = adr.text.replace('\n', '').replace('  ', '').strip()
            print(address)
            if state.lower() not in address.lower():
                continue
            
            # Custom pincode extraction for Zudio
            pincode_match = re.findall(r'\b[0-9]{6}\b', address)
            pincode = pincode_match[0].strip() if pincode_match else ''
            
            # Custom validation
            if len(address) < 5:  # Skip very short addresses
                return None

            all_address.append({
                            'Company': 'Landmark Group',
                            'Brand': 'Max Fashion',
                            'Address': address,
                            'State': None,
                            'City': state,
                            'Locality': locality or '',
                            'Pincode': pincode,
                            'Latitude': None,
                            'Longitude': None,
                            'Country': 'India',
                            'Relevant_Date': today.date(),
                            'Runtime': today
                        })

        return all_address
    except Exception as e:
            print(f'Error in ------ _melorra_custom_extractor\n:{e}')
            return None

# Main execution function
async def run_program():

    today = datetime.now()

    zudio_config=SiteConfig(
            url="https://www.zudio.com/apps/s/zudio/storelocators",
            company_name="Trent Ltd",
            brand_name="Zudio",
            country="India",
            state_selector="select#store_state",
            city_selector="select#store_city",
            data_container_selector="div.storedetails",
            address_selector="p",
            cookie_reject_selector="#onetrust-reject-all-handler",
            cascade_wait_time=3,
            custom_data_extractor=_zudio_custom_extractor  # Optional custom logic
        )

    yousta_config=SiteConfig(
            url="https://relianceretail.com/nps/yousta/stores",
            company_name="Reliance Retail",
            brand_name="Yousta",
            country="India",
            state_selector="select#stateSelect",
            city_selector="select#citySelect",
            data_container_selector="div.scroll-wrapper",
            address_selector="p",
            custom_data_extractor=_yousta_custom_extractor , # Optional custom logic
            cascade_wait_time=3
            
        )

    pngadgil_config=SiteConfig(
            url="https://pngadgilandsons.com/our-showrooms/",
            company_name="P N Gadgil & Sons Ltd",
            brand_name="P N Gadgil",
            country="India",
            state_selector="select#countrySelect",
            city_selector="select#citySelect",
            data_container_selector="div.section_wrapper.mfn-wrapper-for-wraps.mcb-section-inner.mcb-section-inner-b007589d6",
            address_selector="p",
            custom_data_extractor=_pngadgil_custom_extractor , # Optional custom logic
            cascade_wait_time=3
            
        )

    popeyes_config=SiteConfig(
            url="https://www.popeyes.in/stores",
            company_name="Jubilant FoodWorks Limited ",
            brand_name="Popeyes",
            country="India",
            state_selector="div.selectBox",
            state_options_selector="div.cityList div",
            # city_selector="select#citySelect",
            data_container_selector="div.stores-grid",
            address_selector="p",
            custom_data_extractor=_popeyes_custom_extractor , # Optional custom logic
            cascade_wait_time=3
            
        )

    #----------------------------------------------------IN PREOGRESS------------------------------------------------------------------

    melorra_config=SiteConfig(
            url="https://www.melorra.com/store/locator/",
            company_name="Melorra",
            brand_name="Melorra",
            country="India",
            state_selector="div.selectCityMob",
            state_options_selector="div.d-flex.pl-0.pr-3.pt-3.pb-3 div",
            # city_selector="select#citySelect",
            data_container_selector="div.stores-grid",
            address_selector="p",
            custom_data_extractor=_melorra_custom_extractor , # Optional custom logic
            cascade_wait_time=3
            
        )

    lifestyle_config=SiteConfig(
            url="https://stores.lifestylestores.com/",
            company_name="Life Style",
            brand_name="Lifestyle",
            country="India",
            state_selector="#OutletStoreLocatorSearchForm",
            # city_selector="select#citySelect",
            data_container_selector="li.store-list-item",
            address_selector="address",
            search_button="li#actions",
            # custom_data_extractor=_yousta_custom_extractor , # Optional custom logic
            cascade_wait_time=3
            
        )

    maxfashion_config=SiteConfig(
            url="https://www.maxfashion.in/in/en/storelocator",
            company_name="Landmark Group",
            brand_name="Max Fashion",
            country="India",
            state_selector="div.form-group.custom-select:nth-child(1)",
            state_options_selector="span.jcf-list-content li",

            city_selector="select.form-control.city.jcf-hidden:nth-child(1)",
            city_options_selector="div.form-group.city-list-wrap.custom-select option",

            data_container_selector="div.store-list",
            address_selector="address",
            custom_data_extractor=_maxfashion_custom_extractor , # Optional custom logic
            cascade_wait_time=3,
            skip_city_selector=True


        )

    #----------------------------------------------------------------------------------------------------------------------------------
    # result=await collect_custom_site_data(pngadgil_config, today)
    # result.to_excel(f"{pngadgil_config.brand_name}_Stores.xlsx")
    
    #----------------------------------------------------------------------------------------------------------------------------------
    all_configs = [yousta_config, pngadgil_config, popeyes_config] 
    all_configs = [popeyes_config] 
    results = await asyncio.gather(
        *[collect_custom_site_data(config, today) for config in all_configs],
        return_exceptions=True
    )

    for config, result in zip(all_configs, results):
        if isinstance(result, Exception):
            print(f"{config.brand_name} failed:{result}")
        else:
            os.chdir(working_dir)
            print(f"{config.brand_name}: {len(result)} stores collected")
            result.to_excel(f"{config.brand_name}_Stores_{today.strftime('%Y%m%d')}.xlsx")


    print('----------------ALL DONE-----------------------------')

# Run the framework
if __name__ == "__main__":
    asyncio.run(run_program())