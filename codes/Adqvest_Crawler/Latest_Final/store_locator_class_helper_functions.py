# -*- coding: utf-8 -*-
"""
Created on Thu Jul 31 01:36:23 2025

@author: Santonu
"""

from typing import Dict, List, Optional, Callable
from dataclasses import dataclass
from pathlib import Path
import importlib
import inspect
import sys
import re
import os
import asyncio
from camoufox import AsyncNewBrowser
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from crawlee.browsers import (BrowserPool,PlaywrightBrowserController,PlaywrightBrowserPlugin)
from typing_extensions import override

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
        
class CamoufoxPlugin(PlaywrightBrowserPlugin):
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

class DropdownHandler:
    """Enhanced dropdown handler with support for modern UI frameworks"""
    
    def __init__(self, config):
        self.config = config
        self._dropdown_type_cache = {}
        self._strategy_success_count = {}
    
    def _log_strategy_error(self,strategy_name:str):
        exc_type, exc_obj, exc_tb = sys.exc_info()
        error_msg = str(exc_obj)
        line_no = exc_tb.tb_lineno if exc_tb else 'unknown'
        print(f"Error in strategy '{strategy_name}':\nError Message: {error_msg}\nLine: {line_no}\n")

    async def _detect_dropdown_type(self, page, dropdown_selector: str) -> str:
        """Detect the type of dropdown component"""
        try:
            # Check for Ant Design
            antd_indicators = [
                f"{dropdown_selector} .ant-select-selector",
                f"{dropdown_selector} .ant-select-selection-search-input",
                f"{dropdown_selector} .ant-select-arrow",
                ".ant-select-dropdown",  # Global check
                "div.ant-select-selector"
            ]
            
            for indicator in antd_indicators:
                try:
                    element = await page.query_selector(indicator)
                    if element:
                        return "antd"
                except:
                    continue
            
            # Check for Material-UI
            mui_indicators = [
                f"{dropdown_selector} .MuiSelect-root",
                f"{dropdown_selector} .MuiInputBase-root",
                f"{dropdown_selector} .MuiOutlinedInput-root",
            ]
            
            for indicator in mui_indicators:
                try:
                    element = await page.query_selector(indicator)
                    if element:
                        return "mui"
                except:
                    continue
            
            # Check for JCF
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
            
            # Check for native select
            native_select = await page.query_selector(f"{dropdown_selector} select")
            if native_select:
                return "native"
                
            return "custom"
            
        except Exception as e:
            print(f"Error detecting dropdown type: {e}")
            return "custom"

    async def _get_dropdown_options(self, page, dropdown_selector: str =None, selector: str=None) -> List[str]:
        """Get all options from any type of dropdown"""
        try:
            # Cache dropdown type
            if selector not in self._dropdown_type_cache:
                dropdown_type = await self._detect_dropdown_type(page, selector)
                self._dropdown_type_cache[selector] = dropdown_type
                print(f"Detected dropdown type: {dropdown_type} for {selector}")
            else:
                dropdown_type = self._dropdown_type_cache[selector]

            # Get options based on type
            if dropdown_type == "mui":
                return await self._get_mui_options(page, selector)
            elif dropdown_type == "jcf":
                return await self._get_jcf_options(page, selector)
            elif dropdown_type == "native":
                return await self._get_native_options(page, selector)
            else:
                return await self._get_custom_options(page,dropdown_selector, selector)
                
        except Exception as e:
            print(f"Error getting options from {selector}: {self._log_strategy_error('_get_dropdown_options')}")
            return []

    async def _select_dropdown_option(self, page, dropdown_selector: str = None, option: str = None, options_selector: str = None):
        """Select an option based on dropdown type"""
        try:
            dropdown_type = self._dropdown_type_cache.get(dropdown_selector, "custom")
            
            if dropdown_type == "mui":
                return await self._select_mui_option(page, dropdown_selector, option)
            elif dropdown_type == "jcf":
                return await self._select_jcf_option(page,  dropdown_selector, option,options_selector)

            elif dropdown_type == "native":
                return await self._select_native_option(page, options_selector, option)
            else:
                return await self._select_custom_option(page, dropdown_selector, option, options_selector)
                
        except Exception as e:
            print(f"Error selecting option '{option}' in {dropdown_selector}: {self._log_strategy_error('_select_dropdown_option')}")
            return False

    # =================== FALLBACK METHODS ===================

    async def _get_custom_options(self, page,dropdown_selector: str = None, options_selector: str = None) -> List[str]:
        """Fallback method for custom dropdowns"""
        print(dropdown_selector)
        try:
            if dropdown_selector:
               await self._click_option_selector(page,dropdown_selector)
            try:
                await page.wait_for_selector(options_selector, timeout=self.config.page_load_timeout*0.8)
            except:  
                pass 
            
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
                try:
                    options = await page.query_selector_all(pattern)
                    
                    locator = page.locator(pattern)
                    options = await locator.all_text_contents()
                    print(options)
                    await asyncio.sleep(1) 
                    if options:
                        break
                except Exception as e:
                    print(e)
                    continue
            
            

            return options
            
        except Exception as e:
            print(f"Error getting custom dropdown options: {self._log_strategy_error('_get_custom_options')}")
            return []

    async def _select_custom_option(self, page, dropdown_selector: str = None, option: str = None, options_selector: str = None) -> bool:
        """Fallback method for custom dropdown selection"""
        try:
            # All the original strategies you had, but organized
            regular_strategies = [

             # Strategy 1: Use provided options_selector if available
           
            # Strategy 2: Try exact text match
            (
                lambda: self._click_option_selector(page, dropdown_selector),
                lambda: page.click(f"text={option}", timeout=self.config.page_load_timeout),
                "Strategy 2"
            ),
            (
                lambda: self._click_option_selector(page, dropdown_selector),
                lambda: page.click(f"{options_selector}:text('{option}')", timeout=self.config.page_load_timeout) if options_selector else None,
                "Strategy 1"
            ),
            # Strategy 4: Use evaluate to trigger the onclick
            ( lambda: self._click_option_selector(page, dropdown_selector),
              lambda: page.evaluate(f"document.getElementById('{option}').click()"),
              "Strategy 3"
            ),
            # Strategy 3: Try with common dropdown option selectors
            (
                lambda: self._click_option_selector(page, dropdown_selector),
                lambda: page.click(f"div.dropdown-item:text('{option}')", timeout=self.config.page_load_timeout),
                "Strategy 4"
            ),
            
            (
                lambda: self._click_option_selector(page, dropdown_selector),
                lambda: page.click(f"li:text('{option}')", timeout=self.config.page_load_timeout*0.5),
                "Strategy 5"
            ),
            
            (
                lambda: self._click_option_selector(page, dropdown_selector),
                lambda: page.click(f"[role='option']:text('{option}')", timeout=self.config.page_load_timeout*0.5),
                "Strategy 6"
            ),
            (
                lambda: self._click_option_selector(page, dropdown_selector),
                lambda: page.click(f"li[role='option'] span:text('{option}')", timeout=self.config.page_load_timeout),
                "Strategy 11"
            ),
            
            # Strategy 4: Try with case-insensitive matching
            (
                lambda: self._click_option_selector(page, dropdown_selector),
                lambda: page.click(f"text=/{option}/i", timeout=self.config.page_load_timeout),
                "Strategy 6"
            ),
            
            # Strategy 5: Try finding by data attributes
            (
                lambda: self._click_option_selector(page, dropdown_selector),
                lambda: page.click(f"[data-value='{option}']", timeout=self.config.page_load_timeout),
                "Strategy 7"
            ),
            
            (
                lambda: self._click_option_selector(page, dropdown_selector),
                lambda: page.click(f"[value='{option}']", timeout=self.config.page_load_timeout),
                "Strategy 8"
            ),
            
            # Strategy 6: Try with xpath
            (
                lambda: self._click_option_selector(page, dropdown_selector),
                lambda: page.click(f"xpath=//div[contains(text(), '{option}')]", timeout=self.config.page_load_timeout),
                "Strategy 9"
            ),
            
            (
                lambda: self._click_option_selector(page, dropdown_selector),
                lambda: page.click(f"xpath=//li[contains(text(), '{option}')]", timeout=self.config.page_load_timeout),
                "Strategy 10"
            ),

            (
                lambda: self._click_option_selector(page, dropdown_selector),
                lambda: page.click(f"li[role='option'] span:text('{option}')", timeout=self.config.page_load_timeout),
                "Strategy 11"
            ),

            ]
            
            regular_strategies =await  self._reorder_strategies_by_success(regular_strategies)

            # Try each regular strategy
            for i, (click_dropdown, click_option,strategy_name) in enumerate(regular_strategies, 1):
                try:
                    if dropdown_selector:
                       await click_dropdown()
                       await page.locator(dropdown_selector).scroll_into_view_if_needed()

                    await page.wait_for_timeout(5)
                    await click_option()
                    await asyncio.sleep(10)  # Wait for UI to update
        
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
            print(f"Error in custom option selection: {self._log_strategy_error('_select_custom_option')}")
            return False

    async def _click_option_selector(self, page, dropdown_selector: str):
        """Generic dropdown click method"""
        if re.search(r"mob", dropdown_selector, flags=re.IGNORECASE):
            await page.set_viewport_size({"width": 375, "height": 667})
            await page.wait_for_timeout(self.config.page_load_timeout)
    
        await asyncio.sleep(self.config.cascade_wait_time)
        
        try:
            await page.wait_for_selector(dropdown_selector, state='visible', timeout=self.config.page_load_timeout)
        except:
            pass

        await page.click(dropdown_selector, timeout=self.config.page_load_timeout)
        await asyncio.sleep(4)

    # =================== NATIVE SELECT METHODS ===================

    async def _get_native_options(self, page, selector: str) -> List[str]:
        """Get options from native HTML select"""
        try:
            if self.config.site_id=='Decathlon':

                options = await page.evaluate(f'''
                            () => {{
                                const elements = document.querySelectorAll('{selector}');
                                return Array.from(elements).map(el => el.textContent.trim()).filter(text => text);
                            }}
                        ''')
            else:
                options=[]

            if not options:
                try:
                    await self._click_option_selector(page, selector)
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
                print("-----------------------Trying Custom options-----------------")
                options=await self._get_custom_options(page,options_selector=selector)

            return options or []
            
        except Exception as e:
            print(f"Error getting native select options: {e}")
            return []

    async def _select_native_option(self, page, options_selector: str, option: str) -> bool:
        """Select option in native HTML select"""
        try:
            # Try multiple selection methods
            methods = [
                lambda: page.locator(options_selector).select_option(option),
                lambda: page.locator(options_selector).select_option(option.lower()),
                lambda: page.select_option(options_selector, label=option),
                lambda: page.select_option(options_selector, value=option),
                lambda: page.click(f"text={option}", timeout=self.config.page_load_timeout*0.5)
            ]
            
            for method in methods:
                try:
                    if self.config.click_option_before_select:
                        try:
                            await self._click_option_selector(page, options_selector)
                        except:
                            pass
                    
                    await method()
                    return True
                except:
                    continue
            
            return await self._select_custom_option(page, options_selector=options_selector,option=option)
 
        except Exception as e:
            print(f"Error selecting native option: {e}")
            return False

    #-------------------------------------------------------------------------------------------------
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
                lambda: self._click_option_selector(page, dropdown_selector),
                lambda: page.click(f"span.jcf-option:text('{option}')", timeout=self.config.page_load_timeout)
            ),
            # Strategy 2

            (
                lambda: self._click_option_selector(page, dropdown_selector),
                lambda: page.click(f'.jcf-option:has-text("{option}")')
            ),
            
            # Strategy 4
            (
                lambda: self._click_option_selector(page, dropdown_selector),
                lambda: page.click(f"{dropdown_selector} span.jcf-option:text('{option}')", timeout=self.config.page_load_timeout)
            ),
            # Strategy 5
            (
                lambda: self._click_option_selector(page, dropdown_selector),
                lambda: page.click(f"xpath=//span[@class='jcf-option' and contains(text(), '{option}')]", timeout=self.config.page_load_timeout)
            ),

            (
                lambda: self._click_option_selector(page, dropdown_selector),
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

    #------------------------------------------------------------------------------------------------
    async def _reorder_strategies_by_success(self, strategies):
        """Reorder strategies based on success history"""
        return sorted(strategies, key=lambda x: self._strategy_success_count.get(x[2], 0), reverse=True)
    
class PopupHandler:
    
    def __init__(self, config):
        self.config = config
      
    async def _navigate_and_prepare_with_context(self, context: PlaywrightCrawlingContext):
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
            overlay_containers = await context.page.query_selector_all('.modal, .dialog, .popup, .overlay, [role="dialog"], [aria-modal="true"]')
            for container in overlay_containers:
                if await container.is_visible():
                    # Try to find close button within container
                    close_buttons = await container.query_selector_all('.close, [aria-label*="close" i], [data-dismiss], .btn-close')
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
    