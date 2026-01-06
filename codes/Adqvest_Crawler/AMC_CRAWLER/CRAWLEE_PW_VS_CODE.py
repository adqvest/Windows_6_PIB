import asyncio
import json
import logging
import re
import os
import calendar
import pandas as pd
import sqlalchemy
import unicodedata
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Any
from urllib.parse import urljoin
from pathlib import Path
import csv
import sys
from urllib.parse import urlparse

from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from crawlee import Request
from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError

import sys
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import adqvest_ai
import ClickHouse_db

# Virtual environment configuration
REQUIRED_VENV_PATH = r"D:\Work\AMC_Crawlee_code\.venv"
REQUIRED_PYTHON_EXE = r"D:\Work\AMC_Crawlee_code\.venv\Scripts\python.exe"

def is_valid_url(url: str) -> bool:
    """Custom URL validation function"""
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc]) and result.scheme in ['http', 'https']
    except Exception:
        return False

def get_json(response):
    start = response.find('{')
    end = response.rfind('}') + 1
    
    if start != -1 and end != -1:
        json_str = response[start:end]
        data = json.loads(json_str)
        # print("Extracted JSON:\n", data)
        return data
    else:
        {}

    # Verify required packages
    try:
        import crawlee
        import playwright
        print("All required packages available")
    except ImportError as e:
        print(f"Missing required package: {e}")
        print("Please install requirements: pip install crawlee[playwright]")
        os.sys.exit(1)

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
class SiteSelectors:
    year_selector: Optional[str] = None
    month_selector: Optional[str] = None
    data_tab_selector: Optional[str] = None
    data_tab_selector_2: Optional[str] = None
    data_tab_selector_3: Optional[str] = None
    factsheet_tab_selector: Optional[str] = None
    multi_tab_selector: Optional[str] = None
    data_container_selector: Optional[str] = None
    address_selector: Optional[str] = None
    pagination_selector: Optional[str] = None
    download_link_selector: Optional[str] = None
    next_button_selector: Optional[str] = None
    submit_selector: Optional[str] = None
    icon_selector: Optional[str] = None
    year_options_selector: Optional[str] = None
    month_options_selector: Optional[str] = None 
    state_selector: Optional[str] = None
    city_selector: Optional[str] = None
    state_options_selector: Optional[str] = None
    city_options_selector: Optional[str] = None
    search_selector: Optional[str] = None   

@dataclass
class SiteOptions:
    cascade_wait_time: int = 3
    click_option_before_select: bool = False
    regular_fall_back: bool = False
    search_limit: int = None
    max_years_to_process: int = 8  # Limit years to prevent excessive crawling
    max_months_per_year: int = 12  # Limit months per year
    allow_special_year_options: List[str] = None
    year_option: Optional[str] = None

    def __post_init__(self):
        if self.allow_special_year_options is None:
            self.allow_special_year_options = []

@dataclass
class SiteConfig:
    site_id: str
    enabled: bool
    priority: int
    url: str
    company_name: str
    amc_name: str
    country: str
    selectors: SiteSelectors
    options: SiteOptions

@dataclass
class FactsheetResult:
    Company: str
    AMC_Name: str
    File_name: str
    Relevant_date: str
    Month:str
    Year:str
    Runtime: str
    File_Link: str

class AMCFactsheetCrawler:
    def __init__(self, config_path: str, custom_url: str = None):
        self.config_path = config_path
        self.config: List[SiteConfig] = []
        self.results: List[FactsheetResult] = []
        self.setup_logging()
        self.load_config()  

    def setup_logging(self):
        log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        file_handler = logging.FileHandler('amc_crawler.log', encoding='utf-8')
        file_handler.setFormatter(log_formatter)
        
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(log_formatter)
        
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def load_config(self,custom_url=None):
        """Load configuration from JSON file"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as file:
                config_data = json.load(file)
            
            sites_data = config_data.get('sites', [config_data] if 'site_id' in config_data else [])
            # sites_data = [s for s in sites_data if s.get('site_id') == 'JM Financial']
            if not sites_data:
                raise ValueError("site_id  not found in config")
            for site_data in sites_data:
                selectors = SiteSelectors(**site_data.get('selectors', {}))
                options = SiteOptions(**site_data.get('options', {}))
                
                site_config = SiteConfig(site_id=site_data['site_id'], enabled=site_data.get('enabled', True),
                    priority=site_data.get('priority', 1), url=site_data['url'],
                    company_name=site_data['company_name'], amc_name=site_data['amc_name'],
                    country=site_data.get('country', 'India'), selectors=selectors, options=options)
                self.config.append(site_config)
                
            self.logger.info(f"Loaded {len(self.config)} site configurations")
            
        except Exception as e:
            error_msg = str(e) + " line " + str(sys.exc_info()[2].tb_lineno)
            print(error_msg)
            raise Exception(f"Failed to load config file: {e}")
        
    def filter_years_months(self, years, months, amc_name, current_year=None):
        """UNIVERSAL filter - handles ANY format of years and months"""
        try:
            def extract_universal_values(items, data_type="unknown"):
                """Extract meaningful values from ANY format"""
                if not items:
                    return []
                
                extracted = []
                
                for item in items:
                    value = None
                    
                    # Handle different formats
                    if isinstance(item, dict):
                        # Dictionary format: {'value': '2025', 'text': '2025'}
                        print("Getting insdie the first extract years raw condition")
                        value = item.get('value') or item.get('text') or item.get('label') or item.get('name')
                        
                    elif isinstance(item, (list, tuple)) and len(item) >= 1:
                        # List/tuple format: ['2025', 'Twenty Twenty Five'] or ('2025',)
                        value = item[0]
                        
                    elif isinstance(item, str):
                        # String format: '2025' or 'January'
                        value = item.strip()
                        
                    elif isinstance(item, (int, float)):
                        # Numeric format: 2025 or 1.0
                        value = str(int(item))
                        
                    else:
                        # Unknown format - try to convert to string
                        try:
                            value = str(item).strip()
                        except:
                            continue
                    
                    # Clean and validate the extracted value
                    if value:
                        value = str(value).strip()
                        # Remove common HTML entities and extra whitespace
                        value = value.replace('&nbsp;', ' ').replace('\n', ' ').replace('\t', ' ')
                        value = ' '.join(value.split())  # Normalize whitespace
                        
                        if value and value not in extracted:  # Avoid duplicates
                            extracted.append(value)
                
                self.logger.info(f"Extracted {data_type}: {extracted}")
                return extracted
            
            def normalize_year(year_str):
                """Convert any year format to integer"""
                if not year_str:
                    return None
                    
                # Handle common year formats
                year_clean = str(year_str).strip().lower()
                
                # Handle year ranges BEFORE removing dashes
                if '-' in year_clean:
                    # Handle ranges like '2025-2026', '2024-2025', '2024-25'
                    parts = year_clean.split('-')
                    if len(parts) == 2:
                        try:
                            # Take the SECOND year from the range
                            second_year = parts[1].strip()
                            
                            # Remove prefixes from second year
                            second_year = second_year.replace('year', '').replace('yr', '').replace('fy', '').strip()
                            
                            # Handle short format like '2024-25'
                            if len(second_year) == 2 and second_year.isdigit():
                                first_year = parts[0].strip().replace('year', '').replace('yr', '').replace('fy', '').strip()
                                if len(first_year) == 4:
                                    # Combine: '2024-25' becomes '2025'
                                    second_year = first_year[:2] + second_year
                            
                            second_year_int = int(second_year)
                            
                            # Validate reasonable year range
                            if 1900 <= second_year_int <= 2100:
                                return second_year_int
                                
                        except (ValueError, TypeError):
                            pass
                
                # Remove common prefixes/suffixes (after handling ranges)
                year_clean = year_clean.replace('year', '').replace('yr', '').replace('fy', '')
                year_clean = year_clean.replace('-', '').replace('_', '').strip()
                
                # Try direct conversion
                try:
                    year_int = int(float(year_clean))  # Handle '2025.0'
                    # Validate reasonable year range
                    if 1900 <= year_int <= 2100:
                        return year_int
                except (ValueError, TypeError):
                    pass
                
                # Handle 2-digit years
                if len(year_clean) == 2 and year_clean.isdigit():
                    year_2digit = int(year_clean)
                    # Assume 00-30 = 2000-2030, 31-99 = 1931-1999
                    if year_2digit <= 30:
                        return 2000 + year_2digit
                    else:
                        return 1900 + year_2digit
                
                return None
            
            def normalize_month(month_str):
                """Convert any month format to standard number"""
                if not month_str:
                    return None
                    
                month_clean = str(month_str).strip().lower()
                
                # Enhanced month mapping
                month_mappings = {
                    # Full names
                    'january': 1, 'february': 2, 'march': 3, 'april': 4, 'may': 5, 'june': 6,
                    'july': 7, 'august': 8, 'september': 9, 'october': 10, 'november': 11, 'december': 12,
                    
                    # Short names (3 letters)
                    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
                    'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12,
                    
                    # Numbers as strings
                    '1': 1, '2': 2, '3': 3, '4': 4, '5': 5, '6': 6,
                    '7': 7, '8': 8, '9': 9, '10': 10, '11': 11, '12': 12,
                    
                    # Padded numbers
                    '01': 1, '02': 2, '03': 3, '04': 4, '05': 5, '06': 6,
                    '07': 7, '08': 8, '09': 9,
                }
                
                # Try direct lookup
                if month_clean in month_mappings:
                    return month_mappings[month_clean]
                
                # Try numeric conversion
                try:
                    month_num = int(float(month_clean))
                    if 1 <= month_num <= 12:
                        return month_num
                except (ValueError, TypeError):
                    pass
                
                # Try partial matching for longer month names
                for month_name, month_num in month_mappings.items():
                    if len(month_name) >= 3 and (month_name in month_clean or month_clean in month_name):
                        return month_num
                
                return None
            
            # Extract values from any format
            years_raw = extract_universal_values(years, "years")
            print(years_raw)
            months_raw = extract_universal_values(months, "months")
            print(months_raw)
            
            # Normalize to standard formats
            years_normalized = []
            months_normalized = []
            
            for year_raw in years_raw:
                year_int = normalize_year(year_raw)
                if year_int:
                    years_normalized.append((year_raw, year_int))
            
            for month_raw in months_raw:
                month_int = normalize_month(month_raw)
                if month_int:
                    months_normalized.append((month_raw, month_int))
            
            self.logger.info(f"Processing AMC: {amc_name}")
            
            conn = adqvest_db.db_conn()
            amc_name_clean = str(amc_name).strip()
            
            conn = adqvest_db.db_conn()
            query = f"SELECT MAX(Year) as last_year, MAX(Month) as last_month FROM AMC_WISE_MF_FACTSHEETS_MONTHLY_DATA_CORPUS WHERE AMC_Name = '{amc_name}'"
            result = pd.read_sql(query, conn)
            print(result)
            
            if result.empty or pd.isna(result['last_year'].iloc[0]):
                self.logger.info(f"No existing data for {amc_name_clean}. Processing all available data.")
                return years, months
            
            last_year_raw = result['last_year'].iloc[0]
            last_year = normalize_year(last_year_raw)
            last_month = result['last_month'].iloc[0]
            last_month_normalized = normalize_month(last_month)
            
            self.logger.info(f"Last processed: Year {last_year}, Month {last_month} (normalized: {last_month_normalized})")
          
            def find_original_items(original_list, filtered_raw_values):
                """Find original items that match filtered values"""
                if not filtered_raw_values:
                    return []
                
                # If original format was simple, return filtered values directly
                if not original_list:
                    return filtered_raw_values
                
                # For complex formats, find matching original items
                filtered_original = []
                for filtered_value in filtered_raw_values:
                    for original_item in original_list:
                        if isinstance(original_item, dict):
                            if (original_item.get('value') == filtered_value or 
                                original_item.get('text') == filtered_value):
                                filtered_original.append(original_item)
                                break
                        elif str(original_item).strip() == str(filtered_value).strip():
                            filtered_original.append(original_item)
                            break
                    else:
                        # If no match found, include the filtered value
                        filtered_original.append(filtered_value)
                
                return filtered_original
            
            filtered_years_raw = []
            filtered_months_raw = []
                      
            if years and not months:
                self.logger.info("=== FILTERING YEARS ===")
                
                for year_raw, year_int in years_normalized:
                    if year_int > last_year:
                        filtered_years_raw.append(year_raw)
                        self.logger.info(f"✅ Keeping future year: {year_int}")
                    elif year_int == last_year:
                        filtered_years_raw.append(year_raw) 
                        self.logger.info(f"✅ Keeping current year: {year_int}")
                    else:
                        self.logger.info(f"❌ Skipping past year: {year_int}")
            
            # Case 2: Filtering months only
            elif months and current_year:
                self.logger.info(f"=== FILTERING MONTHS FOR YEAR {last_year} ===")
                current_year_int = int(last_year)
                
                
                if current_year_int > last_year:
                    filtered_months_raw = months_raw
                    self.logger.info(f"✅ Future year - keeping all months")
                elif current_year_int == last_year:
                    self.logger.info(f"🔍 Current year - filtering months > {last_month_normalized}")
                    for month_raw, month_int in months_normalized:
                        if month_int > last_month_normalized:
                            filtered_months_raw.append(month_raw)
                            self.logger.info(f"✅ Keeping {month_raw} ({month_int}) > {last_month_normalized}")
            
            if filtered_years_raw:
                filtered_years_final = find_original_items(years, filtered_years_raw)
                self.logger.info(f"=== FILTERING RESULTS FOR YEARS ===")
                self.logger.info(f"Years: {len(years)} → {len(filtered_years_raw)} items")
                self.logger.info(f"Filtered years: {filtered_years_raw}")
                self.logger.info(f"========================")
                return filtered_years_final,[]
            if filtered_months_raw:
                filtered_months_final = find_original_items(months, filtered_months_raw)
                self.logger.info(f"=== FILTERING RESULTS FOR MONTHS ===")
                self.logger.info(f"Months: {len(months)} → {len(filtered_months_raw)} items")
                self.logger.info(f"Filtered months: {filtered_months_raw}")
                self.logger.info(f"========================")
                return [],filtered_months_final
         
        except Exception as e:
            error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            error_msg = str(sys.exc_info()[1]) + " line " + str(exc_tb.tb_lineno)
            print(error_msg)

    # Keep your existing month_name_to_number function - it's perfect!
    def month_name_to_number(self, month_name: str) -> str:
        """Convert month name to number"""
        month_map = { 'january': '01', 'jan': '01', 'february': '02', 'feb': '02',
            'march': '03', 'mar': '03', 'april': '04', 'apr': '04', 'may': '05',
            'june': '06', 'jun': '06', 'july': '07', 'jul': '07', 'august': '08', 'aug': '08',
            'september': '09', 'sep': '09', 'sept': '09', 'october': '10', 'oct': '10', 
            'november': '11', 'nov': '11', 'december': '12', 'dec': '12'}
        
        month_lower = month_name.lower().strip()
        return month_map.get(month_lower, month_name.zfill(2))

    async def detect_javascript_links(self, page: Page) -> int:
        """Detect various types of JavaScript download links"""
        patterns = ["a[href='javascript:void(0)']", "a.file-download-link", "a[onclick*='download']",
            "a[href*='javascript:'][href*='download']"]
        
        total_count = 0
        for pattern in patterns:
            count = await page.locator(pattern).count()
            total_count += count
            if count > 0:
                self.logger.info(f"Found {count} links matching pattern: {pattern}")
        
        return total_count

    def generate_file_name(self, month_text: str, year_text: str) -> str:
        """Generate filename in format: Factsheet_Month_Year"""
        try:
            month_clean = re.sub(r'[^\w\s]', '', month_text).strip()
            year_match = re.search(r'\d{4}', year_text)
            year_clean = year_match.group() if year_match else year_text.strip()
            
            return f"Factsheet_{month_clean}_{year_clean}"
        except Exception:
            return f"Factsheet_{datetime.now().strftime('%B_%Y')}"

    def generate_relevant_date(self, month_text: str, year_text: str) -> str:
        """Generate relevant date (end of month) in format: YYYY-MM-DD"""
        
        year_match = re.search(r'\d{4}', year_text)
        year = int(year_match.group()) if year_match else datetime.now().year
        
        month_clean = re.sub(r'[^\w\s]', '', month_text).strip()
        month_num = self.month_name_to_number(month_clean)
        month = int(month_num)
        
        last_day = calendar.monthrange(year, month)[1]
        return f"{year:04d}-{month:02d}-{last_day:02d}"
    
    @staticmethod
    def generate_relevant_date_for_FY(month_text: str, year_text: str) -> str:
        """Generate relevant date (end of month) in format: YYYY-MM-DD, handling FY ranges."""

        # Helper: convert month name to number
        def month_name_to_number(month_name):
            month_name = month_name.lower().strip()
            months = {
                'january': 1, 'jan': 1,
                'february': 2, 'feb': 2,
                'march': 3, 'mar': 3,
                'april': 4, 'apr': 4,
                'may': 5,
                'june': 6, 'jun': 6,
                'july': 7, 'jul': 7,
                'august': 8, 'aug': 8,
                'september': 9, 'sep': 9,
                'october': 10, 'oct': 10,
                'november': 11, 'nov': 11,
                'december': 12, 'dec': 12
            }
            return months.get(month_name, 0)

        # Extract years
        # years = re.findall(r'\d{4}', year_text)
        # first_year = int(years[0]) if years else datetime.now().year
        # second_year = int(years[1]) if len(years) > 1 else None
        match = re.match(r'\s*(\d{4})\s*-\s*(\d{2}|\d{4})\s*', year_text)
        if match:
            first_year = int(match.group(1))
            second_part = match.group(2)
            if len(second_part) == 2:
                # Infer full year (e.g., 25 -> 2025, assuming same century)
                second_year = int(str(first_year)[:2] + second_part)
            else:
                second_year = int(second_part)
        else:
            # If no match, fallback to current year
            first_year = datetime.now().year
            second_year = None

        month_clean = re.sub(r'[^\w\s]', '', month_text).strip()
        month_num = month_name_to_number(month_clean)

        # Apply Indian FY mapping: April-Dec -> first_year, Jan-Mar -> second_year
        if second_year and month_num:
            if month_num >= 4:  # April to December
                year = first_year
            elif 1 <= month_num <= 3:  # January to March
                year = second_year
            else:
                year = first_year
        else:
            year = first_year

        if month_num == 0:
            # fallback: use January
            month_num = 1

        last_day = calendar.monthrange(year, month_num)[1]
        return f"{year:04d}-{month_num:02d}-{last_day:02d}"
        
    async def Upload_Data_By_AMC_Name(self, AMC_Name, data):
        """
        Insert rows into AMC_WISE_MF_FACTSHEETS_MONTHLY_DATA_CORPUS for a specific AMC_Name,
        skipping File_Link values that already exist for that AMC_Name.
        """
        assert isinstance(data, pd.DataFrame), f"data must be DataFrame, got {type(data)}"
        
        try:
            # conn = Connections()
            engine = sqlalchemy.create_engine('mysql+pymysql://induja:InNew$78@ohiotomumbai.cnfgszrwissj.ap-south-1.rds.amazonaws.com:3306/AdqvestDB?charset=utf8')

            # 1) Fetch existing links for this AMC_Name (parameterized)
            # existing_links_df = pd.read_sql(
            #     """
            #     SELECT File_Link
            #     FROM AMC_WISE_MF_FACTSHEETS_MONTHLY_DATA_CORPUS
            #     WHERE AMC_Name = %s
            #     """,
            #     conn.mysql,
            #     params=[AMC_Name]
            # )
            if data is None or data.empty:
                print(f"⚠ No input rows for AMC_Name={AMC_Name}")
                return
            required = ['Company','AMC_Name','File_name','Relevant_date','Runtime','File_Link','Month','Year']
            missing = [c for c in required if c not in data.columns]
            if missing:
                print(f"❌ Missing required columns for {AMC_Name}: {missing}")
                return

            from sqlalchemy import text

            with engine.connect() as conn:
                existing_links_df = pd.read_sql(text("""SELECT File_Link 
                                                     FROM AMC_WISE_MF_FACTSHEETS_MONTHLY_DATA_CORPUS 
                                                     WHERE AMC_Name = :amc_name"""),con=conn,
                                                params={"amc_name": AMC_Name},)

            existing_links = existing_links_df["File_Link"].tolist() if not existing_links_df.empty else []
            
            new_data = data[~data["File_Link"].isin(existing_links)]
            new_data = new_data[~new_data["File_name"].str.contains("Our Services", case=False, na=False)]
            new_data = new_data[~new_data["File_name"].str.contains("digital", case=False, na=False)]
            # new_data = new_data.drop_duplicates(subset = 'File_Link')
            
            try:
                query_last_date = "SELECT MAX(Relevant_date) as last_date FROM AMC_WISE_MF_FACTSHEETS_MONTHLY_DATA_CORPUS WHERE AMC_Name = %s"
                result = pd.read_sql(query_last_date, con=engine, params=[AMC_Name])
                
                if not result.empty and not pd.isna(result['last_date'].iloc[0]):
                    last_date = pd.to_datetime(result['last_date'].iloc[0])
                    before_count = len(new_data)
                    new_data = new_data[pd.to_datetime(new_data['Relevant_date'], errors='coerce') >= last_date]
                    print(f"Final date filter: {len(new_data)} records after {last_date.date()} (was {before_count})")
                else:
                    print(f"No previous data for {AMC_Name} - proceeding with all {len(new_data)} records")
                    
            except Exception as e:
                print(f"Error in final date filter: {e}")
            
            new_data1 = new_data[~new_data["File_Link"].isin(existing_links)]
            
            if new_data1.empty:
                print(f"⚠ No new data to insert for AMC_Name={AMC_Name}")
                return

            # new_data1.to_sql("AMC_WISE_MF_FACTSHEETS_MONTHLY_DATA_CORPUS", con=engine, if_exists="append", index=False,chunksize=1000, method="multi")

            print(f"✅ Inserted {len(new_data1)} new rows for AMC_Name={AMC_Name}")
            print(new_data1.info())

        except Exception as e:
            error_msg = str(e) + " line " + str(sys.exc_info()[2].tb_lineno)
            print(error_msg)
            print(f"❌ Upload failed for AMC_Name={AMC_Name}: {e}")
            
    async def persist_results(self):
        # 1) Defensive checks
        if not getattr(self, "results", None):
            self.logger.warning("No results to persist")
            return

        # 2) Convert results to DataFrame
        rows = [asdict(r) for r in self.results]
        df = pd.DataFrame(rows)
        print(df,'this is df')

        # Ensure required column exists for the DB function
        required_cols = ['Company','AMC_Name','File_name','Relevant_date','Runtime','File_Link','Month','Year']
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            self.logger.error(f"Missing required columns: {missing}")
            return

        # 3) Group by AMC and upload per AMC
        amc_name = df['AMC_Name'][0]
        print(amc_name,'amc_name')
        print(type(df),'df type')
        try:
            print(df, 'sliced data')
            await self.Upload_Data_By_AMC_Name(amc_name,df)
        except Exception as e:
            self.logger.error(f"Upload failed for AMC_Name={amc_name}: {e}")

    async def crawl_all_sites(self):
        """Main method to crawl all enabled sites"""
        enabled_sites = [site for site in self.config if site.enabled]
        enabled_sites.sort(key=lambda x: x.priority)
        
        self.logger.info(f"Starting crawl for {len(enabled_sites)} sites")
        
        for site in enabled_sites:
            try:
                self.logger.info(f"Processing: {site.amc_name} ({site.site_id})")
                await self.crawl_site(site)
            except Exception as e:
                self.logger.error(f"Error processing {site.site_id}: {e}")
        
        await self.save_results_to_csv()
        await self.persist_results()

    async def auto_handle_popups(self, page: Page):
        """Automatically detect and handle any popups that appear"""
        try:
            self.logger.info('Auto-detecting popups...')
            
            popup_patterns = [ '.modal .close', '.modal-close', '.modal-header .close',
                'button[data-dismiss="modal"]', '[data-bs-dismiss="modal"]',
                '.notification .close', '.alert .close', '.toast .close',
                '.banner-close', '.notification-close', '.popup-close', '.overlay-close', '.dialog-close',
                '.lightbox-close', '.fancybox-close', '.cookie-banner .close', '.cookie-consent .close',
                '.gdpr-banner .close', '#cookie-notice .close', 'button[data-cookie-close]', '.cookie-accept',
                '.close', '.close-btn', '.close-button',
                'button[aria-label*="close" i]', 'button[aria-label*="dismiss" i]',
                '[aria-label*="close" i]', '[title*="close" i]', '.fa-times', '.fa-close', '.icon-close', '.icon-x',
                'button:has-text("×")', 'button:has-text("✕")', 'button:has-text("Close")', 'button:has-text("OK")',
                'button:has-text("Accept")', 'button:has-text("Got it")',
                'a:has-text("Close")', 'span:has-text("Close")']
            
            popup_found = False
            
            for pattern in popup_patterns:
                try:
                    await page.wait_for_selector(pattern, timeout=1000)
                    element = page.locator(pattern).first
                    if await element.is_visible():
                        await element.click()
                        popup_found = True
                        self.logger.info(f'Auto-closed popup using pattern: {pattern}')
                        await asyncio.sleep(0.5)
                        break
                        
                except PlaywrightTimeoutError:
                    continue
                except Exception as e:
                    continue
            
            if not popup_found:
                try:
                    await page.keyboard.press('Escape')
                    await asyncio.sleep(0.5)
                    self.logger.info('Attempted popup dismissal with Escape key')
                except Exception:
                    pass
                
                try:
                    overlay_patterns = ['.overlay', '.modal-backdrop', '.popup-overlay',
                        '.lightbox-overlay', '.dialog-backdrop']
                    
                    for overlay_pattern in overlay_patterns:
                        try:
                            if await page.locator(overlay_pattern).count() > 0:
                                await page.mouse.click(50, 50)
                                self.logger.info(f'Attempted to dismiss overlay: {overlay_pattern}')
                                await asyncio.sleep(0.5)
                                break
                        except Exception:
                            continue
                            
                except Exception:
                    pass
            
            if not popup_found:
                self.logger.info('No popups detected - continuing with normal flow')
                
        except Exception as e:
            self.logger.warning(f'Auto popup handling encountered an error: {e}')

    async def collect_paginated_js_links(self, page: Page, site: SiteConfig) -> List[Dict[str, Any]]:
        """Handle: Material Design paginator with JavaScript link interception"""
        print('USING PAGINATED JS LINKS METHOD')
        download_urls = []
        
        next_button_selector = getattr(site.selectors, 'next_button_selector', None)
        
        # Set up request interception for JavaScript downloads
        async def handle_request(request):
            url = request.url
            if ('.pdf' in url.lower() or 'download' in url.lower() or 'factsheet' in url.lower()):
                print(f"Intercepted download URL: {url}")
                
                # Try to extract month/year from URL
                try:
                    result = self.month_year_from_url(url)
                    year = result[0] 
                    month = result[1]
                    
                    if year and month:
                        file_name = f"Factsheet_{month}_{year}"
                        relevant_date = self.generate_relevant_date(str(month), str(year))
                    else:
                        filename = url.split('/')[-1] if '/' in url else 'Factsheet_Unknown'
                        file_name = filename
                        relevant_date = None
                        year = None
                        month = None
                except:
                    filename = url.split('/')[-1] if '/' in url else 'Factsheet_Unknown'
                    file_name = filename
                    relevant_date = None
                    year = None
                    month = None
                
                download_urls.append({'url': url, 'file_name': file_name, 'year': year, 'month': month,
                                      'relevant_date': relevant_date})
        
        # Enable request interception
        page.on("request", handle_request)
        
        data_container_selector = getattr(site.selectors, "data_container_selector", None) or ""
        
        async def click_js_links_on_page():
            """Click all JavaScript links on current page"""
            try:
                await page.wait_for_selector(data_container_selector, timeout=15000)
                container = page.locator(data_container_selector)
                
                # Look for JavaScript download links
                js_links = container.locator("a[href='javascript:void(0)'], a.file-download-link")
                count = await js_links.count()
                
                print(f"Clicking {count} JavaScript download links on current page")
                
                for i in range(count):
                    try:
                        link = js_links.nth(i)
                        await link.scroll_into_view_if_needed()
                        await link.click()
                        await asyncio.sleep(0.5)  # Brief pause between clicks
                    except Exception as e:
                        self.logger.warning(f"Error clicking JS link {i}: {e}")
                        
            except Exception as e:
                self.logger.error(f"Error clicking JS links on page: {e}")
        
        async def has_next_page():
            """Check if next page button is enabled"""
            try:
                next_button = page.locator(next_button_selector).first
                button_count = await next_button.count()
                print(f"DEBUG: Found {button_count} next buttons")
                
                if button_count == 0:
                    print("DEBUG: No next button found")
                    return False
                    
                is_visible = await next_button.is_visible()
                print(f"DEBUG: Next button visible: {is_visible}")
                
                is_disabled = await next_button.get_attribute('disabled')
                print(f"DEBUG: Next button disabled attribute: {is_disabled}")
                
                # Check for aria-disabled as well
                aria_disabled = await next_button.get_attribute('aria-disabled')
                print(f"DEBUG: Next button aria-disabled: {aria_disabled}")
                
                # Check button classes for disabled state
                button_classes = await next_button.get_attribute('class')
                print(f"DEBUG: Next button classes: {button_classes}")
                
                result = is_disabled is None and aria_disabled != 'true'
                print(f"DEBUG: has_next_page returning: {result}")
                return result
            except Exception as e:
                print(f"DEBUG: Exception in has_next_page: {e}")
                return False    
        
        async def get_paginator_info():
            """Get current pagination info"""
            try:
                range_label = await page.locator('.mat-paginator-range-label').text_content()
                return range_label
            except Exception:
                return "Unknown"
        
        # Click JS links on first page
        print("Extracting JS links from first page...")
        page_info = await get_paginator_info()
        await click_js_links_on_page()
        print(f"Processed first page ({page_info})")
        
        # Get search limit from config
        search_limit = getattr(site.options, "search_limit", None)
        max_pages = int(search_limit) if search_limit and search_limit > 0 else 10
        print(max_pages)
        
        page_count = 1
        
        # Continue clicking next and processing JS links until no more pages
        while await has_next_page() and page_count < max_pages:
            try:
                print(f"Navigating to page {page_count + 1}...")
                
                # Click the next button
                next_button = page.locator(next_button_selector).first
                await next_button.click()
                
                # Wait for page to load
                await page.wait_for_load_state('domcontentloaded')
                await asyncio.sleep(site.options.cascade_wait_time)
                
                # Click JS links on new page
                page_info = await get_paginator_info()
                await click_js_links_on_page()
                
                page_count += 1
                print(f"Processed page {page_count} ({page_info})")
                
            except Exception as e:
                self.logger.error(f"Error navigating to next page: {e}")
                break
        
        print(f"Total download URLs intercepted from {page_count} pages: {len(download_urls)}")
        
        # Remove duplicates
        seen = set()
        unique_results = []
        for item in download_urls:
            if item['url'] not in seen:
                seen.add(item['url'])
                unique_results.append(item)
        
        return unique_results
            
    async def collect_links_for_site(self, page, site):
        """Route to the appropriate collector based on config and site type"""
        
        print(f"DEBUG: Checking site_id = '{site.site_id}'")
       
        if site.site_id == "Motilal Oswal AMC":
            print("🎯 Detected Motilal Oswal AMC - Using specialized crawler")
            return await self.crawl_motilal_oswal_amc(page, site)
        
        if site.site_id == "Trust AMC":
            print("🎯 Detected Trust AMC - Using specialized crawler")
            return await self.crawl_trust_amc(page, site)
        
        if site.site_id == "Bandhan AMC":
            print("🎯 Detected Bandhan AMC - Using specialized crawler")
            return await self.crawl_bandhan_amc(page, site)
        
        if site.site_id == "ICICI AMC":
            print("🎯 Detected ICICI AMC - Using specialized crawler")
            return await self.crawl_icici_amc(page, site)
        
        if site.site_id == "WhiteOak Capital AMC":
            print("🎯 Detected WhiteOak Capital AMC - Using specialized crawler")
            return await self.crawl_whiteoak_amc(page, site)
        
        if site.site_id == "Tata AMC":
            print("🎯 Detected Tata AMC - Using specialized crawler")
            return await self.crawl_tata_amc(page, site)
            
        # Your existing logic for other types of links
        pagination_sel = getattr(site.selectors, "pagination_selector", None) or ""
        next_button_sel = getattr(site.selectors, "next_button_selector", None) or ""
        year_sel = getattr(site.selectors, "year_selector", None) or ""
        month_sel = getattr(site.selectors, "month_selector", None) or ""

        has_year_month = bool(year_sel.strip()) and bool(month_sel.strip())
        has_only_year = bool(year_sel.strip()) and not bool(month_sel.strip())

        if has_only_year:
            return await self.comprehensive_year_crawl(page, site)

        if has_year_month:
            return await self.comprehensive_year_month_crawl(page, site)
        
        if pagination_sel.strip():
            return await self.collect_pdf_links_pagination(page, site)
        
        if next_button_sel.strip():
            search_limit = getattr(site.options, "search_limit", None)
            max_pages = int(search_limit) if search_limit and search_limit > 0 else 1
            print(max_pages)
            return await self.crawl_pages_via_next(page, site, max_pages)

        # Fallback: container + address selector flow
        return await self.collect_pdf_links_simple(page, site)

    async def collect_pgim_factsheets(self, page: Page, site: SiteConfig) -> List[Dict[str, Any]]:
        """Extract PGIM factsheet information and construct real PDF URLs"""
        results = []

        base_url = "https://www.pgimindia.com/api/v1/brochure/about-us/image/"
        
        try:
            data_container_selector = site.selectors.data_container_selector
            await page.wait_for_selector(data_container_selector, timeout=15000)
            
            factsheet_elements = page.locator('.form-card')
            count = await factsheet_elements.count()
            
            self.logger.info(f"Found {count} factsheet elements to process")
            
            for i in range(count):
                try:
                    element = factsheet_elements.nth(i)
                    text_content = await element.text_content()
                    text_content = text_content.strip() if text_content else f"Factsheet_{i}"
                    
                    self.logger.info(f"Processing factsheet: {text_content}")
                    
                    # Construct the real PDF URL
                    from urllib.parse import quote
                    encoded_filename = quote(text_content)
                    print(encoded_filename)
                    real_pdf_url = f"{base_url}{encoded_filename}.pdf"
                    print(real_pdf_url)
                    
                    # Extract month and year from text
                    year, month = self.extract_year_month_from_filename(text_content)
                    
                    if not year or not month:
                        # Alternative extraction for "Factsheet - July 2025" format
                        match = re.search(r'Factsheet\s*-\s*(\w+)\s*(\d{4})', text_content, re.I)
                        if match:
                            month_str, year_str = match.groups()
                            month = month_str.title()
                            year = int(year_str)
                    
                    if year and month:
                        file_name = f"Factsheet_{month}_{year}"
                        relevant_date = self.generate_relevant_date(str(month), str(year))
                    else:
                        # Fallback
                        file_name = text_content.replace(' ', '_').replace('-', '_')
                        relevant_date = datetime.now().strftime('%Y-%m-%d')
                        year = datetime.now().year
                        month = datetime.now().strftime('%B')
                    
                    results.append({ 'url': real_pdf_url, 'file_name': file_name, 'relevant_date': relevant_date,
                        'year': str(year),'month': str(month)})
                    
                    self.logger.info(f"Constructed real URL: {real_pdf_url}")
                    
                except Exception as e:
                    self.logger.warning(f"Error processing factsheet element {i}: {e}")
                    continue
            
            self.logger.info(f"Successfully constructed {len(results)} real PGIM PDF URLs")
            return results
            
        except Exception as e:
            self.logger.error(f"Error in PGIM factsheet collection: {e}")
            return []
        
    async def crawl_motilal_oswal_amc(self, page: Page, site: SiteConfig) -> List[Dict[str, Any]]:
        """Crawl Motilal Oswal AMC using the converted Crawlee approach"""
        results = []
        
        try:
            # Navigate and refresh
            await page.goto(site.url)
            await page.reload()
            await page.wait_for_timeout(5000)
            
            # Get years
            print("Getting years...")
            await page.click("#year")
            await page.wait_for_timeout(2000)
            
            # Get year options
            year_options = await page.query_selector_all('[class*="option"]')
            years = []
            
            for option in year_options:
                text = await option.text_content()
                text = text.strip() if text else ""
                if text.isdigit() and len(text) == 4:
                    years.append(int(text))
            
            # Click outside to close dropdown
            await page.click("body")
            await page.wait_for_timeout(1000)
            
            years = sorted(list(set(years)), reverse=True)
            print(len(years))
            years, _ = self.filter_years_months(years, [], site.amc_name)
            print(len(years))
            print(f"Years: {years}")
            
            # Process each year (limiting to first year like original)
            for year in years:
                print(f"\n=== Processing Year: {year} ===")
                
                # Select year first
                await page.click("#year")
                await page.wait_for_timeout(5000)
                
                year_option_elements = await page.query_selector_all('[class*="option"]')
                for option in year_option_elements:
                    text = await option.text_content()
                    if text and text.strip() == str(year):
                        await option.click()
                        break
                
                await page.wait_for_timeout(8000)
                
                # Now get months for this specific year
                months_for_year = []
                try:
                    print(f"Getting months for year {year}...")
                    await page.click("#month")
                    await page.wait_for_timeout(8000)
                    
                    month_options = await page.query_selector_all('[class*="option"]')
                    for option in month_options:
                        text = await option.text_content()
                        text = text.strip() if text else ""
                        if text and text.lower() != "all":  # Exclude "All" or "all"
                            months_for_year.append(text)
                    
                    await page.click("body")
                    await page.wait_for_timeout(8000)
                    
                except Exception as e:
                    print(f"Month dropdown not found for year {year}: {e}")
                    months_for_year = []
                
                months_for_year = list(dict.fromkeys(months_for_year))
                _, months_for_year = self.filter_years_months([], months_for_year, site.amc_name, current_year=year)
                print(f"Months for {year}: {months_for_year}")
                
                # Process each month for this year
                for month in months_for_year:
                    print(f"\n--- Processing Year: {year}, Month: {month} ---")
                    
                    try:
                        # Select month
                        if len(months_for_year) > 1:
                            try:
                                await page.click("#month")
                                await page.wait_for_timeout(8000)
                                
                                month_option_elements = await page.query_selector_all('[class*="option"]')
                                for option in month_option_elements:
                                    text = await option.text_content()
                                    if text and text.strip() == month:
                                        await option.click()
                                        break
                                
                                await page.wait_for_timeout(8000)
                            except Exception as e:
                                print(f"Error selecting month: {e}")
                        
                        # Find all download cards
                        download_cards = await page.query_selector_all(".downloadWithPagination_downloadCard__4Gsjq")
                        print(f"Found {len(download_cards)} download cards for Year={year}, Month={month}")
                        
                        for card_index, card in enumerate(download_cards):
                            try:
                                # Click on the document icon within this card
                                document_icon = await card.query_selector(".downloadWithPagination_documentIcon__ph7xA")
                                if document_icon:
                                    await document_icon.click()
                                    await page.wait_for_timeout(5000)
                                    
                                    # Get the download link from the options that appear
                                    download_link_element = await page.query_selector(".downloadWithPagination_downLoadOptionList__7Ifg_ a")
                                    if download_link_element:
                                        download_link = await download_link_element.get_attribute("href")
                                        
                                        # Process the date (subtract 1 month like original)
                                        temp_date = pd.to_datetime(f'{year}-{month}', format='%Y-%B')
                                        temp_date = temp_date - pd.DateOffset(months=1)
                                        
                                        processed_year = temp_date.year
                                        processed_month = temp_date.strftime('%B')
                                        
                                        # Generate relevant date (end of month)
                                        relevant_date = pd.to_datetime(f'{processed_year}-{processed_month}', format='%Y-%B') + pd.offsets.MonthEnd(0)
                                        relevant_date = relevant_date.date()
                                        
                                        # Add to results list
                                        result = {
                                            'url': download_link,
                                            'file_name': f"Factsheet_{processed_month}_{processed_year}",
                                            'month': processed_month, 'year': str(processed_year),
                                            'relevant_date': relevant_date.strftime('%Y-%m-%d')}
                                        results.append(result)
                                        
                                        print(f"✓ Added: Year={processed_year}, Month={processed_month}, Card={card_index + 1}")
                                        
                                        # Close the download options (click elsewhere)
                                        await page.click("body")
                                        await page.wait_for_timeout(5000)
                                
                            except Exception as e:
                                print(f"✗ Error with card {card_index + 1}: {e}")
                                continue
                        
                        # Scroll back to top after processing all cards
                        await page.evaluate("window.scrollTo(0, 0)")
                        await page.wait_for_timeout(5000)
                            
                    except Exception as e:
                        print(f"✗ Error processing Year {year}, Month {month}: {e}")
                        continue
            
            print(f"\n=== Final Results: {len(results)} records ===")
            return results
            
        except Exception as e:
            print(f"Error in Motilal Oswal crawling: {e}")
            return results    
        
    async def crawl_trust_amc(self, page: Page, site: SiteConfig) -> List[Dict[str, Any]]:
        """Crawl Trust AMC using year/month iteration pattern converted from Selenium"""
        results = []
        
        try:
            await page.goto(site.url)
            await page.wait_for_timeout(5000)
            
            print("Getting years and months...")
            
            select_elements = await page.query_selector_all('select[name="cars"]')
            if len(select_elements) < 2:
                print("Could not find year/month dropdowns")
                return results
                
            year_select = select_elements[0] 
            month_select = select_elements[1]  
            
            if not year_select or not month_select:
                print("Could not find year/month dropdowns")
                return results
            
            # Get all available years (excluding empty and "all")
            year_options = await year_select.query_selector_all('option')
            years = []
            
            for option in year_options:
                value = await option.get_attribute('value')
                if value and value not in ('', 'all'):
                    years.append(value)

            print(len(years))
            years, _ = self.filter_years_months(years, [], site.amc_name)
            print(len(years))
            print(f"Found years: {years}")

            for year in years:
                print(f"\n=== Processing Year: {year} ===")
                
                await year_select.select_option(year)
                await page.wait_for_timeout(5000)
                
                month_options = await month_select.query_selector_all('option')
                months = []
                
                for option in month_options:
                    value = await option.get_attribute('value')
                    if value and value not in ('', 'all'):
                        months.append(value)
                
                await page.wait_for_timeout(2000)
                
                _, months = self.filter_years_months([], months, site.amc_name, current_year=year)
                print(f"Filtered months for {year}: {months}")
                
                for month in months:
                    print(f"\n=== Processing Month: {month} ===")
                    
                    await month_select.select_option(month)
                    await page.wait_for_timeout(10000)
                    
                    try:
                        factsheet_link = await page.query_selector("div.flex.flex-col.gap-ext-1 a")
                        
                        if factsheet_link:
                            heading_element = await factsheet_link.query_selector("p")
                            heading = await heading_element.inner_text() if heading_element else "No heading"                            
                            print(f"Found heading: {heading}")

                            months = ['january', 'february', 'march', 'april', 'may', 'june','july', 'august',
                                      'september', 'october', 'november', 'december','jan', 'feb', 'mar', 
                                      'apr', 'may', 'jun','jul', 'aug', 'sep', 'oct', 'nov', 'dec']
                            
                            has_month = any(month in heading.lower() for month in months)
                            has_year = bool(re.search(r'\b(19|20)\d{2}\b', heading))
                            
                            if not (has_month and has_year):
                                print(f"Skipping - heading missing month/year: {heading}")
                                continue
                            
                            print(f"Valid heading: {heading}")
                            
                            async with page.expect_popup() as popup_info:
                                await factsheet_link.click()
                            
                            popup = await popup_info.value
                            await popup.wait_for_load_state()
                            await page.wait_for_timeout(3000)
                            
                            file_url = popup.url
                            await popup.close()
                            await page.wait_for_timeout(2000)
                            
                            print(f"Heading: {heading}")
                            print(f"Link: {file_url}")
                            
                            try:
                                heading_parts = heading.split(" ", 1)
                                if len(heading_parts) >= 2:
                                    file_month = heading_parts[0].lower()
                                    file_year = heading_parts[1]
                                    
                                    final_month = file_month if file_month != month.lower() else month
                                    final_year = file_year if file_year != year else year
                                else:
                                    final_month = month
                                    final_year = year
                                
                                relevant_date = self.generate_relevant_date(final_month, final_year)
                                
                            except Exception as date_error:
                                print(f"Date processing error: {date_error}")
                                final_month = month
                                final_year = year
                                relevant_date = self.generate_relevant_date(month, year)
                            
                            result = { 'url': file_url, 'file_name': heading, 'month': final_month,
                                      'year': final_year,'relevant_date': relevant_date}
                            results.append(result)
                            
                            print(f"✓ Added: {heading}")
                        
                        else:
                            print("Element not present")
                            
                    except Exception as e:
                        print(f"Error processing {year}-{month}: {str(e)}")
                        continue
            
            print(f"\n=== Trust AMC Results: {len(results)} records ===")
            return results
            
        except Exception as e:
            print(f"Error in Trust AMC crawling: {e}")
            return results    

    async def crawl_bandhan_amc(self, page: Page, site: SiteConfig) -> List[Dict[str, Any]]:
        """Crawl Bandhan AMC - clicks year buttons and extracts factsheets"""
        results = []
        
        try:
            await page.goto(site.url)
            await page.wait_for_timeout(5000)
            
            print("Getting year buttons...")
            
            await page.wait_for_selector("#apiname button", timeout=15000)
            year_buttons = await page.query_selector_all("#apiname button")
            
            years = []
            for btn in year_buttons:
                text = await btn.inner_text()
                years.append(text.strip())
                
            print(len(years))    
            years, _ = self.filter_years_months(years, [], site.amc_name)
            print(len(years))
            print(f"Found years: {years}")
            
            for year in years:
                print(f"\n=== Processing Year: {year} ===")
                
                buttons = await page.query_selector_all("#apiname button")
                btn_to_click = None
                for btn in buttons:
                    text = await btn.inner_text()
                    if text.strip() == year:
                        btn_to_click = btn
                        break
                
                if btn_to_click:
                    await btn_to_click.scroll_into_view_if_needed()
                    await page.wait_for_timeout(500)

                    await btn_to_click.click()
                    print(f"Clicked year: {year}")

                    await page.wait_for_timeout(10000)
                
                await page.wait_for_selector("div.flex.gap-3.items-center.border-b.border-slate-400.pb-6.md\\:w-full.cursor-pointer", timeout=15000)
                factsheet_divs = await page.query_selector_all("div.flex.gap-3.items-center.border-b.border-slate-400.pb-6.md\\:w-full.cursor-pointer")
                
                factsheet_count = len(factsheet_divs)
                print(f"Found {factsheet_count} factsheets for year {year}")
                
                for i in range(factsheet_count):
                    factsheet_divs = await page.query_selector_all("div.flex.gap-3.items-center.border-b.border-slate-400.pb-6.md\\:w-full.cursor-pointer")
                    div = factsheet_divs[i]

                    heading_element = await div.query_selector("p")
                    heading = await heading_element.inner_text() if heading_element else "No heading"
                    heading = heading.strip()
                    
                    print(f"Processing: {heading}")
                    await div.scroll_into_view_if_needed()
                    await page.wait_for_timeout(500)
                    
                    async with page.expect_popup() as popup_info:
                        await div.click()
                    
                    await page.wait_for_timeout(2000)
                    
                    popup = await popup_info.value
                    await popup.wait_for_load_state()
                    
                    link = popup.url
                    
                    await popup.close()
                    
                    print(f"Heading: {heading}, Link: {link}")
                    
                    import re
                    match = re.search(r'([A-Za-z]+)\s+(\d{4})', heading)
                    
                    if match:
                        month_str = match.group(1)
                        year_str = match.group(2)
                        
                        temp_date = pd.to_datetime(f'{year_str}-{month_str}', format='%Y-%B')
                        temp_date = temp_date - pd.DateOffset(months=1)
                        
                        processed_year = temp_date.year
                        processed_month = temp_date.strftime('%B')

                        relevant_date = pd.to_datetime(f'{processed_year}-{processed_month}', format='%Y-%B') + pd.offsets.MonthEnd(0)
                        relevant_date = relevant_date.date()
                        
                        result = { 'url': link, 'file_name': heading, 'month': processed_month,
                                  'year': str(processed_year), 
                                  'relevant_date': relevant_date.strftime('%Y-%m-%d')}
                        results.append(result)
                        
                        print(f"✓ Added: {heading} (Processed: {processed_month} {processed_year})")
                    else:
                        print(f"Could not extract month/year from: {heading}")
            
            print(f"\n=== Bandhan AMC Results: {len(results)} records ===")
            return results
            
        except Exception as e:
            print(f"Error in Bandhan AMC crawling: {e}")
            
    async def crawl_icici_amc(self, page: Page, site: SiteConfig) -> List[Dict[str, Any]]:
        """Crawl ICICI Prudential AMC - filters by document type, months, and financial years"""
        results = []
        
        try:
            await page.goto(site.url)
            await page.reload()
            await page.wait_for_timeout(10000)
            
            print("Looking for document type buttons...")

            document_buttons = await page.query_selector_all(".forms-types .MuiChip-label span")
            
            doc_types_to_process = []
            for button in document_buttons:
                text = await button.inner_text()
                text = text.strip()
                if text in ["Passive Factsheet", "Complete Factsheet"]:
                    doc_types_to_process.append(text)
            
            print(f"Found document types: {doc_types_to_process}")
            
            for doc_type_idx, doc_type in enumerate(doc_types_to_process):
                print(f"\n{'='*50}")
                print(f"Processing Document Type {doc_type_idx + 1}/{len(doc_types_to_process)}: {doc_type}")
                print(f"{'='*50}\n")
                
                document_buttons = await page.query_selector_all(".forms-types .MuiChip-label span")
                for button in document_buttons:
                    text = await button.inner_text()
                    text = text.strip()
                    print(text)
                    if text == doc_type:
                        await button.click()
                        print(f"Clicked: {text}")
                        await page.wait_for_timeout(10000)
                        break
                
                month_button = await page.wait_for_selector("//button[@name='typesOfForms']/span[contains(text(),'Month')]", timeout=15000)
                await month_button.click()
                print("Clicked Month dropdown")
                
                await page.wait_for_timeout(5000)
                
                month_chips = await page.query_selector_all("div.forms-types span.MuiTypography-menuOpenLink1")
                print(f"Found {len(month_chips)} month chips")
                
                month_names = []
                for chip in month_chips:
                    month_name = await chip.inner_text()
                    month_name = month_name.strip()
                    if month_name:
                        month_names.append(month_name)

                # Filter months (you'll need to set current year context)
                _, filtered_months = self.filter_years_months([], month_names, site.amc_name)
                print(f"Filtered months: {filtered_months}")
                
                for chip in month_chips:
                    month_name = await chip.inner_text()
                    month_name = month_name.strip()
                    if month_name and month_name in filtered_months:
                        print(f"Clicking month: {month_name}")
                        await chip.click()
                        await page.wait_for_timeout(3000)
                
                fy_button = await page.wait_for_selector("//button[@name='typesOfForms']//span[text()='Financial Year']", timeout=15000)
                await fy_button.scroll_into_view_if_needed()
                await fy_button.click()
                print("Clicked Financial Year dropdown")

                await page.wait_for_timeout(5000)

                fy_chips = await page.query_selector_all("div.forms-types span.MuiTypography-menuOpenLink1")
                print(f"Found {len(fy_chips)} financial years")
                
                fy_names = []
                for chip in fy_chips:
                    year = (await chip.inner_text()).strip()
                    if year:
                        # Extract year from financial year format (e.g., "2023-24" -> "2023")
                        fy_year = year.split('-')[0] if '-' in year else year
                        fy_names.append(fy_year)

                # Filter years
                filtered_years, _ = self.filter_years_months(fy_names, [], site.amc_name)
                filtered_fy_names = [fy for fy, year in zip([chip.inner_text() for chip in fy_chips], fy_names) if year in filtered_years]

                fy_index = 0
                while True:
                    fy_chips = await page.query_selector_all("div.forms-types span.MuiTypography-menuOpenLink1")
                    if fy_index >= len(fy_chips):
                        break

                    y_chip = fy_chips[fy_index]
                    year = (await y_chip.inner_text()).strip()

                    if year and year in filtered_fy_names:
                        print(f"\n=== Processing Financial Year: {year} ===")   
                        await y_chip.click()
                        await page.wait_for_timeout(5000)

                        apply_button = await page.wait_for_selector("//button[@name='apply']", timeout=15000)
                        await apply_button.click()
                        print("Clicked APPLY button")

                        await page.wait_for_timeout(20000)

                        try:
                            await page.wait_for_selector("div.learning-card", timeout=15000)
                            document_cards = await page.query_selector_all("div.learning-card")
                            print(f"Found {len(document_cards)} cards")
                        except Exception:
                            print("No learning cards found for this page/year")
                            document_cards = []

                        if document_cards:
                            card_count = len(document_cards)
                            print(f"Found {card_count} document cards for {year}")

                            for j in range(card_count):
                                document_cards = await page.query_selector_all("div.learning-card")
                                card = document_cards[j]

                                try:
                                    title_elem = await card.query_selector("h1.learning-card__info__heading_full")
                                    title = await title_elem.inner_text() if title_elem else "No title"
                                    title = title.strip()

                                    print(f"Processing card {j+1}/{card_count}: {title}")

                                    await card.scroll_into_view_if_needed()
                                    await page.wait_for_timeout(500)
                                    
                                    download_button = await card.query_selector("button:has(span:text('Download'))")

                                    if not download_button:
                                        download_button = await card.evaluate_handle('''
                                            (card) => {
                                                const buttons = card.querySelectorAll('button');
                                                for (let btn of buttons) {
                                                    const span = btn.querySelector('span');
                                                    if (span && span.textContent.trim() === 'Download') {
                                                        return btn;
                                                    }
                                                }
                                                return null;
                                            }
                                        ''')

                                    if download_button:
                                        try:
                                            async with page.expect_popup(timeout=5000) as popup_info:
                                                await download_button.click()

                                            popup = await popup_info.value
                                            await popup.wait_for_load_state()
                                            await page.wait_for_timeout(2000)

                                            link = popup.url
                                            await popup.close()

                                        except Exception as popup_error:
                                            print(f"Popup handling failed: {popup_error}")
                                            try:
                                                link = await download_button.get_attribute('href')
                                            except:
                                                link = None

                                            if not link:
                                                parent_a = await card.query_selector("a")
                                                link = await parent_a.get_attribute('href') if parent_a else None
                                    else:
                                        link = None
                                        print("Download button not found")
                                        continue

                                    # Extract month and year from title
                                    import re
                                    match = re.search(r'([A-Za-z]+)\s+(\d{4})', title)

                                    if match:
                                        month_str = match.group(1).title()
                                        year_str = match.group(2)
                                        
                                        relevant_date = pd.to_datetime(f'{month_str}-{year_str}') + pd.offsets.MonthEnd(0)
                                        relevant_date = relevant_date.date()
                                        
                                        if not link:
                                            raise Exception("Link Not Available")

                                        result = {'url': link, 'file_name': title,'month': month_str,
                                                  'year': year_str,
                                                  'relevant_date': relevant_date.strftime('%Y-%m-%d')
                                        }
                                        results.append(result)

                                        print(f"✓ Added: {title}")
                                    else:
                                        print(f"Could not extract month/year from: {title}")

                                except Exception as e:
                                    print(f"Error processing card: {e}")
                        else:
                            print("No Document Card for ", year)

                        print(f"Total documents processed: {len(results)}")

                        # Reopen Financial Year dropdown
                        fy_button = await page.wait_for_selector("//button[@name='typesOfForms']//span[text()='Financial Year']", timeout=15000)
                        await fy_button.scroll_into_view_if_needed()
                        await page.wait_for_timeout(5000)
                        await fy_button.click()
                        print("Reopened Financial Year dropdown")

                        # Click Clear All to reset for next iteration
                        try:
                            clear_chip = await page.wait_for_selector("//span[text()='Clear All']", timeout=5000)
                            await clear_chip.click()
                            print("Clicked Clear All")
                            await page.wait_for_timeout(5000)
                        except Exception as e:
                            print(f"Could not click Clear All: {e}")

                    fy_index += 1

                print(f"\n=== ICICI AMC Results: {len(results)} records ===")
                return results
            
        except Exception as e:
            print(f"Error in ICICI AMC crawling")
            error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
            print(error_msg)

    async def crawl_whiteoak_amc(self, page: Page, site: SiteConfig) -> List[Dict[str, Any]]:
        """Crawl WhiteOak Capital AMC - pagination-based factsheet extraction"""
        pdf_links = []  
        
        try:
            await page.goto(site.url)
            await page.wait_for_timeout(5000)
            await page.reload()
            await page.wait_for_timeout(5000)
            
            print("Looking for pagination...")
            
            await page.wait_for_selector("ul.pagination", timeout=15000)
            pagination = await page.query_selector("ul.pagination")
            
            page_links = await pagination.query_selector_all("li:not(.disabled):not(:has(a[rel])) a")
            
            total_pages = len(page_links)
            print(f"Found {total_pages} pages to process")
            
            for i in range(total_pages):
                
                await page.wait_for_selector("ul.pagination", timeout=15000)
                pagination = await page.query_selector("ul.pagination")
                page_links = await pagination.query_selector_all("li:not(.disabled):not(:has(a[rel])) a")
                
                await page.wait_for_timeout(5000)
                
                await pagination.scroll_into_view_if_needed()
                await page.wait_for_timeout(10000)
               
                page_text = await page_links[i].inner_text()
                print(f"\n=== Going to page: {page_text} ===")
                await page_links[i].click()
                
                await page.wait_for_selector("div.paginationWrapper", timeout=15000)
                await page.wait_for_timeout(10000)
                
                await page.wait_for_selector("ul.DownloadContainer_downloadList__MOPXJ li", timeout=15000)
                download_list = await page.query_selector_all("ul.DownloadContainer_downloadList__MOPXJ li")
                
                list_count = len(download_list)
                print(f"Found {list_count} items on page {page_text}")
                
                for j in range(1, list_count):
                    download_list = await page.query_selector_all("ul.DownloadContainer_downloadList__MOPXJ li")
                    li = download_list[j]
                    
                    try:
                        text_div = await li.query_selector("div")
                        text = await text_div.inner_text() if text_div else "No text"
                        text = text.strip()
                        print(f"Text: {text}")
                        
                        view_button = await li.query_selector("div.DownloadContainer_viewButton__rQyxT")
                        
                        if view_button:
                            await page.wait_for_timeout(1000)
                            await view_button.scroll_into_view_if_needed()
                            await page.wait_for_timeout(5000)
                            
                            try:
                                async with page.expect_popup(timeout=20000) as popup_info:
                                    await view_button.click()
                                
                                await page.wait_for_timeout(10000)
                                
                                popup = await popup_info.value
                                await popup.wait_for_load_state()
                                await page.wait_for_timeout(3000)
                                
                                link = popup.url
                                
                                await popup.close()
                                
                                print(f"Download URL: {link}")
                                
                                pdf_links.append({"File_Name": text, "File_Link": link})
                                
                                await page.wait_for_timeout(3000)
                                
                            except Exception as popup_error:
                                print(f"Link not available for: {text}")
                        else:
                            print(f"View button not found for: {text}")
                        
                        print("---------------")
                        
                    except Exception as e:
                        print(f"Error processing item: {e}")
                        continue
            
            df = pd.DataFrame(pdf_links)
            
            print(f"\n=== Length of collected DataFrame: {len(df)} ===")
            
            if df.empty:
                print("No data collected")
                return []
            
            df[["Month", "Year"]] = df["File_Name"].str.extract(
                r'(?:as at |-\s*)(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|January|February|March|April|May|June|July|August|September|October|November|December)[\s-]*\d{0,2}[,]?\s*(\d{2,4})'
            )
            
            df['Year'] = df['Year'].astype(str).apply(lambda y: '20' + y if len(y) == 2 else y)
            
            df['Relevant_Date'] = pd.to_datetime(df['Year'] + '-' + df['Month'], format='mixed', 
                                                 errors='coerce') + pd.offsets.MonthEnd(0)
            
            df['Relevant_Date'] = df['Relevant_Date'].dt.date
            
            print(f"Before filtering: {len(df)} records")
            
            try:
                conn = adqvest_db.db_conn()
                query = "SELECT MAX(Relevant_date) as last_date FROM AMC_WISE_MF_FACTSHEETS_MONTHLY_DATA_CORPUS WHERE AMC_Name = %s"
                result = pd.read_sql(query, conn, params=[site.amc_name])
                
                if not result.empty and not pd.isna(result['last_date'].iloc[0]):
                    last_date = pd.to_datetime(result['last_date'].iloc[0])
                    df = df[df['Relevant_Date'] > last_date]
                    print(f"Filtered to records after {last_date.date()}: {len(df)} records")
                    
            except Exception as e:
                print(f"Error filtering: {e}")
                
            print(f"After filtering: {len(df)} records")   
            
            results = []
            for _, row in df.iterrows():
                result = {
                    'url': row['File_Link'], 'file_name': row['File_Name'],
                    'month': row['Month'] if pd.notna(row['Month']) else '',
                    'year': row['Year'] if pd.notna(row['Year']) else '',
                    'relevant_date': row['Relevant_Date'].strftime('%Y-%m-%d') if pd.notna(row['Relevant_Date']) else ''
                }
                results.append(result)
            
            print(f"\n=== WhiteOak Capital AMC Results: {len(results)} records ===")
            return results
            
        except Exception as e:
            print(f"Error in WhiteOak Capital AMC crawling: {e}")

    async def crawl_tata_amc(self, page: Page, site: SiteConfig) -> List[Dict[str, Any]]:
        """Crawl Tata AMC - year/month dropdown with Submit button workflow"""
        downloads = [] 
        
        try:
            await page.goto(site.url)
            await page.wait_for_timeout(5000)
            await page.reload()
            await page.wait_for_timeout(10000)
            
            print("Checking for Continue button...")
            
            try:
                continue_button = await page.wait_for_selector("//button[contains(., 'Continue')]", timeout=5000)
                await continue_button.click()
                print("Clicked on 'Continue' button")
                await page.wait_for_timeout(5000)
            except:
                print("'Continue' button not found or not clickable")
            
            # Year dropdown XPath - finds div with role=button containing a 4-digit span
            dropdown_xpath = (
                "//div[@role='button' and .//span["
                "string-length(normalize-space(.))=4 and "
                "translate(normalize-space(.), '0123456789', '') = ''"
                "]]"
            )
            
            print("Getting years from dropdown...")
           
            year_dropdown = await page.wait_for_selector(dropdown_xpath, timeout=7000)
            await year_dropdown.click()
           
            year_buttons = await page.query_selector_all("//ul[contains(@class,'max-h-48')]//button")
            years = []
            
            for btn in year_buttons:
                text = await btn.inner_text()
                text = text.strip()
                if text.isdigit():
                    years.append(text)
            
            years, _ = self.filter_years_months(years, [], site.amc_name)
            print(f"Years: {years}")
            
            await year_dropdown.click()
            await page.wait_for_timeout(2000)
           
            for year in years:
                print(f"\n=== Processing Year: {year} ===")
                
                year_dropdown = await page.wait_for_selector(dropdown_xpath, timeout=10000)
                await year_dropdown.click()
                await page.wait_for_timeout(5000)
                
                year_buttons = await page.query_selector_all("//ul[contains(@class,'max-h-48')]//button")
                
                for btn in year_buttons:
                    text = await btn.inner_text()
                    if text.strip() == year:
                        await btn.click()
                        print(f"Clicked on year: {year}")
                        break
                
                await page.wait_for_timeout(10000)
                
                print("Finding month dropdown...")
                await page.wait_for_timeout(3000) 

                dropdowns = await page.query_selector_all("div[role='button']")
                print(f"Found {len(dropdowns)} dropdowns with role='button'")

                month_dropdown = None

                for idx, dd in enumerate(dropdowns):
                    try:
                        text_content = await dd.inner_text()
                        text_content = text_content.strip()
                        
                        print(f"Dropdown {idx} text: '{text_content}'")
                        
                        # Check if it contains month name (text without digits, not empty)
                        if text_content and not any(ch.isdigit() for ch in text_content):
                            # Additional check: month names are typically single words
                            # and this will exclude things like "Submit" button
                            month_names = ['january', 'february', 'march', 'april', 'may', 'june', 'july',
                                           'august', 'september', 'october', 'november', 'december']
                            if any(month in text_content.lower() for month in month_names):
                                month_dropdown = dd
                                print(f"✓ Found month dropdown with text: '{text_content}'")
                                break
                    except Exception as e:
                        print(f"Error checking dropdown {idx}: {e}")
                        continue

                if not month_dropdown:
                    print(f"❌ Month dropdown not found for year {year}")
                    continue
            
                await month_dropdown.click()
                await page.wait_for_timeout(5000)
                
                month_buttons = await page.query_selector_all("//ul[contains(@class,'max-h-48')]//button")
                months = []
                
                for btn in month_buttons:
                    text = await btn.inner_text()
                    text = text.strip()
                    if text:
                        months.append(text)
                
                _, months = self.filter_years_months([], months, site.amc_name, current_year=year)
                print(f"Filtered months for {year}: {months}")
                
                await month_dropdown.click()
                await page.wait_for_timeout(2000)
                
                for month in months:
                    print(f"\n--- Processing Month: {month} for Year: {year} ---")
                    
                    dropdowns = await page.query_selector_all("div[role='button']")
                    print(f"Found {len(dropdowns)} dropdowns with role='button'")

                    month_dropdown = None

                    for idx, dd in enumerate(dropdowns):
                        try:
                            text_content = await dd.inner_text()
                            text_content = text_content.strip()
                            
                            print(f"Dropdown {idx} text: '{text_content}'")
                            
                            # Check if it contains month name (text without digits, not empty)
                            if text_content and not any(ch.isdigit() for ch in text_content):
                                # Additional check: month names are typically single words
                                # and this will exclude things like "Submit" button
                                month_names = ['january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october', 'november', 'december']
                                if any(month in text_content.lower() for month in month_names):
                                    month_dropdown = dd
                                    print(f"✓ Found month dropdown with text: '{text_content}'")
                                    break
                                
                        except Exception as e:
                            print(f"Error checking dropdown {idx}: {e}")
                            continue
                        
                    await month_dropdown.click()
                    await page.wait_for_timeout(5000)
                    
                    month_buttons = await page.query_selector_all("//ul[contains(@class,'max-h-48')]//button")
                    
                    for btn in month_buttons:
                        text = await btn.inner_text()
                        if text.strip() == month:
                            await btn.click()
                            print(f"Clicked on month: {month}")
                            break
                    
                    await page.wait_for_timeout(10000)
                    
                    submit_button = await page.query_selector("//button[normalize-space(text())='Submit']")
                    await submit_button.click()
                    print("Clicked Submit button")
                    await page.wait_for_timeout(5000)
                    
                    no_data = await page.query_selector_all("//div[text()='No data found']")
                    if len(no_data) > 0:
                        print(f"No data found, skipping month {month} {year}")
                        continue
                    
                    blocks = await page.query_selector_all("//div[contains(@class,'rounded-2xl border')]")
                    block_count = len(blocks)
                    print(f"Found {block_count} blocks for {month} {year}")
                    
                    for block in blocks:
                        try:
                            link_element = await block.query_selector("a[href]") 
                            if link_element:
                                url = await link_element.get_attribute("href")
                                
                                title_element = await link_element.query_selector("span")
                                title = await title_element.inner_text()
                                title = title.strip()
                                
                                downloads.append({"Year": year, "Month": month, "File_Name": title,
                                                  "File_Link": url})
                                
                                print(f"✓ Added: {title}")
                        except Exception as e:
                            print(f"Error processing block: {e}")
                            continue
                    
                    await page.wait_for_timeout(2000)
            
            print(f"\n=== Total downloads collected: {len(downloads)} ===")
            
            df = pd.DataFrame(downloads)
            
            if df.empty:
                print("No data collected")
                return []
            
            df['Relevant_Date'] = (
                pd.to_datetime(df['Month'].str.strip().str.title() + '-' + df['Year'].astype(str)) + 
                pd.offsets.MonthEnd(0))
            df['Relevant_Date'] = df['Relevant_Date'].dt.date
            
            results = []
            for _, row in df.iterrows():
                result = {
                    'url': row['File_Link'],
                    'file_name': row['File_Name'],
                    'month': row['Month'],
                    'year': row['Year'],
                    'relevant_date': row['Relevant_Date'].strftime('%Y-%m-%d') if pd.notna(row['Relevant_Date']) else ''
                }
                results.append(result)
            
            print(f"\n=== Tata AMC Results: {len(results)} records ===")
            return results
            
        except Exception as e:
            print(f"Error in Tata AMC crawling: {e}")
    
    async def crawl_uti_amc(self, site: SiteConfig) -> List[Dict[str, Any]]:
        """Crawl UTI AMC - complex scrollable virtual viewports for year/month"""

        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.chrome.options import Options
        import time

        options = Options()
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--ignore-ssl-errors')
        options.add_argument('--allow-insecure-localhost')
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-animations')
        options.add_argument('--blink-settings=imagesEnabled=false')
        options.page_load_strategy = 'eager'

        driver = webdriver.Chrome(options=options)
        driver.get(site.url)
        driver.maximize_window()
        driver.refresh()
        time.sleep(5)

        results = []
        
        try:
            dropdown = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[placeholder='Select Year']")))
            dropdown.click()
            time.sleep(1)

            scroll_container = driver.find_element(By.CSS_SELECTOR, "cdk-virtual-scroll-viewport")

            years = []
            last_height = -1

            while True:
                years_elements = driver.find_elements(By.CSS_SELECTOR, "div.option-select.cp.ng-star-inserted")
                for el in years_elements:
                    txt = el.text.strip()
                    if txt:
                        years.append(txt)

                driver.execute_script("arguments[0].scrollTop += 200;", scroll_container)
                time.sleep(1)
                
                new_height = driver.execute_script("return arguments[0].scrollTop;", scroll_container)
                if new_height == last_height:
                    break
                last_height = new_height

            years = sorted(set(years), reverse=True)
            years, _ = self.filter_years_months(years, [], site.amc_name)
            print("Years:", years)
            
            dropdown = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[placeholder='Select Year']")))
            dropdown.click()
            time.sleep(1)
            
            for year in years:
                print(f"\n=== Processing Year: {year} ===")
            
                dropdown = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[placeholder='Select Year']")))
                dropdown.click()
                time.sleep(1)
            
                scroll_container = driver.find_element(By.CSS_SELECTOR, "cdk-virtual-scroll-viewport")
            
                while True:
                    try:
                        year_element = driver.find_element(By.XPATH, f"//div[@class='option-select cp ng-star-inserted' and text()='{year}']")
                        year_element.click()
                        break
                    except:
                        driver.execute_script("arguments[0].scrollTop += 200;", scroll_container)
                        time.sleep(0.5)
            
                time.sleep(5)
            
                month_dropdown = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[placeholder='select ']")))
                month_dropdown.click()
                time.sleep(1)
                
                months = []
                
                if year == "2025":
                    dropdown_container = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.dropdown-pos.ng-star-inserted")))
                    
                    driver.execute_script("arguments[0].scrollTop = 0;", dropdown_container)
                    time.sleep(0.5)
                    
                    last_height = -1
                    
                    while True:
                        month_elements = dropdown_container.find_elements(By.CSS_SELECTOR, "div.option-select.cp.ng-star-inserted")
                        for el in month_elements:
                            txt = el.text.strip()
                            if txt:
                                months.append(txt)
                                
                        driver.execute_script("arguments[0].scrollTop += 100;", dropdown_container)
                        time.sleep(0.5)
                    
                        new_height = driver.execute_script("return arguments[0].scrollTop;", dropdown_container)
                        if new_height == last_height:
                            break
                        last_height = new_height
                    
                    months = list(set(months))
                    print("Months found for 2025:", months)
                
                else:
                    print("Using scrollable month dropdown for older years")
                    scroll_containers = driver.find_elements(By.CSS_SELECTOR, "cdk-virtual-scroll-viewport")
                    for container in scroll_containers:
                        month_elements = container.find_elements(By.CSS_SELECTOR, "div.option-select.cp.ng-star-inserted")
                        if month_elements:
                            scroll_container = container
                            break
                
                    last_height = -1
                    while True:
                        month_elements = scroll_container.find_elements(By.CSS_SELECTOR, "div.option-select.cp.ng-star-inserted")
                        for el in month_elements:
                            txt = el.text.strip()
                            if txt:
                                months.append(txt)
                
                        driver.execute_script("arguments[0].scrollTop += 50;", scroll_container)
                        time.sleep(0.5)
                
                        new_height = driver.execute_script("return arguments[0].scrollTop;", scroll_container)
                        if new_height == last_height:
                            break
                        last_height = new_height
                
                months = sorted(set(months))
                _, months = self.filter_years_months([], months, site.amc_name, current_year=year)
                print("Months:", months)

                month_dropdown = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[placeholder='select ']")))
                month_dropdown.click()
                time.sleep(1)

                for month in months: 
                    print(f"\n--- Processing Year: {year}, Month: {month} ---")
                    
                    month_dropdown = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[placeholder='select ']")))
                    month_dropdown.click()
                    time.sleep(1)
                    
                    scroll_containers = driver.find_elements(By.CSS_SELECTOR, "cdk-virtual-scroll-viewport")
                    for container in scroll_containers:
                        month_elements = container.find_elements(By.CSS_SELECTOR, "div.option-select.cp.ng-star-inserted")
                        if month_elements:
                            scroll_container = container
                            break
                    
                    while True:
                        try:
                            month_element = driver.find_element(By.XPATH, f"//div[@class='option-select cp ng-star-inserted' and text()='{month}']")
                            month_element.click()
                            break
                        except:
                            driver.execute_script("arguments[0].scrollTop += 200;", scroll_container)
                            time.sleep(1)
                            
                    time.sleep(5)
                    
                    driver.find_element(By.XPATH, "//button[normalize-space(text())='Get Fact Sheet']").click()
                    time.sleep(5)
                    
                    download_cards = driver.find_elements(By.CSS_SELECTOR, ".file-container.d-flex.ng-star-inserted")
                    print(f"Found {len(download_cards)} download cards for Year={year}, Month={month}")
                    
                    try:
                        close_btn = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "div.notification-message-hide-div")))
                        close_btn.click()
                        print("✅ Notification popup closed.")
                        time.sleep(5)
                    except:
                        print("⚠️ No notification popup found or not clickable")
                        pass

                    cards = driver.find_elements(By.CSS_SELECTOR, ".file-container.d-flex.ng-star-inserted")
                    self.logger.info(f"Found {len(cards)} cards for {month}-{year}")

                    for card in cards:
                        text = card.text.strip()
                        original_handles = driver.window_handles
                        card.click()
                        time.sleep(10)

                        timeout = 20
                        start_time = time.time()
                        new_tab_opened = False
                        
                        while time.time() - start_time < timeout:
                            if len(driver.window_handles) > len(original_handles):
                                new_tab_opened = True
                                break
                            time.sleep(0.5)

                        if new_tab_opened:
                            driver.switch_to.window(driver.window_handles[-1])
                            link = driver.current_url
                            time.sleep(3)
                            
                            driver.close()
                            time.sleep(1)
                            driver.switch_to.window(original_handles[0])
                            time.sleep(1)
                            
                            print(f"Download URL: {link}")

                            results.append({"Year": year, "Month": month, "File_Name": text, "File_Link": link,
                                "Runtime": datetime.now().isoformat()})
                        else:
                            print(f"No new tab for {text}")

            driver.quit()
            
            df = pd.DataFrame(results)
            
            if df.empty:
                print("No data collected")
                return []
            
            df['Relevant_Date'] = (pd.to_datetime(df['Year'].astype(str) + '-' + df['Month'], format='%Y-%B') + pd.offsets.MonthEnd(0))
            df['Relevant_Date'] = df['Relevant_Date'].dt.date
            
            results = []
            for _, row in df.iterrows():
                result = {
                    'File_Link': row['File_Link'],
                    'File_Name': row['File_Name'],
                    'Month': row['Month'],
                    'Year': row['Year'],
                    'Relevant_Date': row['Relevant_Date'].strftime('%Y-%m-%d') if pd.notna(row['Relevant_Date']) else ''
                }
                results.append(result)
            
            print(f"\n=== UTI AMC Results: {len(results)} records ===")
            return results
        
        except Exception as e:
            print(f"Error in UTI AMC crawling: {e}")
        
    async def crawl_sundaram_amc(self, site: SiteConfig) -> List[Dict[str, Any]]:
        """Crawl Sundaram AMC - complex scrollable virtual viewports for year/month"""

        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.chrome.options import Options
        import time

        options = Options()
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--ignore-ssl-errors')
        options.add_argument('--allow-insecure-localhost')
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-animations')
        options.add_argument('--blink-settings=imagesEnabled=false')
        options.page_load_strategy = 'eager'
        
        driver = webdriver.Chrome(options=options)
        driver.get(site.url)
        driver.maximize_window()
        driver.refresh()
        time.sleep(5)

        results = []
        
        try:
            element = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), 'Consolidated Factsheet')]")))
            element.click()
            time.sleep(5)
            
            wait = WebDriverWait(driver, 10)
            date_input = wait.until(EC.element_to_be_clickable((By.ID, "dt_InfoDoc")))
            date_input.click()
            
            current_year = datetime.now().year
            
            header_elem = driver.find_element(By.XPATH, f"//th[contains(@class,'datepicker-switch') and contains(text(), '{current_year}')]")
            driver.execute_script("arguments[0].click();", header_elem)
            print(f"Clicked year header: {current_year}")

            all_years = set()
            lowest_year= '2018'
            while True:
                year_elements = driver.find_elements(By.CSS_SELECTOR, ".datepicker-years span.year")
                visible_years = [int(y.text) for y in year_elements if y.text.strip().isdigit()]
                all_years.update(visible_years)
                
                if min(all_years) <= lowest_year:
                    break
            
                prev_btn = driver.find_element(By.CSS_SELECTOR, ".datepicker-years th.prev")
                driver.execute_script("arguments[0].click();", prev_btn)
                time.sleep(0.4)
            
            # years = sorted([y for y in all_years if lowest_year <= y <= current_year], reverse=True)
            years, _ = self.filter_years_months(all_years, [], site.amc_name)
            print(sorted(years, reverse=True))
            
            element = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), 'Consolidated Factsheet')]")))
            element.click()
            time.sleep(5)
            
            wait = WebDriverWait(driver, 10)    
            for year in years:
                print(f"Clicking year: {year}")
                target_year = year
                
                wait = WebDriverWait(driver, 10)
                date_input = wait.until(EC.element_to_be_clickable((By.ID, "dt_InfoDoc")))
                date_input.click()
                
                while True:
                    # get the currently visible year from datepicker switch
                    switch_text = driver.find_element(By.CSS_SELECTOR, ".datepicker-months .datepicker-switch").text
                    current_year = int(switch_text.strip())
                    
                    if current_year == target_year:
                        print(f"✅ Reached target year: {target_year}")
                        time.sleep(10)
                        break
                    elif current_year > target_year:
                        # click previous
                        prev_btn = driver.find_element(By.CSS_SELECTOR, ".datepicker-months th.prev")
                        driver.execute_script("arguments[0].click();", prev_btn)
                        time.sleep(3)
                    elif current_year < target_year:
                        # click previous
                        next_btn = driver.find_element(By.CSS_SELECTOR, ".datepicker-months th.next")
                        driver.execute_script("arguments[0].click();", next_btn)
                        time.sleep(3)    
                    else:
                        print(f"⚠️ Current year {current_year} is less than target {target_year}")
                        break
            
                months = wait.until(EC.presence_of_all_elements_located((By.XPATH, "//span[contains(@class,'month') and not(contains(@class,'disabled'))]")))
                
                month_texts = [m.text for m in months]
                _, filtered_month_texts = self.filter_years_months([], month_texts, site.amc_name, current_year=year)
                months_to_process = [m for m in months if m.text in filtered_month_texts]
                print(f"Filtered months for {year}: {filtered_month_texts}")
                
                for month_index in range(len(months_to_process)):            
                    months = driver.find_elements(By.XPATH, "//span[contains(@class,'month') and not(contains(@class,'disabled'))]")
                    month = months[month_index]
                    month_name = month.text
                    original_handles = driver.window_handles
                    print(f"    Selecting month: {month_name}")
                
                    driver.execute_script("arguments[0].click();", month)
                    time.sleep(10) 
                
                    try:
                        download_btn = wait.until(EC.element_to_be_clickable((By.ID, "btn_download")))
                        download_btn.click()
                        print(f"      Clicked 'View' button for {month_name} {target_year}")
                
                    except Exception as e:
                        print(f"      ⚠️ Could not click 'View' button: {e}")
                        
                    timeout = 20
                    start_time = time.time()
                    new_tab_opened = False
                    
                    while time.time() - start_time < timeout:
                        if len(driver.window_handles) > len(original_handles):
                            new_tab_opened = True
                            break
                        time.sleep(0.5)
                
                    if new_tab_opened:
                        driver.switch_to.window(driver.window_handles[-1])
                        link = driver.current_url
                        time.sleep(3)
                        
                        driver.close()
                        driver.switch_to.window(original_handles[0])

                        print(f"Download URL: {link}")
                        results.append({"Year": year,"Month":month_name, "File_Link": link})
                        time.sleep(3)
                    else:
                        print("Link not available")
                        
                    wait = WebDriverWait(driver, 10)
                    date_input = wait.until(EC.element_to_be_clickable((By.ID, "dt_InfoDoc")))
                    date_input.click()
            
            print(f"\n=== Total Links collected: {len(results)} ===")
            
            # Create DataFrame from collected data
            df = pd.DataFrame(results)
            
            if df.empty:
                print("No data collected")
                return []
            
            # Process dates - get end of month (format='%Y-%b' for abbreviated month like 'Jan')
            df['Relevant_Date'] = (
                pd.to_datetime(df['Year'].astype(str) + '-' + df['Month'], format='%Y-%b') + 
                pd.offsets.MonthEnd(0)
            )
            df['Relevant_Date'] = df['Relevant_Date'].dt.date
            
            # Convert DataFrame to results list
            results = []
            for _, row in df.iterrows():
                result = {
                    'File_Link': row['File_Link'],
                    'File_Name': f"Consolidated_Factsheet_{row['Month']}_{row['Year']}",
                    'Month': row['Month'],
                    'Year': str(row['Year']),
                    'Relevant_Date': row['Relevant_Date'].strftime('%Y-%m-%d') if pd.notna(row['Relevant_Date']) else ''
                }
                results.append(result)
            
            print(f"\n=== Sundaram AMC Results: {len(results)} records ===")
            driver.quit()
            return results
            
        except Exception as e:
            print(f"Error in Sundaram AMC crawling: {e}")
            return [] 

    async def crawl_kotak_amc(self, site: SiteConfig) -> List[Dict[str, Any]]:
        """Crawl Kotak AMC - complex scrollable virtual viewports for year/month"""

        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.support.ui import Select
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        import time
        import json
        
        try:
            USER_AGENT = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
            )

            chrome_options = Options()
            chrome_options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
            chrome_options.add_argument('--ignore-certificate-errors')
            chrome_options.add_argument('--ignore-ssl-errors')
            chrome_options.add_argument('--allow-insecure-localhost')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--disable-animations')
            chrome_options.add_argument('--blink-settings=imagesEnabled=false')
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-web-security')
            chrome_options.add_argument('--start-maximized')
            chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
            chrome_options.add_argument(f'user-agent={USER_AGENT}')
            chrome_options.page_load_strategy = 'eager'

            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option("useAutomationExtension", False)
            chrome_options.add_argument("--disable-extensions")
            prefs = {"download_restrictions": 3}
            chrome_options.add_experimental_option("prefs", prefs)

            driver = webdriver.Chrome(options=chrome_options)
            driver.execute_cdp_cmd(
                "Page.addScriptToEvaluateOnNewDocument",
                {
                    "source": """
                    Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                    });
                    Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4]
                    });
                    const originalPlatform = navigator.platform;
                    Object.defineProperty(navigator, 'platform', {
                    get: () => originalPlatform.replace('Linux', 'Win32')
                    });
                    """
                },
            )
            
            url = "https://www.kotakmf.com/Information/statutory-disclosure"
            driver.get(url)
            driver.maximize_window()
            time.sleep(5)

            try:
                close_btn = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, '//a[@id = "smt-close-icon"]'))
                )
                close_btn.click()
                time.sleep(1)
            except Exception:
                pass

            print("✅ Driver launched with custom User-Agent and stealth tweaks")
            
            pdf_links = []
            
            wait = WebDriverWait(driver, 10)
            year_dropdown = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "select.classnameforsetindexvaluefrmts")))

            select = Select(year_dropdown)
            years = [opt.text.strip() for opt in select.options if opt.text.strip() and "Please" not in opt.text]
            years, _ = self.filter_years_months(years, [], site.amc_name)
            print(years)

            try:
                close_btn = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, '//a[@id = "smt-close-icon"]')))
                close_btn.click()
                time.sleep(1)
            except Exception:
                pass

            for year in years:
                print(f"Selecting year: {year}")

                # re-locate the dropdown each time (important after DOM updates)
                year_dropdown = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "select.classnameforsetindexvaluefrmts")))
                select = Select(year_dropdown)

                # select by value
                select.select_by_value(year)
                time.sleep(5)  # wait for the page to refresh / data to load
                
                try:
                    close_btn = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, '//a[@id = "smt-close-icon"]')))
                    close_btn.click()
                    time.sleep(1)
                except Exception:
                    pass
                
                try:
                    wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "button.btn-months")))
                except Exception:
                    print(f"No months found for {year}")
                    continue

                month_buttons = driver.find_elements(By.CSS_SELECTOR, "button.btn-months")
                months = [btn.text.strip() for btn in month_buttons if btn.text.strip()]
                _, months = self.filter_years_months([], months, site.amc_name, current_year=year)
                print(f"Months for {year}: {months}")
                
                for month in months:
                    print(f"   → Clicking month: {month}")

                    month_buttons = driver.find_elements(By.CSS_SELECTOR, "button.btn-months")
                    for btn in month_buttons:
                        if btn.text.strip() == month:
                            driver.execute_script("arguments[0].click();", btn)
                            break

                    time.sleep(15) 
                    
                    try:
                        close_btn = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, '//a[@id = "smt-close-icon"]')))
                        close_btn.click()
                        time.sleep(1)
                    except Exception:
                        pass
                    
                    original_handles = driver.window_handles
                
                    wait = WebDriverWait(driver, 10)
                    download_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//a[contains(text(), "Download")]')))
                    driver.execute_script("arguments[0].click();", download_button)
                    time.sleep(3)
                    
                    new_handles = driver.window_handles
                    new_tab = None
                    for h in new_handles:
                        if h not in original_handles:
                            new_tab = h
                            break
                        
                    pdf_url = None
                    if new_tab:
                        driver.switch_to.window(new_tab)
                        pdf_url = driver.current_url
                        print(f"✅ Captured PDF URL: {pdf_url}")
                    
                        driver.close()
                        driver.switch_to.window(original_handles[0])
                        print("Done using new tab method")
                    else:
                        logs = driver.get_log("performance")
                        for entry in logs:
                            try:
                                msg = json.loads(entry["message"])["message"]
                                if msg["method"] == "Network.requestWillBeSent":
                                    url = msg["params"]["request"]["url"]
                                    if url.lower().endswith(".pdf"):
                                        pdf_url = url
                                        print("Done using log method")
                                        break
                            except Exception:
                                pass
                    if pdf_url:
                        print(f"✅ Captured PDF URL (from logs): {pdf_url}")
                    else:
                        print("⚠️ No PDF URL found.")  
                        
                    pdf_links.append({"Year": year,"Month":month, "File_Link": pdf_url})    
                    
                    try:
                        back_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//a[contains(@class, "backBottonFontSize") and contains(text(), "Back")]')))
                        driver.execute_script("arguments[0].click();", back_button)
                        print("      ↩ Returned to year/month view")
                    except Exception as e:
                        print(f"      ⚠ Could not click Back for {month}: {e}")
                        continue

                    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "select.classnameforsetindexvaluefrmts")))
                    time.sleep(1)
                    
                    year_dropdown = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "select.classnameforsetindexvaluefrmts")))
                    select = Select(year_dropdown)

                    select.select_by_value(year)
                    time.sleep(5) 
                    try:
                        close_btn = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, '//a[@id = "smt-close-icon"]')))
                        close_btn.click()
                        time.sleep(1)
                    except Exception:
                        pass
                    
            driver.quit() 
            
            print(f"\n=== Total Links collected: {len(pdf_links)} ===")
    
            # Create DataFrame from collected data
            df = pd.DataFrame(pdf_links)
            
            if df.empty:
                print("No data collected")
                return []
            
            # Process dates - get end of month (format='%Y-%b' for abbreviated month like 'Jan')
            df['Relevant_Date'] = (
                pd.to_datetime(df['Year'].astype(str) + '-' + df['Month'], format='%Y-%B') + 
                pd.offsets.MonthEnd(0)
            )
            df['Relevant_Date'] = df['Relevant_Date'].dt.date
            
            print(f"Before deduplication: {len(df)} records")
            df = df.drop_duplicates(subset=['File_Link'], keep='first')
            print(f"After deduplication: {len(df)} records")
            
            # Convert DataFrame to results list
            results = []
            for _, row in df.iterrows():
                result = {
                    'File_Link': row['File_Link'],
                    'File_Name': f"Consolidated_Factsheet_{row['Month']}_{row['Year']}",
                    'Month': row['Month'],
                    'Year': str(row['Year']),
                    'Relevant_Date': row['Relevant_Date'].strftime('%Y-%m-%d') if pd.notna(row['Relevant_Date']) else ''
                }
                results.append(result)
            
            print(f"\n=== Kotak AMC Results: {len(results)} records ===")
            driver.quit()
            return results
            
        except Exception as e:
            print(f"Error in Kotak AMC crawling: {e}")
            return []                     
            
    async def crawl_site(self, site: SiteConfig):
        """Crawl a single site for factsheets with comprehensive year/month iteration"""
        
        if site.site_id == "UTI AMC":
            print("🎯 UTI AMC detected - using Selenium, exiting Playwright handler early")
            all_collected_links = await self.crawl_uti_amc(site)
            self.logger.info(f"Successfully collected {len(all_collected_links)} total links from {site.amc_name}")
            runtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print('Before LINK DATA FOR LOOP (UTI direct processing)')
            
            for link_data in all_collected_links:
                result = FactsheetResult(
                    Company=site.company_name,
                    AMC_Name=site.amc_name,
                    File_name=link_data['File_Name'],  # UTI uses 'File_Name'
                    Relevant_date=link_data['Relevant_Date'],  # UTI uses 'Relevant_Date'
                    Month=link_data['Month'],
                    Year=link_data['Year'],
                    Runtime=runtime,
                    File_Link=link_data['File_Link']  # UTI uses 'File_Link'
                )
                self.results.append(result)
            
            print(f"✅ UTI AMC processed completely: {len(all_collected_links)} results added to self.results")
            return 
        
        if site.site_id == "Sundaram AMC":
            print("🎯 Sundaram AMC detected - using Selenium, exiting Playwright handler early")
            all_collected_links = await self.crawl_sundaram_amc(site)
            self.logger.info(f"Successfully collected {len(all_collected_links)} total links from {site.amc_name}")
            runtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print('Before LINK DATA FOR LOOP (Sundaram direct processing)')
            
            for link_data in all_collected_links:
                result = FactsheetResult(
                    Company=site.company_name,
                    AMC_Name=site.amc_name,
                    File_name=link_data['File_Name'],  # Sundaram uses 'File_Name'
                    Relevant_date=link_data['Relevant_Date'],  # Sundaram uses 'Relevant_Date'
                    Month=link_data['Month'],
                    Year=link_data['Year'],
                    Runtime=runtime,
                    File_Link=link_data['File_Link']  # Sundaram uses 'File_Link'
                )
                self.results.append(result)
            
            print(f"✅ Sundaram AMC processed completely: {len(all_collected_links)} results added to self.results")
            print("🔍 DEBUG: About to return from request handler")
            return 
        
        if site.site_id == "Kotak Mahindra AMC":
            print("🎯 Kotak Mahindra AMC detected - using Selenium, exiting Playwright handler early")
            all_collected_links = await self.crawl_kotak_amc(site)
            self.logger.info(f"Successfully collected {len(all_collected_links)} total links from {site.amc_name}")
            runtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print('Before LINK DATA FOR LOOP (Kotak direct processing)')
            
            for link_data in all_collected_links:
                result = FactsheetResult(
                    Company=site.company_name,
                    AMC_Name=site.amc_name,
                    File_name=link_data['File_Name'],  
                    Relevant_date=link_data['Relevant_Date'],  
                    Month=link_data['Month'],
                    Year=link_data['Year'],
                    Runtime=runtime,
                    File_Link=link_data['File_Link'] 
                )
                self.results.append(result)
            
            print(f"✅ Kotak AMC processed completely: {len(all_collected_links)} results added to self.results")
            print("🔍 DEBUG: About to return from request handler")
            return    
           
        print(f"🎯 Starting Playwright crawler for {site.site_id}")
        # crawler = PlaywrightCrawler(
        #     max_requests_per_crawl=20,
        #     headless=False,
        #     browser_type='chromium',
        #     request_handler_timeout=timedelta(seconds=3600),
        #     # Browser launch arguments for maximized window
        #     browser_launch_options={
        #         'args': [
        #             '--start-maximized',
        #             '--disable-web-security',
        #             '--disable-features=VizDisplayCompositor',
        #             '--window-size=1920,1080',
        #             '--window-position=0,0'
        #         ]
        #     }
        # )
        crawler = PlaywrightCrawler(
            max_requests_per_crawl=20,
            headless=False,
            browser_type='chromium',
            request_handler_timeout=timedelta(seconds=3600),
            
            # Your existing browser launch options with timeout added
            browser_launch_options={
                'timeout': 60000,  # Add this - 60 second timeout for browser launch
                'args': [
                    '--start-maximized',
                    '--disable-web-security',
                    '--disable-features=VizDisplayCompositor',
                    '--window-size=1920,1080',
                    '--window-position=0,0',
                    # Add these stability args to reduce initialization issues
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-extensions',
                    '--no-first-run',
                ]
            }
        )

        all_collected_links = []
        
        ###mahindramanu 
        @crawler.pre_navigation_hook
        async def _tune_navigation(ctx):
            # Use BrowserContext if page is not ready yet
            if getattr(ctx, "context", None):
                ctx.context.set_default_navigation_timeout(90000)  # no await [2]
                ctx.context.set_default_timeout(90000)             # no await [2]
            # If a Page is already present in this version, set on it too (no await)
            if getattr(ctx, "page", None):
                ctx.page.set_default_navigation_timeout(90000)     # no await [3]
                ctx.page.set_default_timeout(90000)   
        ##mm

        @crawler.router.default_handler
        async def request_handler(context: PlaywrightCrawlingContext) -> None:
            nonlocal all_collected_links
            page = context.page
            
            try:
                self.logger.info(f"Loading page: {context.request.url}")
                
                await page.wait_for_load_state('domcontentloaded')
                await asyncio.sleep(2)

                await self.auto_handle_popups(page)

                try:
                    await page.wait_for_load_state('networkidle', timeout=5000)
                except PlaywrightTimeoutError:
                    self.logger.info('Network not idle, continuing anyway...')
                await asyncio.sleep(site.options.cascade_wait_time)

                # Handle data tab selection FIRST
                if site.selectors.data_tab_selector:
                    try:
                        await self.handle_tab_selection(page, site)
                    except Exception as e:
                        self.logger.warning(f"Tab 1 selection failed: {e}, continuing...")
                        # Added second seelctor in try and except
                        try:
                            await self.handle_tab_selection_2(page, site)
                        except Exception as e:
                            self.logger.warning(f"Tab 2 selection failed: {e}, continuing...")    
                        
                if site.selectors.multi_tab_selector:
                    clicks = await self.handle_multi_click(page, site)
                    self.logger.info(f"Performed {clicks} load-more clicks")

                all_collected_links = await self.collect_links_for_site(page, site)
                self.logger.info(f"Successfully collected {len(all_collected_links)} total links from {site.amc_name}")

            except Exception as e:
                self.logger.error(f"Error in request handler for {site.site_id}: {e}")
                raise e

        await crawler.add_requests([Request.from_url(site.url)])
        await crawler.run()

        # Process all collected results
        runtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for link_data in all_collected_links:
            result = FactsheetResult(
                Company=site.company_name,
                AMC_Name=site.amc_name,
                File_name=link_data['file_name'],
                Relevant_date=link_data['relevant_date'],
                Month=link_data['month'],
                Year=link_data['year'],
                Runtime=runtime,
                File_Link=link_data['url']
            )
            self.results.append(result)
            
    @staticmethod
    def month_year_from_url(url: str):
        ('starting url function')
        """
        Returns (year:int, month:int, relevant_date:str 'YYYY-MM-DD') from URLs like:
        .../RMF%20Factsheet%20June%202014_.pdf
        .../RMF_Factsheet_Jan_2019.pdf
        .../RMF_Factsheet_December_24.pdf
        Returns (None, None, None) if not found.
        """
        
        MONTH_MAP = {
        "jan": 1, "january": 1,
        "feb": 2, "february": 2,
        "mar": 3, "march": 3,
        "apr": 4, "april": 4,
        "may": 5,
        "jun": 6, "june": 6,
        "jul": 7, "july": 7,
        "aug": 8, "august": 8,
        "sep": 9, "sept": 9, "september": 9,
        "oct": 10, "october": 10,
        "nov": 11, "november": 11,
        "dec": 12, "december": 12,
        }

        # Month name (full or 3-letter, incl. 'Sept'), optional separators, then 2 or 4 digit year
        # MONTH_YEAR_RE = re.compile(
        #     r"""(?ix)
        #     \b
        #     (jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|
        #     jun(?:e)?|jul(?:y)?|aug(?:ust)?|
        #     sep(?:tember)?|sept|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)
        #     [\s_\-]*      # allow space/underscore/dash between month and year
        #     (\d{2}|\d{4}) # year
        #     \b
        #     """
        # )
        # MONTH_YEAR_RE = re.compile(
        #     r"""(?ix)
        #     \b
        #     (jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|
        #     jun(?:e)?|jul(?:y)?|aug(?:ust)?|
        #     sep(?:tember)?|sept|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)
        #     [\s_\-]*      # allow space/underscore/dash between month and year
        #     (\d{2}|\d{4}) # both 2-digit and 4-digit years
        #     \b
        #     """, re.VERBOSE | re.IGNORECASE
        # )
        if not url:
            return (None, None, None)

        # Decode URL-escaped spaces/underscores quickly for matching context
        hay = url.replace("%20", " ").replace("%2F", "/")
        print(f"DEBUG: Searching in URL: {hay}")
        
        patterns = [
            # Pattern 1: With word boundaries (for most cases)
            re.compile(
                r"""(?ix)
                \b
                (jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|
                jun(?:e)?|jul(?:y)?|aug(?:ust)?|
                sep(?:tember)?|sept|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)
                [\s_\-]*
                (\d{2}|\d{4})
                \b
                """, re.VERBOSE | re.IGNORECASE
            ),
            
            # Pattern 2: Without word boundaries (for underscore-heavy URLs)
            re.compile(
                r"""(?ix)
                (jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|
                jun(?:e)?|jul(?:y)?|aug(?:ust)?|
                sep(?:tember)?|sept|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)
                [\s_\-]+
                (\d{4})
                """, re.VERBOSE | re.IGNORECASE
            ),
            
            # Pattern 3: More flexible - just month followed by year anywhere
            re.compile(
                r"""(?i)
                (january|february|march|april|may|june|july|august|september|october|november|december|
                jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)
                [\s_\-]*
                (\d{4}|\d{2})
                """, re.VERBOSE | re.IGNORECASE
            )
        ]
        
        for i, pattern in enumerate(patterns):
            try:
                m = pattern.search(hay)
                if m:
                    print(f"DEBUG: Pattern {i+1} matched!")
                    month_token = m.group(1).lower().rstrip(".")
                    year_token = m.group(2)
                    
                    print(f"DEBUG: Found month_token='{month_token}', year_token='{year_token}'")

                    month_num = MONTH_MAP.get(month_token)
                    if not month_num:
                        print(f"DEBUG: Month '{month_token}' not found in MONTH_MAP")
                        continue  # Try next pattern

                    # Handle both 2-digit and 4-digit years properly
                    year = int(year_token)
                    if len(year_token) == 2:
                        year = 2000 + int(year_token)
                    else:
                        year = int(year_token)
                    
                    month_name = calendar.month_name[month_num]
                    
                    print(f"DEBUG: Final result: year={year}, month={month_name}")
                    return (year, month_name)
                    
            except Exception as e:
                print(f"DEBUG: Pattern {i+1} failed with error: {e}")
                continue

        print(f"DEBUG: No patterns matched the URL")
        return (None, None)
    
    async def comprehensive_year_month_crawl(self, page: Page, site: SiteConfig) -> List[Dict[str, Any]]:
        """Comprehensive crawling through all available years and months (supports native and custom dropdowns)"""
        all_results = []
        
        async def read_options_bajaj(trigger_selector: str, options_selector: Optional[str], option_type: str) -> List[Dict[str, str]]:
            # If custom list selector is provided (e.g., SBI), open and read items
            if trigger_selector:
                try:
                    # Locate all visible <select> elements
                    dropdowns = page.locator('select:visible')
                    print('dropdown with visible',dropdowns)
                    dropdown_count = await dropdowns.count()

                    matching_dropdowns = []

                    # Iterate through all visible dropdowns
                    for i in range(dropdown_count):
                        dropdown = dropdowns.nth(i)
                        try:
                            # Fetch inner text of all options in the dropdown
                            options = await dropdown.locator('option').all_inner_texts()

                            # Check if the dropdown contains "- Please select -"
                            if '- Please select -' in [opt.strip() for opt in options]:
                                matching_dropdowns.append(dropdown)
                        except Exception as e:
                            print(f"Error reading dropdown {i}: {e}")

                    print(f"Found {len(matching_dropdowns)} matching visible dropdown(s).")
                    month_dropdown = None

                    for i in range(dropdown_count):
                        dropdown = dropdowns.nth(i)
                        options = await dropdown.locator('option').all_inner_texts()

                        # Check if options contain month names
                        months = {"April", "May", "June", "July", "August", "September", "October", "November", "December", "January", "February", "March"}
                        if months.intersection(set(option.strip() for option in options)):
                            month_dropdown = dropdown
                            break

                    if month_dropdown:
                        # Print all options of the month dropdown
                        # month_options = await month_dropdown.locator('option').all_inner_texts()
                        options = await month_dropdown.locator('option').all()
    
                        # Collect both text and value attributes into a list of dictionaries
                        month_options = []
                        for option in options:
                            text = await option.inner_text()
                            value = await option.get_attribute('value')
                            month_options.append({'text': text.strip(), 'value': value})
                        
                        print("Month options (text & value):", month_options)
                        
                        if option_type == 'month' and hasattr(self, 'last_year') and hasattr(self, 'last_month'):
                            if hasattr(self, 'current_processing_year') and self.current_processing_year == self.last_year and self.last_month:
                                last_month_num = int(self.month_name_to_number(self.last_month))
                                month_options = [
                                    opt for opt in month_options 
                                    if opt['text'].strip() and int(self.month_name_to_number(opt['text'])) > last_month_num
                                ]
                                print(f"Filtered month options after {self.last_month}: {month_options}")
                                
                    else:
                        print("Month dropdown not found among visible selects.")
                    print('month options processing')
                    await month_dropdown.scroll_into_view_if_needed()
                    await month_dropdown.click()
                    # await page.wait_for_load_state('networkidle')
                    # await page.click(trigger_selector, force=True)
                    # await page.wait_for_selector(trigger_selector,state='visible',timeout= 30000)
                except:
                    print('bajaj month options failing')

            return month_options

        # Helper: read options either from native <select> or custom dropdown list
        async def read_options(trigger_selector: str, options_selector: Optional[str], option_type: str) -> List[Dict[str, str]]:
            # If custom list selector is provided (e.g., SBI), open and read items
            if options_selector and 'li' in options_selector:
                return await self.handle_ul_li_dropdown(page, trigger_selector, options_selector, option_type)
            
            if options_selector:
                try:
                    await page.wait_for_selector(trigger_selector, timeout=15000)
                    await page.locator(trigger_selector).scroll_into_view_if_needed()
                    await page.click(trigger_selector)
                    await asyncio.sleep(3)

                    await page.wait_for_selector(options_selector, timeout=10000)
                    items = await page.evaluate(f"""
                        () => Array.from(document.querySelectorAll('{options_selector}')).map(el => {{
                            const dv = el.getAttribute('data-value') || el.dataset?.value;
                            const txt = (el.textContent||'').trim();
                            return {{ value: dv || txt, text: txt }};
                        }})
                    """)
                    # Close dropdown politely
                    try:
                        await page.keyboard.press("Escape")
                    except Exception:
                        pass

                    # Filter by your existing validators - FIXED: Remove the additional regex filter
                    if option_type == 'year':
                        return [o for o in items if self._is_valid_year_option(o['text'], site)]
                    if option_type == 'month':
                        return [o for o in items if self._is_valid_month_option(o['text'])]
                    return items
                except:
                    try:
                        dropdowns = page.locator('select:visible')
                        print('dropdown with visible',dropdowns)
                        dropdown_count = await dropdowns.count()

                        matching_dropdowns = []

                        # Iterate through all visible dropdowns
                        for i in range(dropdown_count):
                            dropdown = dropdowns.nth(i)
                            try:
                                # Fetch inner text of all options in the dropdown
                                options = await dropdown.locator('option').all_inner_texts()

                                # Check if the dropdown contains "- Please select -"
                                if '- Please select -' in [opt.strip() for opt in options]:
                                    matching_dropdowns.append(dropdown)
                            except Exception as e:
                                print(f"Error reading dropdown {i}: {e}")

                        print(f"Found {len(matching_dropdowns)} matching visible dropdown(s).")
                        month_dropdown = None

                        for i in range(dropdown_count):
                            dropdown = dropdowns.nth(i)
                            options = await dropdown.locator('option').all_inner_texts()

                            # Check if options contain month names
                            months = {"April", "May", "June", "July", "August", "September", "October", "November", "December", "January", "February", "March"}
                            if months.intersection(set(option.strip() for option in options)):
                                month_dropdown = dropdown
                                break

                        if month_dropdown:
                            # Print all options of the month dropdown
                            month_options = await month_dropdown.locator('option').all_inner_texts()
                            print("Month dropdown options:", month_options)
                        else:
                            print("Month dropdown not found among visible selects.")
                        print('month options processing')
                        await month_dropdown.scroll_into_view_if_needed()
                        await month_dropdown.click()
                        # await page.wait_for_load_state('networkidle')
                        # await page.click(trigger_selector, force=True)
                        # await page.wait_for_selector(trigger_selector,state='visible',timeout= 30000)
                    

                        await page.wait_for_selector(options_selector, timeout=10000)
                        items = await page.evaluate(f"""
                            () => Array.from(document.querySelectorAll('{options_selector}')).map(el => {{
                                const dv = el.getAttribute('data-value') || el.dataset?.value;
                                const txt = (el.textContent||'').trim();
                                return {{ value: dv || txt, text: txt }};
                            }})
                        """)
                        # Close dropdown politely
                        try:
                            await page.keyboard.press("Escape")
                        except Exception:
                            pass

                        # Filter by your existing validators
                        if option_type == 'year':
                            # return [o for o in items if self._is_valid_year_option(o['text'])]
                            return [o for o in items if self._is_valid_year_option(o['text']) and not re.search(r'(please|select|all)', o['text'], re.I)]
                        if option_type == 'month':
                            # return [o for o in items if self._is_valid_month_option(o['text'])]
                            return [o for o in items if self._is_valid_month_option(o['text']) and not re.search(r'(please|select|all)', o['text'], re.I)]
                        return items
                    except Exception as e:
                        self.logger.warning(f"Custom dropdown read failed for {option_type}: {e}")
                        return []

            # Native <select> path (use your existing method)
            return await self.get_available_options(page, trigger_selector, option_type, site)
        

        # Helper: select either from custom list or native <select>
        async def select_option(trigger_selector: str, options_selector: Optional[str], value_or_text: str, option_type: str, match_by_text: bool):
            print('Select option in progress')
            
            if options_selector and 'li' in options_selector:
                await self.select_ul_li_option(page, trigger_selector, value_or_text, option_type)
                return
            
            if options_selector:
                print('Options Selector True')
                # Custom list: open -> click matching item
                await page.wait_for_selector(trigger_selector, timeout=15000)
                await page.locator(trigger_selector).scroll_into_view_if_needed()
                await page.click(trigger_selector)
                await asyncio.sleep(5)

                await page.wait_for_selector(options_selector, timeout=15000)
                try:
                    if match_by_text:
                        loc = page.locator(options_selector).filter(
                            has_text=re.compile(rf"^{re.escape(value_or_text)}$", re.I)
                        )
                        if await loc.count() == 0:
                            # Fallback: contains
                            loc = page.locator(options_selector).filter(
                                has_text=re.compile(re.escape(value_or_text), re.I)
                            )
                    else:
                        loc = page.locator(f"{options_selector}[data-value='{value_or_text}']")
                        if await loc.count() == 0:
                            loc = page.locator(options_selector).filter(
                                has_text=re.compile(rf"^{re.escape(value_or_text)}$", re.I)
                            )

                    if await loc.count() == 0:
                        raise Exception(f"{option_type} option not found in custom list: {value_or_text}")

                    target = loc.first
                    await target.scroll_into_view_if_needed()
                    # Let the open animation finish so the option is stable and receives events
                    await asyncio.sleep(0.2)

                    # Move pointer to center to satisfy actionability (prevents "receives events" timeouts)
                    box = await target.bounding_box()
                    if box:
                        await page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)

                    # Click with retry (helps when the first item is partially under the trigger)
                    try:
                        await target.click(timeout=5000)
                    except Exception:
                        try:
                            # Nudge list a bit and retry
                            await page.mouse.wheel(0, 200)
                            await asyncio.sleep(0.2)
                            await target.click(timeout=5000)
                        except Exception as e:
                            # Last resort: force the click
                            try:
                                await target.click(timeout=5000, force=True)
                            except Exception:
                                self.logger.error(f"Click failed on {option_type} '{value_or_text}': {e}")
                                raise
                except Exception as locator_error:
                        # Fallback to debug pattern that works
                        self.logger.warning(f"Locator approach failed: {locator_error}, trying debug pattern")
                        
                        clicked = await page.evaluate(f'''
                            () => {{
                                const elements = document.querySelectorAll('{options_selector}');
                                for (let el of elements) {{
                                    const text = (el.textContent || '').trim();
                                    if (text === '{value_or_text}') {{
                                        el.click();
                                        return true;
                                    }}
                                }}
                                return false;
                            }}
                        ''')
                        
                        if not clicked:
                            raise Exception(f"{option_type} option '{value_or_text}' not found with either approach")
                        
                        self.logger.info(f"Selected {option_type}: {value_or_text} via debug pattern fallback")
    

                # If dropdown didn't close, press Enter to confirm/close
                try:
                    if await page.locator(options_selector).is_visible():
                        await page.keyboard.press("Enter")
                except Exception:
                    pass

                await asyncio.sleep(0.6)
                self.logger.info(f"Selected {option_type}: {value_or_text} via custom dropdown")
                return

            # Native <select> path (your existing method)
            await self.select_dropdown_option(page, trigger_selector, value_or_text, option_type)

        async def select_month_option_bajaj(page, month_to_select, match_by_text=True):
            # Locate all visible dropdowns
            print('select month option bajaj started')
            dropdowns = page.locator('select:visible')
            dropdown_count = await dropdowns.count()

            # Define month names for identification
            months = {"April", "May", "June", "July", "August", "September", "October", "November", "December", "January", "February", "March"}

            # Find the month dropdown by checking options
            month_dropdown = None
            for i in range(dropdown_count):
                dropdown = dropdowns.nth(i)
                options_texts = await dropdown.locator('option').all_inner_texts()
                option_texts_set = {opt.strip() for opt in options_texts}

                if months.intersection(option_texts_set):
                    month_dropdown = dropdown
                    break

            if not month_dropdown:
                print("Month dropdown not found among visible selects.")
                return False

            # Fetch all option elements
            options = await month_dropdown.locator('option').all()

            # Iterate options to find matching one by text or value
            target_value = None
            for option in options:
                text = (await option.inner_text()).strip()
                value = await option.get_attribute('value')

                if match_by_text and text == month_to_select:
                    target_value = value
                    break
                elif not match_by_text and value == month_to_select:
                    target_value = value
                    break

            if not target_value:
                print(f"Option '{month_to_select}' not found in month dropdown.")
                return False

            # Select the option by value
            await month_dropdown.select_option(value=target_value)
            print(f"Selected month option: {month_to_select} (value: {target_value})")
            return True
        
        # Decide whether this site uses custom dropdowns for year/month
        year_options_selector = getattr(site.selectors, 'year_options_selector', None)
        month_options_selector = getattr(site.selectors, 'month_options_selector', None)

        available_years = await read_options(site.selectors.year_selector, year_options_selector, 'year')
        available_years = sorted(available_years,key=lambda y: int(y['text'].split('-')[0]) if '-' in y['text'] else int(y['text']),reverse=True)
        print(available_years)

        if not available_years:
            self.logger.warning("No years found, attempting single crawl")
            links = await self.collect_download_links(page, site)
            return [{'url': link, 'file_name': 'Factsheet', 'relevant_date': datetime.now().strftime('%Y-%m-%d')} for link in links]

        # years_to_process = available_years[:site.options.max_years_to_process]
        year_texts = [y['text'] for y in available_years]
        print(len(year_texts))
        filtered_year_texts, _ = self.filter_years_months(year_texts, [], site.amc_name)
        print(len(filtered_year_texts))
        years_to_process = [y for y in available_years if y['text'] in filtered_year_texts]
        # years_to_process = filtered_years[:site.options.max_years_to_process]
        self.logger.info(f"Processing {len(years_to_process)} years: {[y['text'] for y in years_to_process]}")

        # Match by text for custom (SBI), by value for native (Bajaj)
        year_match_by_text = bool(year_options_selector)
        month_match_by_text = bool(month_options_selector)

        for year_idx, year_option in enumerate(years_to_process):
            try:
                self.logger.info(f"Processing year {year_idx + 1}/{len(years_to_process)}: {year_option['text']}")

                # Select year (text for custom; value for native)
                await select_option(
                    site.selectors.year_selector,
                    year_options_selector,
                    year_option['text'] if year_match_by_text else year_option['value'],
                    'year',
                    match_by_text=year_match_by_text
                )

                # Give the month UI a moment to update
                await asyncio.sleep(1.5)

                # Read months (custom vs native)
                if site.site_id == "bajaj":
                    for i in range(10):
                        print('trying for the months')
                        available_months = await read_options_bajaj(site.selectors.month_selector, month_options_selector, 'month')
                        # available_months = await read_options(site.selectors.month_selector, month_options_selector, 'month')
                        if available_months:
                            break
                        await asyncio.sleep(1.5)   
                else:
                    available_months = await read_options(site.selectors.month_selector, month_options_selector, 'month')
                
                if not available_months:
                    self.logger.warning(f"No months found for year {year_option['text']}")
                    continue

                # months_to_process = available_months[:site.options.max_months_per_year]
                month_texts = [m['text'] for m in available_months]
                _, filtered_month_texts = self.filter_years_months([], month_texts, site.amc_name, current_year=year_option['text'])
                months_to_process = [m for m in available_months if m['text'] in filtered_month_texts]
                # months_to_process = filtered_months[:site.options.max_months_per_year]
                self.logger.info(f"Found {len(months_to_process)} months for year {year_option['text']}")

                for month_idx, month_option in enumerate(months_to_process):
                    try:
                        self.logger.info(f"Processing month {month_idx + 1}/{len(months_to_process)}: {month_option['text']}")
                        
                        if site.site_id == "bajaj":
                            await select_month_option_bajaj(page,month_option['text'], match_by_text=True)

                        else:
                            await select_option(
                            site.selectors.month_selector,
                            month_options_selector,
                            month_option['text'] if month_match_by_text else month_option['value'],
                            'month',
                            match_by_text=month_match_by_text)

                        await asyncio.sleep(1.0)
                        
                        pairs = await self.collect_download_links(page, site)

                        # for link in links:
                        #     file_name = self.generate_file_name(month_option['text'], year_option['text'])
                        #     relevant_date = self.generate_relevant_date(month_option['text'], year_option['text'])
                        #     all_results.append({
                        #         'url': link,
                        #         'file_name': file_name,
                        #         'relevant_date': relevant_date,
                        #         'year': year_option['text'],
                        #         'month': month_option['text']
                        #     })
                        
                        for item in pairs:
                            # If you still generate a normalized file name, you can combine:
                            # file_name = self.generate_file_name_from_title(item["title"])
                            file_name = item["title"] or "Factsheet"
                            print( year_option['text'],'this is year option')
                            if re.match(r"\d{4}\s*-\s*\d{4}", year_option['text']) or re.match(r"\d{4}\s*-\s*\d{2}", year_option['text']):
                                print('condition for year format')
                                # If year is a range like '2025-2026', use your custom FY logic
                                relevant_date = self.generate_relevant_date_for_FY(month_option['text'], year_option['text'])
                                file_name = "Factsheet"+'_'+str(month_option['text'])+'_'+str(year_option['text'])
                                
                            else:
                                # Otherwise, use the normal year logic
                                relevant_date = self.generate_relevant_date(month_option['text'], year_option['text'])
                                file_name = "Factsheet"+'_'+str(month_option['text'])+'_'+str(year_option['text'])

                            if not relevant_date:
                                relevant_date = self.generate_relevant_date(month_option['text'], year_option['text'])
                            
                            # relevant_date can still be computed from parsed month/year if you wish
                            all_results.append({
                                "url": item["url"],
                                "file_name": file_name,
                                "relevant_date": relevant_date,
                                "year": year_option['text'],
                                "month":  month_option['text']
                            })
                           
                        self.logger.info(f"Collected {len(pairs)} links for {month_option['text']} {year_option['text']}")

                    except Exception as e:
                        self.logger.error(f"Error processing month {month_option['text']}: {e}")
                        continue

            except Exception as e:
                self.logger.error(f"Error processing year {year_option['text']}: {e}")
                continue

        return all_results
    
    async def comprehensive_year_crawl(self, page: Page, site: SiteConfig) -> List[Dict[str, Any]]:
        print('USING COMPREHENSIVE YEAR CRAWL')
        all_results = []

        # Special handling for ITI: select "All" and use paginated JS links
        # js_link_count = await page.locator("a[href='javascript:void(0)']").count()
        js_link_count = await self.detect_javascript_links(page)
        if js_link_count > 0:
            print('JavaScript links detected - Using special All + Paginator + JS workflow')
            
            # Helper functions for reading and selecting year options
            async def read_year_options(year_selector: str, year_options_selector: Optional[str]) -> List[Dict[str, str]]:
                if year_options_selector:
                    try:
                        await page.wait_for_selector(year_selector, timeout=15000)
                        await page.locator(year_selector).scroll_into_view_if_needed()
                        await page.click(year_selector)
                        await asyncio.sleep(5)
                        await page.wait_for_selector(year_options_selector, timeout=10000)
                        items = await page.evaluate(f"""
                            () => Array.from(document.querySelectorAll('{year_options_selector}')).map(el => {{
                                const dv = el.getAttribute('data-value') || el.dataset?.value;
                                const txt = (el.textContent||'').trim();
                                return {{ value: dv || txt, text: txt }};
                            }})
                        """)
                        try: await page.keyboard.press("Escape")
                        except: pass
                        return [o for o in items if self._is_valid_year_option(o['text'], site)]
                    except Exception as e:
                        self.logger.warning(f"Custom dropdown read failed for year: {e}")
                        return []
                else:
                    return await self.get_available_options(page, year_selector, 'year', site)

            async def select_year(year_selector: str, year_options_selector: Optional[str], value_or_text: str, match_by_text: bool):
                if year_options_selector:
                    await page.wait_for_selector(year_selector, timeout=15000)
                    await page.click(year_selector)
                    await asyncio.sleep(3)
                    await page.wait_for_selector(year_options_selector, timeout=15000)
                    
                    loc = page.locator(year_options_selector).filter(
                        has_text=re.compile(rf"^{re.escape(value_or_text)}$", re.I)
                    )
                    if await loc.count() == 0:
                        loc = page.locator(year_options_selector).filter(
                            has_text=re.compile(re.escape(value_or_text), re.I)
                        )
                    
                    target = loc.first
                    await target.click()
                    await asyncio.sleep(0.6)
                    self.logger.info(f"Selected year: {value_or_text}")
                else:
                    await self.select_dropdown_option(page, year_selector, value_or_text, 'year')

            # Get available years and look for "All"
            year_selector = getattr(site.selectors, 'year_selector', None)
            year_options_selector = getattr(site.selectors, 'year_options_selector', None)
            available_years = await read_year_options(year_selector, year_options_selector)
            
            # Find "All" option
            all_option = None
            for year_option in available_years:
                if year_option['text'].lower() in ['all', 'सभी']:  # Handle both English and Hindi
                    all_option = year_option
                    break
            
            if all_option:
                self.logger.info(f"ITI: Selecting 'All' option: {all_option['text']}")
                match_by_text = bool(year_options_selector)
                
                await select_year(
                    year_selector,
                    year_options_selector,
                    all_option['text'] if match_by_text else all_option['value'],
                    match_by_text=match_by_text
                )
                await asyncio.sleep(2)
                
                # Now use the paginated JS links method
                return await self.collect_paginated_js_links(page, site)
            else:
                self.logger.warning("ITI: 'All' option not found, falling back to regular year crawl")

        # Regular comprehensive year crawl for other sites
        # Helper: read year options from a native <select> OR custom dropdown
        async def read_year_options(year_selector: str, year_options_selector: Optional[str]) -> List[Dict[str, str]]:
            # Custom dropdown path
            if year_options_selector:
                try:
                    await page.wait_for_selector(year_selector, timeout=15000)
                    await page.locator(year_selector).scroll_into_view_if_needed()
                    await page.click(year_selector)
                    await asyncio.sleep(5)
                    await page.wait_for_selector(year_options_selector, timeout=10000)
                    items = await page.evaluate(f"""
                        () => Array.from(document.querySelectorAll('{year_options_selector}')).map(el => {{
                            const dv = el.getAttribute('data-value') || el.dataset?.value;
                            const txt = (el.textContent||'').trim();
                            return {{ value: dv || txt, text: txt }};
                        }})
                    """)
                    try: await page.keyboard.press("Escape")
                    except: pass
                    print(items)
                    # Filter out non-year options
                    return [o for o in items if self._is_valid_year_option(o['text'], site)]
                except Exception as e:
                    self.logger.warning(f"Custom dropdown read failed for year: {e}")
                    return []
            # Native <select> path
            else:
                return await self.get_available_options(page, year_selector, 'year', site)

        # Helper: select year option from native or custom dropdown
        async def select_year(year_selector: str, year_options_selector: Optional[str], value_or_text: str, match_by_text: bool):
            if year_options_selector:
                await page.wait_for_selector(year_selector, timeout=15000)
                await page.locator(year_selector).scroll_into_view_if_needed()
                await page.click(year_selector)
                await asyncio.sleep(3)
                await page.wait_for_selector(year_options_selector, timeout=15000)
                if match_by_text:
                    loc = page.locator(year_options_selector).filter(
                        has_text=re.compile(rf"^{re.escape(value_or_text)}$", re.I)
                    )
                    if await loc.count() == 0:
                        loc = page.locator(year_options_selector).filter(
                            has_text=re.compile(re.escape(value_or_text), re.I)
                        )
                else:
                    loc = page.locator(f"{year_options_selector}[data-value='{value_or_text}']")
                    if await loc.count() == 0:
                        loc = page.locator(year_options_selector).filter(
                            has_text=re.compile(rf"^{re.escape(value_or_text)}$", re.I)
                        )
                if await loc.count() == 0:
                    raise Exception(f"Year option not found: {value_or_text}")
                target = loc.first
                await target.scroll_into_view_if_needed()
                await asyncio.sleep(0.2)
                box = await target.bounding_box()
                if box:
                    await page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
                try:
                    await target.click(timeout=5000)
                except Exception:
                    try:
                        await page.mouse.wheel(0, 200)
                        await asyncio.sleep(0.2)
                        await target.click(timeout=5000)
                    except Exception as e:
                        try:
                            await target.click(timeout=5000, force=True)
                        except Exception:
                            self.logger.error(f"Click failed on year '{value_or_text}': {e}")
                            raise
                try:
                    if await page.locator(year_options_selector).is_visible():
                        await page.keyboard.press("Enter")
                except Exception:
                    pass
                await asyncio.sleep(0.6)
                self.logger.info(f"Selected year: {value_or_text} via custom dropdown")
            else:
                await self.select_dropdown_option(page, year_selector, value_or_text, 'year')

        # Prepare selectors
        year_selector = getattr(site.selectors, 'year_selector', None)
        year_options_selector = getattr(site.selectors, 'year_options_selector', None)
        available_years = await read_year_options(year_selector, year_options_selector)
        print(available_years)

        if not available_years:
            self.logger.warning("No years found, attempting single crawl")
            links = await self.collect_download_links(page, site)
            return [{'url': link, 'file_name': 'Factsheet', 'relevant_date': datetime.now().strftime('%Y-%m-%d')} for link in links]

        # years_to_process = available_years[:site.options.max_years_to_process]
        year_texts = [y['text'] for y in available_years]
        print(len(year_texts))
        filtered_year_texts, _ = self.filter_years_months(year_texts, [], site.amc_name)
        print(len(filtered_year_texts))
        years_to_process = [y for y in available_years if y['text'] in filtered_year_texts]
        # years_to_process = filtered_years[:site.options.max_years_to_process]
        self.logger.info(f"Processing {len(years_to_process)} years: {[y['text'] for y in years_to_process]}")

        match_by_text = bool(year_options_selector)

        for year_idx, year_option in enumerate(years_to_process):
            try:
                self.logger.info(f"Processing year {year_idx + 1}/{len(years_to_process)}: {year_option['text']}")
                await select_year(
                    year_selector,
                    year_options_selector,
                    year_option['text'] if match_by_text else year_option['value'],
                    match_by_text=match_by_text
                )
                await asyncio.sleep(1.5)
                
                # Check for JavaScript factsheet elements (PGIM pattern)
                has_js_factsheets = await page.evaluate("""
                    () => {
                        const elements = document.querySelectorAll('.form-card, .factsheet-item, [class*="factsheet"]');
                        return Array.from(elements).some(el => 
                            !el.href && 
                            /factsheet/i.test(el.textContent)
                        );
                    }
                """)
                
                year_results=[]
                
                if has_js_factsheets:
                    self.logger.info("Detected JavaScript factsheet downloads, using click & intercept method")
                    self.logger.info(f"Year {year_option['text']}: Detected PGIM factsheet pattern")
                    # ✅ COLLECT for this year, don't return
                    pgim_results = await self.collect_pgim_factsheets(page, site)
                    year_results.extend(pgim_results)
                    # return await self.collect_pgim_factsheets(page, site)
                
                else:
                    # js_link_count = await page.locator("a[href='javascript:void(0)']").count()
                    js_link_count = await self.detect_javascript_links(page)
                    if js_link_count > 0:
                        self.logger.info(f"Detected {js_link_count} JavaScript download links, using interception method")
                        
                        # Check if it also has pagination (next button)
                        next_button_sel = getattr(site.selectors, "next_button_selector", None) or ""
                        
                        if next_button_sel.strip():
                            self.logger.info(f"Year {year_option['text']}: Using paginated JS links")
                            # ✅ COLLECT for this year, don't return
                            js_results = await self.collect_paginated_js_links(page, site)
                            year_results.extend(js_results)
                        else:
                            self.logger.info(f"Year {year_option['text']}: Using JS interception")
                            # ✅ COLLECT for this year, don't return
                            js_results = await self.collect_download_links_with_interception(page, site)
                            year_results.extend(js_results)
                    else:
                        # Regular link collection
                        self.logger.info(f"Year {year_option['text']}: Using regular link collection")
                        pairs = await self.collect_download_links(page, site)
                        year_results.extend(pairs)
                        year_sel = getattr(site.selectors, "year_selector", None) or ""
                        
                # Process results for this year and add to all_results
                for item in year_results:
                    if isinstance(item, dict) and 'url' in item:
                        abs_url = str(item["url"])
                        
                        # Extract month and year from URL
                        result = self.month_year_from_url(abs_url)
                        y = result[0]
                        m = result[1]
                        if y is None or m is None:
                            print('USING HAIKU TO EXTRACT YEAR AND MONTH')
                            m, y = self._extract_month_year_info(abs_url)
                        
                        file_name = 'Factsheet' + '_' + str(m) + '_' + str(y)
                        year_for_date = y if y else year_option["text"]
                        month_for_date = m if m else None
                        relevant_date = self.generate_relevant_date(str(m), str(y))
                        
                        all_results.append({
                            "url": item["url"],
                            "file_name": file_name,
                            "relevant_date": relevant_date,
                            "year": str(year_for_date),
                            "month": str(month_for_date) if month_for_date else ""
                        })
                
                self.logger.info(f"Collected {len(year_results)} links for year {year_option['text']}")
                
            except Exception as e:
                self.logger.error(f"Error processing year {year_option['text']}: {e}")
                continue  # ✅ Continue to next year instead of breaking    
                        
            #             # If has year selector, use comprehensive year crawl (will handle JS + year combinations)
            #             if bool(year_sel.strip()):
            #                 self.logger.info("Site has both JavaScript links AND year selector - using comprehensive year crawl")
            #                 return await self.comprehensive_year_crawl(page, site)  # Will handle JS specially
                        
            #             # If has pagination but no year selector, use paginated JS
            #             elif next_button_sel.strip():
            #                 self.logger.info("Site has JavaScript links with pagination")
            #                 return await self.collect_paginated_js_links(page, site)  
                        
            #             # Just JavaScript links, no pagination or year selection
            #             else:
            #                 self.logger.info("Site has JavaScript links only")
            #                 return await self.collect_download_links_with_interception(page, site)
                    
            #     pairs = await self.collect_download_links(page, site)
            #     print(pairs,'these are pairs')
            #     for item in pairs:
            #         abs_url = str(item["url"])
            #         print(abs_url,'this is url')
            #         # Extract month and year from URL if present, else use year from option
            #         # y, m = self.extract_year_month_from_url2(abs_url)
            #         result = self.month_year_from_url(abs_url)
            #         y = result[0]
            #         m = result[1]
            #         if y is None or m is None:
            #             # handle the case where one or both are None
            #             print('USING HAIKU TO EXTRACT YEAR AND MONTH')
            #             m,y = self._extract_month_year_info(abs_url)
                                        
            #         file_name = 'Factsheet'+'_'+str(m)+'_'+str(y)
            #         print(y,m,file_name,'this is year and month')
            #         year_for_date = y if y else year_option["text"]
            #         month_for_date = m if m else None
            #         relevant_date = self.generate_relevant_date(str(m), str(y))
            #         all_results.append({
            #             "url": item["url"],
            #             "file_name": file_name,
            #             "relevant_date": relevant_date,
            #             "year": str(year_for_date),
            #             "month": str(month_for_date) if month_for_date else ""
            #         })
            #     self.logger.info(f"Collected {len(pairs)} links for year {year_option['text']}")
            # except Exception as e:
            #     self.logger.error(f"Error processing year {year_option['text']}: {e}")
            #     continue

        return all_results
    
    
    @staticmethod
    def _extract_month_year_info(address):
        Instructions="""
            1.Extract the information from given url\n
            2.Extract the following fields\n
                -Month
                -Year
            3.show output in json only\n
            4.Do not Include any other parameter which is not in second point..
            eg.Input is :'https://mf.nipponindiaim.com/InvestorServices/FactsheetsDocuments/RMF%20Factsheet_September%202013_13-09-13.pdf'
        """

        sample_output={"Month":"September","Year":"2025"}
        result=adqvest_ai.generate_answer(address,Instructions,sample_output)
        data = get_json(result)  # expect dict like {"Month":"September","Year":"2013"}

        # Safely pull fields
        month_str = data.get("Month") if isinstance(data, dict) else None  # e.g., "September" [3]
        year_raw = data.get("Year") if isinstance(data, dict) else None     # e.g., "2013" [3]

        # Optional: coerce year to int
        try:
            year = int(year_raw) if year_raw is not None else None
        except Exception:
            year = None

        return month_str, year
        
            
    @staticmethod
    def _normalize_spaces(s: str) -> str:
        return " ".join(
            (s or "")
            .replace("\u200b", "")
            .replace("\u200c", "")
            .replace("\ufeff", "")
            .replace("\u00a0", " ")
            .split()
        )
    
   
    
    @staticmethod
    def generate_filename_from_title(title: str):
        # Extract full month name and 4-digit year from title text
        pattern = re.compile(r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})", re.I)
        match = pattern.search(title)
        if not match:
            return None
        month_name = match.group(1)
        year = match.group(2)
        return f"MF_Factsheet_{month_name}_{year}"
    
    
    @staticmethod
    def extract_year_month_from_filename(filename: str):
        month_full_map = {
            "jan": "January", "january": "January", 
            "feb": "February", "february": "February",
            "mar": "March", "march": "March", 
            "apr": "April", "april": "April",
            "may": "May", 
            "jun": "June", "june": "June",
            "jul": "July", "july": "July", 
            "aug": "August", "august": "August",
            "sep": "September", "september": "September", 
            "oct": "October", "october": "October",
            "nov": "November", "november": "November", 
            "dec": "December", "december": "December",
        }

        filename = filename.lower()
        print(f"DEBUG: Processing '{filename}'")
        
        # Try patterns in order of specificity
        patterns = [
            # Pattern 1: Month Day, Year (e.g., "september 30, 2025")
            r"\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+\d{1,2}[\s,]+(\d{4})",
            
            # Pattern 2: Month Year (e.g., "september 2025")  
            r"\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)[\s_\-]+(\d{4})",
            
            # Pattern 3: Month 2-digit year (e.g., "september 25" for 2025)
            r"\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)[\s_\-]+(\d{2})(?!\d)"
        ]
        
        for i, pattern in enumerate(patterns):
            match = re.search(pattern, filename, re.I)
            if match:
                month_str = match.group(1).lower()[:3] 
                year = int(match.group(2))
                
                # Convert 2-digit years only for pattern 3
                if i == 2 and year < 100:
                    year += 2000
                    
                month = month_full_map.get(month_str)
                print(f"DEBUG: Pattern {i+1} matched '{filename}' -> Month: {month}, Year: {year}")
                return year, month
        
        print(f"DEBUG: No match for '{filename}'")
        return None, None


    async def collect_pdf_links_simple(self, page: Page, site) -> List[Dict[str, Any]]:
        print('CHOOSING COLLECT PDF LINKS SIMPLE')
        results: List[Dict[str, Any]] = []

        data_container_selector: Optional[str] = getattr(site.selectors, "data_container_selector", None) or ""
        address_selector: Optional[str] = getattr(site.selectors, "address_selector", None) or ""

        if not data_container_selector.strip() or not address_selector.strip():
            self.logger.warning("Missing data_container_selector or address_selector in config")
            return results

        # Make the address selector relative to the container
        rel_addr = address_selector.strip()
        if rel_addr.startswith(data_container_selector):
            rel_addr = rel_addr[len(data_container_selector):].strip()
            if rel_addr.startswith((">>", ">")):
                rel_addr = rel_addr.lstrip(">").lstrip(">").strip()
        if '[href$=".pdf"]' not in rel_addr:
            rel_addr = f'{rel_addr}[href$=".pdf"]' if rel_addr else 'a[href$=".pdf"]'

        # Wait for container and scope anchors
        await page.wait_for_selector(data_container_selector, timeout=15000)
        container = page.locator(data_container_selector)

        anchors = container.locator(rel_addr)
        count = await anchors.count()
        base_url = page.url
        pdf_re = re.compile(r"\.pdf($|\?)", re.I)

        for i in range(count):
            a = anchors.nth(i)

            # URL
            href = ((await a.get_attribute("href")) or "").strip()
            if not href or not pdf_re.search(href):
                continue
            abs_url = urljoin(base_url, href)
            print(abs_url, 'these are links')
            title_text = (await a.text_content()) or ""
            title_text = title_text.strip()
            print(f"Title text: {title_text}")
            title_text = unicodedata.normalize('NFKC', title_text)
            title_text = title_text.replace('â€"', "-")
            file_name = title_text

            try:
                try:
                    # year,month = self.month_year_from_url(abs_url)
                    year, month = self.extract_year_month_from_filename(title_text)
                    if int(year) < 2018:
                            print(f"🛑 Year {year} < 2018 — stopping loop")
                            break
                    
                    print(month,year,'this is month year')
                    file_name = "MF_Factsheet "+str(month)+"_"+str(year)
                    print(file_name, 'filename')
                    relevant_date = self.generate_relevant_date(str(month), str(year))
                    
                except:
                    print('DATE MONTH EXTRACTION FAILED TRYING WITH URL')
                    year,month = self.month_year_from_url(abs_url)
                    if int(year) < 2018:
                            print(f"🛑 Year {year} < 2018 — stopping loop")
                            break
                    
                    print(month,year,'this is month year')
                    file_name = "MF_Factsheet "+str(month)+"_"+str(year)
                    print(file_name, 'filename')
                    relevant_date = self.generate_relevant_date(str(month), str(year))

                results.append({
                        "url": abs_url,
                        "file_name": file_name,
                        "month": month,
                        "year":year,
                        "relevant_date": relevant_date
                    })
            except:
                print(f"⚠️ Skipping problematic link {abs_url} due to error")
                continue

        # Deduplicate by URL
        seen, unique = set(), []
        for r in results:
            if r["url"] in seen:
                continue
            seen.add(r["url"])
            unique.append(r)

        return unique
    
    
    async def collect_pdf_links_pagination(self, page: Page, site) -> List[Dict[str, Any]]:
        print('CHOOSING COLLECT PDF LINKS USING PAGINATION')
        results: List[Dict[str, Any]] = []

        data_container_selector: Optional[str] = getattr(site.selectors, "data_container_selector", None) or ""
        address_selector: Optional[str] = getattr(site.selectors, "address_selector", None) or ""
        pagination_selector: Optional[str] = getattr(site.selectors, "pagination_selector", None) or ""
        
        print(f"DEBUG: data_container_selector = '{data_container_selector}'")
        print(f"DEBUG: address_selector = '{address_selector}'")
        print(f"DEBUG: pagination_selector = '{pagination_selector}'")

        if not data_container_selector.strip() or not address_selector.strip():
            self.logger.warning("Missing data_container_selector or address_selector in config")
            return results

        # Prepare relative address selector
        rel_addr = address_selector.strip()
        if rel_addr.startswith(data_container_selector):
            rel_addr = rel_addr[len(data_container_selector):].strip()
            if rel_addr.startswith((">>", ">")):
                rel_addr = rel_addr.lstrip(">").lstrip(">").strip()
        
        if '[href*=".pdf"]' not in rel_addr and '[href$=".pdf"]' not in rel_addr:
            rel_addr = f'{rel_addr}[href*=".pdf"]' if rel_addr else 'a[href*=".pdf"]'

        base_url = page.url
        pdf_re = re.compile(r"\.pdf($|\?)", re.I)

        async def extract_links_from_page_1():
            await page.wait_for_selector(data_container_selector, timeout=15000)
            container = page.locator(data_container_selector)
            
            links_data = await container.locator('a[target="_blank"]').evaluate_all("""
                anchors => anchors.map(a => ({ 
                    url: a.href, 
                    text: a.textContent.trim() 
                }))
            """)
            
            print(f"DEBUG: Total links extracted: {len(links_data)}")
            
            anchors = container.locator(rel_addr)
            count = await anchors.count()
            page_results = []  # Changed: Use page_results consistently

            for i in range(count):
                a = anchors.nth(i)
                href = ((await a.get_attribute("href")) or "").strip()
                if not href or not pdf_re.search(href):
                    continue
                abs_url = urljoin(base_url, href)
                print(abs_url, 'these are links')
                # Extract title text from anchor for month/year extraction
                title_text = (await a.text_content()) or ""
                title_text = title_text.strip()
                print(f"Title text: {title_text}")
                title_text = unicodedata.normalize('NFKC', title_text)
                title_text = title_text.replace('â€"', "-")

                try:
                    # Extract month and year from title text instead of URL
                    year, month = self.extract_year_month_from_filename(title_text)
                    print(month, year, 'this is month year from title')

                    file_name = title_text
                    print(file_name, 'filename')
                    relevant_date = self.generate_relevant_date(str(month), str(year))

                    page_results.append({  # Changed: Append to page_results instead of results
                        "url": abs_url,
                        "file_name": file_name,
                        "month": month,
                        "year": year,
                        "relevant_date": relevant_date,
                    })
                except Exception as e:
                    print(f"⚠️ Skipping problematic link {abs_url} due to error: {e}")
                    continue

            return page_results  # Now returning the actual collected results

        # Extract links from first page before any click (page 1)
        results.extend(await extract_links_from_page_1())
        print(f"Found {len(results)} links on first page")  # Debug print

        search_limit = getattr(site.options, "search_limit", None)
        max_pages = int(search_limit) if search_limit and search_limit > 0 else None
       
        if max_pages == 1:
            self.logger.info("search_limit=1; returning first page results only")
            seen = set()
            unique_results = []
            for r in results:
                if r["url"] in seen:
                    continue
                seen.add(r["url"])
                unique_results.append(r)
            return unique_results

        for target_page in range(2, (max_pages or 10000) + 1):
            pagination_links = page.locator(pagination_selector)
            count = await pagination_links.count()

            available_pages = []
            matched_link = None
            next_button = None
            for i in range(count):
                link = pagination_links.nth(i)
                text = (await link.text_content() or "").strip()
                available_pages.append(text)

                if text.isdigit() and int(text) == target_page:
                    matched_link = link
                # Detect next arrow button by text or css class as needed
                elif text in ('›', '»', 'next', '>'):
                    next_button = link

            print(f"Available pagination links: {available_pages}")

            # If target page link not found, but next button exists, click next button to show more pages
            if not matched_link and next_button:
                print(f"Page {target_page} not visible, clicking next pagination button")
                await next_button.click()
                await page.wait_for_load_state('domcontentloaded')
                await page.wait_for_selector(data_container_selector, state='visible', timeout=15000)
                await asyncio.sleep(site.options.cascade_wait_time)
                # After clicking next, refresh pagination and search again for target page link
                pagination_links = page.locator(pagination_selector)
                count = await pagination_links.count()
                matched_link = None
                for i in range(count):
                    link = pagination_links.nth(i)
                    text = (await link.text_content() or "").strip()
                    if text.isdigit() and int(text) == target_page:
                        matched_link = link
                        break

            if not matched_link:
                print(f"Pagination page {target_page} link still not found after clicking next. Stopping.")
                break

            print(f"Clicking pagination page: {target_page}")
            await matched_link.click()
            await page.wait_for_load_state('domcontentloaded')
            await page.wait_for_selector(data_container_selector, state='visible', timeout=15000)
            await asyncio.sleep(site.options.cascade_wait_time)
            
            page_links = await extract_links_from_page_1()
            results.extend(page_links)
            print(f"Found {len(page_links)} links on page {target_page}")  # Debug print

            if max_pages and target_page >= max_pages:
                print(f"Reached search limit of {max_pages} pages")
                break

        print(f"Total links collected: {len(results)}")  # Debug print

        seen = set()
        unique_results = []
        for r in results:
            if r["url"] in seen:
                continue
            seen.add(r["url"])
            unique_results.append(r)

        return unique_results
    
    async def crawl_pages_via_next(self, page: Page, site, max_pages: int) -> List[Dict[str, Any]]:
        print('USING CRAWL PAGE VIA NEXT')
        results = []

        data_container_selector = getattr(site.selectors, "data_container_selector", None) or ""
        address_selector = getattr(site.selectors, "address_selector", None) or ""
        next_button_selector = getattr(site.selectors, "next_button_selector", None) or ""

        if not data_container_selector.strip() or not address_selector.strip():
            self.logger.warning("Missing data_container_selector or address_selector in config")
            return results

        # Prepare rel_addr same as your main function
        rel_addr = address_selector.strip()
        if rel_addr.startswith(data_container_selector):
            rel_addr = rel_addr[len(data_container_selector):].strip()
            if rel_addr.startswith((">>", ">")):
                rel_addr = rel_addr.lstrip(">").lstrip(">").strip()
        if '[href$=".pdf"]' not in rel_addr:
            rel_addr = f'{rel_addr}[href$=".pdf"]' if rel_addr else 'a[href$=".pdf"]'

        base_url = page.url
        pdf_re = re.compile(r"\.pdf($|\?)", re.I)

        async def extract_links_from_page():
            await page.wait_for_selector(data_container_selector, timeout=15000)
            container = page.locator(data_container_selector)
            anchors = container.locator(rel_addr)
            count = await anchors.count()
            page_results = []
            for i in range(count):
                a = anchors.nth(i)
                href = ((await a.get_attribute("href")) or "").strip()
                if not href or not pdf_re.search(href):
                    continue
                abs_url = urljoin(base_url, href)
                print(abs_url,'these are url collected')
                title_text = (await a.text_content()) or ""
                title_text = title_text.strip()
                title_text = unicodedata.normalize('NFKC', title_text).replace('â€"', "-")
               
                if "factsheet" not  in abs_url.lower():
                    print(f"⏭️ Skipping unwanted link: {abs_url}")
                    continue

                try:
                    result = self.month_year_from_url(abs_url)
                    y = result[0]
                    m = result[1]
                    if y is None or m is None:
                        # handle the case where one or both are None
                        print('USING HAIKU TO EXTRACT YEAR AND MONTH')
                        m,y = self._extract_month_year_info(abs_url)
                                        
                    file_name = 'Factsheet'+'_'+str(m)+'_'+str(y)
                    print(y,m,file_name,'this is year and month')
                    relevant_date = self.generate_relevant_date(str(m), str(y))

                    page_results.append({
                        "url": abs_url,
                        "file_name": file_name,
                        "month": m,
                        "year": y,
                        "relevant_date": relevant_date,
                    })
                except:
                    print('Error in processing month year from url')
                    continue
            return page_results

        # Extract from first page
        results.extend(await extract_links_from_page())

        for page_num in range(1, max_pages):
            await asyncio.sleep(5)
            print(f"Processing page {page_num}")
            
            try:
                next_button = page.locator(next_button_selector)
                button_count = await next_button.count()
                
                if button_count == 0:
                    self.logger.info(f"Next button not found, stopping at page {page_num}")
                    break
                
                is_disabled = await next_button.first.get_attribute('disabled')
                aria_disabled = await next_button.first.get_attribute('aria-disabled')
                
                if is_disabled is not None or aria_disabled == 'true':
                    self.logger.info(f"Next button is disabled, stopping at page {page_num}")
                    break
                
                # Check if button is visible
                is_visible = await next_button.first.is_visible()
                if not is_visible:
                    self.logger.info(f"Next button not visible, stopping at page {page_num}")
                    break
                            
                if await next_button.is_visible():
                    await next_button.click()
                    await page.wait_for_load_state('domcontentloaded')
                    await page.wait_for_selector(data_container_selector, state='visible', timeout=15000)
                    await asyncio.sleep(site.options.cascade_wait_time)
                    results.extend(await extract_links_from_page())
                else:
                    self.logger.info("Next button not visible, stopping pagination")
                    break
            except Exception as e:
                self.logger.error(f"Error clicking next button")
                break

        # Deduplicate
        seen = set()
        unique_results = []
        for r in results:
            if r["url"] in seen:
                continue
            seen.add(r["url"])
            unique_results.append(r)
            
        try:
            conn = adqvest_db.db_conn()
            query = "SELECT MAX(Relevant_date) as last_date FROM AMC_WISE_MF_FACTSHEETS_MONTHLY_DATA_CORPUS WHERE AMC_Name = %s"
            result = pd.read_sql(query, conn, params=[site.amc_name])
            
            if not result.empty and not pd.isna(result['last_date'].iloc[0]):
                last_date = pd.to_datetime(result['last_date'].iloc[0])
                filtered_results = []
                
                for item in unique_results:
                    if item.get('relevant_date'):
                        try:
                            item_date = pd.to_datetime(item['relevant_date'])
                            if item_date > last_date:
                                filtered_results.append(item)
                        except:
                            filtered_results.append(item) 
                    else:
                        filtered_results.append(item) 
                
                print(f"Filtered JS downloads: {len(filtered_results)} newer than {last_date.date()}")
                return filtered_results
                
        except Exception as e:
            print(f"Error filtering JS downloads: {e}")

        return unique_results    

        return unique_results
    
    async def get_available_options(self, page: Page, selector: str, option_type: str) -> List[Dict[str, str]]:
        """Get all available options from a dropdown, filtering out invalid options"""
        if not selector:
            return []
        
        try:
            # Wait for selector with extended timeout
            await page.wait_for_selector(selector, timeout=15000)
            
            # Wait for options to be populated using eval_on_selector
            for attempt in range(10):  # Try up to 10 times
                await asyncio.sleep(1)
                
                options_count = await page.eval_on_selector(selector, 'el => el.options.length')
                if options_count > 1:  # More than just placeholder
                    break
            else:
                self.logger.warning(f"No options populated for {option_type} dropdown after waiting")
                return []
            
            # Get all options
            options = await page.evaluate(f'''
                () => {{
                    const select = document.querySelector(`{selector}`);
                    if (!select) return [];
                    return Array.from(select.options)
                        .filter(opt => opt.value && opt.value !== '' && opt.value !== '0')
                        .map(opt => ({{
                            value: opt.value,
                            text: opt.textContent || opt.innerText || ''
                        }}));
                }}
            ''')
            print(options,'this is options for year')
            
            # Filter out invalid options based on option type
            if option_type == 'year':
                # Filter out non-year options like 'All', 'Please select', etc.
                valid_options = [opt for opt in options if self._is_valid_year_option(opt['text'])]
            elif option_type == 'month':
                # Filter out non-month options
                valid_options = [opt for opt in options if self._is_valid_month_option(opt['text'])]
            else:
                # For other types, just filter out common placeholder texts
                valid_options = [opt for opt in options if not re.match(r'^(all|.*select.*|.*choose.*)$', opt['text'], re.I)]
            
            self.logger.info(f"Found {len(valid_options)} valid {option_type} options: {[opt['text'] for opt in valid_options]}")
            return valid_options
            
        except Exception as e:
            self.logger.error(f"Error getting {option_type} options: {e}")
            return []
        
    def _is_valid_year_option(self, text: str, site_config: SiteConfig = None) -> bool:
        """Check if the option text represents a valid year or financial year"""
        text_lower = text.lower().strip()
        
        if site_config and hasattr(site_config.options, 'allow_special_year_options'):
            allowed_specials = [opt.lower() for opt in site_config.options.allow_special_year_options]
            if text_lower in allowed_specials:
                return True
        
        # Skip common invalid options
        invalid_patterns = [
            r'^.*select.*$',
            r'^.*choose.*$',
            r'^.*please.*$',
            r'^.*option.*$',
            r'^-+$',
            r'^\s*$'
        ]
        
        for pattern in invalid_patterns:
            if re.match(pattern, text_lower, re.I):
                return False
        
        # Check if it contains year patterns (4 digits)
        # Common formats: 2024, 2023-24, 2024-2025, FY 2024, etc.
        year_patterns = [
        r'\b(19|20)\d{2}\b',  # 4-digit years (1900-2099)
        r'\b\d{2}-\d{2}\b',   # Format like 23-24
        r'\b\d{2}/\d{2}\b',   # Format like 23/24
        r'\bfy\s*\d{4}\b',    # FY 2024
        r'\b\d{4}-\d{4}\b',   # 2023-2024
        r'\b\d{4}/\d{4}\b',   # 2023/2024
        ]
        
        for pattern in year_patterns:
            if re.search(pattern, text_lower):
                return True
        
        return False

    def _is_valid_month_option(self, text: str) -> bool:
        """Check if the option text represents a valid month"""
        text_lower = text.lower().strip()
        
        # Skip common invalid options
        invalid_patterns = [
            r'^all$',
            r'^.*select.*$',
            r'^.*choose.*$',
            r'^.*please.*$',
            r'^.*option.*$',
            r'^-+$',
            r'^\s*$'
        ]
        
        for pattern in invalid_patterns:
            if re.match(pattern, text_lower, re.I):
                return False
        
        # Check if it contains month patterns
        month_patterns = [
            r'^(january|february|march|april|may|june|july|august|september|october|november|december)',
            r'^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)',
            r'^(0?[1-9]|1[0-2])$',  # 1-12 or 01-12
            r'^\d{2}-\d{4}$',  # 01-2024 format
        ]
        
        for pattern in month_patterns:
            if re.search(pattern, text_lower):
                return True
        
        return False

    async def select_dropdown_option(self, page: Page, selector: str, value: str, option_type: str):
        """Select a specific option from dropdown"""
        try:
            print('select_dropdown_option started')
            await page.wait_for_selector(selector, timeout=15000)
            
            is_material_design = await page.evaluate(f'''
            () => {{
                const element = document.querySelector(`{selector}`);
                return element && element.innerHTML.includes('mat-mdc-form-field');
            }}
        ''')
        
            if is_material_design:
                print(f"Using Material Design selection for {option_type}: {value}")
                
                # Click to open the dropdown
                await page.click(f"{selector} .mat-mdc-form-field")
                await asyncio.sleep(0.8)
                
                # Find and click the option
                option_locator = page.locator("mat-option").filter(has_text=value)
                await option_locator.click()
                await asyncio.sleep(0.5)
                
                self.logger.info(f"Selected {option_type}: {value} via Material Design dropdown")
                
            else:
                print(f"Using standard select for {option_type}: {value}")
                print(selector,'this is  the selector')
                await page.wait_for_selector(selector, timeout=30000)
                
                # Ensure dropdown is visible
                is_visible = await page.is_visible(selector)
                if not is_visible:
                    await page.locator(selector).scroll_into_view_if_needed()
                    await asyncio.sleep(1)
                
                # Select the option for standard select
                await page.select_option(selector, value)
                
                # Wait for any resulting page changes
                try:
                    await page.wait_for_load_state('networkidle', timeout=3000)
                except PlaywrightTimeoutError:
                    pass
                
                await asyncio.sleep(2)  # Additional wait for dynamic content
                
                self.logger.info(f"Selected {option_type}: {value}")
                
        except Exception as e:
                self.logger.error(f"Error selecting {option_type} option {value}: {e}")
                raise e
        
    async def handle_multi_click(self, page: Page, site) -> int:
        """
        Clicks the configured 'multi click' button up to 8 times to load more documents.
        Uses site.selectors.multitab_selectors as the CSS selector to click.
        Returns the number of successful clicks.
        """
        selector: Optional[str] = getattr(site.selectors, "multi_tab_selector", None)
        if not selector:
            self.logger.warning("multitab_selectors not configured on site.selectors")
            return 0

        max_clicks = 8
        clicks = 0

        for attempt in range(max_clicks):
            try:
                # Ensure the button exists and is visible
                await page.wait_for_selector(selector, state="visible", timeout=8000)
            except PlaywrightTimeoutError:
                self.logger.info(f"Multi-click button not visible on attempt {attempt+1}; stopping")
                break

            btn = page.locator(selector).first

            # Try to ensure actionability
            try:
                await btn.scroll_into_view_if_needed()
            except Exception:
                pass

            # Click
            try:
                await btn.click()
                clicks += 1
            except Exception as e:
                self.logger.info(f"Click failed on attempt {attempt+1}: {e}; stopping")
                break

            # Short settle to allow DOM to update
            await asyncio.sleep(getattr(site.options, "cascade_wait_time", 1) or 1)

            # Optional: light load-state wait (avoid hanging if site keeps streaming)
            try:
                await page.wait_for_load_state("domcontentloaded", timeout=4000)
            except PlaywrightTimeoutError:
                # Proceed even if DOMContentLoaded didn't change; many SPAs update without navigation
                pass

        self.logger.info(f"Finished multi-clicks: {clicks} performed")
        return clicks
    
    async def handle_ion_input_dropdown(self, page: Page, selector: str, option_type: str) -> List[Dict[str, str]]:
            """Handle Ion input dropdowns specifically"""
            try:
                # Wait for the dropdown element
                await page.wait_for_selector(selector, timeout=15000)
                
                # Click to open the dropdown
                await page.click(selector)
                await asyncio.sleep(1)
                
                # Wait for options to appear - try different selectors
                option_selectors = ["ion-item", "ion-select-option", "ion-radio-group ion-item", ".select-option"]
                
                options = []
                for opt_selector in option_selectors:
                    try:
                        await page.wait_for_selector(opt_selector, timeout=3000)
                        
                        # Get all option elements
                        elements = await page.evaluate(f"""
                            () => {{
                                const items = document.querySelectorAll('{opt_selector}');
                                return Array.from(items).map(item => {{
                                    const text = (item.textContent || '').trim();
                                    const value = item.getAttribute('value') || text;
                                    return {{ value, text }};
                                }});
                            }}
                        """)
                        
                        if elements and len(elements) > 0:
                            options = elements
                            self.logger.info(f"Found {len(options)} {option_type} options using {opt_selector}")
                            break
                            
                    except PlaywrightTimeoutError:
                        continue
                
                # Close dropdown
                await page.keyboard.press("Escape")
                await asyncio.sleep(0.5)
                
                # Filter valid options
                if option_type == 'year':
                    valid_options = [opt for opt in options if self._is_valid_year_option(opt['text'])]
                elif option_type == 'month':
                    valid_options = [opt for opt in options if self._is_valid_month_option(opt['text'])]
                else:
                    valid_options = options
                    
                return valid_options
                
            except Exception as e:
                self.logger.error(f"Error handling ion-input dropdown for {option_type}: {e}")
                return []

    async def get_available_options(self, page: Page, selector: str, option_type: str, site_config: SiteConfig = None) -> List[Dict[str, str]]:
        """Get all available options from a dropdown, filtering out invalid options"""
        if not selector:
            return []
        
        try:
            await page.wait_for_selector(selector, timeout=15000)
            
            print(f"✅ Found {option_type} element with selector: {selector}")
            element_info = await page.evaluate(f'''
            () => {{
                const element = document.querySelector(`{selector}`);
                if (!element) return null;
                return {{
                    tagName: element.tagName,
                    className: element.className,
                    innerHTML: element.innerHTML.substring(0, 200), // First 200 chars
                    textContent: element.textContent,
                    hasOptions: !!element.options,
                    optionsLength: element.options ? element.options.length : 'undefined',
                    childrenCount: element.children.length,
                    outerHTML: element.outerHTML.substring(0, 300) // First 300 chars
                }}
            }}
        ''')
                    
            # Add this right after you print the element details (around line 1190)
            if element_info['tagName'] == 'DIV' and 'mat-mdc-form-field' in element_info['innerHTML']:
                print("🔍 This is a Material Design dropdown - attempting to interact with it")
                
                try:
                    # Try clicking the dropdown to open it
                    await page.click(f"{selector} .mat-mdc-form-field")
                    await asyncio.sleep(3)
                    
                    # Check what options appeared
                    all_selectors = [
                        "mat-option", 
                        ".mat-option", 
                        ".mat-mdc-option",
                        "[role='option']",
                        ".mdc-list-item",
                        "ion-item"
                    ]
                    
                    for sel in all_selectors:
                        count = await page.locator(sel).count()
                        if count > 0:
                            print(f"📋 Found {count} options with selector: {sel}")
                            break
                    else:
                        print("❌ No options found with any selector after clicking")
                        
                    # Check if any dropdown-like elements appeared
                    dropdown_count = await page.locator(".cdk-overlay-pane, .mat-select-panel, .mat-autocomplete-panel").count()
                    print(f"📋 Dropdown panels found: {dropdown_count}")
                    
                    # Close the dropdown
                    await page.keyboard.press("Escape")
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    print(f"❌ Error interacting with Material Design dropdown: {e}")        
            
            # Check if it's an ion-input dropdown
            element_tag = await page.evaluate(f"""
                () => {{
                    const el = document.querySelector('{selector}');
                    return el ? el.tagName.toLowerCase() : null;
                }}
            """)
            
            if element_tag == 'ion-input':
                return await self.handle_ion_input_dropdown(page, selector, option_type)
            
            # Original native select logic for other dropdowns
            # Wait for options to be populated using eval_on_selector
            for attempt in range(10):  # Try up to 10 times
                await asyncio.sleep(1)
                
                try:
                    # Try standard select first
                    print("The standard select")
                    options_count = await page.eval_on_selector(selector, 'el => el.options ? el.options.length : 0')
                    if options_count > 1:  # More than just placeholder
                        break
                except Exception:
                    print("Standard select failed")
                    
                try:
                    print("Trying Material Design dropdown")
                    # Find the clickable element within the selector (could be mat-form-field, button, etc.)
                    clickable_element = await page.evaluate(f'''
                        () => {{
                            const container = document.querySelector(`{selector}`);
                            if (!container) return null;
                            
                            // Try common Material Design clickable selectors
                            const selectors = [
                                '.mat-mdc-form-field',
                                '.mat-form-field', 
                                '.mat-select-trigger',
                                'button',
                                '[role="button"]',
                                '.selectInnerDivDownloads'
                            ];
                            
                            for (let sel of selectors) {{
                                const el = container.querySelector(sel);
                                if (el) return sel;
                            }}
                            return null;
                        }}
                    ''')
                    
                    if clickable_element:
                        # Click the detected element
                        await page.click(f"{selector} {clickable_element}")
                        await asyncio.sleep(0.5)
                        
                        # Check if options appeared
                        mat_options = await page.locator("mat-option, .mat-option, [role='option']").count()
                        if mat_options > 0:
                            await page.keyboard.press("Escape")
                            await asyncio.sleep(0.3)
                            break
                except Exception:
                    continue
            else:
                self.logger.warning(f"No options populated for {option_type} dropdown after waiting")
                return []
            
            try:
                # Get all options
                options = await page.evaluate(f'''
                    () => {{
                        const select = document.querySelector(`{selector}`);
                        if (!select) return [];
                        return Array.from(select.options)
                            .filter(opt => opt.value && opt.value !== '' && opt.value !== '0')
                            .map(opt => ({{
                                value: opt.value,
                                text: opt.textContent || opt.innerText || ''
                            }}));
                    }}
                ''')
            except Exception:
                print("Try Material Design extraction")
                try:
                    # Click to open dropdown
                    await page.click(f"{selector} .mat-mdc-form-field")
                    await asyncio.sleep(0.8)
                    
                    # Extract mat-option elements (we know this works from debug)
                    options = await page.evaluate('''
                        () => {
                            const items = document.querySelectorAll('mat-option');
                            return Array.from(items).map(item => {
                                const text = (item.textContent || '').trim();
                                return { value: text, text: text };
                            });
                        }
                    ''')
                    
                    print(f"📋 Successfully extracted {len(options)} options via Material Design")
                    
                    # Close dropdown
                    await page.keyboard.press("Escape")
                    await asyncio.sleep(0.3)
                    
                except Exception as e:
                    self.logger.warning(f"Cannot extract options from {option_type} dropdown: {e}")
                    return []   
            
            # Filter out invalid options based on option type
            if option_type == 'year':
                valid_options = [opt for opt in options if self._is_valid_year_option(opt['text'], site_config)]
            elif option_type == 'month':
                valid_options = [opt for opt in options if self._is_valid_month_option(opt['text'])]
            else:
                valid_options = [opt for opt in options if not re.match(r'^(all|.*select.*|.*choose.*)$', opt['text'], re.I)]    
            
            self.logger.info(f"Found {len(valid_options)} valid {option_type} options: {[opt['text'] for opt in valid_options]}")
            return valid_options
            
        except Exception as e:
            self.logger.error(f"Error getting {option_type} options: {e}")
            return []
    
    async def handle_tab_selection_2(self, page: Page, site: SiteConfig):
        """Handle tab selection before interacting with dropdowns"""
        if not site.selectors.data_tab_selector:
            return
        
        try:
            self.logger.info('Clicking data tab...')
            
            await page.wait_for_selector(site.selectors.data_tab_selector_2, timeout=15000)
            
            is_visible = await page.is_visible(site.selectors.data_tab_selector_2)
            if not is_visible:
                self.logger.warning('Data tab 2 is not visible, scrolling to view')
                await page.locator(site.selectors.data_tab_selector_2).scroll_into_view_if_needed()
                await asyncio.sleep(1)
            
            await page.click(site.selectors.data_tab_selector_2)
            await asyncio.sleep(site.options.cascade_wait_time)
            
            try:
                await page.wait_for_load_state('networkidle', timeout=5000)
            except PlaywrightTimeoutError:
                self.logger.info('Tab content loading - continuing without full network idle...')
            
            self.logger.info('Data tab 2 clicked successfully - dropdowns should now be available')
            await asyncio.sleep(2)
            
        except PlaywrightTimeoutError:
            self.logger.error('Data tab 2 not found within timeout period')
            raise Exception(f"Required data tab 2 not found: {site.selectors.data_tab_selector_2}")
        except Exception as e:
            self.logger.error(f'Critical error in tab 2 selection: {e}')
            raise e
        
        
    async def handle_tab_selection(self, page: Page, site: SiteConfig):
        """Handle tab selection before interacting with dropdowns"""
        if not site.selectors.data_tab_selector:
            return
        
        try:
            self.logger.info('Clicking data tab...')
            
            await page.wait_for_selector(site.selectors.data_tab_selector, timeout=15000)
            
            is_visible = await page.is_visible(site.selectors.data_tab_selector)
            if not is_visible:
                self.logger.warning('Data tab is not visible, scrolling to view')
                await page.locator(site.selectors.data_tab_selector).scroll_into_view_if_needed()
                await asyncio.sleep(1)
            
            await page.click(site.selectors.data_tab_selector)
            await asyncio.sleep(site.options.cascade_wait_time)
            
            try:
                await page.wait_for_load_state('networkidle', timeout=5000)
            except PlaywrightTimeoutError:
                self.logger.info('Tab content loading - continuing without full network idle...')
            
            self.logger.info('Data tab clicked successfully - dropdowns should now be available')
            await asyncio.sleep(2)
            
            if site.selectors.factsheet_tab_selector:
                self.logger.info('Looking for Factsheets tab...')
                await asyncio.sleep(2)
                
                factsheet_element = page.locator(site.selectors.factsheet_tab_selector).filter(has_text="Factsheets")
                factsheet_count = await factsheet_element.count()
                self.logger.info(f"Found {factsheet_count} Factsheet elements")
                
                if factsheet_count > 0:
                    await factsheet_element.first.click()
                    self.logger.info('Factsheet clicked!')
                    await asyncio.sleep(3)
                else:
                    self.logger.error('No Factsheet tab found')
            
        except PlaywrightTimeoutError:
            self.logger.error('Data tab not found within timeout period')
            raise Exception(f"Required data tab not found: {site.selectors.data_tab_selector}")
        except Exception as e:
            self.logger.error(f'Critical error in tab selection: {e}')
            raise e

    async def collect_download_links(self, page: Page, site: SiteConfig) -> List[str]:
        """Collect download links from the page"""
        try:
            self.logger.info('Collecting download links...')
            
            await asyncio.sleep(3)
            
            # js_link_count = await page.locator("a[href='javascript:void(0)']").count()
            js_link_count = await self.detect_javascript_links(page)
            if js_link_count > 0:
                self.logger.info(f'Found {js_link_count} JavaScript download links, using interception method')
                return await self._collect_js_download_links(page, site)

            if site.selectors.data_container_selector:
                try:
                    await page.wait_for_selector(site.selectors.data_container_selector, timeout=15000)
                    self.logger.info('Data container found')
                except PlaywrightTimeoutError:
                    self.logger.warning('Data container not found, continuing with general link search...')

            link_selector = (site.selectors.address_selector or 
                           site.selectors.download_link_selector or 
                           'a[href*=".pdf"], a[href*="factsheet"], a[href*="download"]')
            
            print(f"DEBUG: Using link selector: {link_selector}")
            
            try:
                await page.wait_for_selector(link_selector, timeout=10000)
                self.logger.info('Download links found')
                
                found_links = await page.evaluate(f'''
                () => {{
                    const links = document.querySelectorAll(`{link_selector}`);
                    return Array.from(links).map(link => {{
                        return {{
                            href: link.href,
                            text: link.textContent.trim(),
                            visible: !link.hidden && link.offsetParent !== null
                        }};
                    }});
                }}
            ''')
                print(f"DEBUG: Found {len(found_links)} links:")
                for link in found_links:
                    print(f"  - {link}")
                    
            except PlaywrightTimeoutError:
                self.logger.warning('No links found with primary selector')

            # Extract both href and text
            # Extract both real links and JavaScript-triggered downloads
            extracted = await page.evaluate(f"""
                () => {{
                    const selector = `{link_selector}`;
                    const els = Array.from(document.querySelectorAll(selector));
                    const rows = els.map(el => {{
                        const text = (el.textContent || '').trim();
                        let href = el.href || '';
                        
                        // If no real href but element looks like a factsheet, create synthetic URL
                        if (!href && /factsheet|fact\\s*sheet|monthly/i.test(text)) {{
                            href = `javascript:download_${{text.replace(/\\s+/g, '_')}}`;
                        }}
                        
                        return {{ href, text }};
                    }});
                    
                    // Filter for both real PDF links AND factsheet text matches
                    return rows.filter(r => {{
                        // Real PDF links
                        if (r.href.startsWith('http') && (r.href.includes('.pdf') || /factsheet|fact\\s*sheet|monthly/i.test(r.text))) {{
                            return true;
                        }}
                        // JavaScript-triggered downloads (synthetic URLs)
                        if (r.href.startsWith('javascript:') && /factsheet|fact\\s*sheet|monthly/i.test(r.text)) {{
                            return true;
                        }}
                        return false;
                    }});
                }}
            """)
            
            seen = set()
            pairs = []
            for r in extracted:
                if r["href"] not in seen:
                    seen.add(r["href"])
                    pairs.append({"url": r["href"], "title": r["text"]})

            self.logger.info(f"Collected {len(pairs)} link-title pairs from table")
            return pairs

        except Exception as e:
            self.logger.warning(f'Link collection failed: {e}')
            return []
        
    async def _collect_js_download_links(self, page: Page, site: SiteConfig) -> List[Dict[str, Any]]:
        """Handle JavaScript download links with request interception"""
        download_urls = []
        
        # Set up request interception
        async def handle_request(request):
            url = request.url
            if ('.pdf' in url.lower() or 
                'download' in url.lower() or 
                'factsheet' in url.lower()):
                print(f"Intercepted download URL: {url}")
                
                # Try to extract month/year from URL
                try:
                    result = self.month_year_from_url(url)
                    year = result[0] 
                    month = result[1]
                    
                    if year and month:
                        file_name = f"Factsheet_{month}_{year}"
                        relevant_date = self.generate_relevant_date(str(month), str(year))
                    else:
                        filename = url.split('/')[-1] if '/' in url else 'Factsheet_Unknown'
                        file_name = filename
                        relevant_date = None
                        year = None
                        month = None
                except:
                    filename = url.split('/')[-1] if '/' in url else 'Factsheet_Unknown'
                    file_name = filename
                    relevant_date = None
                    year = None
                    month = None
                
                download_urls.append({
                    'url': url,
                    'title': file_name,  # Use title key to match your existing structure
                    'relevant_date': relevant_date,
                    'month': str(month),
                    'year': str(year)
                })
        
        # Enable request interception
        page.on("request", handle_request)
        
        # Click all JavaScript download links in the container
        if site.selectors.data_container_selector:
            try:
                await page.wait_for_selector(site.selectors.data_container_selector, timeout=15000)
                container = page.locator(site.selectors.data_container_selector)
                
                # Look for JavaScript download links
                js_links = container.locator("a[href='javascript:void(0)'], a.file-download-link")
                count = await js_links.count()
                
                self.logger.info(f"Clicking {count} JavaScript download links")
                
                for i in range(count):
                    try:
                        link = js_links.nth(i)
                        await link.scroll_into_view_if_needed()
                        await link.click()
                        await asyncio.sleep(1)  # Wait for request to be captured
                    except Exception as e:
                        self.logger.warning(f"Error clicking JS link {i}: {e}")
                        
            except Exception as e:
                self.logger.error(f"Error finding download container: {e}")
        
        # Remove duplicates
        seen = set()
        unique_results = []
        for item in download_urls:
            if item['url'] not in seen:
                seen.add(item['url'])
                unique_results.append(item)
        
        return unique_results

    async def collect_download_links_with_interception(self, page: Page, site: SiteConfig) -> List[Dict[str, Any]]:
        """Collect download links by intercepting network requests for JavaScript links"""
        download_urls = []
        base_url = page.url
        
        # Set up request interception
        async def handle_request(request):
            url = request.url
            # Capture PDF downloads and file downloads
            if ('.pdf' in url.lower() or 
                'download' in url.lower() or 
                'factsheet' in url.lower()):
                print(f"Intercepted download URL: {url}")
                
                # Extract filename from URL or use default
                filename = url.split('/')[-1] if '/' in url else 'Factsheet_Unknown'
                
                # Try to extract month/year from URL
                try:
                    year, month = self.month_year_from_url(url)
                    if year and month:
                        file_name = f"Factsheet_{month}_{year}"
                        relevant_date = self.generate_relevant_date(str(month), str(year))
                    else:
                        file_name = filename
                        relevant_date = None
                except:
                    file_name = filename
                    relevant_date = None
                
                download_urls.append({
                    'url': url,
                    'file_name': file_name,
                    'month': month if 'month' in locals() else '',
                    'year': year if 'year' in locals() else '',
                    'relevant_date': relevant_date
                })
        
        # Enable request interception
        page.on("request", handle_request)
        
        # Now click all the JavaScript download links
        try:
            await asyncio.sleep(3)
            
            # Use your existing selectors
            if site.selectors.data_container_selector:
                await page.wait_for_selector(site.selectors.data_container_selector, timeout=15000)
                container = page.locator(site.selectors.data_container_selector)
                
                # Look for JavaScript links specifically
                js_links = container.locator("a[href='javascript:void(0)'], a.file-download-link")
                count = await js_links.count()
                
                self.logger.info(f"Found {count} JavaScript download links")
                
                for i in range(count):
                    try:
                        link = js_links.nth(i)
                        await link.scroll_into_view_if_needed()
                        await link.click()
                        await asyncio.sleep(1)  # Wait for the request to be made
                    except Exception as e:
                        self.logger.warning(f"Error clicking JS link {i}: {e}")
                        
        except Exception as e:
            self.logger.error(f"Error in JS link collection: {e}")
        
        # Remove duplicates
        seen = set()
        unique_results = []
        for item in download_urls:
            if item['url'] not in seen:
                seen.add(item['url'])
                unique_results.append(item)
        
        return unique_results
        
    async def handle_ul_li_dropdown(self, page: Page, container_selector: str, options_selector: str, option_type: str) -> List[Dict[str, str]]:
        """Handle custom ul/li dropdowns like the Invesco site uses"""
        try:
            self.logger.info(f"Handling ul/li dropdown for {option_type}")
            
            # Use first() to avoid strict mode violation when multiple elements exist
            await page.wait_for_selector(container_selector, timeout=15000)
            
            # Get all li options directly from the specific container
            options = await page.evaluate(f"""
                () => {{
                    // Use querySelector to get first matching element to avoid strict mode violation
                    const container = document.querySelector('{container_selector}');
                    if (!container) return [];
                    
                    const items = container.querySelectorAll('ul li');
                    return Array.from(items).map(item => {{
                        const value = item.getAttribute('value') || item.textContent.trim();
                        const text = item.textContent.trim();
                        return {{ value, text }};
                    }});
                }}
            """)
            
            # Filter valid options
            if option_type == 'year':
                valid_options = [opt for opt in options if self._is_valid_year_option(opt['text'])]
            elif option_type == 'month':
                valid_options = [opt for opt in options if self._is_valid_month_option(opt['text'])]
            else:
                valid_options = options
                
            # Remove duplicates based on text
            seen = set()
            unique_options = []
            for opt in valid_options:
                if opt['text'] not in seen:
                    seen.add(opt['text'])
                    unique_options.append(opt)
            
            self.logger.info(f"Found {len(unique_options)} valid {option_type} options: {[opt['text'] for opt in unique_options]}")
            return unique_options
            
        except Exception as e:
            self.logger.error(f"Error handling ul/li dropdown for {option_type}: {e}")
            return []

    async def select_ul_li_option(self, page: Page, container_selector: str, target_value: str, option_type: str):
        """Select an option from ul/li dropdown using specific container"""
        try:
            self.logger.info(f"Selecting {option_type} option: {target_value}")
            
            # Click the matching li element within the specific container
            clicked = await page.evaluate(f"""
                () => {{
                    // Use querySelector to get first matching container
                    const container = document.querySelector('{container_selector}');
                    if (!container) return false;
                    
                    const items = container.querySelectorAll('ul li');
                    for (let item of items) {{
                        const value = item.getAttribute('value') || item.textContent.trim();
                        const text = item.textContent.trim();
                        if (value === '{target_value}' || text === '{target_value}') {{
                            // Add visual feedback
                            item.classList.add('bluetabsel2');
                            // Remove active class from siblings
                            items.forEach(sibling => {{
                                if (sibling !== item) {{
                                    sibling.classList.remove('bluetabsel2');
                                }}
                            }});
                            item.click();
                            return true;
                        }}
                    }}
                    return false;
                }}
            """)
            
            if clicked:
                await asyncio.sleep(2.0)  # Wait longer for content to load
                self.logger.info(f"Successfully selected {option_type}: {target_value}")
            else:
                raise Exception(f"Could not find {option_type} option: {target_value}")
                
        except Exception as e:
            self.logger.error(f"Error selecting {option_type} option {target_value}: {e}")
            raise e    
    
    async def save_results_to_csv(self):
        """Save results to CSV file"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_filename = f'AMC_Factsheet_Links_{timestamp}.csv'
        
        
        try:
            with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['Company', 'AMC_Name', 'File_name', 'Relevant_date', 'Runtime', 'File_Link','Month','Year']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                
                if not self.results:
                    self.logger.warning('No results to save')
                    return
                
                for result in self.results:
                    writer.writerow(asdict(result))
                
            self.logger.info(f'Results saved to: {csv_filename}')
            self.logger.info(f'Total records: {len(self.results)}')
            
            companies = set(result.Company for result in self.results)
            amc_names = set(result.AMC_Name for result in self.results)
            
            print("\nCrawl Summary:")
            print(f"   Companies processed: {len(companies)}")
            print(f"   AMCs processed: {len(amc_names)}")
            print(f"   Total links collected: {len(self.results)}")
            print(f"   Output file: {csv_filename}")
            
        except Exception as e:
            self.logger.error(f'Failed to save CSV file: {e}')

async def main():
    """Main function to run the crawler"""
    import sys
    
    provided =r"C:\Users\Administrator\AdQvestDir\codes\Adqvest_Crawler\AMC_CRAWLER\amc_config.json" # raw string to avoid escape issues
    config_path = Path(provided).resolve()
    print(f"Resolved config: {config_path}")
    
    if not Path(config_path).exists():
        print('Config file not found. Please provide a valid config file path.')
        print('Usage: python Crawlee.py <config_file.json>')
        sys.exit(1)
        
    crawler = AMCFactsheetCrawler(config_path)
    
    # test_url = "https://www.sundarammutual.com/Fundwise-Factsheet"
    # if crawler.config:
    #     crawler.config[0].url = test_url  # <-- Override here
    #     print(f"Overriding URL to: {test_url}")
    
    try:
        await crawler.crawl_all_sites()
        print('Crawling completed successfully!')
    except Exception as e:
        print(f'Crawling failed: {e}')
        sys.exit(1)

if __name__ == '__main__':
    asyncio.run(main())