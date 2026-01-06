import time
import boto3
import os
import requests
import sys
import traceback
import threading
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.config import Config
import httpx
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import re
from pytz import timezone
import datetime

import warnings
warnings.filterwarnings('ignore')

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')

#---- DATABASE AND COMPANY FUNCTIONS
import adqvest_db
import JobLogNew as log
import dbfunctions

from zenrows import ZenRowsClient
zen_req = ZenRowsClient("eabc162057980f957e89b6e71cb469e438b41393")

class ThurroSpyderTest:
    def __init__(self, table_configs):
        self.table_configs = table_configs
        self.table_names = [t[0] for t in table_configs]
        self.s3_folder_map = {t[0]: (t[1], t[2]) for t in table_configs}
        self.is_running = False
        self.processed_count = 0
        self.failed_count = 0
        self.lock = threading.Lock()
        
        # Configuration
        self.max_time_per_table = 60
        self.max_parallel_downloads = 5
        self.batch_size = 5
        
        # 🕐 COOLDOWN/DELAY SETTINGS
        self.request_delay = 5.0  # Delay between requests (seconds)
        self.table_delay = 5.0   # Delay between tables (seconds)
        self.enable_delays = True # Enable/disable delay system
        
        self.setup_spyder_logging()
        
        print("Spyder Test Service Initialized")
        print(f"Tables: {', '.join(self.table_names)}")
        print(f"Max time per table: {self.max_time_per_table}s")
        print(f"Max parallel downloads: {self.max_parallel_downloads}")
        print(f"Batch size: {self.batch_size}")
        print(f"Request delay: {self.request_delay}s")
        print(f"Table delay: {self.table_delay}s")
        print(f"Delays enabled: {self.enable_delays}")
        
    def setup_spyder_logging(self):
        """Simple logging for Spyder console"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(message)s',
            handlers=[logging.StreamHandler(sys.stdout)]
        )
        self.logger = logging.getLogger('SpyderTest')
        
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("httpcore").setLevel(logging.WARNING)
        
    def get_engine(self):
        """Get SQLAlchemy engine"""
        try:
            engine = adqvest_db.db_conn()
            return engine
        except Exception as e:
            print(f"Error getting engine: {e}")
            return None    

    def test_database_connection(self):
        """Test database connection"""
        print("Testing database connection...")
        
        for table_name in self.table_names:
            try:
                engine = self.get_engine()
                if not engine:
                    print("Could not get database engine")
                    return             
                
                with engine.connect() as conn:
                    from sqlalchemy import text
                    
                    query = text(f"""
                        SELECT COUNT(*) 
                        FROM {table_name} 
                        WHERE S3_Upload_Status IS NULL 
                        and (S3_Upload_Comments is Null or S3_Upload_Comments not in ('SITE ISSUE','Not Relevant','SIZE ISSUE','File Not Available')) 
                    """)
                    
                    result = conn.execute(query)
                    count = result.fetchone()[0]
                    
                    print(f"{table_name}: {count} pending links")
                
            except Exception as e:
                print(f"{table_name}: Database error - {e}")
        
        print("Database connection test completed")

    def get_test_links(self, table_name, limit=3):
        """Get few links for testing"""
        try:
            engine = self.get_engine()
            if not engine:
                return []
            
            with engine.connect() as conn:
                from sqlalchemy import text
                
                if table_name == 'BSE_ANNOUNCEMENT_ANNUAL_REPORT_COMPANY_WISE_YEARLY_DATA_CORPUS':
                    query = text(f"""
                        SELECT File_ID, PDF_Link,Encoded_PDF_Link,File_Name, Relevant_Date 
                        FROM {table_name} 
                        WHERE S3_Upload_Status IS NULL 
                        and (S3_Upload_Comments is Null or S3_Upload_Comments not in ('SITE ISSUE','SIZE ISSUE','File Not Available')) 
                        ORDER BY Relevant_Date DESC
                        LIMIT {limit}
                    """)
                    rows = conn.execute(query).fetchall()
                    links = []
                    for row in rows:
                        # Pick preferred link
                        link = row["Encoded_PDF_Link"] or row["PDF_Link"]
    
                        if not link:
                            print("NO LINK → skipping")
                            continue
                        
                        links.append((row["File_ID"], link, row['File_Name'], row["Relevant_Date"]))
                    return [tuple(row) for row in links]
                        
                elif table_name == 'BANK_BASEL_III_QUARTERLY_LINKS':
                    query = text(f"""
                        SELECT File_ID, File_Link, Generated_File_Name, Relevant_Date 
                        FROM {table_name} 
                        WHERE S3_Upload_Status IS NULL 
                        and (S3_Upload_Comments is Null or S3_Upload_Comments not in ('SITE ISSUE','SIZE ISSUE','File Not Available')) 
                        ORDER BY Relevant_Date DESC
                        LIMIT {limit}
                    """)
                    result = conn.execute(query)
                    links = result.fetchall()
                    return [tuple(row) for row in links]
                
                elif table_name == 'CAG_AUDIT_REPORTS_RANDOM_DATA_CORPUS':
                    query = text(f"""
                        SELECT File_ID, Report_Link, File_Name, Relevant_Date 
                        FROM {table_name} 
                        WHERE S3_Upload_Status IS NULL 
                        and (S3_Upload_Comments is Null or S3_Upload_Comments not in ('SITE ISSUE','SIZE ISSUE','File Not Available')) 
                        ORDER BY Relevant_Date DESC
                        LIMIT {limit}
                    """)
                    result = conn.execute(query)
                    links = result.fetchall()
                    return [tuple(row) for row in links]
                
                elif table_name == 'CAG_STATE_WISE_ACCOUNT_MONTHLY_DATA_CORPUS':
                    query = text(f"""
                        SELECT File_ID, File_Link, File_Name, Relevant_Date 
                        FROM {table_name} 
                        where S3_Upload_Status is null and Is_Valid='Yes'
                        ORDER BY Relevant_Date DESC
                        LIMIT {limit}
                    """)
                    result = conn.execute(query)
                    links = result.fetchall()
                    return [tuple(row) for row in links]

                elif table_name == 'CMOTS_BSE_CORPORATE_ANNOUNCEMENTS_COMPANY_WISE_DAILY_DATA_CORPUS':
                    query = text(f"""
                        SELECT File_ID, File_Link, File_Name, Relevant_Date 
                        FROM {table_name} 
                        where S3_Upload_Status is null
                        ORDER BY Relevant_Date DESC
                        LIMIT {limit}
                    """)
                    result = conn.execute(query)
                    links = result.fetchall()
                    return [tuple(row) for row in links]

                elif table_name == 'CMOTS_NSE_CORPORATE_ANNOUNCEMENTS_COMPANY_WISE_DAILY_DATA_CORPUS':
                    query = text(f"""
                        SELECT File_ID, File_Link, File_Name, Relevant_Date 
                        FROM {table_name} 
                        where S3_Upload_Status is null
                        ORDER BY Relevant_Date DESC
                        LIMIT {limit}
                    """)
                    result = conn.execute(query)
                    links = result.fetchall()
                    return [tuple(row) for row in links]            
                                
                try:
                    query = text(f"""
                        SELECT File_ID, File_Link, File_Name, Relevant_Date 
                        FROM {table_name} 
                        WHERE S3_Upload_Status IS NULL 
                        and (S3_Upload_Comments is Null or S3_Upload_Comments not in ('SITE ISSUE','SIZE ISSUE','File Not Available')) 
                        ORDER BY Relevant_Date DESC
                        LIMIT {limit}
                    """)
                    result = conn.execute(query)
                    links = result.fetchall()
                    
                except Exception:
                    try:
                        query = text(f"""
                            SELECT File_ID, Rating_File_Link, Generated_File_Name, Relevant_Date 
                            FROM {table_name} 
                            WHERE S3_Upload_Status IS NULL 
                            and (S3_Upload_Comments is Null or S3_Upload_Comments not in ('SITE ISSUE','SIZE ISSUE','File Not Available')) 
                            ORDER BY Relevant_Date DESC
                            LIMIT {limit}
                        """)
                        result = conn.execute(query)
                        links = result.fetchall()
                    except:
                        query = text(f"""
                            SELECT File_ID, File_Link, Generated_File_Name, Relevant_Date 
                            FROM {table_name} 
                            WHERE S3_Upload_Status IS NULL 
                            and (S3_Upload_Comments is Null or S3_Upload_Comments not in ('SITE ISSUE','SIZE ISSUE','File Not Available')) 
                            ORDER BY Relevant_Date DESC
                            LIMIT {limit}
                        """)
                        result = conn.execute(query)
                        links = result.fetchall()
                        
                
                return [tuple(row) for row in links]
         
        except Exception as e:
            print(f"Error getting test links: {e}")
            return []

    # ========================================
    # PARALLEL PROCESSING METHODS
    # ========================================
    
    def test_multiple_tables_parallel(self, table_limits=None):
        """Process MULTIPLE TABLES simultaneously with parallel downloads"""
        print("\nTESTING MULTIPLE TABLES IN PARALLEL")
        print("=" * 70)
        
        if not table_limits:
            # Default limits for each table
            table_limits = {table: 50 for table in self.table_names}
        
        try:
            start_time = time.time()
            time.sleep(5)
            
            # Create a master thread pool for ALL tables and ALL links
            max_total_workers = self.max_parallel_downloads * len(self.table_names)
                        
            # Collect ALL links from ALL tables
            all_tasks = []
            table_link_counts = {}
            
            print("\n📥 Collecting links from all tables...")
            for table_name in self.table_names:
                limit = table_limits.get(table_name, 50)
                links = self.get_test_links(table_name, limit)
                table_link_counts[table_name] = len(links)
                time.sleep(5)
                
                if links:
                    # Add table name to each link for identification
                    for link in links:
                        all_tasks.append((link, table_name))
                    print(f" {table_name}: Links found")
                else:
                    print(f"{table_name}: No links found")
            
            total_links = len(all_tasks)
            print(f"\nTotal links to process: {total_links}")
            
            if not all_tasks:
                print("No links found in any table")
                return
            
            # Process ALL links from ALL tables in parallel
            with ThreadPoolExecutor(max_workers=max_total_workers, thread_name_prefix="MultiTable") as executor:
                # Submit all tasks at once
                future_to_task = {
                    executor.submit(self.test_single_link_parallel, link_data, table_name): (link_data, table_name)
                    for link_data, table_name in all_tasks
                }
                
                # Track progress
                completed = 0
                successful = 0
                table_success = {table: 0 for table in self.table_names}
                table_completed = {table: 0 for table in self.table_names}
                
                print(f"\nPARALLEL PROCESSING STARTED - {total_links} links across {len(self.table_names)} tables")
                print("-" * 70)
                
                # Process results as they complete
                for future in as_completed(future_to_task):
                    link_data, table_name = future_to_task[future]
                    link_id = link_data[0]
                    time.sleep(2)
                    
                    try:
                        success = future.result()
                        completed += 1
                        table_completed[table_name] += 1
                        
                        if success:
                            successful += 1
                            table_success[table_name] += 1
                      
                        # Show progress with table info
                        table_progress = f"{table_completed[table_name]}/{table_link_counts[table_name]}"
                        print(f"[{completed}/{total_links}] {table_name}[{table_progress}] - Link {link_id}")
                        
                        # Show summary every 10 completions
                        if completed % 10 == 0:
                            self.print_progress_summary(table_completed, table_success, table_link_counts)
                        
                    except Exception as e:
                        completed += 1
                        table_completed[table_name] += 1
                        print(f"[{completed}/{total_links}] {table_name} - Error: {e}")
            
            elapsed_time = time.time() - start_time
            
            # Final comprehensive summary
            print("\n" + "=" * 70)
            print("MULTI-TABLE PARALLEL PROCESSING COMPLETED!")
            print("=" * 70)
            
            print(f"Total time: {elapsed_time:.2f} seconds")
            print(f"Overall success: {successful}/{total_links} ({100*successful/total_links:.1f}%)")
            print(f"Average speed: {total_links/elapsed_time:.1f} links/second")
            
            print("\n📋 Per-table breakdown:")
            for table_name in self.table_names:
                table_total = table_link_counts[table_name]
                table_succ = table_success[table_name]
                time.sleep(3)
                if table_total > 0:
                    success_rate = 100 * table_succ / table_total
                    print(f"  📄 {table_name}: {table_succ}/{table_total} ({success_rate:.1f}%)")
                else:
                    print(f"  📄 {table_name}: No links processed")
            
        except Exception as e:
            print(f"Multi-table parallel processing error: {e}")
            traceback.print_exc()
    
    def print_progress_summary(self, completed, successful, totals):
        """Print progress summary during processing"""
        print("\nPROGRESS UPDATE:")
        for table_name in self.table_names:
            time.sleep(1)
            if totals[table_name] > 0:
                comp = completed[table_name]
                succ = successful[table_name]
                total = totals[table_name]
                rate = 100 * succ / comp if comp > 0 else 0
                print(f"   {table_name}: {comp}/{total} done ({succ}, {rate:.0f}% success)")
        print("-" * 50)
    
    def test_single_link_parallel(self, link_data, table_name):
        """Process one link (thread-safe for parallel)"""
        link_id, url, filename, created_at = link_data
        thread_name = threading.current_thread().name
        
        try:
            
            local_path = self.test_download_parallel(url, filename, table_name, thread_name)
            if not local_path:
                return False
            
            k1, k2 = self.s3_folder_map[table_name]
            
            s3_key = self.test_s3_upload_parallel(local_path, filename, table_name, thread_name,k1,k2)
            
            if not s3_key:
                return False

            status = 'Done'
            success = self.test_db_update_parallel(link_id,filename,url, status, table_name, thread_name)
            
            if os.path.exists(local_path):
                try:
                    os.remove(local_path)
                    print(f"Local file deleted: {local_path}")
                except Exception as e:
                    print(f"Failed to delete local file: {e}")
            
            if success:
                with self.lock:
                    self.processed_count += 1
            
            print(f"Completed: {filename}")
            return True
            
        except Exception as e:
            print(f"Failed: {filename} - {e}")
            return False
            
    def test_download_parallel(self, url, filename, table_name, thread_name):
        """Thread-safe download with shared folder and conflict resolution"""
        try:
            # Use same folder structure as sequential - shared folder
            temp_dir = f"C:/Users/Administrator/AdQvestDir/S3_UPLOAD_FOLDER/{table_name}"
            os.makedirs(temp_dir, exist_ok=True)
            
            safe_filename = "".join(c for c in filename if c.isalnum() or c in (' ', '.', '_', '-')).rstrip()
            local_path = os.path.join(temp_dir, safe_filename)
            
            engine = self.get_engine()
            time.sleep(3)
            
            # Handle filename conflicts by adding thread identifier only if needed
            if os.path.exists(local_path):
                name_part, ext = os.path.splitext(safe_filename)
                safe_filename = f"{name_part}{ext}"
                local_path = os.path.join(temp_dir, safe_filename)

            if table_name == 'NSE_DOC_LINKS_FROM_CORPUS_DAILY_DATA':
                HEADERS = {
                            "User-Agent": (
                                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
                            ),
                            "Accept": "*/*",
                            "Accept-Language": "en-US,en;q=0.9",
                            "Connection": "keep-alive",
                            "Referer": "https://www.google.com/",
                        }

                session = requests.Session()
                retry = Retry(total=5, backoff_factor=5)
                adapter = HTTPAdapter(max_retries=retry)
                session.mount('http://', adapter)
                session.mount('https://', adapter)
                session.verify = True
                pdf_url = re.sub(r"[\x00-\x1F]+", "-", url).rstrip(".")
                pdf_url = re.sub(r'\.pdf.*$', '.pdf', pdf_url, flags=re.IGNORECASE).strip()
                pdf_url = pdf_url.replace('?', '-')
                pdf_url = pdf_url.replace("9nancial", "financial")

                response=requests.get(pdf_url,headers=HEADERS,timeout=60,verify=False)

                if response.status_code == 200:
                    if b"not found" in response.content.lower() or b"internal server error" in response.content.lower():
                        return None
                    with open(local_path, 'wb') as file:
                        file.write(response.content)

                    file_size = os.path.getsize(local_path)  # bytes
                    size_threshold = 1 * 1024  # 1 KB

                    if file_size > size_threshold:
                        print(f"File size ({file_size} bytes) is greater than 1 KB; proceeding with S3 upload.")
                        return local_path

                elif response.status_code == 302:
                    print(f"Status: {response.status_code}")
                    return None
                
                elif response.status_code == 404:
                    print(f"Failed to download file from {url}. Status code: {response.status_code}")
                    connection=engine.connect()
                    connection.execute(f"update {table_name} set S3_Upload_Comments='File Not Available' where File_Name = '" +str(filename)+"'")
                    connection.execute("commit")
                    return None 

                elif response.status_code == 403: 
                    print(f"Failed to download file from {url}. Status code: {response.status_code}")
                    return None    

            if table_name == 'BANK_BASEL_III_QUARTERLY_LINKS':
                print('inside BANK BASEL')
                response=zen_req.get(url,timeout=300)
                if response.status_code!=200:
                    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 Edg/107.0.1418.56"}
                    response=requests.get(url,headers=headers,verify=False)
                    print(response.status_code)
                    print('gotten from req')
                else:
                    print('gotten from zenrows')
                    print(response.status_code)  

                with open(local_path, 'wb') as file:
                    file.write(response.content)

                file_size = os.path.getsize(local_path)  # bytes
                size_threshold = 1 * 1024  # 1 KB

                if file_size > size_threshold:
                    print(f"File size ({file_size} bytes) is greater than 1 KB; proceeding with S3 upload.")
                    return local_path
                           
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'}
            
            time.sleep(self.request_delay)
                        
            response = httpx.get(url, headers=headers, verify=False, follow_redirects=True, timeout=30)
            # print(f"[{thread_name}] Try done - Status: {response.status_code}")
            
            if response.status_code != 200:
                try:
                    time.sleep(self.request_delay)
                    headers = {'user-agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) ''AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36')}
                    response = requests.get(url, headers=headers,verify=False, follow_redirects=True,timeout=30)
                except:
                    response=zen_req.get(url,timeout=100)
                            
            if response.status_code == 200:
                if b"not found" in response.content.lower() or b"internal server error" in response.content.lower():
                    return None
                with open(local_path, 'wb') as file:
                    file.write(response.content)

                file_size = os.path.getsize(local_path)  # bytes
                size_threshold = 1 * 1024  # 1 KB

                if file_size > size_threshold:                
                    return local_path
            
            elif response.status_code == 302:
                return None
            
            elif response.status_code == 404:
                print(f"Failed to download file from {url}. Status code: {response.status_code}")
                connection=engine.connect()
                connection.execute(f"update {table_name} set S3_Upload_Comments='File Not Available' where File_Name = '" +str(filename)+"'")
                connection.execute("commit")
                return None
            
        except Exception as e:
            print(f"Download failed FOR {table_name}: {e}")
            return None

    def test_s3_upload_parallel(self, local_path, filename, table_name, thread_name,k1,k2):
        """Thread-safe S3 upload simulation"""
        try:
            s3_key_main = k1
            s3_key_backup = k2
            
            dbfunctions.to_s3bucket(local_path, s3_key_main)
            dbfunctions.to_s3bucket(local_path, s3_key_backup)
            
            print(f"Uploaded to S3: {s3_key_main} and {s3_key_backup}")
            return s3_key_main  # Return the main S3 key for DB update
            
        except Exception as e:
            print(f"S3 upload failed: {e}")
            return None

    def test_db_update_parallel(self, link_id,filename,url, status, table_name, thread_name):
        """Thread-safe database update"""
        try:
            engine = self.get_engine()
            
            connection=engine.connect()
            time.sleep(2)
            if table_name == 'BANK_BASEL_III_QUARTERLY_LINKS':
                connection.execute(f"UPDATE {table_name} SET S3_Upload_Status='Done' WHERE File_ID = %s AND Generated_File_Name = %s",(link_id, filename))
                print(f"{table_name} Database updated: File_ID {link_id} = {status}")
                return True

            connection.execute(f"UPDATE {table_name} SET S3_Upload_Status='Done' WHERE File_ID = %s AND File_Name = %s",(link_id, filename))
            print(f"{table_name} Database updated: File_ID {link_id} = {status}")
            return True
            
        except Exception as e:
            print(f"{table_name} Database update failed: {e}")
            return False

# ========================================
# TESTING FUNCTIONS
# ========================================

def create_test_service():
    """Create test service instance"""
    
    test_tables = [
        ('NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS', 'NSE_INVESTOR_INFORMATION_CORPUS_2/', 'NSE_INVESTOR_INFORMATION_CORPUS_TO_BE_CHUNKED_2/'),
        ('BSE_ANNOUNCEMENT_ANNUAL_REPORT_COMPANY_WISE_YEARLY_DATA_CORPUS', 'BSE_ANNUAL_REPORTS_CORPUS_NEW/', 'BSE_ANNUAL_REPORTS_CORPUS_TO_BE_CHUNKED_NEW/'),
        ('CARE_RATINGS_DAILY_DATA_CORPUS', 'CARE_RATINGS_CORPUS/', 'CARE_RATINGS_CORPUS_TO_BE_CHUNKED/'),
        ('INDIA_BUDGET_SPEECHES_YEARLY_DATA_CORPUS', 'GOI_BUDGET_SPEECHES_CORPUS/', 'GOI_BUDGET_SPEECHES_CORPUS_TO_BE_CHUNKED/'),
        ('NSE_MARKET_PULSE_MONTHLY_DATA_CORPUS', 'NSE_MARKET_PULSE_CORPUS_2/', 'NSE_MARKET_PULSE_CORPUS_TO_BE_CHUNKED_2/'),
        ('SEBI_FINAL_OFFER_DOCUMENTS_RHP_DRHP_DAILY_DATA_CORPUS', 'SEBI_RHP_DRHP_CORPUS_2/', 'SEBI_RHP_DRHP_CORPUS_TO_BE_CHUNKED_2/'),
        ('RBI_PRESS_RELEASES_DAILY_DATA_CORPUS', 'RBI_PRESS_RELEASES_DAILY_CORPUS/', 'RBI_PRESS_RELEASES_DAILY_CORPUS_TO_BE_CHUNKED/'),
        ('INDIA_UNION_BUDGET_YEARLY_DATA_CORPUS', 'INDIA_BUDGET_YEARLY_CORPUS/', 'INDIA_BUDGET_YEARLY_TO_BE_CHUNKED/'),
        ('INDIA_BUDGET_ECONOMIC_SURVEY_YEARLY_DATA_CORPUS', 'GOI_BUDGET_ECONOMIC_SURVEY_CORPUS/', 'GOI_BUDGET_ECONOMIC_SURVEY_TO_BE_CHUNKED/'),
        ('BANK_BASEL_III_QUARTERLY_LINKS', 'BANK_BASEL_III_DISCLOSURES_CORPUS/', 'BANK_BASEL_III_DISCLOSURES_CORPUS_TO_BE_CHUNKED/'),
        ('DEA_ECONOMY_REPORT_MONTHLY_DATA_CORPUS', 'DEA_MONTHLY_ECONOMY_CORPUS/', 'DEA_MONTHLY_ECONOMY_CORPUS_TO_BE_CHUNKED/'),
        ('NSE_DOC_LINKS_FROM_CORPUS_DAILY_DATA', 'NSE_DOC_LINKS_FROM_CORPUS/', 'NSE_DOC_LINKS_FROM_CORPUS_DAILY_DATA_TO_BE_CHUNKED/'),
        ('RBI_ANNUAL_REPORT_YEARLY_DATA_CORPUS', 'RBI_ANNUAL_REPORT_CORPUS/', 'RBI_ANNUAL_REPORT_CORPUS_TO_BE_CHUNKED/'),
        ('RBI_BULLETIN_REPORTS_MONTHLY_DATA_CORPUS', 'RBI_BULLETIN_REPORT_CORPUS/', 'RBI_BULLETIN_REPORT_CORPUS_TO_BE_CHUNKED/'),
        ('SEBI_INVIT_ISSUES_DAILY_DATA_CORPUS', 'SEBI_INVIT_CORPUS/', 'SEBI_INVIT_CORPUS_TO_BE_CHUNKED/'),
        ('SEBI_REIT_ISSUE_DAILY_DATA_CORPUS', 'SEBI_REIT_CORPUS/', 'SEBI_REIT_CORPUS_TO_BE_CHUNKED/'),
        ('AMC_WISE_MF_FACTSHEETS_MONTHLY_DATA_CORPUS', 'AMC_WISE_MF_FACTSHEETS_CORPUS/', 'AMC_WISE_MF_FACTSHEETS_CORPUS_TO_BE_CHUNKED/'),
        ('SEBI_LEGAL_CIRCULARS_RANDOM_DATA_CORPUS', 'SEBI_LEGAL_CIRCULAR_CORPUS/', 'SEBI_LEGAL_CIRCULAR_CORPUS_TO_BE_CHUNKED/'),
        ('CMOTS_BSE_CORPORATE_ANNOUNCEMENTS_COMPANY_WISE_DAILY_DATA_CORPUS', 'CMOTS_BSE_CORPUS/', 'CMOTS_BSE_CORPUS_TO_BE_CHUNKED/'),
        ('CMOTS_NSE_CORPORATE_ANNOUNCEMENTS_COMPANY_WISE_DAILY_DATA_CORPUS','CMOTS_NSE_CORPUS/','CMOTS_NSE_CORPUS_TO_BE_CHUNKED/')

        ]

    service = ThurroSpyderTest(test_tables)
    return service

def quick_parallel_test():
    """Quick parallel test"""
    print("QUICK PARALLEL TEST STARTING")
    
    try:
        service = create_test_service()
        service.test_database_connection()
        
        # Test parallel processing
        if service.table_names:
            table_limits = {table: 100 for table in service.table_names}  # 50 links per table
            service.test_multiple_tables_parallel(table_limits)
        
        print("Quick parallel test completed")
        
    except Exception as e:
        print(f"Quick parallel test failed: {e}")
        traceback.print_exc()

# ========================================
# SPYDER USAGE INSTRUCTIONS
# ========================================

def run_program(run_by = 'Adqvest_Bot', py_file_name = None):
    # os.chdir('/home/ubuntu/AdQvestDir')
    os.chdir('C:/Users/Administrator/AdQvestDir')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    india_time = timezone('Asia/Kolkata')

    job_start_time = datetime.datetime.now(india_time)
    table_name = 'GENERIC_S3_UPLOAD'
    scheduler = ''
    no_of_ping = 0

    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]

    try :
        if(run_by == 'Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        quick_parallel_test()
        
        log.job_end_log(table_name,job_start_time,no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        error_msg = str(sys.exc_info()[1]) + " line " + str(exc_tb.tb_lineno)
        print(error_msg)

        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')        
    
    print("""
🎯 SPYDER PARALLEL TESTING INSTRUCTIONS:
('CRISIL_DAILY_DATA_CORPUS', 'CRISIL_RATINGS_CORPUS/', 'CRISIL_RATINGS_CORPUS_TO_BE_CHUNKED/'),
('ICRA_DAILY_DATA_CORPUS', 'ICRA_CORPUS/', 'ICRA_CORPUS_TO_BE_CHUNKED/'),

('PIB_REPORTS_DAILY_DATA_CORPUS', 'PIB_CORPUS/', 'PIB_CORPUS_TO_BE_CHUNKED/'),
('ANAROCK_MARKET_VIEWPOINTS_MONTHLY_DATA_CORPUS', 'ANAROCK_HOSPITALITY_MARKET_VIEWPOINTS_CORPUS/', 'ANAROCK_HOSPITALITY_MARKET_VIEWPOINTS_CORPUS_TO_BE_CHUNKED/'),
('CAG_STATE_WISE_ACCOUNT_MONTHLY_DATA_CORPUS', 'CAG_STATE_WISE_ACCOUNT_MONTHLY_DATA_CORPUS/', 'CAG_STATE_WISE_ACCOUNT_MONTHLY_DATA_CORPUS_TO_BE_CHUNKED/'),
('CAG_AUDIT_REPORTS_RANDOM_DATA_CORPUS', 'CAG_AUDIT_REPORT_FILES/', 'CAG_AUDIT_REPORT_FILES_TO_BE_CHUNKED/'),

('TRANSUNION_CIBIL_REPORTS_MONTHLY_DATA_CORPUS', 'TRANSUNION_CIBIL_REPORTS_MONTHLY_DATA_CORPUS/', 'TRANSUNION_CIBIL_REPORTS_MONTHLY_DATA_CORPUS_TO_BE_CHUNKED/')""")

    
