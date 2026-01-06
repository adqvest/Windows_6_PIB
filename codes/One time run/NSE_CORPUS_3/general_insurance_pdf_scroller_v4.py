import os, sys, requests, re, tldextract, time, shutil, pytz, ast, PyPDF2, pathlib, httpx
import pandas as pd
from google import genai
from google.genai import types
from PyPDF2 import PdfFileReader, PdfFileWriter
from io import BytesIO
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from itertools import product
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options


urls = [
 'https://www.acko.com/gi/public-disclosure/',
 'https://www.bajajallianz.com/about-us/financial-information.html',
 'https://www.cholainsurance.com/public-disclosure',
 'https://www.hizuno.com/public-disclosure',
 'https://life.futuregenerali.in/about-us/public-disclosures',
 'https://www.godigit.com/financials',
 'https://www.hdfcergo.com/about-us/financial/public-disclosures',
 'https://www.icicilombard.com/about-us/public-disclosure',
 'https://www.iffcotokio.co.in/about-us/public-disclosure',
 'https://www.zurichkotak.com/documents/public-disclosure',
 'https://www.libertyinsurance.in/products/irdai/irdaiindex',
 'https://www.magmahdi.com/public-disclosures',
 'https://nationalinsurance.nic.co.in/about-us/public-disclosure/non-life-disclosure-forms',
 'https://navi.com/insurance/corporate-governance/public-disclosure',
 'https://www.rahejaqbe.com/public-disclosures',
 'https://www.reliancegeneral.co.in/insurance/about-us/public-disclosure-rgi.aspx',
 'https://www.royalsundaram.in/about-us/public-disclosures',
 'https://www.sbigeneral.in/about-us/public-disclosure',
 'https://www.shriramgi.com/download/disclosure',
 'https://www.tataaig.com/public-disclosures',
 'https://www.newindia.co.in/public-disclosure',
 'https://orientalinsurance.org.in/public-disclosures',
 'https://uiic.co.in/public-disclosures',
 'https://www.universalsompo.com/public-disclosure',
 'https://transactions.nivabupa.com/pages/public-disclosures.aspx',
 'https://www.adityabirlacapital.com/healthinsurance/downloads',
 'https://www.careinsurance.com/public-disclosures.html',
 'https://www.galaxyhealth.com/public-disclosures',
 'https://www.manipalcigna.com/disclosures/public-disclosures',
 'https://www.narayanahealth.insurance/disclosures/',
 'https://www.starhealth.in/media-center/',
 'https://kshema.co/public-disclosures/',
 'https://www.aicofindia.com/public-disclosures',
 'https://main.ecgc.in/english/public-disclosures/'
]

HEADERS = {
 "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:110.0) Gecko/20100101 Firefox/110.0",
 "Accept-Language": "en-US,en;q=0.9",
 "Accept-Encoding": "gzip, deflate, br",
     #"Accept": "*/*",
 "Referer": "https://www.google.com/",
 "DNT": "1",
 "Connection": "keep-alive",
 "Upgrade-Insecure-Requests": "1",
 "Sec-Fetch-Mode": "navigate",
 "Sec-Fetch-Site": "same-origin",
 "Sec-Fetch-Dest": "document",
}
ABS_URL_STARTING = ('http://', 'https://', 'www.')
URL_ENDING = ('.html', '.htm', '.php', '.asp', '.aspx', '.jsp', '.cfm', '.shtml', '/')
web_pages = []
meta_data_framework = pd.DataFrame({'PDF_File': [], 'Info': [], 'Detailed_Info': [], 'Company_Name': [], 'Report': [], 'Report_Type': [], 'Quarter': [], 'Year': [], 'Relevent_Date': [], 'Date': [], 'RunTime': [], 'URL': [], 'Relevant_Quarter': [], 'File_ID': [], 'File_Name': []})
non_meta_data_framework = pd.DataFrame({'PDF_File': [], 'Detailed_Info': [], 'Company_Name': [], 'Quarter': [], 'Year': [], 'URL': []})
downloads = Path.home() / 'Downloads'
pdf_dir = Path.cwd() / 'pdf_files'

def scrape_website(i, url):
 web_pages[-1].append(url)
 for j, web_page in enumerate(web_pages[-1]):
  if j == 1:
   print('First level completed')
   break

  try:
   response = requests.get(web_page, timeout=20)
   if response.status_code == 403 or response.status_code == 406:
    try:
     response = requests.get(web_page, headers=HEADERS, timeout=20)
    except requests.exceptions.ConnectTimeout:
     get_non_meta_data(i, False, web_page, None, web_page)
     continue
    except requests.exceptions.ConnectionError:
     get_non_meta_data(i, False, web_page, None, web_page)
     continue
    except KeyboardInterrupt:
     print('data collection stopped')
     sys.exit()
    except Exception as e:
     print(e)
     get_non_meta_data(i, False, web_page, None, web_page)
     continue
  except requests.exceptions.ConnectTimeout:
   get_non_meta_data(i, False, web_page, None, web_page)
   continue
  except requests.exceptions.ConnectionError:
   get_non_meta_data(i, False, web_page, None, web_page)
   continue
  except KeyboardInterrupt:
   print('Data collection stopped')
   sys.exit()
  except Exception as e:
   print(e)
   get_non_meta_data(i, False, web_page, None, web_page)
   continue


  if response.status_code != 200:
   get_non_meta_data(i, False, web_page, None, web_page)
   print(f"{response.status_code} Could not access {web_page}\nTry again later")
   continue

  soup = BeautifulSoup(response.text, "html.parser")
  if not soup.find_all("a", href=True):
   driver = webdriver.Chrome()
   driver.get(web_page)
   time.sleep(3)
   current_url = driver.current_url
   if is_pdf_url(current_url):
    if driver:
     try:
      driver.quit()
     except Exception as e:
      pass
   else:
    response = scrape_by_selenium(web_page)
    soup = BeautifulSoup(response, "html.parser")


  for link in soup.find_all("a", href=True):
   href = link["href"]
   if href.startswith(ABS_URL_STARTING):
    if href not in web_pages[-1]:
     print(detect_pdf(i, href, link, web_page))
     if href not in web_pages[-1] and tldextract.extract(web_pages[-1][0]).domain == tldextract.extract(href).domain:
      web_pages[-1].append(href)
   else:
    full_url = urljoin(web_page, href)
    if full_url not in web_pages[-1]:
     print(detect_pdf(i, full_url, link, web_page))
     if full_url not in web_pages[-1] and tldextract.extract(web_pages[-1][0]).domain == tldextract.extract(full_url).domain:
      web_pages[-1].append(full_url)

 select_dropdown(i, url)


def select_dropdown(I, url):
 #Set up headless Chrome
 options = Options()
 options.add_argument('--headless')
 options.add_argument('--no-sandbox')
 options.add_argument('--disable-dev-shm-usage')
 driver = webdriver.Chrome(options=options)

 try:
  driver.get(url)
  time.sleep(10)  # Let the page load fully

  # Find all <select> dropdowns
  dropdowns = driver.find_elements(By.TAG_NAME, "select")
  print(f"Found {len(dropdowns)} dropdown(s)")

  for dropdown in dropdowns:
   select = Select(dropdown)
   dropdown_options = [o.text for o in select.options]
   year_re = re.compile(r'\d\d\d\d')
   if len(select.options) > 1 and dropdown_options[0] and year_re.search(dropdown_options[1]):
    print('dropdown_options_count', len(dropdown_options))
    for i in range(len(dropdown_options)):
     select.select_by_index(i)
     time.sleep(20)  # Wait for the page to update

     # Get updated HTML
     html_doc = driver.page_source
     soup = BeautifulSoup(html_doc, "html.parser")
     for link in soup.find_all("a", href=True):
      href = link["href"]
      if href.startswith(ABS_URL_STARTING):
       if href not in web_pages[-1]:
        print(detect_pdf(I, href, link, url))
        if href not in web_pages[-1] and tldextract.extract(web_pages[-1][0]).domain == tldextract.extract(href).domain:
         web_pages[-1].append(href)
      else:
       full_url = urljoin(web_page, href)
       if full_url not in web_pages[-1]:
        print(detect_pdf(I, full_url, link, url))
        if full_url not in web_pages[-1] and tldextract.extract(web_pages[-1][0]).domain == tldextract.extract(full_url).domain:
         web_pages[-1].append(full_url)

 except KeyboardInterrupt:
  print('Data collection stopped')
  sys.exit()
 except Exception as e:
  print(e)
  get_non_meta_data(i, False, web_page, None, web_page)
 finally:
  driver.quit()


def get_meta_data(i, pdf_file, url, a_element, web_page):
 detailed_info = None
 company_name = tldextract.extract(urls[i]).domain.title()
 Quarter = None
 Year = None
 report = None
 report_type = None
 relevant_date = None
 date = None
 runtime = None
 Relevant_Quarter = None
 relevant_year = None
 File_ID = len(meta_data_framework) + 1
 File_Name = None

 if not pdf_file:
  File_ID = None

 info = url.split("/")[-1]
 if url.split("/")[-1].endswith('.pdf'):
  info = url.split("/")[-1][:-4]
 else:
  info = url.split("/")[-1]

 if a_element.text:
  detailed_info = a_element.text

 try:
  response = requests.head(url, timeout=20)
  if response.status_code == 403 or response.status_code == 406:
   try:
    response = requests.head(url, headers=HEADERS, timeout=20)
   except KeyboardInterrupt:
    print('5 Data collection stopped')
    sys.exit()
   except Exception as e:
    pass
 except KeyboardInterrupt:
  print('4 Data collection stopped')
  sys.exit()
 except Exception as e:
  pass

 try:
  if response.status_code == 200:
   try:
    relevant_date = response.headers['Last-Modified']
    day, month_str, year_str = relevant_date.split(' ')[1:4]
    Year = int(year_str) + 1
    relevant_year = int(year_str[-2:]) + 1
    month_num = datetime.strptime(month_str, "%b").month
    if 3 < month_num < 7:
     Quarter = 1
    elif 6 < month_num < 10:
     Quarter = 2
    elif 9 < month_num:
     Quarter = 3
    elif 0 < month_num < 4:
     Quarter = 4
     Year = int(year_str)
     relevant_year = int(year_str[-2:])
    if pdf_file:
     if Quarter is not None and relevant_year is not None:
      Relevant_Quarter = f'Q{Quarter}_FY{relevant_year}'
     elif Quarter is None and relevant_year is not None:
      Relevant_Quarter = f'FY{relevant_year}'
     else:
      Relevant_Quarter = None
    date = datetime(int(Year), month_num, int(day)).date()
   except KeyError:
    pass
   except ValueError:
    pass
 except UnboundLocalError:
  pass

 try:
  year_re = re.compile(r'(\.|/|-|_|Mar|MAR|mar|March|MARCH|march|Jun|JUN|jun|June|JUNE|june|Sep|SEP|sep|sept|Sept|SEPT|September|SEPTEMBER|september|Dec|DEC|dec|December|DECEMBER|december|q[1-4]|Q[1-4])(1\d{3}-1\d{3}|1\d{3}_1\d{3}|1\d{3}-20\d\d|1\d{3}_20\d\d|20\d\d-20\d\d|20\d\d_20\d\d1\d{3}-\d\d|1\d{3}_\d\d|20\d\d-\d\d|20\d\d_\d\d|[1-2]\d-[1-2]\d|[1-2]\d_[1-2]\d|1\d{3}|20\d\d|[1-2]\d)(\.|/|-|_)')
  year_match = year_re.search(url)
  year_text = year_match.group(2)
  relevant_year = int(year_text[-2:])
  if len(year_text) != 4 and relevant_year <= 30:
   Year = relevant_year + 2000
  elif len(year_text) != 4 and relevant_year > 30:
   Year = relevant_year + 1900
  elif len(year_text) == 4:
    Year = year_text
 except AttributeError:
  pass

 try:
  if pdf_file and Year is None:
   Year = get_meta_data_from_gemini(url, "Read the pdf file and Tell me only the financial year the file relates to. Otherwise, provide only None.")
   if Year =='None':
    Year = None
 except:
  pass

 try:
  quarter_re = re.compile(r'(-|_|/|\.| )(Mar|MAR|mar|March|MARCH|march|Jun|JUN|jun|June|JUNE|june|Sep|SEP|sep|sept|Sept|SEPT|September|SEPTEMBER|september|Dec|DEC|dec|December|DECEMBER|december|q[1-4]|Q[1-4])(-|_|/|\.| |\d)')
  quarter_match = quarter_re.search(url)
  quarter = quarter_match.group(2)
  if quarter == 'mar' or quarter == 'Mar' or quarter == 'MAR' or quarter == 'march' or quarter == 'March' or quarter == 'MARCH' or quarter == 'q4' or quarter == 'Q4':
   if pdf_file:
    report = 'quarterly report'
    Quarter = 4
  elif quarter == 'jun' or quarter == 'Jun' or quarter == 'JUN' or quarter == 'june' or quarter == 'June' or quarter == 'JUNE' or quarter == 'q1' or quarter == 'Q1':
   if pdf_file:
    report = 'quarterly report'
    Quarter = 1
  elif quarter == 'sep' or quarter == 'Sep' or quarter == 'SEP' or quarter == 'september' or quarter == 'September' or quarter == 'SEPTEMBER' or quarter == 'q2' or quarter == 'Q2' or quarter == 'Sept' or quarter == 'sept' or quarter == 'SEPT':
   if pdf_file:
    report = 'quarterly report'
    Quarter = 2
  elif quarter == 'dec' or quarter == 'Dec' or quarter == 'DEC' or quarter == 'december' or quarter == 'December' or quarter == 'DECEMBER' or quarter == 'q3' or quarter == 'Q3':
   if pdf_file:
    report = 'quarterly report'
    Quarter = 3
 except AttributeError:
  pass

 try:
  if pdf_file and Quarter is None:
   Quarter = get_meta_data_from_gemini(url, "read the pdf file. If the file refers to a quarter, provide the quarter number only (e.g., 1, 2, 3, or 4). Otherwise, provide only None.")
   if Quarter =='None':
    Quarter = None
 except :
  pass


 try:
  annual_re = re.compile(r'Annual|annual|ANNUAL')
  annual_match = annual_re.search(url)
  if pdf_file and annual_match.group():
   report = 'Annual_Report'
   report_type = 'Financial_report'
 except AttributeError:
  pass

 try:
  report_re = re.compile(r'(\.|/|-|_)([a-zA-Z])([a-zA-Z]+)(-|_|/)(Report|REPORT|report)')
  report_match = report_re.search(url)
  if pdf_file:
   report = report_match.group(2) + report_match.group(3) + report_match.group(4) + report_match.group(5)
 except AttributeError:
  pass

 try:
  if pdf_file and report is None:
   report = get_meta_data_from_gemini(url, "read the pdf file and tell me only what document this is less than 5 words such as annual report or quarterly report.")
   if report =='None':
    report = None
 except:
  pass

 try:
  if pdf_file and Quarter is not None and Year is not None:
   Relevant_Quarter = f'Q{Quarter}_FY{str(Year)[2:]}'
  elif pdf_file and Quarter is None and Year is not None:
   Relevant_Quarter = f'FY{str(Year)[2:]}'
  else:
   Relevant_Quarter = None
 except ValueError:
  pass

 india_time = pytz.timezone('Asia/Kolkata')
 Runtime = datetime.now(india_time)

 if pdf_file:
  try:
   try:
    response = requests.get(urls[i], timeout=20)
    if response.status_code == 403 or response.status_code == 406:
     try:
      response = requests.get(url[i], headers=HEADERS, timeout=20)
     except requests.exceptions.ConnectTimeout:
      pass
     except requests.exceptions.ConnectionError:
      pass
     except KeyboardInterrupt:
      print('data collection stopped')
      sys.exit()
     except Exception as e:
      pass
   except requests.exceptions.ConnectTimeout:
    pass
   except requests.exceptions.ConnectionError:
    pass
   except KeyboardInterrupt:
    print('Data collection stopped')
    sys.exit()
   except Exception as e:
    pass

   try:
    soup = BeautifulSoup(response.text, "html.parser")
    title = soup.title.string
   except AttributeError:
    pass
   if response.status_code == 200 and soup.title.string is not None:
    try:
     type_re = re.compile(r'public(_| |/|-| - | _ )disclosure|financial|investor(_| |/|-| - | _ )presentations|quarter|annual')
     type_match = type_re.search(title.lower())
     if type_match.group() == 'financial' or type_match.group() == 'quarter' or type_match.group() == 'annual':
      report_type = 'Financial_report'
     else:
      report_type = type_match.group().title().replace(' ', '_')
    except AttributeError:
     try:
      type_re = re.compile(r'public(_| |/|-| - | _ )disclosure|financial|investor(_| |/|-| - | _ )presentations|quarter|annual')
      type_match = type_re.search(url.lower())
      if type_match.group() == 'financial' or type_match.group() == 'quarter' or type_match.group() == 'annual':
       report_type = 'Financial_report'
      else:
       report_type = type_match.group().title().replace(' ', '_')
     except AttributeError:
      pass
  except UnboundLocalError:
   pass

 try:
  if pdf_file and report_type is None:
   report_type = get_meta_data_from_gemini(url, "read the pdf file and tell me only what the file category  this is less than 5 words. some of the examples are: Financial_Report, Public_Disclosure, Press_Release. Capitalize the first letter of each word and use underscores if there are multiple words such as annual report or quarterly report.")
   if report =='None':
    report_type = None
 except:
  pass


 try:
  if pdf_file and company_name is not None and report_type is not None and Relevant_Quarter is not None:
   File_Name = f'{company_name}_0_{report_type}_{Relevant_Quarter}_{File_ID}.pdf'
 except ValueError:
  pass

 meta_data_framework.loc[len(meta_data_framework)] = [pdf_file, info, detailed_info, company_name, report, report_type, Quarter, Year, relevant_date, date, Runtime, url, Relevant_Quarter, File_ID, File_Name]
 meta_data_framework.to_csv('General_insurance.csv', index=False)


def get_non_meta_data(i, pdf_file, url, a_element, web_page):
 detailed_info = None
 company_name = tldextract.extract(urls[i]).domain.title()
 Quarter = None
 Year = None

 if a_element is not None:
  if a_element.text:
   detailed_info = a_element.text

 try:
  response = requests.head(url, timeout=20)
  if response.status_code == 403 or response.status_code == 406:
   try:
    response = requests.head(url, headers=HEADERS, timeout=20)
   except KeyboardInterrupt:
    print('5 Data collection stopped')
    sys.exit()
   except Exception as e:
    print(e)
 except KeyboardInterrupt:
  print('4 Data collection stopped')
  sys.exit()
 except Exception as e:
  print(e)

 try:
  if response.status_code == 200:
   try:
    relevant_date = response.headers['Last-Modified']
    day, month_str, year_str = relevant_date.split(' ')[1:4]
    Year = int(year_str) - 1
    relevant_year = int(year_str[-2:]) - 1
    month_num = datetime.strptime(month_str, "%b").month
    if 3 < month_num < 7:
     Quarter = 1
    elif 6 < month_num < 10:
     Quarter = 2
    elif 9 < month_num:
     Quarter = 3
    elif 0 < month_num < 4:
     Quarter = 4
     Year = int(year_str)
     relevant_year = int(year_str[-2:])
    if pdf_file:
     if Quarter is not None and relevant_year is not None:
      Relevant_Quarter = f'Q{Quarter}_FY{relevant_year}'
     elif Quarter is None and relevant_year is not None:
      Relevant_Quarter = f'FY{relevant_year}'
     else:
      Relevant_Quarter = None
    date = datetime(int(Year), month_num, int(day)).date()
   except KeyError:
    pass
   except ValueError:
    pass
 except UnboundLocalError:
  pass

 try:
  year_re = re.compile(r'(\.|/|-|_|Mar|MAR|mar|March|MARCH|march|Jun|JUN|jun|June|JUNE|june|Sep|SEP|sep|sept|Sept|SEPT|September|SEPTEMBER|september|Dec|DEC|dec|December|DECEMBER|december|q[1-4]|Q[1-4])(1\d{3}-1\d{3}|1\d{3}_1\d{3}|1\d{3}-20\d\d|1\d{3}_20\d\d|20\d\d-20\d\d|20\d\d_20\d\d1\d{3}-\d\d|1\d{3}_\d\d|20\d\d-\d\d|20\d\d_\d\d|[1-2]\d-[1-2]\d|[1-2]\d_[1-2]\d|1\d{3}|20\d\d|[1-2]\d)(\.|/|-|_)')
  year_match = year_re.search(url)
  year_text = year_match.group(2)
  relevant_year = int(year_text[-2:])
  if len(year_text) != 4 and relevant_year <= 30:
   Year = relevant_year + 2000
  elif len(year_text) != 4 and relevant_year > 30:
   Year = relevant_year + 1900
  elif len(year_text) == 4:
    Year = year_text
 except AttributeError:
  pass

 try:
  if pdf_file and Year is None:
   Year = get_meta_data_from_gemini(url, "Read the pdf file and Tell me only the financial year the file relates to. Otherwise, provide only None.")
   if Year =='None':
    Year = None
 except:
  pass

 try:
  quarter_re = re.compile(r'(-|_|/|\.| )(Mar|MAR|mar|March|MARCH|march|Jun|JUN|jun|June|JUNE|june|Sep|SEP|sep|sept|Sept|SEPT|September|SEPTEMBER|september|Dec|DEC|dec|December|DECEMBER|december|q[1-4]|Q[1-4])(-|_|/|\.| |\d)')
  quarter_match = quarter_re.search(url)
  quarter = quarter_match.group(2)
  if quarter == 'mar' or quarter == 'Mar' or quarter == 'MAR' or quarter == 'march' or quarter == 'March' or quarter == 'MARCH' or quarter == 'q4' or quarter == 'Q4':
   if pdf_file:
    report = 'quarterly report'
    Quarter = 4
  elif quarter == 'jun' or quarter == 'Jun' or quarter == 'JUN' or quarter == 'june' or quarter == 'June' or quarter == 'JUNE' or quarter == 'q1' or quarter == 'Q1':
   if pdf_file:
    report = 'quarterly report'
    Quarter = 1
  elif quarter == 'sep' or quarter == 'Sep' or quarter == 'SEP' or quarter == 'september' or quarter == 'September' or quarter == 'SEPTEMBER' or quarter == 'q2' or quarter == 'Q2' or quarter == 'Sept' or quarter == 'sept' or quarter == 'SEPT':
   if pdf_file:
    report = 'quarterly report'
    Quarter = 2
  elif quarter == 'dec' or quarter == 'Dec' or quarter == 'DEC' or quarter == 'december' or quarter == 'December' or quarter == 'DECEMBER' or quarter == 'q3' or quarter == 'Q3':
   if pdf_file:
    report = 'quarterly report'
    Quarter = 3
 except AttributeError:
  pass

 try:
  if pdf_file and Quarter is None:
   Quarter = get_meta_data_from_gemini(url, "read the pdf file. If the file refers to a quarter, provide the quarter number only (e.g., 1, 2, 3, or 4). Otherwise, provide only None.")
   if Quarter =='None':
    Quarter = None
 except :
  pass

 non_meta_data_framework.loc[len(non_meta_data_framework)] = [pdf_file, detailed_info, company_name, Quarter, Year, url]
 non_meta_data_framework.to_csv('failed_urls.csv', index=False)


def detect_pdf(index, pdf_url, a_element, web_page):
 print(len(meta_data_framework))
 if pdf_url.endswith('.pdf') and pdf_url not in meta_data_framework['URL'].values:
  get_meta_data(index, True, pdf_url, a_element, web_page)
  print('pdf found')
  return True
 else:
  try:
   response = requests.head(pdf_url, timeout=20)
   if response.status_code == 403 or response.status_code == 406:
    try:
     response = requests.head(pdf_url, headers=HEADERS, timeout=20)
    except KeyboardInterrupt:
     print('5 Data collection stopped')
     sys.exit()
    except Exception as e:
     get_non_meta_data(index, False, pdf_url, a_element, web_page)
     return
  except KeyboardInterrupt:
   print('4 Data collection stopped')
   sys.exit()
  except Exception as e:
   get_non_meta_data(index, False, pdf_url, a_element, web_page)
   return

  if response.status_code != 200:
   get_non_meta_data(index, False, pdf_url, a_element, web_page)
   return
  elif response.status_code == 200:
   try:
    if response.headers['content-type'] == 'application/pdf'  and pdf_url not in meta_data_framework['URL'].values:
     get_meta_data(index, True, pdf_url, a_element, web_page)
     print('pdf found')
    else:
     if pdf_url not in web_pages[-1]:
      web_pages[-1].append(pdf_url)
      get_meta_data(index, False, pdf_url, a_element, web_page)
   except KeyError:
    if pdf_url not in web_pages[-1]:
     web_pages[-1].append(pdf_url)
     get_meta_data(index, False, pdf_url, a_element, web_page)


def generate_combinations(options):
    keys = options.keys()
    values = options.values()

    return [dict(zip(keys, combination)) for combination in product(*values)]

def scrape_by_selenium(url):
 options = webdriver.ChromeOptions()
 options.headless = False  # Run in normal mode to load JavaScript
 driver = webdriver.Chrome(options=options)
 driver.get(url)
 time.sleep(10)
 response = driver.page_source
 time.sleep(10)
 driver.quit()
 return response


def wait_for_download(previous_files, timeout=15):
 start_time = time.time()
 
 while time.time() - start_time < timeout:
  current_files = set(downloads.glob("*.pdf"))
  new_files = current_files - previous_files
  
  if new_files:
   return new_files.pop()
  
  time.sleep(1)
 
 return None


def download_by_click(index, url, response):
 try:
  if isinstance(response, str):
   soup = BeautifulSoup(response, "html.parser")
  else:
   soup = BeautifulSoup(response.text, "html.parser")

  elements = soup.find_all("a", href=True)
  if not elements:
   return

  print("Elements found")

  previous_files = downloads.glob("*.pdf")

  for element in elements:
   if element.name == 'a':
    href = element['href']
    try:
     driver = webdriver.Chrome()
     driver.get(url)
     driver.find_element(By.XPATH, f'//a[contains(@href, "{href}")]').click()

     new_file = wait_for_download(previous_files)

     if new_file:
      print("File was downloaded:", new_file)

      website_folder = pdf_dir / f'website{index + 1}'
      website_folder.mkdir(parents=True, exist_ok=True)
      
      shutil.move(str(new_file), str(website_folder))
      print(f"Moved to {website_folder}")
      get_meta_data(url, element)


      if href.startswith(ABS_URL_STARTING):
       if href not in web_pages[-1]:
        web_pages[-1].append(href)
      else:
       full_url = urljoin(url, href)
       if full_url not in web_pages[-1]:
        web_pages[-1].append(full_url)

    except KeyboardInterrupt:
     meta_data_framework.to_csv('General_insurance.csv', index=False)
     print(' 6 Data collection stopped')
     sys.exit()
    except Exception as e:
     pass
    finally:
     driver.quit()

 except KeyboardInterrupt:
  meta_data_framework.to_csv('General_insurance.csv', index=False)
  print(' 6 Data collection stopped')
  sys.exit()
 except Exception as e:
  pass


def is_pdf_url(url):
 try:
  response = requests.get(url, stream=True, timeout=10)
  first_bytes = response.raw.read(5)
  return first_bytes == b'%PDF-'
 except Exception as e:
  print(f"Error checking PDF: {e}")
  return False


def get_meta_data_from_gemini(url, prompt_text):
 try:
  response = requests.get(url)
  if response.status_code == 403 or response.status_code == 406:
   try:
    response = requests.get(url, headers=HEADERS)
   except requests.exceptions.ConnectTimeout:
    return
   except requests.exceptions.ConnectionError:
    return
   except KeyboardInterrupt:
    meta_data_framework.to_csv('General_insurance.csv', index=False)
    print('data collection stopped')
    sys.exit()
   except Exception as e:
    return
 except requests.exceptions.ConnectTimeout:
  return
 except requests.exceptions.ConnectionError:
  return
 except KeyboardInterrupt:
  meta_data_framework.to_csv('General_insurance.csv', index=False)
  print('Data collection stopped')
  sys.exit()
 except Exception as e:
  return


 if response.status_code != 200:
  print(f"{response.status_code} Could not access {web_page}\nTry again later")
  return

 pdf_file = BytesIO(response.content)
 reader = PdfFileReader(pdf_file)
 writer = PdfFileWriter()
 writer.addPage(reader.pages[0])  # Extract the first page
 with open("first_page_only.pdf", "wb") as f:
      writer.write(f)
 client = genai.Client(api_key="AIzaSyCDmY--povskk7tiIkN63FLu1DbBCahMLU")
 filepath = pathlib.Path('first_page_only.pdf')  # make sure 'file.pdf' exists in your current directory

 response = client.models.generate_content(
   model="gemini-1.5-flash",
   contents=[
       types.Part.from_bytes(
         data=filepath.read_bytes(),
         mime_type='application/pdf',
       ),
      prompt_text])

 return response.text


try:
 for index, link in enumerate(urls):
  web_pages.append([])
  scrape_website(index, link)
 meta_data_framework.to_csv('General_insurance.csv', index=False)
 non_meta_data_framework.to_csv('failed_urls.csv', index=False)
except KeyboardInterrupt:
 meta_data_framework.to_csv('General_insurance.csv', index=False)
 non_meta_data_framework.to_csv('failed_urls.csv', index=False)
 print('scraping stopped')
 sys.exit()