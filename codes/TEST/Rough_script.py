import datetime
import os
import re
import sys
import warnings
import time
warnings.filterwarnings('ignore')
import pandas as pd
import zipfile
from pytz import timezone
from requests_html import HTMLSession
from playwright.sync_api import sync_playwright

from zenrows import ZenRowsClient
wd = r'C:\Users\Administrator\Junk'
file_name = 'fobhavcopy_26-FEB-2024.csv.zip'
pw = sync_playwright().start()
browser = pw.firefox.launch(headless = False, ignore_default_args=["start-maximized"])
context = browser.new_context(java_script_enabled = True,bypass_csp=True)
page = context.new_page()

with page.expect_download() as download_info:
    try:
        page.goto("https://nsearchives.nseindia.com/content/historical/DERIVATIVES/2024/FEB/fo27FEB2024bhav.csv.zip", timeout= 5000)
    except:
        print("Saving file to ", wd, file_name)
        download = download_info.value
        print(download.path())
        download.save_as(os.path.join(wd, file_name))
    page.wait_for_timeout(200)
pw.stop()