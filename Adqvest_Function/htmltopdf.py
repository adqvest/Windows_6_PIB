from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
import base64


def htmltopdf(url,path,landscape=False):
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    
    driver = webdriver.Chrome(options=chrome_options)
    
    driver.get(url)
    
    time.sleep(5)  
    
    driver.execute_cdp_cmd("Page.enable", {})
    params = {
        'printBackground': True,
        'format': 'A4',
        'landscape': landscape
    }
    result = driver.execute_cdp_cmd("Page.printToPDF", params)
    with open(path, 'wb') as file:
        file.write(base64.b64decode(result['data']))
    
    driver.quit() 