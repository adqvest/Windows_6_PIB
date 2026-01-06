#!/usr/bin/env python
# coding: utf-8

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
import os
import time

def pdftoexcel(path,filename):
    limit = 0
    print(f"{path}\\{filename}.pdf")
    prefs = {
                "download.default_directory": path,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True
                        }
    chrome_driver = "C:/Users/Administrator/AdQvestDir/chromedriver.exe"
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-infobars")
    options.add_argument("start-maximized")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-notifications")
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--no-sandbox')
    options.add_experimental_option('prefs', prefs)
    service = Service(chrome_driver)
    driver = webdriver.Chrome(service=service, options=options)
    
    driver.get("https://www.ilovepdf.com/")
    driver.maximize_window()

    driver.find_element(By.XPATH, "//*[contains(text(),'Login')]").click()
    email = driver.find_element(By.XPATH, "//*[@id='loginEmail']")
    email.send_keys("kartmrinal101@outlook.com")
    password = driver.find_element(By.XPATH, "//*[@id='inputPasswordAuth']")
    password.send_keys("zugsik-zuqzuH-jyvno4")
    time.sleep(1)
    driver.find_element(By.XPATH, "//*[@id='loginBtn']").click()
    time.sleep(1)

    driver.find_element(By.XPATH, "//*[contains(text(),'PDF to Excel')]").click()

    time.sleep(20)
    input_element = driver.find_element(By.XPATH, "//*[@type='file']")

    #input_element.send_keys(os.getcwd()+"/"+i)
    input_element.send_keys(path +"\\"+ filename + ".pdf")

    time.sleep(100)
    driver.find_element(By.XPATH, '//*[@id="processTask"]').click()
    time.sleep(100)
    driver.find_element(By.CLASS_NAME,"downloader__extra").click()
    time.sleep(10)
    path = path+"\\" + filename+".xlsx"
    print(f"Converted PDF to Excel! File Uploaded to {path}")
    #break
    
    return path