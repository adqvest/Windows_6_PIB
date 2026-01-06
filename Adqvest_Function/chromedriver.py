#!/usr/bin/env python
# coding: utf-8

from selenium import webdriver

def get_driver(download_folder):
    download_file_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
    prefs = {
        "download.default_directory": download_folder,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        'profile.default_content_setting_values.automatic_downloads': 1
            }
    option = webdriver.ChromeOptions()

    option.add_experimental_option('prefs', prefs)

    #option = Options()
    option.add_argument("--disable-infobars")
    option.add_argument("start-maximized")
    option.add_argument("--disable-extensions")
    option.add_argument("--disable-notifications")
    option.add_argument('--ignore-certificate-errors')
    option.add_argument('--no-sandbox')


    driver = webdriver.Chrome(executable_path=download_file_path,options = option)

    return driver