from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from bs4 import BeautifulSoup
import pandas as pd
import datetime    
from pytz import timezone
india_time = timezone('Asia/Kolkata')
today = datetime.datetime.now(india_time)

driver = webdriver.Chrome()
driver.maximize_window()
driver.get("https://www.royalsundaram.in/about-us/public-disclosures")
main_years = []
main_quarters = []
main_urls = []

parent_div = driver.find_element(By.ID, "tab-1-div")
child_divs = parent_div.find_elements(By.CSS_SELECTOR, ".selectBox.col-md-4.my-2")

for div in child_divs:
    main_year = div.text
    driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'nearest'});", div)
    time.sleep(0.5)  
    div.click()
    time.sleep(3)

    dropdown_menu = div.find_element(By.CLASS_NAME, "dropdown-menu")
    links = dropdown_menu.find_elements(By.TAG_NAME, "a") 
    for link in links:
        main_href = link.get_attribute('href')
        main_quarter = link.text
        print(f"Year: {main_year}")
        print(f"Quarter: {main_quarter}")
        print(f"Link: {main_href}")
        print('__________________________________________________________________')
        
        main_urls.append(main_href)
        main_years.append(main_year)
        main_quarters.append(main_quarter)
    
temp_df = pd.DataFrame()

temp_df = pd.DataFrame({
     "Main_Url": main_urls,
     "Company":'Royal Sundaram',
     "Main_Year":main_years,
     "Main_Quarter": main_quarters,
 })

for index, row in temp_df.iterrows():
    url = row["Url"]
    company = row["Company"]
    year = row["Year"]
    quarter = row["Quarter"]
    
    # Navigate to the URL
    driver.get(url)
    print(f"Visiting {company} - {year} - {quarter}: {url}")
    time.sleep(5)
    
    table1 = driver.find_element(By.ID, "table1")
    view_links = table1.find_elements(By.XPATH, ".//a[text()='View']")
    for link in view_links:
        initial_tabs = driver.window_handles
        if not link.is_displayed():
            try:
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'nearest'});", link)
                    time.sleep(0.5)  # Wait briefly for the scrolling effect
            except Exception as e:
                    print(f"Error scrolling to button: {e}")
        link.click()
        time.sleep(2)
        new_tabs = driver.window_handles
        new_tab = list(set(new_tabs) - set(initial_tabs))[0] 
        driver.switch_to.window(new_tab)
        current_url = driver.current_url
        print("Opened URL:", current_url)
        break
    
        urls.append(current_url)
        years.append(year_text)
        quarters.append(quarter_text)
        driver.close()
        driver.switch_to.window(initial_tabs[0])
        break
    time.sleep(2)
       
temp_df1 = temp_df.drop_duplicates(subset=['Url','Year'])

temp_df1['Year'] = temp_df1['Year'].str.replace("Financial Year", "").str.strip()
       



import pandas as pd

temp_df = pd.DataFrame()


temp_df = pd.DataFrame({
     "Url": urls,
     "Company":'SBI General',
     "Year":years,
     "Quarter": quarters,
     "Relevant_Date": today.date(),
     "Runtime": today
 })

df_filtered = temp_df[temp_df['Year'] != 'FY 2021-2022']


quarter_mapping = {
    'First Quarter': 'Q1',
    'Second Quarter': 'Q2',
    'Third Quarter': 'Q3',
    'Fourth Quarter': 'Q4'
}

# Apply mapping to the DataFrame
df_filtered['Quarter'] = df_filtered['Quarter'].map(quarter_mapping)

df = pd.read_excel(r"C:\Users\GOKUL\Documents\SBI.xlsx" )

df_main = pd.concat([df, df_filtered], ignore_index=True)
df_main = df_main.drop(columns=['Relevant_Date', 'Runtime'])

df_main['Year'] = df_main['Year'].str.replace("FY ", "").str.strip()

df_main["Relevant_Date"]= today.date()
df_main["Runtime"]= today


        
        
# for btn in download_buttons:
#     initial_tabs = driver.window_handles

#     if not btn.is_displayed():
#         driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn)
#     btn.click()
#     time.sleep(2)
#     new_tabs = driver.window_handles
#     new_tab = list(set(new_tabs) - set(initial_tabs))[0] 
#     driver.switch_to.window(new_tab)
#     current_url = driver.current_url
#     print("Opened URL:", current_url)
#     urls.append(current_url)
#     years.append(year_option.text)
#     quarters.append(quarter_option.text)
#     driver.close()
#     driver.switch_to.window(initial_tabs[0])
