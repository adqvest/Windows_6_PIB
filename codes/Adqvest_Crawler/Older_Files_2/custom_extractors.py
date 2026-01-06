# -*- coding: utf-8 -*-
"""
Created on Sat Jul 19 03:18:48 2025
@author: Santonu
"""
import os
import sys
import re
import calendar
import time
from pytz import timezone
import datetime
from bs4 import BeautifulSoup
import json
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_ai

# working_dir=r"C:\Users\Santonu\Adqvest_Crawler"
working_dir=r"C:\Users\Administrator\AdQvestDir\codes\Adqvest_Crawler"
os.chdir(working_dir)
from search_data_provider  import InputDataSource

Instructions="""
             1.Extract the information from given text\n
             2.Extract the following fields\n
                 -State
                 -City
                 -Country
             3.show output in json only\n
             4.Do not Include any other parameter which is not in second point..
            """

sample_output={"State":"Assam","City":"Guwahati","Country":"India"}

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

def normalize_address(addr):
    addr = addr.lower()
    # Replace multiple dots with a single space
    addr = re.sub(r'\.+', ' ', addr)
    # Remove punctuation (keep letters, numbers, and spaces)
    addr = re.sub(r'[^a-z0-9\s]', ' ', addr)
    # Collapse multiple spaces into one
    addr = re.sub(r'\s+', ' ', addr)
    # Strip leading/trailing spaces
    return addr.strip()

def _extract_state_city_info(address,Instructions=Instructions,sample_output=sample_output):
    result=adqvest_ai.generate_answer(address,Instructions,sample_output)
    return get_json(result)


def _log_strategy_error(strategy_name:str):
        exc_type, exc_obj, exc_tb = sys.exc_info()
        error_msg = str(exc_obj)
        line_no = exc_tb.tb_lineno if exc_tb else 'unknown'
        print(f"Error in strategy '{strategy_name}':\nError Message: {error_msg}\nLine: {line_no}\n")

def _validate_address(address,filter_flag,state,city,locality):
    
    try: 
        if filter_flag==None:
            filter_flag=['state']

        if isinstance(filter_flag, str):
            filter_flag = [filter_flag]

        if city==None:
            if 'city' in filter_flag:
                filter_flag.remove('city')

        if locality==None:
            if 'locality' in filter_flag:
                filter_flag.remove('locality')
           
        filter_values = {'state': state,'city': city,'locality': locality}
        filter_values={k:v for k,v in filter_values.items() if k in filter_flag}
        # print(filter_values)
        return all(
        i.lower() in address.lower() 
        for i in filter_values.values()
        )
    except:
        _log_strategy_error("_validate_address")
        return False 

def _get_pincode_from_address(address):
    # Custom pincode extraction for Zudio
    pincode_match = re.findall(r'\b[0-9]{6}\b', address)
    pincode = pincode_match[0].strip() if pincode_match else ''
    return pincode

def _create_recors(address,base_record,lat_long, filter_flag, state, city, locality):
    """
    Args:
        address: Raw address string
        lat_long: Dictionary with Latitude and Longitude
        base_record: Base record data to merge
        filter_flag: Filter configuration for validation
        state: State name
        city: City name  
        locality: Locality name
    
    Returns:
        Dictionary record or None if invalid
    """
    try:
        address = ' '.join(address.split())
        address=re.sub(r"View all designs in store |View all designs in store|Timing:".lower(),'',address.lower()).title()
        address=normalize_address(address)
        print(f"Extracted address: {address}")
        
        # Skip if no address found
        if not address or address.strip() == '':
            return None
        
        state_city = _extract_state_city_info(address) if address else {'State': state, 'City': '','Country':''}

        # Apply filter validation if configured
        if filter_flag is not None:
            if not _validate_address(address, filter_flag, state, city, locality):
               return None

        return {**base_record,**state_city,**lat_long,'Address': address,'Locality': locality or '','Pincode': _get_pincode_from_address(address)}

    except:
        InputDataSource._log_strategy_error('_universal_custom_extractor._create_recors')
    
        

#----------------------------------------------------------------------------------------------------------------------------------
def _universal_custom_extractor(container, state, city, locality, address_id, filter_flag, base_record, site_id='general'):
    """
     Universal custom data extractor that handles different extraction patterns
    
    Args:
        container: BeautifulSoup container element
        state: State name
        city: City name  
        locality: Locality name
        address_id: CSS selector for address elements
        filter_flag: Filter validation flag
        base_record: Base record dictionary
        extractor_config: site_id
    """
    try:
        all_address = []
        address_elem = container.select(address_id)
        
        if not address_elem:
            return None
            
        print(f"Found {len(address_elem)} address elements")
        for adr in address_elem:
            # print(adr)
            print("---------------------------------------------------------------------------------------------------")
            address = None
            lat_long={'Latitude':None,'Longitude':None}
            
            # Extract address based on configuration type
            
            #----------------------------------------------------------------------------------------------
            if site_id == 'citycart':
                try:
                    address=adr.select('div.elementor-widget-container ul li span.elementor-icon-list-text')[2].get_text(separator=' ', strip=True)
                except:
                    address=adr.get_text(separator=' ', strip=True)

                record=_create_recors(address,base_record,lat_long, filter_flag, state, city, locality)
                if record:
                    all_address.append(record)
            #----------------------------------------------------------------------------------------------
            elif site_id == 'McDonalds_South_and_west_India':
                 # li_elements = adr.select("li.viewmap.map_menu")
                 # print(li_elements)
                 # for li in li_elements:
                li=adr
                title = li.get_text().strip()  
                latitude = li.get('lat')       
                longitude = li.get('long')    
                contain_html = li.get('contain')
                # print(contain_html)

                if contain_html:
                    # Parse the nested HTML
                    contain_soup = BeautifulSoup(contain_html, 'html.parser')
                    
                    # Extract location name and address
                    location_name = contain_soup.find('h1')
                    location_name = location_name.get_text().strip() if location_name else title

                    address_elem = contain_soup.find('h2')
                    actual_address = address_elem.get_text().strip() if address_elem else ''
    
                    lat_long={'Latitude':latitude,'Longitude':longitude}
                    print(actual_address,latitude,longitude)
                    record=_create_recors(actual_address,base_record,lat_long, filter_flag, state, city, locality)
                    if record:
                            all_address.append(record)
        

            #----------------------------------------------------------------------------------------------
            elif site_id == 'Zara':
                state=adr.select("span.zds-accordion-item__title-text")[0].get_text()
                stores=adr.select("li.store-sub-accordions__city-stores-item")
                for store in stores:
                    address=state +' '+store.get_text()
                    record=_create_recors(address,base_record,lat_long, filter_flag, state, city, locality)
                    if record:
                        all_address.append(record)
            #----------------------------------------------------------------------------------------------
            elif site_id == 'reliance_smart_bazaar':
                address=adr.select("li.outlet-address div.info-text")
                address=address[0].get_text(separator=' ', strip=True)
                latitude=adr.select('input.outlet-latitude')[0].get('value')
                longitude=adr.select('input.outlet-longitude')[0].get('value')
                lat_long={'Latitude':latitude,'Longitude':longitude}
                record=_create_recors(address,base_record,lat_long, filter_flag, state, city, locality)
                if record:
                    all_address.append(record)

            #----------------------------------------------------------------------------------------------
            elif site_id == 'manyavar_mohey':
                addr_dict = json.loads(adr.find('input')['data-store-info'])
                # print(addr_dict)
                if addr_dict['address2'] == None: addr_dict['address2'] = ''
                if addr_dict['countryCode'] == 'IN':
                    address = addr_dict['address1'] +  addr_dict['address2']
                    city = addr_dict['city']
                    pincode = addr_dict['postalCode']
                    state = addr_dict['stateCode']
                    latitude = addr_dict['latitude']
                    longitude = addr_dict['longitude']
                    
                    record={**base_record,**{'State':state,'City':city},**{'Latitude':latitude,'Longitude':longitude},'Address': address,'Locality':'','Pincode': pincode}
                    print(record)
                    if record:
                        all_address.append(record)
            #----------------------------------------------------------------------------------------------
            elif site_id == 'Group_Landmark':
                sub_cat1=adr.select("div.info-1")[0].get_text()
                sub_brand=adr.select("div.d-flex.align-items-center.mb-3 span")[0].get_text()
                address=adr.select("div.address-container div")[0].get_text()
                record=_create_recors(address,base_record,lat_long, filter_flag, state, city, locality)
                record_new={**record,**{'Sub_Category_1':sub_cat1,'Sub_Brand':sub_brand}}
                if record:
                    all_address.append(record_new)

            #----------------------------------------------------------------------------------------------
            elif site_id == 'Havells':
                adr=adr.get_text(separator="<br>").split("<br>")[:3]
                address=adr[0]+' '+adr[0]+' '+adr[0]
                print(address)
                record=_create_recors(address,base_record,lat_long, filter_flag, state, city, locality)
                if record:
                    all_address.append(record)
            #----------------------------------------------------------------------------------------------
            elif site_id == 'Naturals_Salon':
                latitude=adr.select('input.outlet-latitude')[0].get('value')
                longitude=adr.select('input.outlet-longitude')[0].get('value')
                lat_long={'Latitude':latitude,'Longitude':longitude}
                address=adr.select("div.store_outlet_01__list:has(.icn-address) .store_outlet_01__text")[0].get_text()
                record=_create_recors(address,base_record,lat_long, filter_flag, state, city, locality)
                if record:
                    all_address.append(record)

           #----------------------------------------------------------------------------------------------
            elif site_id == 'Finolex_Pipes':
                address=adr.select("div.address-box div:nth-of-type(-n+2)")[0].get_text()
                print(address)
                record=_create_recors(address,base_record,lat_long, filter_flag, state, city, locality)
                if record:
                    all_address.append(record)

            #----------------------------------------------------------------------------------------------
            # elif site_id == 'Royal_Enfield':
            #     dealer=adr.select("div.re-storelist-title div h2")[0].get_text()
            #     address=adr.select("div.re-storelist-addr p")[0].get_text()
            #     print(address)
            #     Dealer_Name
            #     record=_create_recors(address,base_record,lat_long, filter_flag, state, city, locality)
            #     if record:
            #         all_address.append({record,**{"Dealer_Name":dealer}})

            #----------------------------------------------------------------------------------------------
            # elif site_id == 'Kajaria_Ceramics':
            #     dealer=adr.select("div.col-sm-9 div")[0].get_text()
            #     address1=adr.select("div.div_width_dealer div.row:nth-of-type(2) div.col-sm-9")[0].get_text()
            #     address2=adr.select("div.row:nth-of-type(3) div.col-sm-9")[0].get_text()
            #     address=address1+' '+address2
            #     # print(address)
            #     record=_create_recors(address,base_record,lat_long, filter_flag, state, city, locality)
            #     if record:
            #         all_address.append({record,**{"Dealer_Name":dealer}})

            #----------------------------------------------------------------------------------------------
            # elif site_id == 'kalyan_jewellers':

            #     state=adr.find('a', href='#')
            #     if state==None:
            #         continue

            #     for ci in adr.select('ul.placeList li'):
            #         city = ci.select_one('a').get_text(separator=' ', strip=True)
                    
            #         for store in ci.select('ul.storeList li'):
            #             store_link = store.select_one('a')
            #             if store_link:
            #                 latitude = store_link.get('data-lat')
            #                 longitude = store_link.get('data-lng')
            #                 address = store_link.get_text(separator=' ', strip=True)
            #                 state.get_text(strip=True)
            #                 lat_long={'Latitude':latitude,'Longitude':longitude}
            #                 record=_create_recors(address,base_record,lat_long, filter_flag, state, city, locality)
            #                 if record:
            #                     all_address.append(record)

            #----------------------------------------------------------------------------------------------
            elif site_id == 'Addidas':
                store_name=adr.select("div._store-list-item-name_1a9ex_36 > strong")[0].get_text(separator=' ', strip=True)
                store_adr=adr.select("div._store-list-item-name_1a9ex_36 > div:nth-of-type(1) > span")[0].get_text(separator=' ', strip=True)
                address=store_name+' '+store_adr
                print(adr.select("div._store-list-item-name_1a9ex_36 > strong"))
                record=_create_recors(address,base_record,lat_long, filter_flag, state, city, locality)
                if record:
                    all_address.append(record)

            
            #----------------------------------------------------------------------------------------------
            else:
                try:

                    address=adr.get_text(separator=' ', strip=True)
                    record=_create_recors(address,base_record,lat_long, filter_flag, state, city, locality)
                    if record:
                        all_address.append(record)
                except Exception as e:
                    print(e)

        return all_address
        
    except Exception as e:
        _log_strategy_error('_universal_custom_extractor')
        return None

