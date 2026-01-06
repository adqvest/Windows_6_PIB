# -*- coding: utf-8 -*-
"""
Created on Sat Jul 19 03:18:48 2025
@author: Santonu
"""
import pandas as pd
import os
import sys
import re
import calendar
import time
from pytz import timezone
import datetime
from bs4 import BeautifulSoup
import json
from functools import lru_cache
# from sentence_transformers import SentenceTransformer,util
import importlib.util
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
# sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Windows_Main/Adqvest_Function')
# sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Windows_3/Adqvest_Function')

import adqvest_ai
from GetState import find_district,find_state
from State_Dist_Pin_Locator import LocationFinder
# loc_finder = LocationFinder()

# working_dir = r"C:\Users\Santonu\Adqvest_Crawler"
working_dir=r"C:\Users\Administrator\AdQvestDir\codes\Adqvest_Crawler"
# working_dir=r"C:\Users\Administrator\AdQvestDir\Windows_Main\codes\Adqvest_Crawler"
# working_dir=r"C:\Users\Administrator\AdQvestDir\Windows_3\codes\Adqvest_Crawler"
os.chdir(working_dir)
from search_data_provider  import InputDataSource


Instructions="""
             1.Extract the information from given text\n
             2.Extract the following fields\n
                 -State
                 -City
             3.show output in json only\n
             4.Do not Include any other parameter which is not in second point..
            """

sample_output={"State":"Assam","City":"Guwahati"}

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
    return addr.strip().title()

def dedup_similar_address(df,req_col:str='Address',threshould:float=0.90):
    # from sentence_transformers import SentenceTransformer,util
    model = SentenceTransformer('all-MiniLM-L6-v2')
    
    df[req_col]=df[req_col].apply(lambda x:normalize_address(str(x)))
    corpus_to_compare=df[req_col].to_list()
    paraphrases = util.paraphrase_mining(model, corpus_to_compare,corpus_chunk_size=len(corpus_to_compare),
                                         query_chunk_size =1000,top_k=10)

    df_matching_2=pd.DataFrame()
    for ph in paraphrases[0:]:
        if ph[0]>threshould:
            df1=pd.DataFrame()
            df1[f'{req_col}_Clean']=[corpus_to_compare[ph[2]]]  
            df1[f'{req_col}_Mapped']=[corpus_to_compare[ph[1]]]  
            df1['Similarity_Score']=[ph[0]]  
            df_matching_2=pd.concat([df_matching_2,df1])
    
    df1=df[~df[req_col].isin(df_matching_2[f'{req_col}_Clean'].to_list())]
    return df1

@lru_cache(maxsize=10)  
def _extract_state_city_info(address):
    result=adqvest_ai.generate_answer(address,Instructions,sample_output)
    return get_json(result)

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
        InputDataSource._log_strategy_error('_universal_custom_extractor._validate_address')
        return False 

def _get_pincode_from_address(address):
        pin_patterns = [
            r'(?:pin\s*code?|postal\s*code|zip)\s*:?\s*([0-9]{6}|[0-9]{3}\s[0-9]{3})',
            r'([0-9]{6})\s*(?:pin\s*code?|postal\s*code)',
            # r'([0-9]{6}|[0-9]{3}\s[0-9]{3})',
        ]
        for pattern in pin_patterns:
            match = re.search(pattern, address, re.IGNORECASE)
            if match:
                return match.group(1)

        potential_pincodes = re.findall(r'\b(?:[0-9]{6}|[0-9]{3}\s[0-9]{3})\b', address)
        for pincode in potential_pincodes:
            pin_int = int(str(pincode).replace(" ", "").strip())
            if 100000 <= pin_int <= 855999:
                return pin_int
        return ''

def _create_recors(address,base_record,lat_long, filter_flag, state, city, locality,pin=None):
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
        address=re.sub(r"View all designs in store |View all designs in store|Timing:|Tel:|Tel :".lower(),'',address.lower()).title()
        address = re.sub(r'\+[0-9]{1,3}[0-9]{10,15}|\b[0-9]{10,15}\b|[\w\.-]+@[\w\.-]+\.\w+', '', address)

        print(f"Extracted address: {address}")
        # Skip if no address found
        if not address or address.strip() == '':
           return None
        #------------------------------------------------------------------------------------------------------
        if address!='':
            #This one will explicitly look for Address Not in -City,State
            # extracted_pin=loc_finder._get_pincode_from_address(str(address)+' '+str(state))
            extracted_pin=_get_pincode_from_address(str(address))
            
            if pin:
              # extracted_pin=loc_finder._get_pincode_from_address(address) ##Reason for this logic:Some cases we provide One pin but Output gives data nearby PIN also
              extracted_pin=_get_pincode_from_address(address)
              if extracted_pin=='':
                 extracted_pin=pin

              city = find_district(str(extracted_pin), address)
              state=find_state(str(extracted_pin), address)
              state_city = {'State': state, 'City': city}

            elif (state and city):
               state_city = {'State': state, 'City': city}

            # elif extracted_pin!='':
            #   city = find_district(extracted_pin, address)
            #   state=find_state(extracted_pin, address)
            #   state_city = {'State': state, 'City': city}

            else:
              state_city = _extract_state_city_info(address) if address else {'State': state, 'City': '','Country':''}

        #-------------------------------------------------------------------------------------------------------
        # Apply filter validation if configured
        if filter_flag is not None:
            if not _validate_address(address, filter_flag, state, city, locality):
               return None

        # if extracted_pin=='':
        #    extracted_pin=loc_finder.get_pincode(str(locality)+' '+state_city.get('City',city)+' '+state_city.get('State',state))

        return {**base_record,**state_city,**lat_long,'Address': address,'Locality': locality or '','Pincode': extracted_pin}
    except:
        InputDataSource._log_strategy_error('_universal_custom_extractor._create_recors')
    
#----------------------------------------------------------------------------------------------------------------------------------
def _universal_custom_extractor(container, state, city, locality,pin, address_id, filter_flag, base_record, site_id='general'):
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

        address_elem_gen=iter(address_elem)
        del address_elem
        for adr in address_elem_gen:
            # print(adr)
            # links=adr.find('a')
            # print(adr.get_text(separator=' ', strip=True), adr.find('a'))
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
                    # print(actual_address,latitude,longitude)
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
            elif site_id == 'reliance_digital':
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
                    # print(record)
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
            elif site_id  in ["Havells_dealers","Havells_brand","Havells_utsav","Havells_branch","Havells_ec"]:
                adr=adr.get_text(separator="<br>").split("<br>")[:3]
                address=adr[0]+' '+adr[1]+' '+adr[2]
                # print(address)
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
                dealer=adr.select("div.address-box div:nth-of-type(-n+2)")[0].get_text()
                dealer = ' '.join(dealer.split())
                dealer={"Dealer_Name":dealer}
                address=adr.select("div.fz-14.cl-5")[0].get_text()    
                address=address+' '+adr.get('class')[-2].replace('state_','')+' '+adr.get('class')[-1].replace('city_','')
                # print(address)
                record=_create_recors(address,base_record,lat_long, filter_flag, state, city, locality)
                if record:
                    all_address.append({**record,**dealer})

            #----------------------------------------------------------------------------------------------
            elif site_id == 'Royal_Enfield':
                dealer=adr.select("div.re-storelist-title div h2")[0].get_text()
                address=adr.select_one("div.re-storelist-addr")
                address = [p.get_text(" ", strip=True) for p in address.find_all("p")[:3]]
                address = ", ".join(address)
                address=address.replace(',,',',')
                record=_create_recors(address,base_record,lat_long, filter_flag, state, city, locality)
                if record:
                    all_address.append({**record,**{"Dealer_Name":' '.join(dealer.split())}})

            #----------------------------------------------------------------------------------------------
            elif site_id == 'Kajaria_Ceramics':
                dealer=adr.select("div.col-sm-9 div")[0].get_text()
                address=adr.select("p i.icon-location_pin")[0].get_text()
                print(address)
                record=_create_recors(address,base_record,lat_long, filter_flag, state, city, locality)
                if record:
                    all_address.append({**record,**{"Dealer_Name":dealer}})


             #----------------------------------------------------------------------------------------------
            elif site_id == 'SOTC':
                address=adr.select("span[itemprop='streetAddress']")[0].get_text()+' '+adr.select("span[itemprop='addressLocality']")[0].get_text()+' '+adr.select("span[itemprop='addressRegion']")[0].get_text()+' '+adr.select("span[property='postalCode']")[0].get_text()
                record=_create_recors(address,base_record,lat_long, filter_flag, state, city, locality)
                if record:
                    all_address.append(record)

            #----------------------------------------------------------------------------------------------
            elif site_id in ['Aurelia','W_for_Woman']:
                address=adr.get_text()
                record=_create_recors(address.split('WhatsApp')[0],base_record,lat_long, filter_flag, state, city, locality)
                if record:
                    all_address.append(record)

            #----------------------------------------------------------------------------------------------
            elif site_id in ['Apollo_Pharmacy']:
                address=adr.get_text()
                city = find_district(pin, address)
                state=find_state(pin, address)
                # print(address)
                record={**base_record,**{'State':state,'City':city},**lat_long,'Address': address,'Locality':'','Pincode': pin}
                # print(record)
                if record:
                    all_address.append(record)
            #----------------------------------------------------------------------------------------------
            elif site_id in ['Medplus_Pharmacy']:
                address=adr.get_text()
                record=_create_recors(address,base_record,lat_long, filter_flag, state, city, locality)
                # print(record)
                sub_cat1={"Sub_Category_1":"Pharmacies"}
                if record:
                    all_address.append({**record,**sub_cat1})

            #----------------------------------------------------------------------------------------------
            elif site_id in ['Senco_Gold']:
                state=adr.select("td:nth-of-type(n+2)")[0].get_text()
                city=adr.select("td:nth-of-type(n+3)")[0].get_text()
                address=adr.select("td:nth-of-type(n+5)")[0].get_text()
                record={**base_record,**{'State':state,'City':city},**lat_long,'Address': address,'Locality':''}
                # print(record)
                if record:
                    all_address.append(record)
            
            #----------------------------------------------------------------------------------------------
            elif site_id in ['Maruti_Suzuki']:
                dealer=adr.select("div.dealer-title h3")[0].get_text()
                address=adr.select("div.dealer-location div.dealer-address")[0].get_text()
                record=_create_recors(address,base_record,lat_long, filter_flag, state, city, locality)
                if record:
                    all_address.append({**record,**{"Dealer_Name":dealer}})

            #----------------------------------------------------------------------------------------------
            elif site_id in ['nexa_maruti_suzuki']:
                dealer=adr.select("span.dealer-title-text")[0].get_text()
                address=adr.select("div.dealer-location div.dealer-address")[0].get_text()
                # print(address)
                record=_create_recors(address,base_record,lat_long, filter_flag, state, city, locality)
                if record:
                    all_address.append({**record,**{"Dealer_Name":dealer}})

            #----------------------------------------------------------------------------------------------
            elif site_id in ['baja_auto']:
                dealer=adr.select("h6")[0].get_text()
                address=adr.select("p")[0].get_text()
                # print(address)
                record=_create_recors(address,base_record,lat_long, filter_flag, state, city, locality)
                if record:
                    all_address.append({**record,**{"Dealer_Name":dealer}})

            #----------------------------------------------------------------------------------------------
            elif site_id in ['grt_jewellers']:
                address=adr.get_text()
                record=_create_recors(address.split('Phone')[-1],base_record,lat_long, filter_flag, state, city, locality)
                if record:
                    all_address.append(record)

            #----------------------------------------------------------------------------------------------
            elif site_id in ['fastrack']:
                if 'fastrack' in adr.select('span.store-name')[0].get_text().lower().strip():
                    address=adr.select('p.store-address')[0].get_text(separator=' ', strip=True)
                    record=_create_recors(address.split('Phone')[-1],base_record,lat_long, filter_flag, state, city, locality)
                    if record:
                        all_address.append(record)

            #----------------------------------------------------------------------------------------------
            elif site_id in ['hush_puppies']:
                if 'fastrack' in adr.select('span.store-name')[0].get_text().lower().strip():
                    address=adr.select('p.store-address')[0].get_text(separator=' ', strip=True)
                    record=_create_recors(address.split('Phone')[-1],base_record,lat_long, filter_flag, state, city, locality)
                    if record:
                        all_address.append(record)

            #----------------------------------------------------------------------------------------------
            elif site_id == 'Addidas':
                store_name=adr.select("div._store-list-item-name_1a9ex_36 > strong")[0].get_text(separator=' ', strip=True)
                store_adr=adr.select("div._store-list-item-name_1a9ex_36 > div:nth-of-type(1) > span")[0].get_text(separator=' ', strip=True)
                address=store_name+' '+store_adr
                # print(adr.select("div._store-list-item-name_1a9ex_36 > strong"))
                record=_create_recors(address,base_record,lat_long, filter_flag, state, city, locality)
                if record:
                    all_address.append(record)

            
            #----------------------------------------------------------------------------------------------
            else:
                try:
                    address=adr.get_text(separator=' ', strip=True)
                    record=_create_recors(address,base_record,lat_long, filter_flag, state, city, locality,pin)
                    if record:
                        all_address.append(record)
                except Exception as e:
                    print(e)

        return all_address
        
    except Exception as e:
        InputDataSource._log_strategy_error('_universal_custom_extractor')
        return None

