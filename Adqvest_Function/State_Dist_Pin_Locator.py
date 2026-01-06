# -*- coding: utf-8 -*-
"""
Created on Thu Aug 28 20:10:20 2025
@author: Santonu
"""
import sys
import sqlite3
import duckdb
import pandas as pd
from functools import lru_cache
from rapidfuzz import process, fuzz
from collections import defaultdict
import json
import os
import re

"""
Source:: LGD Village info from Data.gov.in 
Required Files available on: S3 Bucket --->Village_LGD (Chandigarh data is not available)
"""


# sys.path.insert(0, '/home/ubuntu/AdQvestDir/Adqvest_Function')
# sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
# sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Windows_3/Adqvest_Function')
# sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Windows_Main/Adqvest_Function')

# DB_PATH = r"C:\Users\Administrator\AdQvestDir\Windows_Main\Adqvest_Function\India_Post_Location_Pin.db"
# DB_PATH = r"C:\Users\Administrator\AdQvestDir\Windows_3\Adqvest_Function\India_Post_Location_Pin.db"
# DB_PATH = r"C:\Users\Administrator\AdQvestDir\Adqvest_Function\India_Post_Location_Pin.db"

working_dir=r"C:\Users\Administrator\AdQvestDir\Adqvest_Function"
# working_dir=r"C:\Users\Administrator\AdQvestDir\Windows_3\Adqvest_Function"
# working_dir=r"C:\Users\Administrator\AdQvestDir\Windows_Main\Adqvest_Function"
os.chdir(working_dir)

#%%
class LocationFinder:
    def __init__(self, csv_path=None, db_name="india_post_location_pin.duckdb",filter_states:list=[]):
        
        # self.db_path = r"C:\Users\Administrator\AdQvestDir\Windows_Main\Adqvest_Function\india_post_location_pin.duckdb"
        # self.db_path = r"C:\Users\Administrator\AdQvestDir\Windows_3\Adqvest_Function\india_post_location_pin.duckdb"
        self.db_path = r"C:\Users\Administrator\AdQvestDir\Adqvest_Function\india_post_location_pin.duckdb"

        # self.db_path = db_name
        self.filter_states=filter_states
        self.setup_database(csv_path)
        self.load_frequent_lookups()
        self.common_side_words = [
                # Administrative
                "city", "town", "village", "district", "mandal", "municipality","barrage","hindi","Institution","vidyapeeth","rainfall","Secretariat"
                "corporation", "taluka", "taluk", "tehsil", "zone", "nagar",'dam','reservoir','school','board','primary',"project","bridge",
            
                # Military / Cantonment
                "cantt", "cantonment", "camp",
            
                # Markets / Commercial
                "bazar", "bazaar", "market", "circle", "chowk", "gate","square", "complex",
            
                # Directional | "east", "west", "north", "south",
                "old", "new","upper", "lower", "central", "middle", "outer","Mini","river","village","office", 
            
                # Industrial / Zones
                "industrial area", "industrial estate", "sez", "it park", "tech park",
            
                # Transit / Landmarks
                "station", "road", "rd", "street", "st", "highway",
                "junction", "terminal", "airport", "colony",
            
                # Rural / Semi-urban
                "gaon", "gram", "wadi", "halli", "patti", "mangalam", "khera", "khed",'bus', 'stand','lane','near',

                '', ':','ground', 'floor','bye', 'pass','opp','survey','shop', 'no.','final', 'plot',
            ]
        
        self.district_score=96
        self.sub_dist_score=90
        self.locality_score=85
 
    def setup_database(self, csv_path):
        if os.path.exists(self.db_path):
            # print(f"Database {self.db_path} already exists. Skipping creation")
            return
        
        # conn = sqlite3.connect(self.db_path)
        conn = duckdb.connect(self.db_path)
        
        #--------------------------------SQLite----------------------------------------------------------------
        # chunk_size = 100000
        # total_rows = 0

        # for i, chunk in enumerate(pd.read_csv(csv_path, chunksize=chunk_size)):
        #     chunk.columns = ['locality', 'pincode', 'sub_dist', 'dist', 'state']
            
        #     for col in ['locality', 'dist', 'state', 'sub_dist']:
        #         chunk[col] = chunk[col].str.lower().str.strip()
                
        #     chunk['pincode'] = chunk['pincode'].astype(str).str.strip()
            
        #     # Remove duplicates within chunk
        #     chunk = chunk.drop_duplicates()
        #     chunk.to_sql('India_Post_Location_Pin', conn, if_exists='append' if i > 0 else 'replace', index=False)
        #     total_rows += len(chunk)
            
        #     if i % 5 == 0:
        #         print(f"Processed {total_rows:,} rows...")
        
        # Create optimized indexes
        # conn.execute('''CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_loc_pin 
        #                 ON India_Post_Location_Pin (locality, pincode, sub_dist, dist, state)''')
        # conn.execute('CREATE INDEX IF NOT EXISTS idx_locality ON India_Post_Location_Pin(locality)')
        # conn.execute('CREATE INDEX IF NOT EXISTS idx_state ON India_Post_Location_Pin(state)')
        # conn.execute('CREATE INDEX IF NOT EXISTS idx_dist ON India_Post_Location_Pin(dist)')
        #----------------------------------------------------------------------------------------------
        # df=pd.read_csv(csv_path)
        # df=df[['locality', 'pincode', 'sub_dist', 'dist', 'state']]
        # conn.execute("INSERT INTO india_post_location_pin SELECT * FROM df")
        
        #----------------------------------------------------------------------------------------------
        # conn.execute(f"""
        #         CREATE TABLE india_post_location_pin  AS
        #         SELECT 
        #             LOWER(TRIM(locality)) AS locality,
        #             TRIM(CAST(pincode AS VARCHAR)) AS pincode,
        #             LOWER(TRIM(sub_dist)) AS sub_dist,
        #             LOWER(TRIM(dist)) AS dist,
        #             LOWER(TRIM(state)) AS state
        #         FROM read_csv_auto('{csv_path}', header=True)
        #     """)
            
        print("Data loaded successfully!")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pincode ON india_post_location_pin(pincode)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_dist ON india_post_location_pin(dist)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_locality ON india_post_location_pin(locality)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sub_dist ON india_post_location_pin(sub_dist)")


        # conn.commit()
        conn.close()

    def load_frequent_lookups(self):
        # conn = sqlite3.connect(self.db_path)
        # conn = duckdb.connect(self.db_path)
        conn = duckdb.connect(self.db_path, config={"access_mode": "read_only"})

        # print(conn.execute("SHOW TABLES").fetchall())
        self.states = set(row[0] for row in conn.execute("SELECT DISTINCT state FROM india_post_location_pin").fetchall())
        self.dist_to_state = defaultdict(set)
        dict_st = conn.execute("SELECT dist, state FROM india_post_location_pin GROUP BY dist, state").fetchall()
        for dist, state in dict_st:
            if dist and state:  # avoid nulls
                self.dist_to_state[dist.strip().lower()].add(state.strip().lower())
        

        # self.sub_dists = set(row[0] for row in conn.execute("SELECT DISTINCT sub_dist FROM india_post_location_pin "))
        subdist_to_dist =conn.execute("SELECT sub_dist, dist FROM india_post_location_pin GROUP BY sub_dist,dist").fetchall()
        self.subdist_to_dist = defaultdict(set)
        for sub_dist, dist in subdist_to_dist:
            if sub_dist and dist:  # avoid nulls
                self.subdist_to_dist[sub_dist.strip().lower()].add(dist.strip().lower())

        pin_df  = conn.sql("SELECT  pincode, ANY_VALUE(dist) AS dist, ANY_VALUE(state) AS state FROM india_post_location_pin group by pincode").df()
        self.pin_map = {str(row['pincode']): (row['dist'], row['state']) for _, row in pin_df.iterrows()}
        conn.close()
    
    @staticmethod
    def _get_pincode_from_address(address):
        pin_patterns = [
            r'(?:pin\s*code?|postal\s*code|zip)\s*:?\s*([0-9]{6}|[0-9]{3}\s[0-9]{3})',
            r'([0-9]{6})\s*(?:pin\s*code?|postal\s*code)',
            # r'([0-9]{6}|[0-9]{3}\s[0-9]{3})',
        ]
        for pattern in pin_patterns:
            match = re.search(pattern, address, re.IGNORECASE)

            if match:
                pincode=match.group(1)
                pin_int = int(str(pincode).replace(" ", "").strip())
                return pin_int

        potential_pincodes = re.findall(r'\b(?:[0-9]{6}|[0-9]{3}\s*[0-9]{3})\b', address)
        for pincode in potential_pincodes:
            pin_int = int(str(pincode).replace(" ", "").strip())
            if 100000 <= pin_int <= 855999:
                return pin_int
        return None

    @classmethod
    # @lru_cache(maxsize=50000)
    def get_pincode(cls, token_texts,retry=False):
        self=cls()
        # conn = sqlite3.connect(self.db_path)
        conn = duckdb.connect(self.db_path,config={"access_mode": "read_only"})
        #------------------------------------------------------------------------------------
        extracted_pin = self._get_pincode_from_address(token_texts)
        if extracted_pin:
            return extracted_pin
           
        #---------------------------------------------------------------------------------------
        city,st,_=cls.match_location(token_texts) 
        print(city,st,_)
        pin_info = conn.sql(f"SELECT Distinct dist,sub_dist,locality,pincode FROM india_post_location_pin where dist='{city.lower()}'").df()
        # print(len(pin_info))
        conn.close()
        
        #---------------------------------------------------------------------------------------------
        columns = [c for c in pin_info.columns if c != 'pincode']
        input_val = str(token_texts).strip().lower()
        results = []
        # print(columns)
        
        # Find best match in each column
        for col in columns:
            col_values = pin_info[col].astype(str).str.lower().unique()
            # print(col_values)
            best_match1 = process.extractOne(input_val, col_values, scorer=fuzz.token_set_ratio, score_cutoff=80,processor=lambda s: s.lower())
            if best_match1:
                value, score, _ = best_match1
                results.append((col, value, score))
                # print(value, score, _)

        # If no good matches found
        if not results:
            print(f'No Proper Pincode found for this ---> {token_texts}')
            return None

        # Pick best overall match
        results.sort(key=lambda x: x[2], reverse=True)
        best_col, best_val, best_score = results[0]

        # Get all pincodes linked to that match
        matching_pincodes = (
            pin_info[pin_info[best_col].astype(str).str.lower() == best_val]['pincode']
            .dropna()
            .unique()
            .tolist()
        )
        print(
            f"\nBest Column: {best_col}"
            f"\nBest Value: {best_val}"
            f"\nBest Score: {best_score}"
            f"\nMatching Pincodes: {matching_pincodes}"
            f"\nAll Matches: {results}\n"
        )
        return matching_pincodes[0]

    def smart_split(self,text):
        protected_words = ["West Bengal","Uttar Pradesh","Tamil Nadu","Madhya Pradesh","Jammu & Kashmir","Himachal Pradesh","Andhra Pradesh",
                           "Daman & Diu","Dadra & Nagar Haveli","Arunachal Pradesh","Andaman & Nicobar Islands","Jammu and Kashmir",
                           "Andaman and Nicobar Islands","Dadra and Nagar Haveli","Daman and Diu",'Andaman And Nicobar','Andaman and Nicobar',
                           # Arunachal Pradesh
                            'Upper Siang',
                            'Upper Subansiri',
                            'Lower Subansiri',
                            'Lower Dibang Valley',
                            'Upper Dibang Valley',  
                            'Lower Siang',          
                            'East Siang',
                            'West Siang',
                            'East Kameng',
                            'West Kameng',

                            # Sikkim
                            'North Sikkim',
                            'South Sikkim',
                            'East Sikkim',
                            'West Sikkim',

                            # West Bengal (Uttar/Dakshin/Purba/Paschim)
                            'Uttar Dinajpur (North Dinajpur)',
                            'Dakshin Dinajpur (South Dinajpur)',
                            'North Twenty Four Parganas',
                            'South Twenty Four Parganas',
                            'Purba Medinipur (East Medinipur)',
                            'Paschim Medinipur (West Medinipur)',
                            'Purba Bardhaman (East Bardhaman)',
                            'Paschim Bardhaman (West Bardhaman)',

                            # Meghalaya 
                            'East Garo Hills',
                            'West Garo Hills',
                            'North Garo Hills',    
                            'South Garo Hills',
                            'South West Garo Hills', 
                            'East Khasi Hills',
                            'West Khasi Hills',
                            'South West Khasi Hills',
                            'East Jaintia Hills',    
                            'West Jaintia Hills',   
                            'Eastern West Khasi Hills',

                            # Tripura
                            'North Tripura',
                            'South Tripura',
                            'West Tripura',

                            # Manipur
                            'Imphal East',
                            'Imphal West',

                            # Goa
                            'North Goa',
                            'South Goa',

                            # Uttarakhand
                            'Uttarkashi (North Kashi)',

                            # Delhi (National Capital Territory)
                            'North Delhi',
                            'South Delhi',
                            'East Delhi',
                            'West Delhi',
                            'North East Delhi',
                            'North West Delhi',
                            'South East Delhi',
                            'South West Delhi',

                            # Southern/Central India
                            'East Godavari',
                            'West Godavari',
                            'Dakshina Kannada (South Kannada - Karnataka)',
                            'Uttara Kannada (North Kannada - Karnataka)',
                            'East Nimar (Khandwa - Madhya Pradesh)',
                            'West Nimar (Khargone - Madhya Pradesh)',
                            'Purbi Singhbhum (East Singhbhum - Jharkhand)',
                            'Pashchimi Singhbhum (West Singhbhum - Jharkhand)'
                           ]
    
        # Step 1: Protect by replacing spaces with an underscore
        for phrase in protected_words:
            safe_phrase = phrase.replace(" ", "_")
            text = re.sub(rf"\b{re.escape(phrase)}\b", safe_phrase, text, flags=re.IGNORECASE)
    
        # Step 2: Split using regex
        # tokens = re.split(r"[,\s\\-]+", text.strip())
        tokens = re.split(r"[,\s\-\(\)\\]+", text.strip())
    
        # Step 3: Convert underscores back to spaces
        tokens = [t.replace("_", " ") for t in tokens if t]
        return tokens

    def clean_city_name(self,city):
        city = city.lower().strip()
        for word in self.common_side_words:
            city = re.sub(rf'\b{word}\b', '', city)
        return re.sub(r'\s+', ' ', city).strip().title()
    #-----------------------------------------------------------------------------------------------------------
    def find_normalized_district(search_term, district_list):
        # 1. Define directional keyword synonyms for normalization
        directional_map = {
            'north': ['north', 'uttar', 'uttara'],
            'south': ['south', 'dakshin', 'dakshina'],
            'east': ['east', 'purba', 'purbi', 'eastern'],
            'west': ['west', 'paschim', 'pashchimi', 'western']
        }
        # 2. Normalize the search term: convert to lowercase and replace synonyms
        normalized_search = search_term.lower()
        for standard_dir, synonyms in directional_map.items():
            for synonym in synonyms:
                normalized_search = normalized_search.replace(synonym, standard_dir)
        
        normalized_search = ' '.join(normalized_search.split())

    
    
        # 3. Create a normalized version of the master list for searching
        normalized_district_map = defaultdict(list)
        for district in district_list:
            normalized_name = district.split('(')[0].strip().lower()
            for standard_dir, synonyms in directional_map.items():
                for synonym in synonyms:
                    normalized_name = normalized_name.replace(synonym, standard_dir)
            normalized_name = ' '.join(normalized_name.split())
            normalized_district_map[normalized_name].append(district)
        
        matches = normalized_district_map.get(normalized_search, [])
        if len(matches) > 1:
            print(f"Multiple possible matches: {matches}")
        
        if len(matches)==1:
            return matches
        else:
            return None
    #----------------------------------------------------------------------------------------------
    @classmethod
    # @lru_cache(maxsize=50000)
    def match_location(cls, token_texts):
        self=cls()
        if not token_texts or len(token_texts.strip()) < 2:
            return None, None, 0
        
        token_texts = token_texts.lower().strip()
        # token_texts = re.split(r"[,\s\\-]+", token_texts.strip())
        token_texts=self.smart_split(token_texts)
        token_texts = [self.clean_city_name(w) for w in token_texts if w and (len(w) > 2)]
        token_texts = [w for w in token_texts if w not in ['',':']]  
        # conn = sqlite3.connect(self.db_path)
        conn = duckdb.connect(self.db_path,config={"access_mode": "read_only"})

        #----------------------------------------------------PIN--------------------------------------------------------
        for _,token_text in enumerate(token_texts):
            extracted_pin = self._get_pincode_from_address(token_text)
            if extracted_pin:
                # pin_df  = conn.sql("SELECT  pincode, ANY_VALUE(dist) AS dist, ANY_VALUE(state) AS state FROM india_post_location_pin group by pincode").df()
                # pin_map = {str(row['pincode']): (row['dist'], row['state']) for _, row in pin_df.iterrows()}

                pin_info = self.pin_map.get(str(extracted_pin))
                if pin_info:
                    dist, state = pin_info
                    return dist, state, 100




                # pin_info = conn.execute(
                #     "SELECT locality, sub_dist, dist, state FROM india_post_location_pin  WHERE pincode = ? LIMIT 1",
                #     (extracted_pin,)
                # ).fetchone()
                # if pin_info:
                #     locality, subdist, dist, state = pin_info
                #     conn.close()
                #     return dist, state, 100
        #--------------------------------------------------State----------------------------------------------------------
        detected_state = None
        token_texts1=token_texts.copy()
        # print(self.states)
        for token_text in token_texts1:
            if token_text in list(self.states):
               detected_state = token_text
               token_texts.remove(token_text)
               # print('I am here too')
               break
            # print('I am here')
            # Fuzzy match for state name
            state_match = process.extractOne(token_text, self.states, scorer=fuzz.token_set_ratio, score_cutoff=95,processor=lambda s: s.lower())
            if state_match:
                detected_state = state_match[0]
                break
        
        irgonre_detected_state=["West Bengal","Uttar Pradesh","Tamil Nadu","Madhya Pradesh","Jammu & Kashmir","Himachal Pradesh",
                                "Daman & Diu","Dadra & Nagar Haveli","Arunachal Pradesh","Andaman & Nicobar Islands",
                                "Dadra and Nagar Haveli and Daman and Diu"
                            ]
        
        irgonre_detected_state=[i.lower() for i in irgonre_detected_state]
        irgonre_detected_state = list(set(irgonre_detected_state) - set(self.filter_states))
        
        if detected_state:
            self.district_score=97
            self.sub_dist_score=96
            self.locality_score=95
            if detected_state.title().strip() in irgonre_detected_state:
               detected_state = None
        
        
        if detected_state:
           token_texts=[i for i in token_texts if not re.search(detected_state, i,flags=re.IGNORECASE)]
           # print(token_texts)
           
        print(token_texts,detected_state)
        #--------------------------------------------------District-------------------------------------------------------
        # print("Looking in :: District")
        if detected_state:
            query = "SELECT DISTINCT dist FROM india_post_location_pin"
            params = ()
            query += " WHERE state = ?"
            params = (detected_state,)

            districts_in_state = set(row[0] for row in conn.execute(query, params).fetchall())
            # print(districts_in_state)
            for _,token_text in enumerate(token_texts):
                # print(token_text)
                if token_text.lower() in districts_in_state:
                    conn.close()
                    return token_text, detected_state, 100
                
            district_match = process.extractOne(token_text, districts_in_state, scorer=fuzz.token_set_ratio, score_cutoff=96,processor=lambda s: s.lower())
        else:
            for _,token_text in enumerate(token_texts):
                if token_text.lower() in self.dist_to_state:
                    states = self.dist_to_state[token_text.lower()]
                    if len(states) > 1:
                        print(f"Multiple states found for {token_text}: {states}")

                    states_txt=', '.join(map(str, states))
                    conn.close()
                    return token_text, states_txt, 100
            
            self.districts = set(row[0] for row in conn.execute("SELECT DISTINCT dist FROM india_post_location_pin").fetchall())
            district_match = process.extractOne(token_text, self.districts, scorer=fuzz.token_set_ratio, score_cutoff=self.district_score,processor=lambda s: s.lower())
        
        if district_match:
            dist, score,_ = district_match
            state = self.dist_to_state[dist.strip().lower()]
            if len(state) > 1:
                print(f"Multiple states found for {dist}: {state}")
                
            dist_txt=', '.join(map(str, dist))
            states_txt=', '.join(map(str, state))

            conn.close()
            return dist_txt, states_txt, score
        
        #--------------------------------------------------Subdist--------------------------------------------------------
        if detected_state:
            query = "SELECT sub_dist, dist FROM india_post_location_pin"
            params = ()
            query += " WHERE state = ? GROUP BY sub_dist,dist"
            params = (detected_state,)
            
            subdist_to_dist_1 =conn.execute(query,params).fetchall()
            self.subdist_to_dist_1 = defaultdict(set)
            for sub_dist, dist in subdist_to_dist_1:
                if sub_dist and dist:  # avoid nulls
                    self.subdist_to_dist_1[sub_dist.strip().lower()].add(dist.strip().lower())
                    
            for _,token_text in enumerate(token_texts):
                # print(token_text)
                if token_text.lower() in self.subdist_to_dist_1:
                    conn.close()
                    dist = self.subdist_to_dist_1.get(token_text.lower())
                    if len(dist) > 1:
                        print(f"Multiple Distrct found for {token_text}: {dist}")
                        
                    dist_txt=', '.join(map(str, dist)) 
                    conn.close()
                    return dist_txt, detected_state, 100
                
            subdist_match1 = process.extractOne(token_text, self.subdist_to_dist_1, scorer=fuzz.token_set_ratio, score_cutoff=96,processor=lambda s: s.lower())
        else:
            for _,token_text in enumerate(token_texts):
                if token_text.lower() in self.sub_dists:
                    dist = self.subdist_to_dist.get(token_text.lower())
                    if len(dist) > 1:
                        print(f"Multiple Distrct found for {token_text}: {dist}")

                    if dist:
                        for d in dist:
                            state = self.dist_to_state[d.strip().lower()]
                            if len(state) > 1:
                               print(f"Multiple states found for {token_text}|{dist}: {state}")

                        dist_txt=', '.join(map(str, dist))
                        states_txt=', '.join(map(str, state))
                        conn.close()
                        return dist_txt, states_txt, 100
                
                subdist_match1 = process.extractOne(token_text, self.sub_dists, scorer=fuzz.token_set_ratio, score_cutoff=self.sub_dist_score,processor=lambda s: s.lower())
        
        if subdist_match1:
            subdist, score,_ = subdist_match1
            dist = self.subdist_to_dist.get(subdist)
            if len(dist) > 1:
                print(f"Multiple Distrct found for {token_text}: {dist}")

            if dist:
                for d in dist:
                    state = self.dist_to_state[d.strip().lower()]
                    if len(state) > 1:
                       print(f"Multiple states found for {token_text}|{subdist}|{dist}: {state}")

                dist_txt=', '.join(map(str, dist))
                states_txt=', '.join(map(str, state))
            
                conn.close()
                return dist_txt, states_txt, score
            
        #------------------------------------------ Locality--Depricated ---------------------------------------------------------------
        # print("Looking in :: Locality")
        # for _,token_text in enumerate(token_texts):
        #     if detected_state:
        #         query = """
        #             SELECT locality, sub_dist, dist, state 
        #             FROM india_post_location_pin  
        #             WHERE locality = ? COLLATE NOCASE AND state = ? LIMIT 1
        #         """
        #         params = (token_text, detected_state)
        #     else:
        #         query = """
        #             SELECT locality, sub_dist, dist, state 
        #             FROM india_post_location_pin  
        #             WHERE locality = ? COLLATE NOCASE LIMIT 1
        #         """
        #         params = (token_text,)
    
        #     row = conn.execute(query, params).fetchone()
        #     if row:
        #         locality, subdist, dist, state = row
        #         conn.close()
        #         return dist, state, 80

        #-----------------------------------------Fuzzy match locality-----------------------------------------------------
        # conn = sqlite3.connect(self.db_path)
        # conn = duckdb.connect(self.db_path)
        for _,token_text in enumerate(token_texts):
            query = "SELECT DISTINCT locality FROM india_post_location_pin"
            params = ()
            if detected_state:
                query += " WHERE state = ?"
                params = (detected_state,)
        
            # row = conn.execute(query, params).fetchall()
            candidates = set(r[0] for r in conn.execute(query, params).fetchall())
            # print(candidates)
            match = process.extractOne(token_text, candidates, scorer=fuzz.token_set_ratio, score_cutoff=self.locality_score,processor=lambda s: s.lower())
            if match:
                best, score,_  = match
                # print(best)
                locality_res = conn.execute(
                    "SELECT locality, sub_dist, dist, state FROM india_post_location_pin WHERE locality = ? LIMIT 1",
                    (best,)
                ).fetchone()
                
                if locality_res:
                    locality, subdist, dist, state = locality_res
                    conn.close()
                    return dist, state, score

        # conn.close()
        #----------------------------------------------------------------------------------------------------------------------
        return '', '', 0

#%%      
# def main():
    # csv_path = 'G:/cls_LEARNINGS/STATE_MAPPING_NEW/India_VIll_SUBdiv_DIST_STATE_NER_CLEAN_2024_07_19.csv'
    # os.chdir(r"G:/SELF_LEARNINGS/STATE_MAPPING_NEW/DATAGOV_IN")
    # csv_path = 'G:/SELF_LEARNINGS/STATE_MAPPING_NEW/DATAGOV_IN/INDIA_GOVT_LGD_VILLAGE_PIN.csv'
   
    # matcher = LocationFinder(csv_path=csv_path,db_name="india_post_location_pin.duckdb")
    

    # location='matabari,'
    # city, state, score = LocationFinder.match_location('kashipur uttarakhand') 
    # print(city, state, score)
    
    # pin=LocationFinder.get_pincode("Warisnagar") 
    # print(pin)
    
    #-------------------------------------------------------------------------------------------------------
    # csv_path = 'G:/SELF_LEARNINGS/STATE_MAPPING_NEW/DATAGOV_IN/Unmapped_upload_LGD_For_Later.xlsx'
    # conn = duckdb.connect("india_post_location_pin.duckdb")
    # # print(conn.execute("DESCRIBE india_post_location_pin").fetchall())
    
    # df=pd.read_excel(csv_path)
    # df=df[['locality', 'pincode', 'sub_dist', 'dist', 'state']]
    # df['pincode']=df['pincode'].astype(int)
    # df['pincode']=df['pincode'].astype(str)
    # # conn.register("df_view", df)
    
    # conn.execute(f"""
    #        INSERT INTO india_post_location_pin
    #         SELECT 
    #             LOWER(TRIM(locality)) AS locality,
    #             TRIM(CAST(pincode AS VARCHAR)) AS pincode,
    #             LOWER(TRIM(sub_dist)) AS sub_dist,
    #             LOWER(TRIM(dist)) AS dist,
    #             LOWER(TRIM(state)) AS state
    #         FROM df
    #     """)
    #-------------------------------------------------------------------------------------------------------
    # print(conn.execute("DESCRIBE df_view").fetchall())
    # conn.execute("INSERT INTO india_post_location_pin SELECT * FROM df_view")
    # conn.execute("INSERT INTO india_post_location_pin SELECT * FROM df")
    # duckdb.write_df(df, "india_post_location_pin", conn, mode="append")

    # duckdb.write_df(df, 'india_post_location_pin', conn, mode='append')
    # df_db1 = conn.sql("SELECT  Distinct dist,state,sub_dist,pincode FROM india_post_location_pin where sub_dist='kashipur'").df()
    # df_db1 = conn.sql("SELECT  distinct dist as dist FROM india_post_location_pin").df()
    # df_dist_clean=pd.read_sql("SELECT  distinct Location_Clean_Name as dist FROM GENERIC_LOCATION_LOOK_UP_TABLE WHERE Context='District' and  Location_Clean_Name is not null group by Location_Clean_Name",engine)
    # df_dist_clean=pd.read_sql("SELECT  distinct Location_Clean_Name as dist FROM GENERIC_LOCATION_LOOK_UP_TABLE WHERE Context='District' and  Location_Clean_Name is not null group by Location_Clean_Name",engine)

    # df_dist_clean['dist']=df_dist_clean['dist'].apply(lambda x:x.lower())
    # not_there=set(df_db1['dist']) - set(df_dist_clean['dist'])
    # df_db1=df_db1[df_db1['dist'].isin(list(not_there))]
    # df_db1.to_csv('DIST_State.csv',index=False)
    # df_db=pd.concat([df_db,df])
    # conn.execute("Drop table india_post_location_pin")
    
    # df_db.to_csv('india_post_location_pin.csv',index=False)
    # df_db.drop_duplicates(inplace=True)
    # conn.execute("INSERT INTO india_post_location_pin SELECT * FROM df_db")
    
    # for i,r in df.iterrows():
    #     conn.execute(f"""
    #                 UPDATE india_post_location_pin
    #                 SET dist = '{ r['dist'].lower()}'
    #                 WHERE dist ='{ r['dist']}'
    #                 """)
        
         
    
    #----------------------------------------------------------------------------------------------------------
    # pin_df  = conn.sql("SELECT  pincode, ANY_VALUE(dist) AS dist, ANY_VALUE(state) AS state FROM india_post_location_pin group by pincode").df()


                
                
                
                
                
                
                
                
                
                
                