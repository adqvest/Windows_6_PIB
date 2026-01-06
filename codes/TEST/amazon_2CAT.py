import scrapy
from bs4 import BeautifulSoup
import pandas as pd
import urllib.parse
import re
import numpy as np
import time
from numpy import random
from scrapy.crawler import CrawlerProcess
import datetime
from pytz import timezone
import sys
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import ClickHouse_db
client = ClickHouse_db.db_conn()

pd.set_option('display.max_columns', None)  
pd.set_option('display.max_rows', None)  
pd.set_option('display.max_colwidth', None)
#categories = ["Amazon Launchpad","Watches","Beauty","Baby Products","Car & Motorbike","Clothing & Accessories","Health & Personal Care","Home & Kitchen","Home Improvement","Watches","Grocery & Gourmet Foods","Office Products","Shoes & Handbags"]
df2 = pd.DataFrame(columns = ['Name','Price','Rank','Category','Sub_Category_1','Sub_Category_2','Sub_Category_3','Sub_Category_4','Min_Price','Max_Price','Brand','SKU','SKU_Units','QTY','QTY_Units','Is_Prime','Relevant_Date','Runtime','Category_Index','Sub_Category_5','Sub_Category_6'])
link_pool = pd.DataFrame(columns = ['Link','Parsed'])
categories = ["Home & Kitchen","Grocery & Gourmet Foods","Beauty","Health & Personal Care","Shoes & Handbags"]
HEADERS = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:98.0) Gecko/20100101 Firefox/98.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
        }

class data_clean:

    def get_brand(x):
        if (len(x)>0):
            return(x[0])
        else:
            return('')

    def Min_Price(x):

        if(len(x)==0):
            return np.nan
        elif(len(x)==1):
            return x[0]
        else:
            return x[0]
    
    @staticmethod
    def Max_Price(x):
        try:
            if(len(x)==0):
                return np.nan
            elif(len(x)==1):
                return x[0]
            else:
                return x[1]
        except:
            return x

    @staticmethod
    def segregate_categories(df,item):

        df = df.loc[df['Category'] == item]
        # if len(df) > 0:
        #     if "Health" in item:
        #         client.execute("INSERT INTO AMAZON_HEALTH_AND_PERSONAL_CARE_Temp_Nidhi VALUES",df.values.tolist())
        #     else:
        #         client.execute("INSERT INTO AMAZON_LAUNCHPAD_Temp_Nidhi VALUES",df.values.tolist())
        # else:
        #     pass
        client.execute("INSERT INTO AMAZON_LAUNCHPAD_Temp_Nidhi VALUES",df.values.tolist())


    @staticmethod
    def extractPiecesAndSku(myDf):
        ''' (pandas.DataFrame) -> (pandas.DataFrame)
            extracts qty and sku from product name
        '''
        import gc
    
        # converting to lower case
        myDf['Name'] = myDf['Name'].str.lower()
    
        # varibles for different units
        sku_units = 'milliliters|ml|mg|kg|kilogram|gram|gr|g|lb|ltr|lt|l|fl. oz|oz|ounce|fluid ounce|ounces|kilo'
        dim_units = 'inches|inch|cm|mm|m|in|foot|feet|ft|f'
        qty_units = 'pieces|pc|units|pairs|singles|tea bags|envolopes|bunch|bunches|envolope|count|teabox|teabags|teabags|bag|pouches|pouch|tabs|candies|pellets|pellet|leaves|leave|pods|pod|combo|tablets|tablet|hamper|count|wipes|box|sheets|packs|slab|cup|cups|sachet|pallets|set|pack|tins|tin|bars|bar|jars|jar|sticks|stick|sachets|sachet|pcs|pbottles|bottle'
    
        # regex patterns
        # strings like , 0.25gm etc. (sku)
        pat_sku = f'(\(?,?\s?(?P<sku>[0-9]*\.?[0-9]+?)\)?\s?-?\s?_?(?P<sku_units>{sku_units})\s?)'
    
        # strings like pack of 10, pack 23, (qty)
        pat_qty_1 = f'(?:\(?_?-?\s?(?P<qty_units>{qty_units})_?-?:?\s?(?:of)?_?-?\s(?P<qty>[0-9]+)\)?)' # FIXED BUG
    
        # strings like 10 pieces etc. 88 (qty)
        pat_qty_2 = f'(?:\(?_?-?\s?(?P<qty>[0-9]+)_?-?\s?(?P<qty_units>{qty_units})\)?)'
    
        # strings like (200gm x 3) (qty)
        pat_qty_3 = f'\(?(?:[0-9]*\.?[0-9]+?)\s?(?:{sku_units})\s?x\s?(?P<qty>[0-9]+)\)?'
    
        # strings like (24 x 13g) (qty)
        pat_qty_4 = f'\(?\s?(?P<qty>[0-9]+)\s?x\s?(?:[0-9]*\.?[0-9]+?)\s?(?:{sku_units})\)?'
    
        ################# SKU #################
    
        nameList = myDf['Name'].str.extract(pat = pat_sku)
    
        # inserted into dataframe
        myDf['SKU'] = nameList['sku']
        myDf['SKU_Units'] = nameList['sku_units']
    
        del nameList
        gc.collect()
    
        ################# QTY #################
    
        # myDf to store results
        results = pd.DataFrame(np.ones(len(myDf)))
        results.rename(columns={0:'qty'}, inplace=True)
        # results['qty_units'] = ''
        # results['qty'] = results['qty'].astype(int)
    
        patList = [pat_qty_1, pat_qty_2, pat_qty_3, pat_qty_4]
    
        for myPat in patList:
            # print(myPat)
            qtyDf = myDf['Name'].str.extract(pat = myPat).dropna()
            if myPat in [pat_qty_3, pat_qty_4]:
                qtyDf['qty_units'] = 'pieces'
            qtyDf['qty'] = qtyDf['qty'].apply(lambda x: int(x))
    
            # replace items on those indexes
            try:
                results.loc[qtyDf.index, 'qty'] = qtyDf['qty']
                results.loc[qtyDf.index, 'qty_units'] = qtyDf['qty_units']
            except:
                pass
    
        # results['qty_units'] = results['qty_units'].apply(lambda x: 'pieces' if x == '' else x)
        results['qty_units'] = results['qty_units'].fillna('piece')
    
        # innserting qty and qty_unit into myDf
        myDf['QTY'] = results['qty'].apply(lambda x: str(x))
        myDf['QTY_Units'] = results['qty_units']
    
        del results, qtyDf
        gc.collect()
        
class Amazon(scrapy.Spider):
    name = "amazonbestseller"
    start_urls = ["https://www.amazon.in/gp/bestsellers/"]

    custom_settings = {
        'DOWNLOAD_DELAY': 3, #3 seconds of delay
        'ROBOTSTXT_OBEY' : True,
        'CONCURRENT_REQUESTS' : 10
        }

    def __init__(self,category):

        self.category = category

    def parse(self, response):

        link2 = response.request.url
        print(link2)

        for i in range(len(link_pool['Link'])):
            if link_pool['Link'][i] == link2:
                link_pool['Parsed'][i] = 'Yes'


        for r in response.css('div[role=treeitem] a'):
                    url = r.css('::attr(href)').get()
                    txt = r.css('::text').get()
                    if txt in self.category:
                        print(txt)
                        try:
                            link = re.search(".*/ref", response.urljoin(url)).group().replace('ref','')
                        except:
                            link = response.urljoin(url)
                        link_data = [link,None]
                        if bool(link_pool['Link'].apply(str).str.lower().str.contains(link.lower()).any()):
                            pass
                        else:
                            link_pool.loc[len(link_pool)] = link_data

        for i in range(len(link_pool['Link'])):
            if link_pool['Parsed'][i] == None:
                yield scrapy.Request(url = link_pool['Link'][i],callback = self.parse_next, headers=HEADERS)
                link_pool.loc[i, 'Parsed'] = 'Yes'

    def parse_next(self, response):

        global df2

        india_time = timezone('Asia/Kolkata')
        today      = datetime.datetime.now(india_time)
        print("############ IN ############")
        
        link2 = response.request.url
        print(link2)

        for i in range(len(link_pool['Link'])):
            if link_pool['Link'][i] == link2:
                print("YEEEEEEESSSSSSSSSS!!!!!")
                link_pool['Parsed'][i] = 'Yes'
        
        Sub_Category_1 = None
        Sub_Category_2 = None
        Sub_Category_3 = None
        Sub_Category_4 = None
        Sub_Category_5 = None
        Sub_Category_6 = None
        Category = None
        Name = None
        Price = None
        Rank = None
        SKU = None
        SKU_Units = None
        QTY = None
        QTY_Units = None
        Min_Price = None
        Max_Price = None
        Brand = None
        Is_Prime = None
        Category_Index = None
        
        time.sleep(5)

        soup = BeautifulSoup(response.text, "lxml")
        cat = soup.find('span', attrs = {'class' : re.compile('.*selected.*')})

        try:
            main_cat = soup.find('div',attrs = {'class' : '_p13n-zg-nav-tree-all_style_zg-browse-item__1rdKf _p13n-zg-nav-tree-all_style_zg-browse-height-large__1z5B8 _p13n-zg-nav-tree-all_style_zg-browse-up__XTlqh'}).text
            main_cat = main_cat.replace('‹\xa0','')
        except:
            main_cat = "None"

        sub_cat = soup.find_all('div', attrs = {'role' : 'treeitem','class':'_p13n-zg-nav-tree-all_style_zg-browse-item__1rdKf _p13n-zg-nav-tree-all_style_zg-browse-height-large__1z5B8'})

        for i in sub_cat:
            sub_cats = i.find('a')
            if sub_cats is not None:
                sub_cats = sub_cats.text
        if cat is not None:
            print(cat)
            print("**************"+cat.text+"**********")
        cats = soup.find_all('div', attrs = {'role' : 'treeitem'})
        cat_list = []
        sub_cat = []
        for i in cats:
            if "‹" in i.text and "Any Department" not in i.text:
                cat_list.append(i.text.replace('‹\xa0',''))
            if "‹" not in i.text:
                sub_cat.append(i.text)

        if len(sub_cat) != 0:
            cat_list.append(sub_cat[0])
        if cat.text not in cat_list:
            cat_list.append(cat.text)

        print(cat_list)

        try:
            Category = cat_list[0]
        except:
            pass
        try:
            Sub_Category_1 = cat_list[1]
        except:
            pass
        try:
            Sub_Category_2 = cat_list[2]
        except:
            pass
        try:
            Sub_Category_3 = cat_list[3]
        except:
            pass
        try:
            Sub_Category_4 = cat_list[4]
        except:
            pass
        try:
            Sub_Category_5 = cat_list[5]
        except:
            pass
        try:
            Sub_Category_6 = cat_list[6]
        except:
            pass

        try:
            itms = soup.find_all('div',attrs={'id': 'gridItemRoot'})
        except:
            itms = soup.find_all('div',attrs={'class': re.compile('.*grid-column.*')})

        for x in itms[0:30]:
            try:
                print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
                print(x.text)
                #res = urllib.parse.urlsplit(x.find('a')['href'])
                Name = x.find('div',attrs = {'class':re.compile('.*line-clamp.*')}).text
                try:
                    Price = x.find('span',attrs = {'class':re.compile('.*-price_.*')}).text
                    Price = ''.join(re.findall("[0-9].+",Price))
                except:
                    Price = None
            except:
                pass

            query = f"SELECT Category_Index FROM AdqvestDB.AMAZON_BESTSELLER_INDEX where Category ='{Category}';"
            Category_Index = client.execute(query)[0][0]
            Brand = Name.split(' ')
            Brand = data_clean.get_brand(Brand)
            Rank = x.find('span',attrs = {'class':'zg-bdg-text'}).text
            Relevant_Date = today.date()
            Runtime = datetime.datetime.now()
            data = [Name,Price,Rank,Category,Sub_Category_1,Sub_Category_2,Sub_Category_3,Sub_Category_4,Min_Price,Max_Price,Brand,SKU,SKU_Units,QTY,QTY_Units,Is_Prime]
            data = [str(i) if i is not None else None for i in data]
            data.append(Relevant_Date)
            data.append(Runtime)
            data.append(Category_Index)
            if Sub_Category_5 is not None:
                data.append(str(Sub_Category_5))
            else:
                data.append(Sub_Category_5)
            if Sub_Category_6 is not None:
                data.append(str(Sub_Category_6))
            else:
                data.append(Sub_Category_6)
            df2.loc[len(df2)] = data

      

        #df2['Min_Price'] = df2['Price'].map(data_clean.Min_Price)
        #df2['Max_Price'] = df2['Price'].map(data_clean.Max_Price)
        df2.drop_duplicates(inplace=True)
        data_clean.extractPiecesAndSku(df2)
        df2.fillna(value = '',inplace=True)
        data_clean.segregate_categories(df2,self.category)
        df2 = df2.iloc[0:0]

        for r in response.css('div[role=treeitem] a'):
            url = r.css('::attr(href)').get()
            txt = r.css('::text').get()
            print(txt)
            link = re.search(".*/ref", response.urljoin(url)).group().replace('ref','')
            link_data = [link,None]
            if bool(link_pool['Link'].apply(str).str.lower().str.contains(link.lower()).any()) or "Any Department" in txt:
                pass
            elif bool(link_pool['Link'].apply(str).str.lower().str.contains(link.lower()).any()) == False and (cat.text in self.category or  main_cat in self.category):
                link_pool.loc[len(link_pool)] = link_data

        for i in range(len(link_pool['Link'])):
            if link_pool['Parsed'][i] == None:
                yield scrapy.Request(url = link_pool['Link'][i],callback=self.parse_next,headers=HEADERS)
                
def run_program(run_by='Adqvest_Bot', py_file_name=None):

    ## job log details
    table_name = 'AMAZON'
    global df2    
    
    for i in categories:
        process = CrawlerProcess({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:98.0) Gecko/20100101 Firefox/98.0",})  
        process.crawl(Amazon,category = i)
        process.start()
        import sys
        del sys.modules['twisted.internet.reactor']
        from twisted.internet import reactor
        from twisted.internet import default
        default.install()

if(__name__=='__main__'):
    run_program(run_by='manual')
    