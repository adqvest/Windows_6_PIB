# -*- coding: utf-8 -*-
"""
Created on Mon Aug 17 15:41:49 2020

@author: DELL
"""

from spacy.matcher import PhraseMatcher
from spacy.pipeline import EntityRuler
from spacy.matcher import Matcher
from spacy import displacy
from collections import Counter
import en_core_web_sm
import spacy
from spacy import displacy
#from collections import Counter
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import sklearn
from pytz import timezone
import en_core_web_sm
import pandas as pd
import numpy as np
import sqlalchemy
import unidecode
import os
import datetime
import re
from quantities import units
import unidecode
import re
from quantulum3 import parser
import sys
import adqvest_db
import JobLogNew as log
os.chdir(r"C:\Users\Administrator\AdQvestDir\codes\One time run\AMAZON_CLEANING_FILES")


# os.chdir('E:/Adqvest files')
#os.chdir('C:/Users/krang/Dropbox/Subrata/Python')
#os.chdir('D:/Adqvest_Office_work/R_Script')

#DB Connection
properties = pd.read_csv('Amazon_AdQvest_properties_AdqvestDB.txt',delim_whitespace=True)
#properties = pd.read_csv('AdQvest_properties.txt',delim_whitespace=True)

host    = list(properties.loc[properties['Env'] == 'Host'].iloc[:,1])[0]
port    = list(properties.loc[properties['Env'] == 'port'].iloc[:,1])[0]
db_name = list(properties.loc[properties['Env'] == 'DBname'].iloc[:,1])[0]

con_string = 'mysql+pymysql://' + host + ':' + port + '/' + db_name + '?charset=utf8'
engine1     = sqlalchemy.create_engine(con_string,encoding='utf-8')


#DB Connection
properties = pd.read_csv('Amazon_AdQvest_properties_AmazonDB.txt',delim_whitespace=True)
#properties = pd.read_csv('AdQvest_properties.txt',delim_whitespace=True)

host    = list(properties.loc[properties['Env'] == 'Host'].iloc[:,1])[0]
port    = list(properties.loc[properties['Env'] == 'port'].iloc[:,1])[0]
db_name = list(properties.loc[properties['Env'] == 'DBname'].iloc[:,1])[0]

con_string = 'mysql+pymysql://' + host + ':' + port + '/' + db_name + '?charset=utf8'
engine2     = sqlalchemy.create_engine(con_string,encoding='utf-8')

def cleaner(text):
    text = text.lower()
    text = text.replace(',',' ')
    text = text.replace('|',' ')
    text = re.sub(r'#',' ',text) # Removing hash(#) symbol .
    text = re.sub(r'@',' ',text) # replace @ with space
    text = re.sub(r'\$',' ',text) # replace $ with space
    text = re.sub(r'%',' ',text) # replace % with space
    text = re.sub(r'\^',' ',text) # replace ^ with space
    text = re.sub(r'&',' ',text) # replace & with space
    text = re.sub(r'\d+ml', ' ', text)
    text = re.sub(r'\((.*?)\)', ' ', text)
    text = re.sub(r'[-]', ' ', text)
    text = re.sub(r'  +', ' ', text).strip()
    text = re.sub(r'\d+ ml', ' ', text)
    text = re.sub(r'\d+', ' ', text)
    text = re.sub(r'  +', ' ', text).strip()
    return text

def full_clean(text):
    """Clean the text by replacing unwanted characters ('#', '@', '$', '%', '^', '&', '.', '!', '*', ';', ':', "'", '"', '?', '-', '(', ')', '[', ']', '{', '}', '\', '~', '`', '/', '_', '+', '=', ',', '|', '<', '>')



       Args:
       text : pass a string

       Output:
       Return a clean string by removing all unwanted characters"""
    #https://stackoverflow.com/questions/4278313/python-how-to-cut-off-sequences-of-more-than-2-equal-characters-in-a-string?fbclid=IwAR397HRMq3BILIL5VIs3kFBPf_KF7Z0siH2FG3MubLYPkOXIdcbVAaGj_Is
    #https://stackoverflow.com/questions/4278313/python-how-to-cut-off-sequences-of-more-than-2-equal-characters-in-a-string?fbclid=IwAR397HRMq3BILIL5VIs3kFBPf_KF7Z0siH2FG3MubLYPkOXIdcbVAaGj_Is

    #text = text.lower() # convert text to lower case string or lower case text

    #text = re.sub(r'((www\.[S]+)|(https?://[\S]+))','URL',text)# replcae the website link to only "URL"

    text = re.sub(r'#',' ',text) # Removing hash(#) symbol .
    text = re.sub(r'@',' ',text) # replace @ with space
    text = re.sub(r'\$',' ',text) # replace $ with space
    text = re.sub(r'%',' ',text) # replace % with space
    text = re.sub(r'\^',' ',text) # replace ^ with space
    text = re.sub(r'&',' ',text) # replace & with space

#    text = re.sub(r'\.+', ' ', text) # Replacing one or more dot to single space

    text = re.sub(r'!+',' ',text) # Replacing single or more than one explanation symbol to single space
    text = re.sub(r'\*+',' ',text) # Replacing one or more star(*) symbol to single space
    text = re.sub(r';+',' ',text)   # Replacing  single semicolor(;)  or more semicolon(;) symbol to single space
    text = re.sub(r':+',' ',text)   # Replacing single colon(:) or more colon(:) sybol to single space
    text = re.sub(r"\'+",' ', text) # Replacing single quote or more single quote to single space

    text = re.sub(r'\"+',' ', text) # Replacing double quote or more double  quote to single space
    text = re.sub(r'\?+',' ', text) # Replacing single question mark or more question mark to single space
    text = re.sub(r'-+', '', text) # Replacing single hypen  symbol or more hypen symbol  to single space
    text = re.sub(r'[\(\)]+',' ', text) # Replacing left paranthesis or right paraenthesis to single space
    text = re.sub(r'[\[\]]+',' ', text) # replacing [ and ]
    text = re.sub(r'[\{\}]+',' ', text) #replace { and } with space
    text = re.sub(r'[\\]+',' ', text) # replace \ with space
    text = re.sub(r'[~`]+',' ', text) # replace ~ and ` with space
    text = re.sub(r'[/]+',' ', text) # replace / with space
    text = re.sub(r'[_+=,]+',' ', text) # replace _ + = and , with space
    text = re.sub(r'[|]+',' ', text) # replace | with space
    text = re.sub(r'[<>]+',' ', text) # replace < and > with space

    #text = ReplaceThreeOrMore(text) # call the function ReplaceThreeOrMore(text)
    text = re.sub(r'  +',' ',text).strip() # Replacing two or more white space to single white space
    text = text.strip()

    return text  # return the text

def pipeline(table_name):
    path = "C:\\Users\\Administrator\\AdQvestDir\\codes\\One time run\\AMAZON_CLEANING_FILES\\"
    if "BEAUTY" in table_name:
        nlp = spacy.load(path+"BEAUTY.jsonl")
    elif "GROCERY" in table_name:
        nlp = spacy.load(path+"GROCERY.jsonl")
    elif "CAR" in table_name:
        nlp = spacy.load(path+"CARS.jsonl")
    elif "BABY" in table_name:
        nlp = spacy.load(path+"BABY.jsonl")
    elif "OFFICE" in table_name:
        nlp = spacy.load(path+"OFFICE.jsonl")
    elif "HEALTH" in table_name:
        nlp = spacy.load(path+"HEALTH.jsonl")
    elif "HOME" in table_name:
        nlp = spacy.load(path+"HOME.jsonl")
    elif "OFFICE" in table_name:
        nlp = spacy.load(path+"OFFICE.jsonl")
    elif "SHOES" in table_name:
        nlp = spacy.load(path+"SHOES.jsonl")
    elif "WATCHES" in table_name:
        nlp = spacy.load(path+"WATCHES.jsonl")
    else:
        raise Exception("Pipeline Brand Not Matching")

    return nlp

def brand(df,nlp):
    df['Name1'] = df['Name']
    df['Name1'] = df['Name1'].apply(lambda x: cleaner(x))
    df['Name1'] = df['Name1'].apply(lambda x : x.lower())
    df['Name1'] = df['Name1'].apply(lambda x:unidecode.unidecode(x))
    tokens = []
    lemma = []
    for doc in nlp.pipe(df['Name1'], batch_size=10000):    # Do something with the doc here
        try:
            tokens.append(([(ent.text, ent.label_) for ent in doc.ents][0][0]))
            lemma.append(str(doc).replace(str(([(ent.text, ent.label_) for ent in doc.ents][0][0])),""))
        except:
            tokens.append(None)
            lemma.append(str(doc))

    df['Brands_NER'] = tokens
    df['Clean_Name'] = lemma

    df = df.drop(['Name1'],axis=1)

    #df['Clean_Name'] = df['Clean_Name'].apply(lambda x:re.sub(r'  +',' ').strip())
    df['Clean_Name'] = df['Clean_Name'].str.replace(r'  +',' ')

    return df

def extractPiecesAndSku(df1):
    ''' (pandas.DataFrame) -> (pandas.DataFrame)
        extracts qty and sku from product name
    '''
    import gc
    from quantulum3 import parser

    df1['Name1'] = df1['Name']
    df1['Name1'] = df1['Name1'].str.lower()
    df1['Name1'] = df1['Name1'].apply(lambda x: full_clean(x))
    df1['Name1'] = df1['Name1'].apply(lambda x:unidecode.unidecode(x))

    def unit_parser(product):
        try:
            stp1=parser.parse(product)
            stp2=[x.unit.name for x in stp1 if x.unit.name != 'dimensionless']
            stp3=[re.findall(r'[\d\.\d]+', x.surface) for x in stp1 if x.unit.name != 'dimensionless']
        except:
            stp2 = " "
            stp3 = " "

    #    stp4=[x.surface for x in stp1 if x.unit.name == 'dimensionless']
        return stp2,stp3



    df1['unit']=df1['Name1'].apply(lambda x: unit_parser(x))

    df1 = df1.join(pd.DataFrame(df1.unit.values.tolist(), df1.index).add_prefix('unit_'))
    df1['unit_1'] = df1['unit_1'].apply(lambda x:  [z for y in x for z in y])
#    df['unit_1'] = df['unit_1'].apply(lambda x:  [z for y in x for z in y])
    try:
        df1[['SKU_0','SKU_1','SKU_2']]= df1['unit_1'].apply(pd.Series).iloc[:,:3]
    except:
        df1[['SKU_0','SKU_1']] = df1['unit_1'].apply(pd.Series).iloc[:,:3]

    try:
        df1[['SKU_Units0','SKU_Units1','SKU_Units2']]= df1['unit_0'].apply(pd.Series).iloc[:,:3]
    except:
        df1[['SKU_Units0','SKU_Units1']]= df1['unit_0'].apply(pd.Series).iloc[:,:3]
#    df1 = df1.join(pd.DataFrame(df1.unit_1.values.tolist(), df1.index).add_prefix('SKU_'))
#    df1 = df1.join(pd.DataFrame(df1.unit_0.values.tolist(), df1.index).add_prefix('SKU_Units'))
#    #df1=df1.assign(**pd.DataFrame(df1.unit_1.values.tolist()[0]).add_prefix('val_'))
    df1 = df1.drop(['unit','unit_0', 'unit_1'],axis=1)

    #thresh = len(a) * .1
    df1 = df1.drop(df1.columns[df1.isnull().mean() >= 0.998],axis=1)
    #b = a[a.apply(lambda row: row.astype(str).str.contains('pack of|set of|x').any(), axis=1)]

    #pat = '(?P<before>(?:\w+\W+){,3})pack of\W+(?P<after>(?:\w+\W+){,3})|(?P<before>(?:\w+\W+){,3})set of\W+(?P<after>(?:\w+\W+){,3})'
    #new = a.Name.str.extract(r"(pack of+\w+\s)")
    '''

    SCRAPY CODE for units extraction

    '''
    results = pd.DataFrame(np.ones(len(df1)))
    #def nos(x)
    # varibles for different units
    qty_units = 'pieces|pc|units|pairs|singles|tea bags|envolopes|bunch|bunches|envolope|count|teabox|teabags|teabags|bag|pouches|pouch|tabs|candies|pellets|pellet|leaves|leave|pods|pod|combo|tablets|tablet|hamper|count|wipes|box|sheets|packs|slab|cup|cups|sachet|pallets|set|pack|tins|tin|bars|bar|jars|jar|sticks|stick|sachets|sachet|pcs|pbottles|bottle'
    # regex patterns
    # strings like pack of 10, pack 23, (qty)
    pat_qty_1 = f'(?:\(?_?-?\s?(?P<qty_units>{qty_units})_?-?:?\s?(?:of)?_?-?\s(?P<qty>[0-9]+)\)?)' # FIXED BUG

    # strings like 10 pieces etc. 88 (qty)
    pat_qty_2 = f'(?:\(?_?-?\s?(?P<qty>[0-9]+)_?-?\s?(?P<qty_units>{qty_units})\)?)'

    # inserted into dataframe
    patList = [pat_qty_1, pat_qty_2]

    for myPat in patList:
        # print(myPat)
        qtydf1 = df1['Name1'].str.extract(pat = myPat).dropna()

        qtydf1['qty'] = qtydf1['qty'].apply(lambda x: int(x))

        # replace items on those indexes
        results.loc[qtydf1.index, 'Nos'] = qtydf1['qty']
        results.loc[qtydf1.index, 'Nos_Units'] = qtydf1['qty_units']
    # results['qty_units'] = results['qty_units'].apply(lambda x: 'pieces' if x == '' else x)
    df1['QTY'] = results['Nos'].apply(lambda x: str(x))
    df1['QTY_Units'] = results['Nos_Units']
    #a['Dimension'] = np.where(a['Nos_Units']=='piece','D','ND')
    df1 = df1.drop(['Name1'],axis=1)
    #df1['SKU_0'] = pd.to_numeric(df1['SKU_0'],errors='coerce')
    #df1['SKU_1'] = pd.to_numeric(df1['SKU_1'],errors='coerce')
    #df1['SKU_2'] = pd.to_numeric(df1['SKU_2'],errors='coerce')

    return df1

def table_check(table_name,engine2):
        there = pd.read_sql("SELECT * FROM information_schema.tables WHERE table_schema = 'AmazonDB' AND table_name = '"+table_name+"' LIMIT 1;",con=engine2)
        return there

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name_1 = "AMAZON CLEANING"

    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name_1,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name_1,py_file_name,job_start_time,scheduler)

        #main_table = pd.read_sql("Select * from AdqvestDB.AMAZON_SCRAPY_CONF_TABLE",con=engine1)
        #tables  = main_table['Table_Name'].tolist()
        #tables = [x for x in tables if "LAUNCHPAD" not in x]
        tables = ["AMAZON_HEALTH_AND_PERSONAL_CARE"]
        for table_name in tables:
            if table_check(table_name,engine2).empty:
                date = pd.read_sql("select min(Relevant_Date) as Relevant_Date from AdqvestDB."+table_name,con=engine1)['Relevant_Date'][0]
            else:
                date = pd.read_sql("select max(Relevant_Date) as Relevant_Date from AmazonDB."+table_name,con=engine2)['Relevant_Date'][0]
                date = date + datetime.timedelta(1)
            try:
                start_date = date.date()
            except:
                 start_date = date
            try:
                end_date = today.date()
            except:
                end_date = date
            #try:
            nlp = pipeline(table_name)
            print(table_name)
            #start_date = datetime.date(2019,14,4)
            while start_date <= end_date :
                df = pd.read_sql("Select * from AdqvestDB."+table_name+" where Relevant_Date = '"+str(start_date)+"'",con=engine1)
                if df.empty:
                    try:
                        start_date = pd.read_sql("Select min(Relevant_Date) as RD from AdqvestDB.AMAZON_HEALTH_AND_PERSONAL_CARE where Relevant_Date > '"+str(start_date)+"'",con=engine1)['RD'][0]
                        df = pd.read_sql("Select * from AdqvestDB."+table_name+" where Relevant_Date = '"+str(start_date)+"'",con=engine1)
                        if df.empty:
                            print("DataFrame is Empty and Clean Database is Upto date")
                            break
                    except:
                        raise Exception("Data For "+str(start_date)+" "+table_name+" "+"Not Available")
                print(start_date)
                drop1 = ['SKU', 'SKU_Units', 'QTY', 'QTY_Units']
                df = df.drop(drop1,axis=1)
                #drop columns


                try:
                    df = extractPiecesAndSku(df.copy())
#                    break
                except:
                    raise Exception("Data Units Wrong")





                df['Name'] = df['Name'].apply(lambda x: cleaner(x))
                df['Name'] = df['Name'].apply(lambda x : x.lower())
                df['Name'] = df['Name'].apply(lambda x:unidecode.unidecode(x))


                try:
                    df = brand(df.copy(),nlp)
#                    break
                except:
                    raise Exception("Data Units Wrong")


                df['Brands_NER'] = np.where(df.Brands_NER.isin(df.Name), df.Brands_NER,df.Brand)
                df = df.drop(['Brand'],axis=1)

                df["Rank"] = df['Rank'].str[1:]

                df[['Category', 'Sub_Category_1', 'Sub_Category_2',
                       'Sub_Category_3', 'Sub_Category_4']] = df[['Category', 'Sub_Category_1', 'Sub_Category_2',
                                                                  'Sub_Category_3', 'Sub_Category_4']].apply(lambda x:x.str.strip())

                df['Price'] = pd.to_numeric(df['Price'],errors='coerce')
                df['Min_Price'] = pd.to_numeric(df['Min_Price'],errors='coerce')
                df['Max_Price'] = pd.to_numeric(df['Max_Price'],errors='coerce')
                df['Rank'] =  pd.to_numeric(df['Rank'],errors='coerce')
                df['QTY'] =  pd.to_numeric(df['QTY'],errors='coerce')

                #df['Name'] = df['Name'].map(lambda x: re.sub(r'\W+', '', x))
                df = df.rename(columns = {'Brands_NER':'Brand'})
                df['Brand'] = df['Brand'].apply(lambda x:unidecode.unidecode(x))

                value = [x for x in df.columns[df.columns.str.contains('SKU')].tolist() if 'Units' not in x ]
                value_SKU = value
                if len(value) == 3:
                    sku1 = value
                elif len(value)>3:
                    sku1 = value[:3]
                elif len(value) == 2:
                    sku1 = value[:2]
                    sku1.append(str(sku1[-1][:-1])+str(int(sku1[-1][-1])+1))
                    df[str(sku1[-1][:-1])+str(int(sku1[-1][-1])+1)] = np.nan
                elif len(value) ==1 :
                    sku1 = value[:1]
                    sku1.append(str(sku1[0][:-1])+str(int(sku1[0][-1])+1))
                    sku1.append(str(sku1[0][:-1])+str(int(sku1[0][-1])+2))
                    df[str(sku1[0][:-1])+str(int(sku1[0][-1])+1)] = np.nan
                    df[str(sku1[0][:-1])+str(int(sku1[0][-1])+2)] = np.nan


                value = [x for x in df.columns[df.columns.str.contains('SKU')].tolist() if 'Units' in x ]
                value_SKU_Units = value
                if len(value) == 3:
                    sku2 = value
                elif len(value)>=3:
                    sku2 = value[:3]
                elif len(value) == 2:
                    sku2 = value[:2]
                    sku2.append(str(sku2[0][:-1])+str(int(sku2[0][-1])+1))
                    df[str(sku2[0][:-1])+str(int(sku2[0][-1])+1)] = np.nan
                elif len(value) ==1 :
                    sku2 = value[:1]
                    sku2.append(str(sku2[0][:-1])+str(int(sku2[0][-1])+1))
                    sku2.append(str(sku2[0][:-1])+str(int(sku2[0][-1])+2))
                    df[str(sku2[0][:-1])+str(int(sku2[0][-1])+1)] = np.nan
                    df[str(sku2[0][:-1])+str(int(sku2[0][-1])+2)] = np.nan


                try:
                    cols = ['Name', 'Clean_Name','Price', 'Rank', 'Category', 'Sub_Category_1', 'Sub_Category_2',
                       'Sub_Category_3', 'Sub_Category_4', 'Min_Price', 'Max_Price', 'Brand','Category_Index']+ sku1 + sku2 + ['QTY', 'QTY_Units','Relevant_Date', 'Runtime']
                    df = df[cols]
                except:
                    cols = ['Name', 'Clean_Name','Price', 'Rank', 'Category', 'Sub_Category_1', 'Sub_Category_2',
                       'Sub_Category_3', 'Sub_Category_4', 'Min_Price', 'Max_Price', 'Brand','Category_Index']+ value_SKU + value_SKU_Units + ['QTY', 'QTY_Units','Relevant_Date', 'Runtime']
                    df = df[cols]
                print(cols)




                sku_float = [x for x in df.columns[df.columns.str.contains('SKU')].tolist() if 'Units' not in x ]

                for sku in sku_float:
                    df[sku] =  pd.to_numeric(df[sku],errors='coerce')



                df['Relevant_Date'] = start_date
                df['Runtime'] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
                df.to_csv("Error_check.csv",index=False)
                print(df.head(5))
                print(len(df.columns))
                print(df.columns)
                try:
                    df.to_sql(name=table_name,con=engine2,if_exists='append',index=False)
                except:
                    df.to_sql(name=table_name,con=engine2,if_exists='append',index=False)

                del df

                print("Data for the day "+ str(start_date) +" Uploaded Succesfully")

                start_date += datetime.timedelta(1)
            #except:
                #raise Exception("Table Error/Code Error " + table_name)


        log.job_end_log(table_name_1,job_start_time, no_of_ping)

    except:
        try:
            connection.close()
        except:
            pass
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])
        print(error_msg)

        log.job_error_log(table_name_1,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
