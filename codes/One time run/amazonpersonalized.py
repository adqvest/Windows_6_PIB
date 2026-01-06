# -*- coding: utf-8 -*-
"""
Created on Fri Mar 24 08:17:09 2023

@author: Abdulmuizz
"""
import sys
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import ClickHouse_db
import igraph as ig
from itertools import combinations
import pandas as pd
import numpy as np
import re
# from symspellpy.symspellpy import SymSpell
# from symspellpy import Verbosity
# import spacy
import os
import json
translationTable = str.maketrans("éàèùâêîôûç", "eaeuaeiouc")

class AmazonPersonalizedRank:
    
    def __init__(self,given_df):
        
        self.top_df = given_df[['Clean_Name', 'Category', 'Sub_Category_1', 'Sub_Category_2', 'Sub_Category_3', 'Sub_Category_4','Rank','Combination','Relevant_Date']]
        self.top_df['Clean_Name'] = self.top_df['Clean_Name'] + '<product>'
        self.top_df['Category'] = self.top_df['Category'] + '<category>'
        self.top_df['Sub_Category_1'] = self.top_df['Sub_Category_1'] + '<category>'
        self.top_df['Sub_Category_2'] = self.top_df['Sub_Category_2'] + '<category>'
        self.top_df['Sub_Category_3'] = self.top_df['Sub_Category_3'] + '<category>'
        self.top_df['Sub_Category_4'] = self.top_df['Sub_Category_4'] + '<category>'
        
    @staticmethod
    def check_node(x):
        
        if x == '<product>' or x == '<category>':
            return False
        else:
            return True
        
    def createProductRankBranch(self):
        
        data = self.top_df.copy()
        data['Name'] = data['Clean_Name']
        

        # top 30 products
        data = data[data['Rank'] <= 30]

        
        data = data[['Name','Rank',"Combination","Relevant_Date"]]

        

        data1 = data.copy()
        data1.columns = ["Name1", "Rank1","Combination","Relevant_Date"]

        data1 = data1.sort_values(by=['Rank1'])
        data = data.sort_values(by=['Rank'])

        pair_data = data.merge(data1)
        pair_data['WEIGHT'] = np.where(pair_data['Rank1'] <= (pair_data['Rank']-1),1,0)
        pair_data = pair_data[pair_data['WEIGHT']==1]
        x = pair_data.groupby(['Name','Name1'])['WEIGHT'].sum().reset_index()
        pair = x
        pair.columns = ["from", "to", "weight"]
        
        self.pair = pair
        
    def createBidirectedCategoryBranch(self):
        
        df = self.top_df.copy()
        df = df.drop(['Combination','Relevant_Date','Rank'], axis = 1)
        
        combos = list(combinations(df.columns, 2))
        
        final = []


        for _,row in df.iterrows():
            
            for combo in combos:
                
                if 'Clean_Name' in combo:
                    
                    if AmazonPersonalizedRank.check_node(row[combo[0]]) and AmazonPersonalizedRank.check_node(row[combo[1]]):
                        
                        temp = {
                            'from' : row[combo[0]],
                            'to' : row[combo[1]],
                            }
                        temp_reverse = {
                            'from' : row[combo[1]],
                            'to' : row[combo[0]],
                            }
                        final.append(temp)
                        final.append(temp_reverse)
            
                
                
        final_df = pd.DataFrame(final)
        
        # self.final_df = final_df
        # self.final_df['weight'] = 1
        final_df['weight'] = 1
        return final_df
        
    def createGraph(self):  
        
        self.createProductRankBranch()
        self.createBidirectedCategoryBranch()
        
        total_df= pd.concat([self.final_df,self.pair])
        # total_df = self.pair
        
        self.G=ig.Graph.TupleList(total_df.itertuples(index=False), directed=True, weights=True)
        self.word=[v['name'] for v in self.G.vs]
        
    def createPersonalizedRank(self,all_vertices):
        
        self.personalized_ranking=self.G.personalized_pagerank(damping=0.85,reset_vertices=all_vertices,weights='weight')
        
        vec=pd.DataFrame({'Keyword' : self.word, 'Rank' : [x*100 for x in np.array(self.personalized_ranking)]})
    
        vec= vec.sort_values('Rank',ascending=False).reset_index(drop = True)
        products = vec[vec['Keyword'].str.contains('<product>')]     
        products_vec_sum = sum(products['Rank'].to_list())
        products['Rank'] = (products['Rank']/products_vec_sum)*100
        products['Keyword'] = products['Keyword'].str.replace('<product>','')
        
        return products
    
    
# def spell_check(sent, sym_spell):

#     sent = sent.strip()

#     # new_sent = sent_sugg[0].term

#     sent_sugg = sym_spell.lookup_compound(sent, ignore_non_words = True, ignore_term_with_digits = True, max_edit_distance=3)

#     sent = sent_sugg[0].term

#     sent = sent.split()

#     # new_sent = sym_spell.word_segmentation(sent, max_edit_distance = 2)

#     new_sent = []
#     for i,word in enumerate(sent):

#         if i < 3:
#             suggestions = sym_spell.lookup(word, Verbosity.TOP, max_edit_distance=2)

#             if len(suggestions) > 0:

#                 suggestion = suggestions[0].term

#                 new_sent.append(suggestion)

#             else:

#                 new_sent.append(word)

#         else:

#             new_sent.append(word)

#     new_sent = ' '.join(new_sent)

#     # new_sent = sym_spell.lookup_compound(new_sent, ignore_non_words = True, ignore_term_with_digits = True,max_edit_distance=2)

#     # new_sent = new_sent[0].term

#     return new_sent



# def getBrands(ner_model,text):
#     doc = ner_model(text)
#     results = []
#     for ent in doc.ents:
#         if ent.start_char == 0:
#             results.append(ent.text)
#     return (results)

# def getFirstBrands(ner_model,text):
#     doc = ner_model(text)
#     results = []
#     for ent in doc.ents:
#         results.append(ent.text)

#     if results != []:
#         return (results[0])
#     else:
#         return None


# def Brand_Entity_Recognition(df, ner_model,spell_checker):

#     df['Product_Name'] = df['Clean_Name'].str.lower()
#     df['Product_Name'] = df['Product_Name'].apply(lambda x: x.replace('&', 'and'))
#     df['Product_Name'] = df['Product_Name'].apply(lambda x: x.replace(',', ' '))
#     df['Product_Name'] = df['Product_Name'].apply(lambda x: x.replace('é', 'e'))
#     df['Product_Name'] = df['Product_Name'].apply(lambda x: x.replace('è', 'e'))
#     df['Product_Name'] = df['Product_Name'].apply(lambda x: re.sub('[^\w ]+','',x))
#     df['Product_Name'] = df['Product_Name'].apply(lambda x: re.sub('  +',' ',x).strip())

#     df['Model_Brands'] = df['Product_Name'].apply(lambda x: getBrands(ner_model,x))
#     df['Model_Brands'] = [x[0] if x != [] else None for x in df['Model_Brands']]

#     df_blank = df[df['Model_Brands'].isna()]

#     df.dropna(subset = ['Model_Brands'], inplace = True)

#     df_blank['Product_Name_2'] = df_blank['Product_Name'].apply(lambda x: spell_check(x,spell_checker))
#     df_blank['Model_Brands'] = df_blank['Product_Name_2'].apply(lambda x: getBrands(ner_model,x))
#     df_blank['Model_Brands'] = [x[0] if x != [] else None for x in df_blank['Model_Brands']]
#     df_blank['First_Word'] = [x.split()[0].strip() if len(x.split()[0].strip()) > 2 else ' '.join(x.split()[:2]).strip() for x in df_blank['Product_Name']]
#     df_blank['Product_Name'] = np.where(df_blank['Model_Brands'].isna() == False, df_blank['Product_Name'], df_blank['Product_Name_2'])

#     print('Hi')
    
#     df_blank['Recognition_Type'] = None
#     df_blank['Recognition_Type'] = np.where(df_blank['Model_Brands'].isna(), 'First Brand with NER', df_blank['Recognition_Type'])
#     df_blank['Model_Brands'] = [getFirstBrands(ner_model,name) if brand == None else brand for name,brand in zip(df_blank['Product_Name'],df_blank['Model_Brands'])]
#     # df_blank['Model_Brands'] = np.where(df_blank['Model_Brands'].isna(), getFirstBrands(ner_model,df_blank['Product_Name']), df_blank['Model_Brands'])

#     df_blank['Recognition_Type'] = np.where(df_blank['Model_Brands'].isna(), 'First Word', df_blank['Recognition_Type'])
#     df_blank['Model_Brands'] = np.where(df_blank['Model_Brands'].isna(), df_blank['First_Word'], df_blank['Model_Brands'])

#     # uploadUnrecognizedBrands(df_blank[['Product_Name','Model_Brands','Recognition_Type','Category','Required_Category','Relevant_Date']].copy())
    
#     df_blank = df_blank.drop(['First_Word','Product_Name_2','Recognition_Type'], axis = 1)

#     total_df = pd.concat([df,df_blank])
#     total_df['Clean_Name'] = total_df['Product_Name']
#     total_df['Brand'] = total_df['Model_Brands']
#     total_df = total_df.drop(['Product_Name','Model_Brands'], axis = 1)

#     return total_df
            


# direc = "C:/Users/Abdulmuizz/Desktop/ADQVest/Amazon Analysis/Amazon_Directory/"
# global_amazon_category = "Beauty"
# # Load Spell Checker
# sym_spell = SymSpell()
# unigram_path = direc + f"unigrams_{global_amazon_category.replace(' ','_').lower()}.txt"
# #unigram_path = r"C:\Users\Abdulmuizz\Desktop\ADQVest\Amazon Analysis\corpus_all.txt"#Test purpose
# sym_spell.load_dictionary(unigram_path, 0,1, separator = '@@')
# bigram_path = direc + f"bigrams_{global_amazon_category.replace(' ','_').lower()}.txt"
# #bigram_path = r"C:\Users\Abdulmuizz\Desktop\ADQVest\Amazon Analysis\corpus_all_bigram.txt"#Test purpose
# sym_spell.load_bigram_dictionary(bigram_path, 0, 2)
# sym_spell._max_dictionary_edit_distance = 3
# sym_spell._count_threshold = 100


# # Load Spacy Model
# os.chdir("C:/Users/Abdulmuizz/Desktop/ADQVest/Amazon Analysis/Amazon_Directory/")
# nlp = spacy.load(direc + f"amazon_ner_{global_amazon_category.replace(' ','_').lower()}_ml_model")
# #nlp = spacy.load("amazon_ner")#Test purpose


# #Parent Brands
# with open(direc + f"parent_brands_{global_amazon_category.replace(' ','_').lower()}.json", 'r') as f:
#     parent_brands = json.load(f)


# client = ClickHouse_db.db_conn()
# d = client.query_dataframe("Select * from AMAZON_CLEAN_4_ALL_CATGS_DAILY_DATA WHERE Category = 'Beauty' and Relevant_Date between '2022-12-01' and '2023-02-28'")

# rank = AmazonPersonalizedRank(d)

# rank.createGraph()
#%%
# com = 'Beauty<category>|Skin Care<category>|Face<category>|Creams and Moisturisers<category>'.split('|')
# ranking_vec = rank.createPersonalizedRank(com) 

# ranking_vec.rename(columns = {"Keyword" : "Clean_Name"}, inplace = True)

# ranking_vec = Brand_Entity_Recognition(ranking_vec,nlp,sym_spell)

# ranking_vec['Parent_Brand'] = [parent_brands[x] if x in parent_brands.keys() else x for x in ranking_vec['Brand']]
# ranking_vec['Brand'] = ranking_vec['Brand'].str.title()
# ranking_vec['Parent_Brand'] = ranking_vec['Parent_Brand'].str.title()
# ranking_vec.rename(columns = {'Brand' : 'Child_Brand'}, inplace = True)
# ranking_vec.rename(columns = {'Parent_Brand' : 'Brand'}, inplace = True)


# brand_vec = ranking_vec.groupby(['Brand']).sum().sort_values('Rank',ascending = False)


#%%

client = ClickHouse_db.db_conn()
categories = client.query_dataframe("Select distinct Combination from AMAZON_CLEAN_4_ALL_CATGS_DAILY_DATA WHERE Category = 'Beauty' order by Combination")['Combination'].to_list()


for i,category in enumerate(categories):
    
    print(i,'---->',category)
    
    d = client.query_dataframe(f"Select * from AMAZON_CLEAN_4_ALL_CATGS_DAILY_DATA where Combination = '{category}'")
    
    rank = AmazonPersonalizedRank(d)
    
    graph = rank.createBidirectedCategoryBranch()
    
    client.execute("INSERT INTO AMAZON_BIDIRECTED_GRAPH VALUES", graph.values.tolist())



