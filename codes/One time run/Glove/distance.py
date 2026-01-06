# -*- coding: utf-8 -*-
"""
Created on Wed Apr 12 11:27:10 2023

@author: hp
"""

from functools import lru_cache
import os
import collections
import re
import textdistance
from polyleven import levenshtein
from math import log10
import pandas as pd
import textdistance

os.chdir(r'C:\Users\Administrator\AdQvestDir\codes\One time run\Glove')
infile = 'COMPANY_JSFILE.csv'

df = pd.read_csv(infile, encoding= 'unicode_escape')

df['words'] = [x.lower() for x in df['words']]
df['words'] = [x.replace('private', 'pvt').replace('limited', 'ltd') for x in df['words']]
df['words'] = [x.replace('.', '').strip() for x in df['words']]
df

# entry = pd.DataFrame(columns=('words', 'mapping', 'score'))
# for i in df['words']:
#     for j in df['words']:
#         score = textdistance.jaro_winkler.normalized_similarity(i,j)
#         if (score != 1 and score >0.95):
#             print(i,j, score)
#             entry.loc[len(entry.index)] = [i,j, score] 

entry = pd.DataFrame(columns=('words', 'mapping', 'score', 'js_file'))
for file in df['js_file'].unique():
    com = df[df['js_file']==file]
    for i in com['words']:
        for j in com['words']:
            score = textdistance.jaro_winkler.normalized_similarity(i,j)
            if (score != 1 and score >0.9):
                print(i,j, score)
                entry.loc[len(entry.index)] = [i,j, score, file] 
            
entry.to_csv('distance_file.csv')
            
