# -*- coding: utf-8 -*-
"""
Created on Thu Sep 14 14:42:46 2023

@author: hp
"""

import pandas as pd
import os

os.chdir('C:/Users/Administrator/AdQvestDir/codes/One time run/widgets_new')


def vocab_freq(filename):
    df = pd.read_csv(filename, sep = '\t')
    df.columns = ['Company', 'Freq']
    
    vocab = dict()
    for index, row in df.iterrows():
        for word in row['Company'].split(" "):
            if word in vocab:
                vocab[word] += row['Freq']
            else:
                vocab[word] = row['Freq']
                
    with open("pharma.txt", 'w') as f: 
        for key, value in vocab.items(): 
            f.write('%s\t%s\n' % (key, value))
            
def write_csv(file, filename):
    df = pd.DataFrame(file)
    df.to_csv(filename)
    

