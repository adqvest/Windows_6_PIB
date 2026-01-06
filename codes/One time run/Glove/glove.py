# -*- coding: utf-8 -*-
"""
Created on Tue Mar 21 16:00:23 2023

@author: hp
"""

import numpy as np
from scipy import spatial
import matplotlib.pyplot as plt
from sklearn.manifold import TSNE
import os
import pandas as pd
import itertools

os.chdir(r'C:\Users\Administrator\AdQvestDir\codes\One time run\Glove')

embeddings_dict = {}

with open("glove.6B.300d.txt", 'r', encoding="utf-8") as f:
    for line in f:
            values = line.split()
            word = values[0]
            vector = np.asarray(values[1:], "float32")
            embeddings_dict[word] = vector

def find_closest_embeddings(embedding):
    score = [(word, spatial.distance.cosine(embeddings_dict[word], embedding)) for word in embeddings_dict.keys()]
    score = dict([(key, value) for key, value in score])
    score = dict(sorted(score.items(), key=lambda item: item[1]))
    out = {k:v for k,v in score.items() if v < 0.3}
    return out

words = pd.read_csv('corpus.csv')
words
df = pd.DataFrame(columns = ['words','mapping'])
for i in words['words']:
    print([i])
    try:
        similar = find_closest_embeddings(embeddings_dict[i.lower()])
    except:
        similar = []
    df.loc[len(df)] = [i, similar]
    
df.to_csv('segment_synonms.csv')