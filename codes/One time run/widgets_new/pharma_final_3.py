from functools import lru_cache
import collections,itertools
from companyspellcorrector import edits1,edits2,Lookup,PdistR
from companyspellcorrector import write_corpus,datafile,avoid_long_words, final_edits
import tiktoken,math,os
from vocab import vocab_freq, write_csv
import pandas as pd
import time


# def final_call():
def segment(text):
    "Return a list of words that is the best segmentation of text."
    if not text: return []
    candidates = ([first.strip()]+segment(rem) for first,rem in splits(text))
    return max(candidates, key=logPwords)

def splits(text, L=200):
    "Return a list of all possible (first, rem) pairs, len(first)<=L." 
    return [(text[:i+1], text[i+1:])
               for i in range(min(len(text), L))]

def logPwords(words):
    "The Naive Bayes probability of a sequence of words." 
    return sum(math.log(PVOCAB(w)) for w in words)

def mid_candidates(word): 
    "Generate intermediate spelling corrections for word."
    tmp = set([word]).union(edits1(word),edits2(word))
    tmp = [t for t in tmp if t in SYMSPELL]
    return tmp if tmp else[word]

def suggestions(word,N=5):
    "Generate candidates and return top word as candidate"
    cdt     = candidates(word)
    return sorted(cdt,key=PVOCAB,reverse=True)[:N]

def candidates(word): 
    "Generate possible spelling corrections for word."
    ints    = mid_candidates(word)
    cdt     = [item for i in ints if i in SYMSPELL 
               for item in SYMSPELL[i]]
    return list(collections.Counter(cdt)) if cdt else [word]

def correct(text,K=2):
    "Spell-correct all words in text."
    tokens      =  segment(text.strip('\n'))
    tokens      =  tokens.remove(' ') if ' ' in tokens  else tokens
    cdt_tks     =  [suggestions(tk) for tk in tokens]
    cdts        =  itertools.product(*cdt_tks)
    out         =  max([' '.join(t) for t in cdts],key=PNAME)
    
    return out if (PNAME(out)/PNAME(text) > K and out in NAMES) else text

DECIMALS = 5
CORPUS_SIZE = 20000000

os.chdir('F:/ADQVest Captial/Company_Mapping/widgets_new')

print('checking 3')
PNAME         =   PdistR(datafile('company_corpus.txt'), 
                               N=CORPUS_SIZE, missingfn=avoid_long_words)
print('checking 4')
vocab_freq('company_corpus.txt')

print('checking 5')
PVOCAB        =   PdistR(datafile('word_freq.txt'), 
                               N=CORPUS_SIZE, missingfn=avoid_long_words)
print('checking 6')
final_edits()
print('checking 7')
SYMSPELL      =   Lookup(datafile('edits2.txt'))

NAMES         =   list(PNAME.keys())

spell_output = {x:correct(x) for x in NAMES}
first_level   =   [(x,spell_output[spell_output[x]]) for x in spell_output.keys()]
# write_corpus(first_level,'first_level.txt')

encoding = tiktoken.get_encoding("cl100k_base")
encoding = tiktoken.encoding_for_model("gpt-3.5-turbo")

def cosine(a,b):
    inter = set(a).intersection(set(b))
    dott = sum((IDF[i] for i in inter),0)
    moda = sum((IDF[i] for i in set(a)),0)
    modb = sum((IDF[i] for i in set(b)),0)
    try:
        check = dott/math.sqrt(moda*modb)
    except:
        print(a, b)
    return check


def invdocfreq(x):
    out={}
    N = len(x)
    tot = [item for m,_ in x for item in m]
    out = dict(collections.Counter(tot))
    idf = {t:math.log(1+N)-math.log(1+out[t]) for t in out.keys()}
    return idf

encoded_names   =   [(encoding.encode(x),PNAME(x)*CORPUS_SIZE) for x in list(PNAME.keys())]
IDF             =   invdocfreq(encoded_names)

out             =   [(encoding.decode(y),[encoding.decode(x) for x,_ in encoded_names if cosine(x,y)>0.70]) 
                      for y,_ in encoded_names]

# out             =   [(encoding.decode(y),[encoding.decode(x) for x,_ in encoded_names[:1702] if cosine(x,y)>0.70]) 
#                       for y,_ in encoded_names[1702:]]

cosine_output   =   {x:max(y,key=PNAME) if y != [] else '' for x,y in out}

stage1          =   {x:max([cosine_output[x],spell_output[x]],key=PNAME)
                      for x in list(PNAME.keys())}

# stage1          =   {x:max([cosine_output[x]],key=PNAME)
#                       for x in list(PNAME.keys())[1702:]}

full_output   =   [(x,stage1[stage1[x]]) for x in stage1.keys()]
# full_output   =   [(x,stage1[x]) for x in stage1.keys()]
full_output   =   [(x,y,x==y) for x,y in full_output]
full_output   =   sorted(full_output,key=lambda x:PNAME(x[1]),reverse=True)
# write_corpus(full_output,'second_level.txt')

# write_csv(full_output, 'second_level.csv')
print("done")
    # return first_level, full_output


# first = []
# second = []
# company_list = pd.read_csv('company_list.csv')

# for j in ['auto']:
#     print(j)
#     company = company_list[company_list['js_file'] == j]
    
#     company = company[['company', 'count']]
    
#     company.to_csv('company_corpus.txt', sep = '\t', index=False, header = False)
#     a, b = final_call()
#     first.extend(a)
#     second.extend(b)

# write_corpus(first,'first_level.txt')
# write_corpus(second,'second_level.txt')
# write_csv(second, 'second_level.csv')
