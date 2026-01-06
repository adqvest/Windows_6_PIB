import os,math
from functools import lru_cache

DECIMALS = 5
CORPUS_SIZE = 20000000

class PdistR(dict):  
    "A probability distribution estimated from counts in datafile."
    def __init__(self, data, N=None, missingfn=None):
        print(data)
        for key,count in data:
            key = key.strip().lower().replace(" and "," & ")
            self[key] = self.get(key, 0) + int(count)
        self.N = N or sum(self.values())
        self.missingfn = missingfn or (lambda k,N : 1)
        
    def __call__(self, key):
            return self.get(key.lower(),self.missingfn(key,self.N))/self.N

class Lookup(dict):
    "A lookup disctionary from misspelled words to correctly spelled words."
    def __init__(self, data):
        for val,key in data:
            ikey = standardise(key)
            if len(ikey)>1:
                self.ikey = self.setdefault(ikey, set()).add(val.lower())
            
    def __call__(self, key):
        key = standardise(key)
        if key in self: return list(self.get(key)) 
        else: return ['']
        
def avoid_long_words(key,N):
    if key.isnumeric(): return 10/N 
    else : return 10/(N * 10**max(1,len(key)))

@lru_cache
def edits(word): 
    "Generate delete-only spelling corrections for word."
    if word.isnumeric(): return set([word])
    return set([word]).union(edits1(word),edits2(word))

def standardise(text):
    return text.strip().lower().replace(" and "," & ")

@lru_cache()
def edits1(word):
    "All edits that are one edit away from `word`."
    if len(word)<5: return set()
    splits      = [(word[:i], word[i:])    for i in range(len(word) + 1)]
    deletes     = [L + R[1:]               for L, R in splits if R and L]
    return set(deletes)

@lru_cache()
def edits2(word):
    "All edits that are two edits away from `word`."
    if len(word)>5:
        return set(e2 for e1 in edits1(word) for e2 in edits1(e1)) 
    return set()

def datafile(name, sep='\t'):
    "Read text files for dictionary making"
    with open(name, 'r',errors='ignore') as file:
        for line in file: yield line.split(sep)

def symdelete(location_count):
    tmp = [( token, j )  
           for token,count in location_count for j in edits(token)]
    return tmp

def corpuscount():
    listOfLocations = list(Pw.keys())
    return [( token,int(Pw(token)*Pw.N) ) for token in listOfLocations]


def write_corpus(table,file):
    with open(file, 'w') as f:
        for row in table:
            line = '\t'.join(str(tk) for tk in row)
            f.write(line + '\n')
    return 0

def segment(text):
       "Return a list of words that is the best segmentation of text."
       if not text: return []
       candidates = ([first]+segment(rem) for first,rem in splits(text))
       return max(candidates, key=logPwords)

def splits(text, L=200):
    "Return a list of all possible (first, rem) pairs, len(first)<=L." 
    return [(text[:i+1], text[i+1:])
               for i in range(min(len(text), L))]

def logPwords(words):
    "The Naive Bayes probability of a sequence of words." 
    return sum(math.log(Pw(w)) for w in words)

os.chdir('C:/Users/Administrator/AdQvestDir/codes/One time run/widgets_new_1/widgets_new')

# Pw                  =   PdistR(datafile('word_freq.txt'),   
#                                 N=CORPUS_SIZE,   
#                                 missingfn=avoid_long_words)

def final_edits():
    print('checking 2')
    global Pw
    Pw                  =   PdistR(datafile('word_freq.txt'),   
                                N=CORPUS_SIZE,   
                                missingfn=avoid_long_words)
    print('checking 1')
    EDIT_FILE           =   'edits2.txt'
    location_corpus     =   corpuscount()
    corpus              =   symdelete(location_corpus)
    write_corpus(corpus,EDIT_FILE)



