from functools import lru_cache
import collections,itertools
import tiktoken,math,os
import pandas as pd
import time
import os,math

os.chdir('F:/ADQVest Captial/Company_Mapping/widgets_new')

def write_corpus(table,file):
    with open(file, 'w') as f:
        for row in table:
            line = '\t'.join(str(tk) for tk in row)
            f.write(line + '\n')
    return 0

def write_csv(file, filename):
    df = pd.DataFrame(file)
    df.to_csv(filename)
        
def final_call():
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
        listOfLocations = list(PVOCAB.keys())
        return [( token,int(PVOCAB(token)*PVOCAB.N) ) for token in listOfLocations]
    
    
    def write_corpus(table,file):
        with open(file, 'w') as f:
            for row in table:
                line = '\t'.join(str(tk) for tk in row)
                f.write(line + '\n')
        return 0
    
    @lru_cache    
    def final_edits():
        EDIT_FILE           =   'edits2.txt'
        location_corpus     =   corpuscount()
        corpus              =   symdelete(location_corpus)
        write_corpus(corpus,EDIT_FILE)
        
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
                    
        with open("word_freq.txt", 'w') as f: 
            for key, value in vocab.items(): 
                f.write('%s\t%s\n' % (key, value))
                
    def write_csv(file, filename):
        df = pd.DataFrame(file)
        df.to_csv(filename)
        
    
    
    
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
    
    @lru_cache
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
    
    @lru_cache
    def correct(text,K=2):
        "Spell-correct all words in text."
        tokens      =  segment(text.strip('\n'))
        tokens      =  tokens.remove(' ') if ' ' in tokens  else tokens
        cdt_tks     =  [suggestions(tk) for tk in tokens]
        cdts        =  itertools.product(*cdt_tks)
        out         =  max([' '.join(t) for t in cdts],key=PNAME)
        
        return out if (PNAME(out)/PNAME(text) > K and out in NAMES) else text
    
    
    PNAME         =   PdistR(datafile('company_corpus.txt'), 
                                   N=CORPUS_SIZE, missingfn=avoid_long_words)
    vocab_freq('company_corpus.txt')
    
    PVOCAB        =   PdistR(datafile('word_freq.txt'), 
                                   N=CORPUS_SIZE, missingfn=avoid_long_words)
    
    final_edits()
    
    SYMSPELL      =   Lookup(datafile('edits2.txt'))
    
    NAMES         =   list(PNAME.keys())
    
    print('checking 1')
    
    spell_output = {x:correct(x) for x in NAMES}
    print('checking 1.5')
    first_level   =   [(x,spell_output[spell_output[x]]) for x in spell_output.keys()]
    # write_corpus(first_level,'first_level.txt')
    print('checking 2')
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
    print('checking 3')
    cosine_output   =   {x:max(y,key=PNAME) if y != [] else '' for x,y in out}
    
    stage1          =   {x:max([cosine_output[x],spell_output[x]],key=PNAME)
                          for x in list(PNAME.keys())}
    
    # stage1          =   {x:max([cosine_output[x]],key=PNAME)
    #                       for x in list(PNAME.keys())[1702:]}
    print('checking 4')
    full_output   =   [(x,stage1[stage1[x]]) for x in stage1.keys()]
    # full_output   =   [(x,stage1[x]) for x in stage1.keys()]
    full_output   =   [(x,y,x==y) for x,y in full_output]
    full_output   =   sorted(full_output,key=lambda x:PNAME(x[1]),reverse=True)
    # write_corpus(full_output,'second_level.txt')
    print('checking 5')
    # write_csv(full_output, 'second_level.csv')
    return first_level, full_output

first = []
second = []
company_list = pd.read_csv('company_list.csv')

# for j in company_list['js_file'].unique():
for j in ['auto']:
    print(j)
    company = company_list[company_list['js_file'] == j]
    
    company = company[['company', 'count']]
    
    company.to_csv('company_corpus.txt', sep = '\t', index=False, header = False)
    
    time.sleep(5)
    a, b = final_call()
    first.extend(a)
    second.extend(b)

write_corpus(first,'first_level.txt')
write_corpus(second,'second_level.txt')
write_csv(second, 'second_level.csv')