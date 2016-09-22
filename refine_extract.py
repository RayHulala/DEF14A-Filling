
# coding: utf-8

# # A refinement of 1st version of pledge information extraction

# In[ ]:

import pandas as pd
import numpy as np
from joblib import Parallel, delayed
import multiprocessing
import pickle
import spacy
import re
import string
import deepdish as dd


# In[ ]:

nlp = spacy.load('en')


# In[ ]:

info_dict = dd.io.load('cleaned_info.h5')


# In[ ]:

info = pd.DataFrame.from_dict(info_dict,orient='index')


# In[ ]:

raw = pd.read_csv('def14a.csv')


# In[ ]:

def parallel(function, loop):
    core = multiprocessing.cpu_count()
    Parallel(n_jobs= core, backend='threading')(delayed(function)(i) for i in loop)


# In[ ]:

plginfo = info[1]
link = info_dict[2][0]


# In[ ]:

result = {}


# In[ ]:

def extract(ind):
    text = plginfo[ind]
    FName = link[ind]
    token = nlp(text)
    sents = text.split('. ')
    tags = [(i, x, x.tag_) for i, x in enumerate(token)]
    number = [str(s[1]) for s in tags if s[2] == 'CD' and 
          ('shares' in str(token[max(0,s[0]-3):min(s[0]+5, len(token)-1)])
           and 'pledg' in str(token[max(0,s[0]-3):min(s[0]+5, len(token)-1)]))]
    if number != []:
        sents_num = [i for i in sents if any(num in i for num in number)]
        entities= []
        share_num = []
        pair = []
        for sent in sents_num:
            i = sents_num.index(sent)
            token1 = nlp(sent)
            ents = list(token1.ents)
            e = ''.join(str(p) for p in ents if p.label_ in ['PERSON', 'ORG'])
            s = ''.join(str(r) for r in ents if r.label_ == 'CARDINAL')
            entities.append(e)
            share_num.append(s)
            pair.append((e,s))
            #entities.append(list((e for e in ents if e.label_ in ['PERSON', 'ORG'])))
            #share_num.append(list((r for r in ents if r.label_ == 'CARDINAL')))
            #pair.append(list(((e,r) for e in ents if e.label_ in ['PERSON', 'ORG'])))
    else:
        entities = 'None'
        share_num = 'None'
        pair = 'None'
    result[ind] = (FName, entities, share_num, pair)
    return 


# In[ ]:

def process(ind):
    try:
        extract(ind)
        print(ind)
    except:
        print('error')
        result[ind] = ()


# In[ ]:

parallel(process, plginfo.index)


# In[ ]:

dd.io.save('refined_pldinfo.h5', result, compression = 'blosc')


# In[ ]:



