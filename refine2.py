
# coding: utf-8

# # A refinement of 1st version of pledge information extraction

# In[1]:

import pandas as pd
import numpy as np
from joblib import Parallel, delayed
import multiprocessing
import pickle  
import spacy
import re
import string
import deepdish as dd


# In[2]:

nlp = spacy.load('en')


# In[3]:

info_dict = dd.io.load('cleaned_info.h5')


# In[4]:

info = pd.DataFrame.from_dict(info_dict,orient='index')


# In[5]:

def parallel(function, loop):
    core = multiprocessing.cpu_count()
    Parallel(n_jobs= core, backend='threading')(delayed(function)(i) for i in loop)


# In[6]:

plginfo = info[1]
link = info_dict[2][0]


# In[48]:

result = {}


# In[49]:

def extract(ind):
    text = plginfo[ind]
    FName = link[ind]
    sents = text.split('. ')
    sents_plg = [sent for sent in sents if ('share' or 'Share' or 'SHARE') in sent and  ('pledg'or 'Pledg'or'PLEDG') in sent]
    token = nlp(' '.join(sents_plg))
    tags = [(i, x, x.tag_) for i, x in enumerate(token)]
    number = [str(s[1]) for s in tags if s[2] == 'CD' and ',' in str(s[1])]
    if number != []:
        entities= []
        share_num = []
        pair = []
        for sent in sents_plg:
            tk = nlp(sent)
            ents = list(tk.ents)
            e = ';'.join(str(p) for p in ents if p.label_ in ['PERSON', 'ORG'])
            s = ';'.join(str(r) for r in ents if r.label_ == 'CARDINAL' and str(r) in number)
            entities.append(e)
            share_num.append(s)
            pair.append((e,s))
            e = ''
            s = ''
    else:
        entities = 'None'
        share_num = 'None'
        pair = 'None'
    result[ind] = (FName, entities, number, share_num, pair)
    return 


# In[16]:

def process(ind):
    try:
        extract(ind)
        print(ind)
    except:
        print('error')
        result[ind] = ()


# In[46]:

parallel(process, plginfo.index)


# In[ ]:

dd.io.save('refined_plginfo.h5', result, compression = 'blosc')


# In[ ]:



