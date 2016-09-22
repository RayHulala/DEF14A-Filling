
# coding: utf-8

# In[ ]:

import pandas as pd
import numpy as np
from joblib import Parallel, delayed
import multiprocessing
import pickle
import spacy
import re


# In[ ]:

nlp = spacy.load('en')


# In[ ]:

info = pd.read_excel('info.xlsx')
info['share_indicator'] = np.nan
info['num'] = np.nan
info['person'] = np.nan
info['num_person'] = np.nan


# In[ ]:

token_dict = {}
pos_dict = {}
ent_dict = {}
share_num = {}
share_num_all = {}


# In[ ]:

key = re.compile(r'(share.+?)', re.I)


# In[ ]:

def parallel(function, loop):
    core = multiprocessing.cpu_count()
    Parallel(n_jobs= core, backend='threading')(delayed(function)(i) for i in loop)


# In[ ]:

def num_person(i, name, entities):
    for p in name:
        idx = p[0]
        if idx == len(entities):
            break
        if entities[idx+1][2] == 'CARDINAL':
            m = entities[idx+1][1]
            for n in number:
                if str(m) == str(n):
                    share_num[i].append((p[1], n))
                    entities[idx+1] = ('DONE','DONE','DONE')
                else:
                    idx+=1
        else:
            idx+=1
    return


# In[ ]:

def share_token(i):
    text = info['infoclean2'].loc[i]
    find_share = re.findall(key,text)
    if find_share != []:
        info['share_indicator'].loc[i] = 1
        token_dict[i] = nlp(text)
    else:
        info['share_indicator'].loc[i] = 0
        token_dict[i] = ['empty']
    return print(i, 'find')


# In[ ]:

def find_person(i):
    token = token_dict[i]
    if token != ['empty']:
        tags = [(i, x, x.tag_) for i, x in enumerate(token)]
        ents = list(token.ents)
        entities = [(i,e, e.label_) for i, e in enumerate(ents) if e.label_ == 'PERSON' or e.label_ == 'CARDINAL']
        pos_dict[i] = tags
        ent_dict[i] = entities
        number = [s[1] for s in tags if s[2] == 'CD' and ('shares' in str(token[s[0]-3:s[0]+5]) and 'pledg' in str(token[s[0]-3:s[0]+5]))]
        name = [(t[0],t[1]) for t in entities if t[2] == 'PERSON']
        info['num'].loc[i] = ';'.join(list(number))
        share_num[i] = []
        if  number != [] and name != []:
            num_person(i, name, entities)
            info['num_person'].loc[i] = ';'.join(list(share_num[i]))
        else:
            info['num_person'].loc[i] = 'empty'
    return print (i)


# In[ ]:

parallel(share_token, info.index)


  

# In[ ]:

parallel(find_person, info.index)


# In[ ]:

info.to_excel('share_num_person.xlsx', index = False)


# In[ ]:




# In[ ]:



