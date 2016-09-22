
# coding: utf-8

# In[ ]:

import pandas as pd
import numpy as np
from joblib import Parallel, delayed
import multiprocessing
import pickle
import spacy
import re
import string


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
number_dict = {}
name_dict = {}


# In[ ]:

key = re.compile(r'(share.+?)', re.I)


# In[ ]:

def parallel(function, loop):
    core = multiprocessing.cpu_count()
    Parallel(n_jobs= core, backend='threading')(delayed(function)(i) for i in loop)


# In[ ]:

def num_person(i, name, number,entities):
    for p in name:
        idx = int(p[0])
        while (idx < len(entities)-1):
            if entities[idx+1][2] == 'CARDINAL':
                m = str(entities[idx+1][1])
                if m in number:
                    share_num[i].append((p[1], m))
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
    return


# In[ ]:

def find_person(i):
    token = token_dict[i]
    if token != ['empty']:
        tags = [(i, x, x.tag_) for i, x in enumerate(token)]
        ents = list(token.ents)
        entities = [(i,e, e.label_) for i, e in enumerate(ents) if e.label_ == 'PERSON' or e.label_ == 'CARDINAL']
        pos_dict[i] = tags
        ent_dict[i] = entities
        number = [str(s[1]) for s in tags if s[2] == 'CD' and ('shares' in str(token[s[0]-3:s[0]+5]) and 'pledg' in str(token[s[0]-3:s[0]+5]))]
        name = [(t[0],t[1]) for t in entities if t[2] == 'PERSON']
        name_dict[i] = name
        number_dict[i]  = number
        info['num'].loc[i] = ';'.join(str(number))
        share_num[i] = []
        if  number != [] and name != [] and entities != []:
            num_person(i, name, number, entities)
            info['num_person'].loc[i] = str(share_num[i]).strip('[]')
    return


# In[ ]:

def extract(i):
    share_token(i)
    find_person(i)
    return print(i)


# In[ ]:

parallel(extract, info.index)


# In[ ]:

info.to_excel('share_num_person.xlsx', index = False)


# In[ ]:




# In[ ]:



