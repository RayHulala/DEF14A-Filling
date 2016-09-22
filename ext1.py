
# coding: utf-8

# # 1. Import modules and data

# In[ ]:

import pandas as pd
import numpy as np
from nltk.tokenize import StanfordTokenizer
from nltk.tag import StanfordPOSTagger
from nltk.parse.stanford import StanfordParser
from nltk.parse.stanford import StanfordDependencyParser
from nltk.tag import StanfordNERTagger
from joblib import Parallel, delayed
import multiprocessing
import pickle


# In[ ]:

info = pd.read_excel('info.xlsx')


# In[ ]:

info['share_indicator'] = np.nan
info['num'] = np.nan
info['person'] = np.nan
info['num_person'] = np.nan


# In[ ]:

share_num = {}
#token_dict = {}
pos_dict = {}
ent_dict = {}
share_num_all = {}


# In[ ]:

tokenizer = StanfordTokenizer()

pos= StanfordPOSTagger('english-bidirectional-distsim.tagger')

parser = StanfordParser(model_path=u'edu/stanford/nlp/models/lexparser/englishPCFG.ser.gz')

dp = StanfordDependencyParser(model_path=u'edu/stanford/nlp/models/lexparser/englishPCFG.ser.gz')

ner = StanfordNERTagger('english.conll.4class.distsim.crf.ser.gz') 


# # 2. Define functions

# ## 2.1 Joblib paralell cumputing function for loops

# In[ ]:

def parallel(function, loop):
    core = multiprocessing.cpu_count()
    Parallel(n_jobs= core, backend='threading')(delayed(function)(i) for i in loop)


# ## 2.2 Find out data with "pledg" but not share pledging

# ### Generating indicator for "share pledging" and tokenize text

# In[ ]:

def find_share(i):
    text = info['infoclean2'].loc[i]
    if 'share' in text:
        info['share_indicator'].loc[i] = 1
        words = tokenizer.tokenize(text)
        token_dict[i] = words
    else:
        info['share_indicator'].loc[i] = 0
        token_dict[i] = ['empty']
    return print(i, 'find')


# ## 2.3 Find numbers near "share" and find name entities near such numbers

# #### Still cannot deal with parallel structure well; A, B and C pledge 1, 2, 3 shares

# In[ ]:

def find_person(i):
    token = token_dict[i]
    tags = pos_dict[i]
    entities = ent_dict[i]
    number = [s[0] for s in tags if s[1] == 'CD' and ('shares' and 'pledg') in ' '.join(token[token.index(s[0])-3:token.index(s[0])+5])]
    name = [t[0] for t in entities if t[1] == 'PERSON']
    info['num'].loc[i] = ';'.join(list(number))
    info['person'].loc[i] = ';'.join(list(name))
    share_num[i] = []
    for n in number:
        for p in name:
            if p in token[token.index(n)-5:token.index(n)+5]:
                share_num[i].append((p, n))
    info['num_person'].loc[i] = ';'.join(list(share_num[i]))
    return 


# ## 2.4 store results 

# In[ ]:

def num_person(i):
    if info['share_indicator'].loc[i] == 1:
        find_person(i)
    else:
        share_num[i] = 'None'
    return print(i)


# # 3 Computing and store results

# In[ ]:

# parallel(find_share, info.index)


# In[ ]:

#with open('token.pickle', 'wb') as f:
# Pickle the 'data' dictionary using the highest protocol available.
#    pickle.dump(token_dict, f, pickle.HIGHEST_PROTOCOL)

with open('token.pickle', 'rb') as f:
    # The protocol version used is detected automatically, so we do not
    # have to specify it.
    token_dict = pickle.load(f)

# In[ ]:

for i in info.index:
    token = token_dict[i]
    pos_dict[i] = pos.tag(token)
    ent_dict[i] = ner.tag(token)
    print (i, 'nlp')


# In[ ]:

with open('tag.pickle', 'wb') as g:
    pickle.dump(pos_dict, g, pickle.HIGHEST_PROTOCOL) 


# In[ ]:

with open('ent.pickle', 'wb') as h:
    pickle.dump(ent_dict, h, pickle.HIGHEST_PROTOCOL)


# In[ ]:

parallel(num_person, info.index)


# In[ ]:

info.to_excel('detailinfo.xlsx')


# ### merge with column index

# In[ ]:



