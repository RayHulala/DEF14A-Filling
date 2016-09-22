
# coding: utf-8

# In[1]:

import pandas as pd
import numpy as np
import urllib.request
from urllib.request import urlopen
from bs4 import BeautifulSoup
import re
from joblib import Parallel, delayed
import multiprocessing
import unicodedata


# In[26]:

df = pd.read_csv('def_form.csv')


# In[27]:

df['pledgeinfo'] = np.nan


# In[49]:

return_dict = {}
clean_dict ={}


# In[78]:

def getpledge(i):
    try:
        path = df['FName'].loc[i]
        link = 'https://www.sec.gov/Archives/' + path
        htmlfile = urllib.request.urlopen(link)
        htmltext = htmlfile.read().decode('utf-8')
        text = htmltext.split('. ')
        allmatch = [text[s] + '. ' for s in range (0, len(text)) if 'pledg' in ''.join(text[s-1:s+1])]
        pledge = ''.join(allmatch)
        return_dict[i] = pledge
        print (i)
    except:
        return_dict[i] = ''
        print ('error', i)
        return


# In[76]:

def cleanpledge(i):
    if return_dict[i] != None:
        try:
            path = df['FName'].loc[i]
            clean1 = return_dict[i].replace('\n', " ")
            clean2 = re.sub('<(.*?)>',"", clean1)
            unicode_str = str(BeautifulSoup(clean2))
            cleaned = unicodedata.normalize("NFKD", unicode_str)
            clean_dict[i] = (cleaned, i, path)
        except:
            print('clean error', i)
        return
    else:
        print('empty', i)
        return


# In[95]:

def savepledge(i):
    getpledge(i)
    cleanpledge(i)
    return


# In[100]:

def parallel(function, loop):
    core = multiprocessing.cpu_count()
    Parallel(n_jobs= core, backend='threading')(delayed(function)(i) for i in loop)


# In[101]:

parallel(savepledge, df.index)


# In[102]:

pledgedf = pd.DataFrame.from_dict(clean_dict, orient = 'index' )


# In[71]:

pledgedf.to_excel('pledgeinfo.xlsx')


# In[ ]:



