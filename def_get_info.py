
# coding: utf-8

# In[108]:

import pandas as pd
import numpy as np
import urllib.request
from urllib.request import urlopen
from bs4 import BeautifulSoup
import re
import unicodedata
import html


# In[109]:

df = pd.read_excel('info.xlsx')


# In[110]:

return_dict = {}
clean_dict ={}


# In[111]:

def getpledge(i):
    try:
        path = df['FName'].loc[i]
        link = 'https://www.sec.gov/Archives/' + path
        htmlfile = urllib.request.urlopen(link)
        htmltext = htmlfile.read().decode('utf-8')
        htmltext_1 = html.unescape(htmltext)
        soup = BeautifulSoup(htmltext_1, 'lxml')
        x = soup.get_text()
        return_dict[i] = x
        print (i)
    except:
        return_dict[i] = ''
        print ('error', i)
        return


# In[126]:

def cleanpledge(i):
    if return_dict[i] != None:
        try:
            path = df['FName'].loc[i]
            index = df['index'].loc[i]
            clean0 = return_dict[i].replace('\n', " ")
            clean1 = clean0.replace('\t', " ")
            clean2 = re.sub(r'<(.*?)>',"", clean1)
            clean2 = " ".join(clean2.split())
            unicode_str = str(clean2)
            cleaned = unicodedata.normalize("NFKC", unicode_str)
            text = cleaned.split('. ')
            allmatch = [text[s] + '. ' for s in range (0, len(text)) if 'pledg' in ''.join(text[s:s+1])]
            pledge = ''.join(allmatch)
            clean_dict[i] = (pledge, index, path)
        except:
            print('clean error', i)
        return
    else:
        print('empty', i)
        return


# In[127]:

def savepledge(i):
    getpledge(i)
    cleanpledge(i)
    return


# In[128]:

def parallel(function, loop):
    from joblib import Parallel, delayed
    import multiprocessing
    core = multiprocessing.cpu_count()
    Parallel(n_jobs= core, backend='threading')(delayed(function)(i) for i in loop)


# In[ ]:

parallel(savepledge, df.index)


# In[ ]:

pledgedf = pd.DataFrame.from_dict(clean_dict, orient = 'index' )


# In[ ]:

pledgedf.columns = ['infoclean2', 'index', 'path']


# In[ ]:

pledgedf.to_excel('pledgeinfo.xlsx')

