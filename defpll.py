
# coding: utf-8

# In[1]:

import pandas as pd
import numpy as np
import urllib.request
from urllib.request import urlopen
import re
from joblib import Parallel, delayed
import multiprocessing




# In[4]:

def getpledge(i):
    try:
        link = 'https://www.sec.gov/Archives/' + df['FName'].loc[i]
        htmlfile = urllib.request.urlopen(link)
        htmltext = htmlfile.read().decode('utf-8')
        text = htmltext.split('. ')
        allmatch = [text[s] + '. ' for s in range (0, len(text)) if 'pledg' in ''.join(text[s-2:s+2])]
        pledge = ''.join(allmatch)
        pledge = pledge.replace('\n', "")
        pledge = re.sub('<(.*?)>',"", pledge)
        df['pledgeinfo'].loc[i] = pledge
        print (i)
    except:
        df['pledgeinfo'].loc[i] = 'error'
        print ('error', i)
    return

if __name__ == '__main__':
    df = pd.read_csv('def_form.csv')
    df['pledgeinfo'] = np.nan
    Parallel(n_jobs=4)(delayed(getpledge)(i) for i in range(32155,32165))


# In[ ]:



#df.to_excel('pledgeinfo.xlsx')


# In[ ]:



