# -*- coding: utf-8 -*-
"""
Created on Mon May 08 19:51:42 2017

@author: lizard
"""
import datetime
import pandas as pd
from WindPy import w
w.start()

with open('futureInfo.xlsx','rb') as f:
    df = pd.read_excel(f,skip_footer=2)
    
def standardize(data):
    data = data.dropna()
    data.index = data.index+datetime.timedelta(minutes=1)
    data.index = data.index.map(lambda t:t.replace(microsecond=0))
    return data

for k,row in df.iterrows():
    code = row[u'证券代码']
    code = ''.join(i for i in code if not i.isdigit())  #获得主力合约代码
    data = w.wsi("IF.CFE", "open,high,low,close,volume,oi", "2017-05-07 09:00:00", "2017-05-08 20:18:41", "")
    data = pd.DataFrame(index=data.Fields, columns=data.Times, data=data.Data).T
    standardize(data)