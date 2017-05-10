# -*- coding: utf-8 -*-
"""
Created on Mon May 08 19:51:42 2017

@author: lizard
"""
import pandas as pd
from WindPy import w
from datetime import datetime, timedelta
import pymongo
from time import time
from multiprocessing.pool import ThreadPool

from ctaBase import *
from vtConstant import *
from vtFunction import loadMongoSetting
from datayesClient import DatayesClient

    
def standardize(data):
    data = data.dropna()
    data.index = data.index+timedelta(minutes=1)
    data.index = data.index.map(lambda t:t.replace(microsecond=0))
    return data

def insertWindDataFrame(data, dbName, symbol):
    """将Wind导出的Dataframe历史数据插入到Mongo数据库中
        数据样本：
        //时间,开盘价,最高价,最低价,收盘价,成交量,持仓量
        2017/04/05 09:00,3200,3240,3173,3187,312690,2453850
    """
    
    start = time()
    print u'开始读取数据插入到%s的%s中' %(dbName, symbol)
    
    # 锁定集合，并创建索引
    host, port, logging = loadMongoSetting()
    
    client = pymongo.MongoClient(host, port)    
    collection = client[dbName][symbol]
    collection.ensure_index([('datetime', pymongo.ASCENDING)], unique=True)
    
    # 读取数据和插入到数据库
    for barDatetime,row in data.iterrows():
        bar = CtaBarData()
        bar.vtSymbol = symbol
        bar.symbol = symbol
        
        bar.datetime = barDatetime
        bar.date = bar.datetime.date().strftime('%Y%m%d')
        bar.time = bar.datetime.time().strftime('%H:%M:%S')
        
        bar.open = float(row['open'])
        bar.high = float(row['high'])
        bar.low = float(row['low'])
        bar.close = float(row['close'])
        
        bar.volume = float(row['volume'])
        bar.openInterest = float(row['position'])

        flt = {'datetime': bar.datetime}
        collection.update_one(flt, {'$set':bar.__dict__}, upsert=True)
    
    print u'插入完毕，耗时：%s' % (time()-start)

def insertAllHistoryData():
    w.start()
    with open('futureInfo.xlsx','rb') as f:
        futuresInfo = pd.read_excel(f,skip_footer=2)
    
    for k,row in futuresInfo.iterrows():
        code = row[u'证券代码']
        code = ''.join(i for i in code if not i.isdigit())  #获得主力合约代码
        print u"开始从Wind下载%s历史数据" %code
        
        data = w.wsi(code, "open,high,low,close,volume,oi", "2013-01-01 09:00:00", datetime.now(), "")
        data = pd.DataFrame(index=data.Fields, columns=data.Times, data=data.Data).T
        data = standardize(data)
        print u"下载完毕，共%i条数据" %len(data)
        if code[1] == '.':
            symbol = code[0]+'0000'
        else:
            symbol = code[:2]+'0000'
        insertWindDataFrame(data, MINUTE_DB_NAME, symbol)
        
        
if __name__ == '__main__':
    insertAllHistoryData()