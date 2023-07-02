import os
import re  # 正则表达式提取文本
from jsonpath import jsonpath  # 解析json数据
import requests  # 发送请求
import pandas as pd  # 存取csv文件
import datetime  # 
import csv
import random
from fake_useragent import UserAgent
import time
import pickle

    
    
def trans_time(v_str):
	"""转换GMT时间为标准格式"""
	GMT_FORMAT = '%a %b %d %H:%M:%S +0800 %Y'
	timeArray = datetime.datetime.strptime(v_str, GMT_FORMAT)
	ret_time = timeArray.strftime("%Y-%m-%d %H:%M:%S")
	return ret_time


# 请求地址
url = 'https://blocks.flashbots.net/v1/blocks'


# ·······························
txMap={}

for i in range(12000000, 17010000,10000):
    TABKEY="tab"+str(i)
    print(i)

    # ua = UserAgent(path="./fake_useragent.json") 
    ua = UserAgent() 
    # ua.random随机获取一个请求头
    print(ua.random)

    headers = {
        "User-Agent": ua.random,
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "accept-encoding": "gzip, deflate, br",
    }

    params = {
        "before":i-1,
        "limit":10000
    }
    # 发送请求

    try:
        r = requests.get(url, headers=headers, params=params)
        rjson=r.json()
    except:
        i-=10000
        continue
    
    
    for block in rjson["blocks"]:
        for transaction in block["transactions"]:
            
            transaction_hash=transaction["transaction_hash"]
            txMap[transaction_hash]=transaction
            # print(transaction)
            # exit

    if i%100000==0:
        with open("./flashbots_13_16.map", "wb") as tf:
            pickle.dump(txMap,tf)
        
with open("./flashbots_13_16.map", "wb") as tf:
    pickle.dump(txMap,tf)
		
    