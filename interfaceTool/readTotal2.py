import numpy as np
import csv    #加载csv包便于读取csv文件
import zipfile
import os
import json
import pickle
import os
import datetime
import time
from datetime import timedelta
import pandas as pd
    
with open('/mnt/sde1/peilin_defi/data/tokenPrice/dict/date2token2price_v1.pkl', 'rb') as f:
    date2token2price_v1 = pickle.load(f)
with open('/mnt/sde1/peilin_defi/data/tokenPrice/dict/date2token2price_v2.pkl', 'rb') as f:
    date2token2price_v2 = pickle.load(f)

for dateOne in date2token2price_v1:
    if "ether" in date2token2price_v1[dateOne]:
        date2token2price_v1[dateOne]["0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"] = date2token2price_v1[dateOne]["ether"] 

print(len(date2token2price_v1), len(date2token2price_v2))

U_ADDR_s= set(["0xdac17f958d2ee523a2206206994597c13d831ec7", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"])

class TokenPriceCenter():
    def __init__(self, use_v1=True, use_v2 = True):
        self.use_v1 = use_v1
        self.use_v2 = use_v2

    def swap(self, tokenAddress, timestamp_removeHour,inputAmount):
        # print("swaping", tokenAddress, inputAmount)
        if tokenAddress=="0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee":
            tokenAddress="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
            
        if tokenAddress in U_ADDR_s:
            return inputAmount / 1000000
        else:
            t = time.strftime("%Y-%m-%d", time.gmtime(timestamp_removeHour))
            dateOne = time.strptime(t, "%Y-%m-%d")
            
            if dateOne in date2token2price_v2 and self.use_v2:
                if tokenAddress in date2token2price_v2[dateOne]:
                    return date2token2price_v2[dateOne][tokenAddress] * inputAmount / 1000000
            if dateOne in date2token2price_v1 and self.use_v1:
                if tokenAddress in date2token2price_v1[dateOne]:
                    return date2token2price_v1[dateOne][tokenAddress] * inputAmount / 1000000

        return float(0)

TOKEN_PRICE_CENTER = TokenPriceCenter()

    
# ·············································································
def date_2_timestamp(year,mon,day):
    tempString=str(year)+"-"+str(mon)+"-"+str(day)
    tempTime=time.strptime(tempString, "%Y-%m-%d")
    return time.mktime(tempTime)

def timestamp_2_date(un_time):
    return datetime.datetime.fromtimestamp(un_time)


def timestamp_removeHour_reduce(temp_timestamp):
    temp_date=timestamp_2_date(int(temp_timestamp))
    temp_timestamp=date_2_timestamp(temp_date.year,temp_date.month,temp_date.day)
    return int(temp_timestamp)

def timestamp_removeDay_reduce(temp_timestamp):
    temp_date=timestamp_2_date(int(temp_timestamp))
    temp_timestamp=date_2_timestamp(temp_date.year,temp_date.month,1)
    return int(temp_timestamp)
# ·············································································
def readERC20TokenInfo():
    with open("/mnt/sde1/peilin_defi/data/interfaceTool/dict/ERC20TokenInfo.map", "rb") as tf:
        tokenInfoMap=pickle.load(tf) 
        return tokenInfoMap


def readUniswapV1ExchangeInfo():
    with open("/mnt/sde1/peilin_defi/data/interfaceTool/dict/UniswapV1_ExchangeInfo.map", "rb") as tf:
        tempMap=pickle.load(tf)
        return tempMap

def readUniswapV2PairInfo():
    with open("/mnt/sde1/peilin_defi/data/interfaceTool/dict/UniswapV2_PairInfo.map", "rb") as tf:
        tempMap=pickle.load(tf) 
        return tempMap

def readUniswapV3PoolInfo():
    with open("/mnt/sde1/peilin_defi/data/interfaceTool/dict/UniswapV3_PoolInfo.map", "rb") as tf:
        tempMap=pickle.load(tf) 
        return tempMap
    
    
def readShibaSwapPairInfo():
    with open("/mnt/sde1/peilin_defi/data/interfaceTool/dict/ShibaSwap_PairInfo.map", "rb") as tf:
        tempMap=pickle.load(tf) 
        return tempMap

def readSushiSwapPairInfo():
    with open("/mnt/sde1/peilin_defi/data/interfaceTool/dict/SushiSwap_PairInfo.map", "rb") as tf:
        tempMap=pickle.load(tf) 
        return tempMap




def None2Float(temp):
    if temp=="None":
        return 0.0
    else:
        return float(temp)