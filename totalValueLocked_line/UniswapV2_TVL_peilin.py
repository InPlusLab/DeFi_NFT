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
print(len(date2token2price_v1), len(date2token2price_v2))

U_ADDR_s= set(["0xdac17f958d2ee523a2206206994597c13d831ec7", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"])

class TokenPriceCenter():    
    def swap(self, tokenAddress, dateOne,inputAmount):
        if tokenAddress in U_ADDR_s:
            return inputAmount / 1000000
        else:
            if dateOne in date2token2price_v2:
                if tokenAddress in date2token2price_v2[dateOne]:
                    return date2token2price_v2[dateOne][tokenAddress] * inputAmount / 1000000
            if dateOne in date2token2price_v1:
                if tokenAddress in date2token2price_v1[dateOne]:
                    return date2token2price_v1[dateOne][tokenAddress] * inputAmount / 1000000

        return float(0)

TOKEN_PRICE_CENTER = TokenPriceCenter()


output_dict="/mnt/sde1/peilin_defi/data/totalValueLocked_line/dict/timeWithTVL/UniswapV2.map"
fileDir = "/mnt/sde1/peilin_defi/defi_1650w/"


reservesOf = {}
datesOf = {}
tokensOf = {}
txcountOf = {}
datesAll = [""]

# UniswapV2
file = "UniswapV2"
theZIP = zipfile.ZipFile(fileDir+file+".zip", 'r')

# UniswapV2_PairInfo
theCSV = theZIP.open("UniswapV2_PairInfo.csv")    
head = theCSV.readline()    
oneLine = theCSV.readline().decode("utf-8").strip()
while (oneLine!=""):
    oneArray = oneLine.split(",")    
    pairAddress                = oneArray[0]
    tokenAddress0            = oneArray[1]
    tokenAddress1            = oneArray[2]

    reservesOf[pairAddress] = []
    datesOf[pairAddress] = [""]
    tokensOf[pairAddress]    = [tokenAddress0, tokenAddress1]
    txcountOf[pairAddress] = 0

    oneLine = theCSV.readline().decode("utf-8").strip()
theCSV.close()

print("finish pair")


theCSV = theZIP.open("UniswapV2_Transaction.csv")    

head = theCSV.readline()
oneLine = theCSV.readline().decode("utf-8").strip()

while (oneLine!=""):
    oneArray = oneLine.split(",")
    oneLine = theCSV.readline().decode("utf-8").strip()
    blockNumber            = int(oneArray[0])
    timestamp            = int(oneArray[1])
    pairAddress            = oneArray[3]
    reserve0            = int(oneArray[11])
    reserve1            = int(oneArray[12])

    t = time.strftime("%Y-%m-%d", time.gmtime(timestamp))
    dateTemp = time.strptime(t, "%Y-%m-%d")
    # print("dateTemp",time.mktime(dateTemp))

    if datesAll[-1] != dateTemp:
        datesAll.append(dateTemp)
        print(blockNumber, theCSV.tell(), t, flush=True)
    if datesOf[pairAddress][-1] != dateTemp:
        datesOf[pairAddress].append(dateTemp)
        reservesOf[pairAddress].append([])

    reservesOf[pairAddress][-1] = [reserve0, reserve1]
    txcountOf[pairAddress] += 1

theCSV.close()

datesAll = datesAll[1:]
for i in datesOf:
    datesOf[i] = datesOf[i][1:]
print("finish read")

def getReserve(dateTarget, pairAddress):
    dates = datesOf[pairAddress]
    for i in range(len(dates)-1, -1 ,-1):
        if dates[i] <= dateTarget:
            return reservesOf[pairAddress][i]
    return None

timeMap={}

for dateOne in datesAll:
    priceOf = {}
    remain = set()
    dateStr = time.strftime("%Y-%m-%d", dateOne )

    tvlOf = {}
    for pairAddress in tokensOf:
        token0, token1 = tokensOf[pairAddress]
        reserves = getReserve(dateOne, pairAddress)
        if reserves==None:
            continue
        
        tvlOf[pairAddress] = TOKEN_PRICE_CENTER.swap(token0, dateOne, reserves[0]) +  TOKEN_PRICE_CENTER.swap(token1, dateOne, reserves[1])
    
    tvl = 0
    for pairAddress in tvlOf:
        tvl += tvlOf[pairAddress]


    print(dateStr, tvl)
    
    tempData = time.strptime(dateStr, "%Y-%m-%d")
    tempTimestamp=time.mktime(tempData)
    timeMap[tempTimestamp]=tvl
    
    
with open(output_dict, "wb") as tf:
    pickle.dump(timeMap,tf) 

theZIP.close()




