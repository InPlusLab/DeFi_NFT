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

import sys


import sys
sys.path.append("/mnt/sde1/peilin_defi/code/interfaceTool")
from readTotal2 import *

timeMap={}
reserves={}

input_csv="/mnt/sde1/peilin_defi/defi_1650w/Compound_Transaction.csv"

last_timestamp_removeHour = 0

def readCompoundCtokens():
    cTokenMap={}
    with open("/mnt/sde1/peilin_defi/defi_1650w/Compound_cTokens.json", "r", encoding="utf-8") as f:
        content = json.load(f)
        for key,value in content.items():
            cTokenAddress=value["address"].lower()
            underlyingAddress=value["underlying"].lower()
            
            cTokenMap[cTokenAddress]=underlyingAddress
            
    
    cTokenMap["0x4ddc2d193948926d02f9b1fe9e1daa0718270ed5"]="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"

    return cTokenMap   

cTokenMap=readCompoundCtokens()
print(cTokenMap)            


def getTVLtoTimeMap(tempTimestamp):
    tvl = 0
    for tokenAddress in reserves:
        if float(reserves[tokenAddress])<0:
            print(tokenAddress, reserves[tokenAddress])
            continue
        
        # tvl += tokenPriceMap[tokenAddress].swap(tempTimestamp,float(reserves[tokenAddress]))
        token_tvl = TOKEN_PRICE_CENTER.swap(tokenAddress, tempTimestamp,float(reserves[tokenAddress]))
        tvl += token_tvl

    timeMap[tempTimestamp] = tvl
    print(timestamp_2_date(tempTimestamp), tvl, flush=True)


with open(input_csv,'r', encoding="UTF8") as f:
    reader = csv.reader(f)
    header_row=next(reader)
    
    i=0
    for row in reader:
        # if i%10000==0:
        #     print(i)
        i+=1
        
        timestamp=int(row[1])
        timestamp_removeHour=timestamp_removeHour_reduce(timestamp)

        if last_timestamp_removeHour == 0:
            last_timestamp_removeHour = timestamp_removeHour
        else:
            if last_timestamp_removeHour < timestamp_removeHour:
                getTVLtoTimeMap(last_timestamp_removeHour)
                last_timestamp_removeHour = timestamp_removeHour
        
        cTokenAddress=row[3]
        type=row[4]
        amount=None2Float(row[5])
        cTokens=None2Float(row[7])
        
        underlyingAddress=cTokenMap[cTokenAddress]
        
        if type=="add":
            if underlyingAddress not in reserves:
                reserves[underlyingAddress]=0
            reserves[underlyingAddress] += amount
            
        elif type=="remove":
            if underlyingAddress not in reserves:
                reserves[underlyingAddress]=0
            reserves[underlyingAddress] -= amount            

        elif type=="borrow":
            if underlyingAddress not in reserves:
                reserves[underlyingAddress]=0
            reserves[underlyingAddress] -= amount
            
        elif type=="repay":
            if underlyingAddress not in reserves:
                reserves[underlyingAddress]=0
            reserves[underlyingAddress] += amount

        elif type=="liquidate":
            if underlyingAddress not in reserves:
                reserves[underlyingAddress]=0
            reserves[underlyingAddress] += amount

    getTVLtoTimeMap(last_timestamp_removeHour)    


# print(row[2])
# for tokenAddress in reserves:    
#     token_tvl = TOKEN_PRICE_CENTER.swap(tokenAddress, last_timestamp_removeHour,float(reserves[tokenAddress]))
#     print(tokenAddress, token_tvl, flush=True)

with open("/mnt/sde1/peilin_defi/data/totalValueLocked_line/dict/timeWithTVL/Compound.map", "wb") as tf:
    pickle.dump(timeMap,tf) 
