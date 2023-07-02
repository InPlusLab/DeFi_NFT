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

input_csv="/mnt/sde1/peilin_defi/defi_1650w/Cream_Transaction.csv"

last_timestamp_removeHour = 0

def readCreamCrtokens():
    cTokenMap={}
    with open("/mnt/sde1/peilin_defi/defi_1650w/Cream_crTokens.json", "r", encoding="utf-8") as f:
        content = json.load(f)
        for item in content:
            cTokenAddress=item["token_address"].lower()
            underlyingAddress=item["underlying"].lower()
            
            cTokenMap[cTokenAddress]=underlyingAddress
            
    
    cTokenMap["0xd06527d5e56a3495252a528c4987003b712860ee"]="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"

    return cTokenMap   

cTokenMap=readCreamCrtokens()
# print(crTokenMap)            


def getTVLtoTimeMap(tempTimestamp):
    tvl = 0

    t = time.strftime("%Y-%m-%d", time.gmtime(timestamp))
    dateTemp = time.strptime(t, "%Y-%m-%d")
    dateStr = time.strftime("%Y-%m-%d", dateTemp )

    for tokenAddress in reserves:
        if float(reserves[tokenAddress])<0:
            # print(tokenAddress, reserves[tokenAddress])
            continue
        
        # tvl += tokenPriceMap[tokenAddress].swap(tempTimestamp,float(reserves[tokenAddress]))
        token_tvl = TOKEN_PRICE_CENTER.swap(tokenAddress, tempTimestamp,float(reserves[tokenAddress]))
        # if dateStr == "2021-10-10":
        #     print(dateStr, tokenAddress, reserves[tokenAddress], token_tvl)
        # if tokenAddress=="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2":
        #     print(dateStr, tokenAddress, reserves[tokenAddress], token_tvl)
        tvl += token_tvl

    timeMap[tempTimestamp] = tvl
    # print(timestamp_2_date(tempTimestamp), tvl, flush=True)

    # if dateStr == "2021-10-10":
    #     exit()


    


with open(input_csv,'r', encoding="UTF8") as f:
    reader = csv.reader(f)
    header_row=next(reader)
    
    i=0
    for row in reader:
        if i%10000==0:
            print(i)
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
            
        # if i>50000 and underlyingAddress=="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2":
        #     print(row[2])
        #     getTVLtoTimeMap(last_timestamp_removeHour) 
        #     break

    getTVLtoTimeMap(last_timestamp_removeHour)    


# print(row[2])
# for tokenAddress in reserves:    
#     token_tvl = TOKEN_PRICE_CENTER.swap(tokenAddress, last_timestamp_removeHour,float(reserves[tokenAddress]))
#     print(tokenAddress, token_tvl, flush=True)

with open("/mnt/sde1/peilin_defi/data/totalValueLocked_line/dict/timeWithTVL/Cream.map", "wb") as tf:
    pickle.dump(timeMap,tf) 
