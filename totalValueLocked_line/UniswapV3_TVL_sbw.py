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
from readTotal import *

tokenPriceMap=readTokenPrice()

timeMap={}
reserves={}

pairInfoMap=readUniswapV3PoolInfo()

input_csv="/mnt/sde1/peilin_defi/defi_1650w/UniswapV3_Transaction.csv"

last_timestamp_removeHour = 0


def getTVLtoTimeMap(tempTimestamp):
    tvl = 0
    for tokenAddress in reserves:
        if float(reserves[tokenAddress])<0:
            continue
    
        if tokenAddress not in tokenPriceMap:
            continue
        
        tvl += tokenPriceMap[tokenAddress].swap(tempTimestamp,float(reserves[tokenAddress]))
        # tvl += TOKEN_PRICE_CENTER.swap(tokenAddress, tempTimestamp,float(reserves[tokenAddress]))

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
        
        poolAddress=row[3]
        type=row[4]
            
        tokenAddress0=pairInfoMap[poolAddress]["tokenAddress0"]
        tokenAddress1=pairInfoMap[poolAddress]["tokenAddress1"]
        
        amount0=None2Float(row[7])
        amount1=None2Float(row[8])
        
        if type=="add" or type=="swap":
            if tokenAddress0 not in reserves:
                reserves[tokenAddress0]=0
            reserves[tokenAddress0] += amount0

            if tokenAddress1 not in reserves:
                reserves[tokenAddress1]=0
            reserves[tokenAddress1] += amount1

        elif type=="collect":
            if tokenAddress0 not in reserves:
                reserves[tokenAddress0]=0
            reserves[tokenAddress0] -= amount0

            if tokenAddress1 not in reserves:
                reserves[tokenAddress1]=0
            reserves[tokenAddress1] -= amount1
    
    # 最后一天的
    getTVLtoTimeMap(last_timestamp_removeHour)    
        

with open("/mnt/sde1/peilin_defi/data/totalValueLocked_line/dict/timeWithTVL/UniswapV3.map", "wb") as tf:
    pickle.dump(timeMap,tf) 
