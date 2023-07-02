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

input_csv="/mnt/sde1/peilin_defi/defi_1650w/BalancerV2_Transaction.csv"

last_timestamp_removeHour = 0


def getTVLtoTimeMap(tempTimestamp):
    tvl = 0
    for tokenAddress in reserves:
        if float(reserves[tokenAddress])<0:
            continue
        
        # tvl += tokenPriceMap[tokenAddress].swap(tempTimestamp,float(reserves[tokenAddress]))
        tvl += TOKEN_PRICE_CENTER.swap(tokenAddress, tempTimestamp,float(reserves[tokenAddress]))

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
        tokenIn_address=row[7]
        amountIn=None2Float(row[8])
        tokenOut_address=row[9]
        amountOut=None2Float(row[10])
        tokenAddress=row[11]
        amount=None2Float(row[12])
        feeAmount=None2Float(row[14])
        
        if type=="externaltx" or type=="internaltx":
            continue
        
        if type=="liquidity":
            if tokenAddress!="None" and  amount!=0:
                if tokenAddress not in reserves:
                    reserves[tokenAddress]=0
                reserves[tokenAddress] += amount
                
        elif type=="flashloan":
            if tokenAddress!="None" and  feeAmount!=0 and type=="flashloan":
                if tokenAddress not in reserves:
                    reserves[tokenAddress]=0
                reserves[tokenAddress] += feeAmount
        else:
            if tokenIn_address!="None" and  amountIn!=0:
                if tokenIn_address not in reserves:
                    reserves[tokenIn_address]=0
                reserves[tokenIn_address] += amountIn

            if tokenOut_address!="None" and  amountOut!=0:
                if tokenOut_address not in reserves:
                    reserves[tokenOut_address]=0
                reserves[tokenOut_address] -= amountOut
            
    
    # 最后一天的
    getTVLtoTimeMap(last_timestamp_removeHour)    
        

with open("/mnt/sde1/peilin_defi/data/totalValueLocked_line/dict/timeWithTVL/BalancerV2.map", "wb") as tf:
    pickle.dump(timeMap,tf) 
