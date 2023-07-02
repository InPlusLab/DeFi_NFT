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

# from readTotal import *
# tokenPriceMap=readTokenPrice()

timeMap={}
reserves_s={}

input_csv="/mnt/sde1/peilin_defi/defi_1650w/BalancerV1_Transaction.csv"

last_timestamp_removeHour = 0

# TODO: Type少了

def getTVLtoTimeMap(tempTimestamp):
    tvl = 0
    for poolAddress in reserves_s:
        for tokenAddress in reserves_s[poolAddress]:
            if float(reserves_s[poolAddress][tokenAddress])<0:
                continue
                
            tvl += TOKEN_PRICE_CENTER.swap(tokenAddress, tempTimestamp,float(reserves_s[poolAddress][tokenAddress]))

    timeMap[tempTimestamp] = tvl
    print(timestamp_2_date(tempTimestamp), tvl, flush=True)



with open(input_csv,'r', encoding="UTF8") as f:
    reader = csv.reader(f)
    header_row=next(reader)
    print(header_row)
    
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
                getTVLtoTimeMap(timestamp_removeHour)
                last_timestamp_removeHour = timestamp_removeHour
        
        poolAddress=row[3]
        type=row[4]
        

        tokenIn_address=row[6]
        amountIn=None2Float(row[7])
        tokenOut_address=row[8]
        amountOut=None2Float(row[9])
        
        if poolAddress not in reserves_s:
            reserves_s[poolAddress] = {}

        if tokenIn_address!="None" and  amountIn!=0:
            if tokenIn_address not in reserves_s[poolAddress]:
                reserves_s[poolAddress][tokenIn_address]=0
            reserves_s[poolAddress][tokenIn_address] += amountIn

        if tokenOut_address!="None" and  amountOut!=0:
            if tokenOut_address not in reserves_s[poolAddress]:
                reserves_s[poolAddress][tokenOut_address]=0
            reserves_s[poolAddress][tokenOut_address] -= amountOut
            
    
        # if tokenIn_address == "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2" or tokenOut_address == "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2":
        #     print(row)
        #     print(poolAddress, reserves_s[poolAddress]["0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"])
        #     if reserves_s[poolAddress]["0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"] <0 :
        #         exit()


    # 最后一天的
    getTVLtoTimeMap(timestamp_removeHour)    
        

with open("/mnt/sde1/peilin_defi/data/totalValueLocked_line/dict/timeWithTVL/BalancerV1.map", "wb") as tf:
    pickle.dump(timeMap,tf) 
