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

input_csv="/mnt/sde1/peilin_defi/defi_1650w/Curve_Transaction.csv"

last_timestamp_removeHour = 0


poolInfoPath="/mnt/sde1/peilin_defi/defi_1650w/Curve_PoolInfo.json"
with open(poolInfoPath, "r", encoding="utf-8") as f:
    poolInfo = json.load(f)

def getTVLtoTimeMap(tempTimestamp):
    tvl = 0
    for tokenAddress in reserves:
        if float(reserves[tokenAddress])<0:
            continue
        
        token_tvl = TOKEN_PRICE_CENTER.swap(tokenAddress, tempTimestamp,float(reserves[tokenAddress]))
        tvl += token_tvl
        if token_tvl > 1e17:
            print(tokenAddress, token_tvl, reserves[tokenAddress])
            exit()

    timeMap[tempTimestamp] = tvl
    print(timestamp_2_date(tempTimestamp), tvl, flush=True)


with open(input_csv,'r', encoding="UTF8") as f:
    reader = csv.reader(f)
    header_row=next(reader)
    
    i=0

    previous_row = None

    for row in reader:
        # if i%10000==0:
        #     print(i)
        # i+=1
        
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
        
        tokenIn_address=row[6]
        amountIn=None2Float(row[7])
        tokenOut_address=row[8]
        amountOut=None2Float(row[9])
        
        # blockNum=int(row[0])
        # if blockNum>9468007:
        #     break


        coins = poolInfo[poolAddress]["coins"]

        if type=="erc20":
            if tokenIn_address!="None" and  amountIn!=0 and tokenIn_address in coins:
                if tokenIn_address not in reserves:
                    reserves[tokenIn_address]=0
                reserves[tokenIn_address] += amountIn

            if tokenOut_address!="None" and  amountOut!=0 and tokenOut_address in coins:
                if tokenOut_address not in reserves:
                    reserves[tokenOut_address]=0
                reserves[tokenOut_address] -= amountOut
        # remove_one: ether
        elif type == "remove" and tokenOut_address == "None" and amountOut!=0:
                # ether_related
                if "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee" in poolInfo[poolAddress]["coins"]:
                    is_erc20_remove = True
                    if previous_row[4] != "erc20" or previous_row[8] not in poolInfo[poolAddress]["coins"]:
                        is_erc20_remove = False
                    for row_index in range(len(row)):
                        # the same, except for type, tokenOut_address, amountOut
                        if row_index != 4 and row_index != 8 and row_index != 9 and  previous_row[row_index] != row[row_index]:
                            is_erc20_remove = False
                    if not is_erc20_remove:
                        reserves["0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"] -= amountOut
        # add remove swap: ether
        else:
            if tokenIn_address=="0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee" and  amountIn!=0 and tokenIn_address in coins:
                if tokenIn_address not in reserves:
                    reserves[tokenIn_address]=0
                reserves[tokenIn_address] += amountIn

            if tokenOut_address=="0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee" and  amountOut!=0 and tokenOut_address in coins:
                if tokenOut_address not in reserves:
                    reserves[tokenOut_address]=0
                reserves[tokenOut_address] -= amountOut

        
        previous_row = row
        

    
    # 最后一天的
    getTVLtoTimeMap(last_timestamp_removeHour) 
    
    
for tokenAddress in reserves:
    # if float(reserves[tokenAddress])<0:
    #     continue
    
    price=TOKEN_PRICE_CENTER.swap(tokenAddress, last_timestamp_removeHour,float(reserves[tokenAddress]))
    print(tokenAddress,price,float(reserves[tokenAddress]))
        

with open("/mnt/sde1/peilin_defi/data/totalValueLocked_line/dict/timeWithTVL/Curve.map", "wb") as tf:
    pickle.dump(timeMap,tf) 
