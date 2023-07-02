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
sys.path.append("/mnt/sde1/peilin_defi/code/interfaceTool")
from readTotal2 import *

FactoryProblem = set([
    "0xc6a8466d128fbfd34ada64a9fffce325d57c9a52",
    "0x213be373fdff327658139c7df330817dad2d5bbe",
    "0x33bb0e62d5e8c688e645dd46dfb48cd613250067",
    "0x008cfa89df5b0c780ca3462fc2602d7f8c7ac315"
])

poolInfoPath="/mnt/sde1/peilin_defi/defi_1650w/Curve_PoolInfo.json"
with open(poolInfoPath, "r", encoding="utf-8") as f:
    poolInfo = json.load(f)

def Curve():
    rivetTokenList=['0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2','0x2260fac5e5542a773aa44fbcfedf7c193bc2c599',
                    '0x6b175474e89094c44da98b954eedeac495271d0f','0xdac17f958d2ee523a2206206994597c13d831ec7',
                    '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48']
    
    input_csv="/mnt/sde1/peilin_defi/defi_1650w/Curve_Transaction.csv"
    output_dict="/mnt/sde1/peilin_defi/data/volume_line/dict/timeWithVolume/Curve.map"

    
    timeMap={}
    
    with open(input_csv,'r', encoding="UTF8") as f:
        # f.seek(1000000000)
        reader = csv.reader(f)
        header_row=next(reader)
        
        i=0
        for row in reader:
            if i%10000==0:
                print(row[0], i, flush=True)
            i+=1
            
            blockNumber=row[0]
            timestamp=int(row[1])
            timestamp_removeHour=timestamp_removeHour_reduce(timestamp)
            timestamp_removeDay=timestamp_removeDay_reduce(timestamp)
            transactionHash=row[2]
            poolAddress=row[3]
            type=row[4]
            
            tokenIn_address=row[6]
            amountIn=None2Float(row[7])
            tokenOut_address=row[8]
            amountOut=None2Float(row[9])
    
            if type!="swap":
                continue
            
            # if tokenIn_address not in rivetTokenList and tokenOut_address not in rivetTokenList:
            #     continue
            
            volumeIn=0.0
            volumeOut=0.0

            # if tokenIn_address != "None":
            #     if "implementation" in poolInfo[poolAddress]:
            #         if poolInfo[poolAddress]["implementation"] in FactoryProblem:
            #             tokenIn_address = poolInfo[poolAddress]["coins"][1]

            
            if amountIn!=0.0:
                volumeIn=TOKEN_PRICE_CENTER.swap(tokenIn_address, timestamp_removeHour,amountIn)

            if amountOut!=0.0:
                volumeOut=TOKEN_PRICE_CENTER.swap(tokenOut_address, timestamp_removeHour,amountOut)
            
            resVolume = max([volumeIn, volumeOut])
            if resVolume==0.0:
                continue
            
            try:
                timeMap[timestamp_removeDay]+=resVolume
            except:
                timeMap[timestamp_removeDay]=resVolume
            
            # if timeMap[timestamp_removeDay] > 1e15:
            #     print(row)
            #     print(poolInfo[poolAddress])
            #     print(tokenIn_address, tokenOut_address)
            #     print(volumeIn, volumeOut)
            #     print(tokenIn_address != "None")
            #     print("implementation" in poolInfo[poolAddress])
            #     print(poolInfo[poolAddress]["implementation"] in FactoryProblem)
            #     print(poolInfo[poolAddress]["coins"][1])

            #     print(timestamp_removeDay, timeMap[timestamp_removeDay], resVolume)
            #     exit()
                
                
            # test
            # if timestamp_removeDay>1651377600:
            #     break
            
            # if timestamp_removeDay==1651377600:
            #     print(transactionHash,tokenIn_address,amountIn,volumeIn)
            #     print(transactionHash,tokenOut_address,amountOut,volumeOut)

    timestamps = []   
    for i in timeMap:
        timestamps.append(i)
    timestamps.sort()
    for timestamp in timestamps:
        print(timestamp, timeMap[timestamp])

    with open(output_dict, "wb") as tf:
        pickle.dump(timeMap,tf) 
        
            
if __name__ == '__main__':
    Curve()