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


print("read tokenInfo")
tokenInfoMap=readERC20TokenInfo()

print("read UniswapV2_PairInfo")
pairInfoMap=readUniswapV2PairInfo()


input_csv="/mnt/sde1/peilin_defi/defi_1650w/UniswapV2_Transaction.csv"
output_dict="/mnt/sde1/peilin_defi/data/totalValueLocked_line/dict/timeWithTVL/UniswapV2.map"


reserves_s={}
paircnt = {}
timeMap={}
last_timestamp_removeHour = 0



def getTVLtoTimeMap(tempTimestamp):
    tvl = 0
    for pairAddress in reserves_s:
        reserves = reserves_s[pairAddress]
        for tokenAddress in reserves:
            token_tvl = TOKEN_PRICE_CENTER.swap(tokenAddress, tempTimestamp,float(reserves[tokenAddress]))
            tvl += token_tvl

    timeMap[tempTimestamp] = tvl
    print(timestamp_2_date(tempTimestamp), tvl, flush=True)

with open(input_csv,'r', encoding="UTF8") as f:
    # f.seek(18000000000)
    reader = csv.reader(f)
    header_row=next(reader)

    i=0
    for row in reader:
        blockNumber=row[0]
        i+=1
        
        timestamp=int(row[1])
        timestamp_removeHour=timestamp_removeHour_reduce(timestamp)
        transactionHash=row[2]
        pairAddress=row[3]
        type=row[4]
        
        if last_timestamp_removeHour == 0:
            last_timestamp_removeHour = timestamp_removeHour
        else:
            if last_timestamp_removeHour < timestamp_removeHour:
                getTVLtoTimeMap(last_timestamp_removeHour)
                last_timestamp_removeHour = timestamp_removeHour

        reserve0=float(row[11])
        reserve1=float(row[12])
        
        tokenAddress0=pairInfoMap[pairAddress]["tokenAddress0"]
        tokenAddress1=pairInfoMap[pairAddress]["tokenAddress1"]

        #if paircnt[pairAddress] > 100:
        reserves_s[pairAddress] = {
            tokenAddress0: reserve0,
            tokenAddress1: reserve1
        }

    # 最后一天的
    getTVLtoTimeMap(last_timestamp_removeHour)
                    
        
with open(output_dict, "wb") as tf:
    pickle.dump(timeMap,tf) 
