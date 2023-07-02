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

input_csv="/mnt/sde1/peilin_defi/defi_1650w/MakerDAO_Transaction.csv"

last_timestamp_removeHour = 0



def readCompoundCtokens():
    idMap={}
    with open("/mnt/sde1/peilin_defi/defi_1650w/MakerDAO_CollateralInfo.json", "r", encoding="utf-8") as f:
        content = json.load(f)
        for key,value in content.items():
            if "id" not in value or "underlying" not in value:
                continue
            
            id=value["id"].lower()
            underlyingAddress=value["underlying"].lower()
            
            idMap[id]=underlyingAddress

    return idMap     


def getTVLtoTimeMap(tempTimestamp):
    tvl = 0
    for tokenAddress in reserves:
        if float(reserves[tokenAddress])<0:
            # print(tokenAddress, reserves[tokenAddress])
            continue
        
        token_tvl = TOKEN_PRICE_CENTER.swap(tokenAddress, tempTimestamp,float(reserves[tokenAddress]))
        tvl += token_tvl

    timeMap[tempTimestamp] = tvl

    print(timestamp_2_date(tempTimestamp), tvl, flush=True)
    
def printAll(tempTimestamp):
    tvl = 0
    for tokenAddress in reserves:
        if float(reserves[tokenAddress])<0:
            print(tokenAddress, reserves[tokenAddress])
            continue
        
        token_tvl = TOKEN_PRICE_CENTER.swap(tokenAddress, tempTimestamp,float(reserves[tokenAddress]))
        tvl += token_tvl
        
        print(tokenAddress,token_tvl,reserves[tokenAddress])


# start·····································
tokenInfoMap=readERC20TokenInfo()
idMap=readCompoundCtokens()

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
        
        collateralId=row[4]
        collateralAmount=None2Float(row[8])
        
        if collateralId not in idMap:
            continue
        
        underlyingAddress=idMap[collateralId]
        decimal=int(tokenInfoMap[underlyingAddress]["decimal"])
        
        # DEBUG
        # if underlyingAddress != "0x028171bca77440897b824ca71d1c56cac55b68a3":
        #     continue

        
        if underlyingAddress not in reserves:
            reserves[underlyingAddress]=0
        
        # print("tx", row[2])
        # print("before", '%d' % reserves[underlyingAddress] )
        reserves[underlyingAddress] += collateralAmount/pow(10,(18-decimal))
        # print("delta", '%d' % (collateralAmount/pow(10,(18-decimal))))
        # print("after", '%d' % reserves[underlyingAddress] )
        # print("")

        # if reserves[underlyingAddress] < 0:
        #     exit()


    getTVLtoTimeMap(last_timestamp_removeHour)
    
    
    printAll(last_timestamp_removeHour)

with open("/mnt/sde1/peilin_defi/data/totalValueLocked_line/dict/timeWithTVL/MakerDAO.map", "wb") as tf:
    pickle.dump(timeMap,tf) 
