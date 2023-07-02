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

input_csv="/mnt/sde1/peilin_defi/defi_1650w/AaveV2_Transaction.csv"

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
        collateralAddress=row[6]
        amount=None2Float(row[7])

        premium=None2Float(row[16])
        liquidatedCollateralAddress=row[18]
        debtToCover=None2Float(row[19])
        liquidatedCollateralAmount=None2Float(row[20])
        
        if type=="add":
            if collateralAddress not in reserves:
                reserves[collateralAddress]=0
            reserves[collateralAddress] += amount
            
        elif type=="remove":
            if collateralAddress not in reserves:
                reserves[collateralAddress]=0
            reserves[collateralAddress] -= amount
            
        elif type=="borrow":
            if collateralAddress not in reserves:
                reserves[collateralAddress]=0
            reserves[collateralAddress] -= amount
            
        elif type=="repay":
            if collateralAddress not in reserves:
                reserves[collateralAddress]=0
            reserves[collateralAddress] += amount
            
        elif type=="flashloan":
            if collateralAddress not in reserves:
                reserves[collateralAddress]=0
            reserves[collateralAddress] += premium
            
        elif type=="liquidate":
            if collateralAddress not in reserves:
                reserves[collateralAddress]=0
            reserves[collateralAddress] += debtToCover
            
            if liquidatedCollateralAddress not in reserves:
                reserves[liquidatedCollateralAddress]=0
            reserves[liquidatedCollateralAddress] -= liquidatedCollateralAmount
            
        
        
    
    # 最后一天的
    getTVLtoTimeMap(last_timestamp_removeHour)    
        

with open("/mnt/sde1/peilin_defi/data/totalValueLocked_line/dict/timeWithTVL/AaveV2.map", "wb") as tf:
    pickle.dump(timeMap,tf) 
