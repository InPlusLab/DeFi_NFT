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

last_timestamp_removeHour = 0
timeMap={}
reservesLiquidity={}

def getTVLtoTimeMap(tempTimestamp):        
    tvlLiquidity = 0
    for tokenAddress in reservesLiquidity:
        if float(reservesLiquidity[tokenAddress])<0:
            continue
        
        tvlLiquidity += TOKEN_PRICE_CENTER.swap(tokenAddress, tempTimestamp,float(reservesLiquidity[tokenAddress]))

    timeMap[tempTimestamp] = tvlLiquidity
    print(timestamp_2_date(tempTimestamp), tvlLiquidity, flush=True)
    

def Curve_reservesLiquidity():
    global last_timestamp_removeHour
    output_dict="/mnt/sde1/peilin_defi/data/impermanentLoss/dict/CurveLiquidity.map"    
                
    theZIP = zipfile.ZipFile("/mnt/sde1/peilin_defi/defi_1650w/Curve.zip", 'r')
    theCSV = theZIP.open("Curve_Transaction.csv")	
    head = theCSV.readline()
    oneLine = theCSV.readline().decode("utf-8").strip()
    i=0
    while (oneLine!=""):
        if i%10000==0:
            print(i, flush=True)
        i+=1
        row = oneLine.split(",")
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
        
        if last_timestamp_removeHour == 0:
            last_timestamp_removeHour = timestamp_removeHour
        else:
            if last_timestamp_removeHour < timestamp_removeHour:
                getTVLtoTimeMap(last_timestamp_removeHour)
                last_timestamp_removeHour = timestamp_removeHour
        
        
        if type=="add":
            # Liquidity
            if tokenIn_address not in reservesLiquidity:
                reservesLiquidity[tokenIn_address]=0
            reservesLiquidity[tokenIn_address] += amountIn
            
            
        elif type=="remove":
            # Liquidity
            if tokenOut_address not in reservesLiquidity:
                reservesLiquidity[tokenOut_address]=0
            reservesLiquidity[tokenOut_address] -= amountOut
            

        oneLine = theCSV.readline().decode("utf-8").strip()

    getTVLtoTimeMap(last_timestamp_removeHour)
    
    with open(output_dict, "wb") as tf:
        pickle.dump(timeMap,tf) 
        
        
def Cruve_impermanentLoss():
    timeMap_liquidity={}
    with open("/mnt/sde1/peilin_defi/data/impermanentLoss/dict/CurveLiquidity.map", "rb") as tf:
        timeMap_liquidity=pickle.load(tf) 
        
    timeMap_tvl={}
    with open("/mnt/sde1/peilin_defi/data/totalValueLocked_line/dict/timeWithTVL/Curve.map", "rb") as tf:
        timeMap_tvl=pickle.load(tf)
        
    for key,value in timeMap_liquidity.items():
        timeMap_liquidity[key]=value-timeMap_tvl[key]
        
    with open("/mnt/sde1/peilin_defi/data/impermanentLoss/dict/Curve.map", "wb") as tf:
        pickle.dump(timeMap_liquidity,tf) 
        
            
if __name__ == '__main__':
    # Curve_reservesLiquidity()
    Cruve_impermanentLoss()