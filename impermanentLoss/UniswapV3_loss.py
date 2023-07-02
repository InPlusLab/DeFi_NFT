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
reserves={}
reservesLiquidity={}

def getTVLtoTimeMap(tempTimestamp):
    tvl = 0
    for tokenAddress in reserves:
        if float(reserves[tokenAddress])<0:
            continue
        
        tvl += TOKEN_PRICE_CENTER.swap(tokenAddress, tempTimestamp,float(reserves[tokenAddress]))
        
        
    tvlLiquidity = 0
    for tokenAddress in reservesLiquidity:
        if float(reservesLiquidity[tokenAddress])<0:
            continue
        
        tvlLiquidity += TOKEN_PRICE_CENTER.swap(tokenAddress, tempTimestamp,float(reservesLiquidity[tokenAddress]))

    timeMap[tempTimestamp] = (tvlLiquidity-tvl)
    print(timestamp_2_date(tempTimestamp), tvl, flush=True)
    

def readPool():
    pairMap={}
    
    theZIP = zipfile.ZipFile("/mnt/sde1/peilin_defi/defi_1650w/UniswapV3.zip", 'r')
    theCSV = theZIP.open("UniswapV3_PoolInfo.csv")	
    head = theCSV.readline()	

    oneLine = theCSV.readline().decode("utf-8").strip()
    i=0
    while (oneLine!=""):
        if i%10000==0:
            print(i)
        i+=1
        oneArray = oneLine.split(",")	

        # address,name,symbol,totalSupply,decimal
        poolAddress	 = oneArray[0]	
        tokenAddress0 = oneArray[1]
        tokenAddress1 = oneArray[2]	

        pairMap[poolAddress]={"tokenAddress0":tokenAddress0,"tokenAddress1":tokenAddress1}
        
        oneLine = theCSV.readline().decode("utf-8").strip()	
    
    theCSV.close()
    theZIP.close()
    return pairMap

def UniswapV3():
    global last_timestamp_removeHour
    pairMap=readPool()
    output_dict="/mnt/sde1/peilin_defi/data/impermanentLoss/dict/UniswapV3.map"    
                
    theZIP = zipfile.ZipFile("/mnt/sde1/peilin_defi/defi_1650w/UniswapV3.zip", 'r')
    theCSV = theZIP.open("UniswapV3_Transaction.csv")	
    head = theCSV.readline()
    oneLine = theCSV.readline().decode("utf-8").strip()
    i=0
    while (oneLine!=""):
        if i%10000==0:
            print(i, flush=True)
        i+=1
        row = oneLine.split(",")
        timestamp=int(row[1])
        timestamp_removeHour=timestamp_removeHour_reduce(timestamp)
        poolAddress=row[3]
        type=row[4]
        amount0=None2Float(row[7])
        amount1=None2Float(row[8])
        tokenAddress0=pairMap[poolAddress]["tokenAddress0"]
        tokenAddress1=pairMap[poolAddress]["tokenAddress1"]

        if last_timestamp_removeHour == 0:
            last_timestamp_removeHour = timestamp_removeHour
        else:
            if last_timestamp_removeHour < timestamp_removeHour:
                getTVLtoTimeMap(last_timestamp_removeHour)
                last_timestamp_removeHour = timestamp_removeHour
                
        if type=="add":
            if tokenAddress0 not in reserves:
                reserves[tokenAddress0]=0
            reserves[tokenAddress0] += amount0

            if tokenAddress1 not in reserves:
                reserves[tokenAddress1]=0
            reserves[tokenAddress1] += amount1
            
            # Liquidity
            if tokenAddress0 not in reservesLiquidity:
                reservesLiquidity[tokenAddress0]=0
            reservesLiquidity[tokenAddress0] += amount0

            if tokenAddress1 not in reservesLiquidity:
                reservesLiquidity[tokenAddress1]=0
            reservesLiquidity[tokenAddress1] += amount1
            
            
        elif type=="collect":
            if tokenAddress0 not in reserves:
                reserves[tokenAddress0]=0
            reserves[tokenAddress0] += amount0

            if tokenAddress1 not in reserves:
                reserves[tokenAddress1]=0
            reserves[tokenAddress1] += amount1
            
            # Liquidity
            if tokenAddress0 not in reservesLiquidity:
                reservesLiquidity[tokenAddress0]=0
            reservesLiquidity[tokenAddress0] += amount0

            if tokenAddress1 not in reservesLiquidity:
                reservesLiquidity[tokenAddress1]=0
            reservesLiquidity[tokenAddress1] += amount1
            
            
        elif type=="swap":
            if tokenAddress0 not in reserves:
                reserves[tokenAddress0]=0
            reserves[tokenAddress0] += amount0

            if tokenAddress1 not in reserves:
                reserves[tokenAddress1]=0
            reserves[tokenAddress1] += amount1
            

        oneLine = theCSV.readline().decode("utf-8").strip()

    getTVLtoTimeMap(last_timestamp_removeHour)

    with open(output_dict, "wb") as tf:
        pickle.dump(timeMap,tf) 
            
if __name__ == '__main__':
    UniswapV3()