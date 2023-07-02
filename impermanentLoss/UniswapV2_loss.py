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
    rivetTokenList=['0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2','0x2260fac5e5542a773aa44fbcfedf7c193bc2c599',
                    '0x6b175474e89094c44da98b954eedeac495271d0f','0xdac17f958d2ee523a2206206994597c13d831ec7',
                    '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48']
    
    tvl = 0
    for tokenAddress in reserves:
        if tokenAddress not in rivetTokenList:
            continue
        if float(reserves[tokenAddress])<0:
            continue
        
        tvl += TOKEN_PRICE_CENTER.swap(tokenAddress, tempTimestamp,float(reserves[tokenAddress]))
        
        
    tvlLiquidity = 0
    for tokenAddress in reservesLiquidity:
        if tokenAddress not in rivetTokenList:
            continue
        if float(reservesLiquidity[tokenAddress])<0:
            continue
        
        tvlLiquidity += TOKEN_PRICE_CENTER.swap(tokenAddress, tempTimestamp,float(reservesLiquidity[tokenAddress]))

    timeMap[tempTimestamp] = (tvlLiquidity-tvl)
    print(timestamp_2_date(tempTimestamp), tvl, flush=True)
    

def readPool():
    pairMap={}
    
    theZIP = zipfile.ZipFile("/mnt/sde1/peilin_defi/defi_1650w/UniswapV2.zip", 'r')
    theCSV = theZIP.open("UniswapV2_PairInfo.csv")	
    head = theCSV.readline()	

    oneLine = theCSV.readline().decode("utf-8").strip()
    i=0
    while (oneLine!=""):
        if i%10000==0:
            print(i)
        i+=1
        oneArray = oneLine.split(",")	

        # address,name,symbol,totalSupply,decimal
        pairAddress	 = oneArray[0]	
        tokenAddress0 = oneArray[1]
        tokenAddress1 = oneArray[2]	

        pairMap[pairAddress]={"tokenAddress0":tokenAddress0,"tokenAddress1":tokenAddress1}
        
        oneLine = theCSV.readline().decode("utf-8").strip()	
    
    theCSV.close()
    theZIP.close()
    return pairMap

def UniswapV2():
    global last_timestamp_removeHour
    pairMap=readPool()
    output_dict="/mnt/sde1/peilin_defi/data/impermanentLoss/dict/UniswapV2.map"    
                
    theZIP = zipfile.ZipFile("/mnt/sde1/peilin_defi/defi_1650w/UniswapV2.zip", 'r')
    theCSV = theZIP.open("UniswapV2_Transaction.csv")	
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
        timestamp_removeDay=timestamp_removeDay_reduce(timestamp)
        transactionHash=row[2]
        pairAddress=row[3]
        type=row[4]
        amount0In=None2Float(row[7])
        amount1In=None2Float(row[8])
        amount0Out=None2Float(row[9])
        amount1Out=None2Float(row[10])
        
        tokenAddress0=pairMap[pairAddress]["tokenAddress0"]
        tokenAddress1=pairMap[pairAddress]["tokenAddress1"]

        if last_timestamp_removeHour == 0:
            last_timestamp_removeHour = timestamp_removeHour
        else:
            if last_timestamp_removeHour < timestamp_removeHour:
                getTVLtoTimeMap(last_timestamp_removeHour)
                last_timestamp_removeHour = timestamp_removeHour
                
                
        if type=="add":
            if tokenAddress0 not in reserves:
                reserves[tokenAddress0]=0
            reserves[tokenAddress0] += amount0In

            if tokenAddress1 not in reserves:
                reserves[tokenAddress1]=0
            reserves[tokenAddress1] += amount1In
            
            # Liquidity
            if tokenAddress0 not in reservesLiquidity:
                reservesLiquidity[tokenAddress0]=0
            reservesLiquidity[tokenAddress0] += amount0In

            if tokenAddress1 not in reservesLiquidity:
                reservesLiquidity[tokenAddress1]=0
            reservesLiquidity[tokenAddress1] += amount1In
            
            
        elif type=="remove":
            if tokenAddress0 not in reserves:
                reserves[tokenAddress0]=0
            reserves[tokenAddress0] -= amount0Out

            if tokenAddress1 not in reserves:
                reserves[tokenAddress1]=0
            reserves[tokenAddress1] -= amount1Out
            
            # Liquidity
            if tokenAddress0 not in reservesLiquidity:
                reservesLiquidity[tokenAddress0]=0
            reservesLiquidity[tokenAddress0] -= amount0Out

            if tokenAddress1 not in reservesLiquidity:
                reservesLiquidity[tokenAddress1]=0
            reservesLiquidity[tokenAddress1] -= amount1Out
            
            
        elif type=="swap":
            if tokenAddress0 not in reserves:
                reserves[tokenAddress0]=0
            if amount0In>amount0Out:
                reserves[tokenAddress0] += amount0In
            else:
                reserves[tokenAddress0] -= amount0Out
                

            if tokenAddress1 not in reserves:
                reserves[tokenAddress1]=0
            if amount1In>amount1Out:
                reserves[tokenAddress1] += amount1In
            else:
                reserves[tokenAddress1] -= amount1Out

        oneLine = theCSV.readline().decode("utf-8").strip()

    getTVLtoTimeMap(last_timestamp_removeHour)
    
    with open(output_dict, "wb") as tf:
        pickle.dump(timeMap,tf) 
            
if __name__ == '__main__':
    UniswapV2()