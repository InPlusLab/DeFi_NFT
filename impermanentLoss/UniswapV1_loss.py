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
    
    theZIP = zipfile.ZipFile("/mnt/sde1/peilin_defi/defi_1650w/UniswapV1.zip", 'r')
    theCSV = theZIP.open("UniswapV1_ExchangeInfo.csv")	
    head = theCSV.readline()	

    oneLine = theCSV.readline().decode("utf-8").strip()
    i=0
    while (oneLine!=""):
        if i%10000==0:
            print(i)
        i+=1
        oneArray = oneLine.split(",")
        exchangeAddress	 = oneArray[0]	
        tokenAddress = oneArray[1]	

        pairMap[exchangeAddress]=tokenAddress
        
        oneLine = theCSV.readline().decode("utf-8").strip()	
    
    theCSV.close()
    theZIP.close()
    return pairMap

def UniswapV1():
    global last_timestamp_removeHour
    pairMap=readPool()
    output_dict="/mnt/sde1/peilin_defi/data/impermanentLoss/dict/UniswapV1.map"    
                
    theZIP = zipfile.ZipFile("/mnt/sde1/peilin_defi/defi_1650w/UniswapV1.zip", 'r')
    theCSV = theZIP.open("UniswapV1_Transaction.csv")	
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
        exchangeAddress=row[3]
        type=row[4]
        ethAmount=float(row[6])
        tokenAmount=float(row[7])
        tokenAddress=pairMap[exchangeAddress]
        wethAddress="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"

        if last_timestamp_removeHour == 0:
            last_timestamp_removeHour = timestamp_removeHour
        else:
            if last_timestamp_removeHour < timestamp_removeHour:
                getTVLtoTimeMap(last_timestamp_removeHour)
                last_timestamp_removeHour = timestamp_removeHour
                
        if type=="add":
            if wethAddress not in reserves:
                reserves[wethAddress]=0
            reserves[wethAddress] += ethAmount

            if tokenAddress not in reserves:
                reserves[tokenAddress]=0
            reserves[tokenAddress] += tokenAmount
            
            # Liquidity
            if wethAddress not in reservesLiquidity:
                reservesLiquidity[wethAddress]=0
            reservesLiquidity[wethAddress] += ethAmount

            if tokenAddress not in reservesLiquidity:
                reservesLiquidity[tokenAddress]=0
            reservesLiquidity[tokenAddress] += tokenAmount
            
            
        elif type=="remove":
            if wethAddress not in reserves:
                reserves[wethAddress]=0
            reserves[wethAddress] -= ethAmount

            if tokenAddress not in reserves:
                reserves[tokenAddress]=0
            reserves[tokenAddress] -= tokenAmount
            
            # Liquidity
            if wethAddress not in reservesLiquidity:
                reservesLiquidity[wethAddress]=0
            reservesLiquidity[wethAddress] -= ethAmount

            if tokenAddress not in reservesLiquidity:
                reservesLiquidity[tokenAddress]=0
            reservesLiquidity[tokenAddress] -= tokenAmount
            
            
        elif type=="swapTokenToEth":
            if wethAddress not in reserves:
                reserves[wethAddress]=0
            reserves[wethAddress] -= ethAmount

            if tokenAddress not in reserves:
                reserves[tokenAddress]=0
            reserves[tokenAddress] += tokenAmount
            
        elif type=="swapEthToToken":
            if wethAddress not in reserves:
                reserves[wethAddress]=0
            reserves[wethAddress] += ethAmount

            if tokenAddress not in reserves:
                reserves[tokenAddress]=0
            reserves[tokenAddress] -= tokenAmount
            

        oneLine = theCSV.readline().decode("utf-8").strip()

    getTVLtoTimeMap(last_timestamp_removeHour)

    with open(output_dict, "wb") as tf:
        pickle.dump(timeMap,tf) 
            
if __name__ == '__main__':
    UniswapV1()