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
    

def BalancerV2():
    global last_timestamp_removeHour
    output_dict="/mnt/sde1/peilin_defi/data/impermanentLoss/dict/BalancerV2.map"    
                
    theZIP = zipfile.ZipFile("/mnt/sde1/peilin_defi/defi_1650w/BalancerV2.zip", 'r')
    theCSV = theZIP.open("BalancerV2_Transaction.csv")	
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
        tokenIn_address=row[7]
        amountIn=None2Float(row[8])
        tokenOut_address=row[9]
        amountOut=None2Float(row[10])
        tokenAddress=row[11]
        amount=None2Float(row[12])
        feeAmount=None2Float(row[14])
        
        if last_timestamp_removeHour == 0:
            last_timestamp_removeHour = timestamp_removeHour
        else:
            if last_timestamp_removeHour < timestamp_removeHour:
                getTVLtoTimeMap(last_timestamp_removeHour)
                last_timestamp_removeHour = timestamp_removeHour
        
        if type=="externaltx" or type=="internaltx":
            oneLine = theCSV.readline().decode("utf-8").strip()
            continue
        
        if type=="liquidity":
            if tokenAddress!="None" and  amount!=0:
                if tokenAddress not in reserves:
                    reserves[tokenAddress]=0
                reserves[tokenAddress] += amount
                
            if tokenAddress!="None" and  amount!=0:
                if tokenAddress not in reservesLiquidity:
                    reservesLiquidity[tokenAddress]=0
                reservesLiquidity[tokenAddress] += amount
                
        elif type=="flashloan":
            if tokenAddress!="None" and  feeAmount!=0 and type=="flashloan":
                if tokenAddress not in reserves:
                    reserves[tokenAddress]=0
                reserves[tokenAddress] += feeAmount
                
        elif type=="swap":
            if tokenIn_address!="None" and  amountIn!=0:
                if tokenIn_address not in reserves:
                    reserves[tokenIn_address]=0
                reserves[tokenIn_address] += amountIn

            if tokenOut_address!="None" and  amountOut!=0:
                if tokenOut_address not in reserves:
                    reserves[tokenOut_address]=0
                reserves[tokenOut_address] -= amountOut
            

        oneLine = theCSV.readline().decode("utf-8").strip()

    getTVLtoTimeMap(last_timestamp_removeHour)
    
    with open(output_dict, "wb") as tf:
        pickle.dump(timeMap,tf) 
            
if __name__ == '__main__':
    BalancerV2()