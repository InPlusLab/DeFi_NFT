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

def readPool():
    pairMap={}
    
    with open("/mnt/sde1/peilin_defi/defi_1650w/BalancerV2_PoolInfo.csv",'r', encoding="UTF8") as f:
        reader = csv.reader(f)
        header_row=next(reader)
        
        i=0
        for row in reader:
            if i%10000==0:
                print(i)
            i+=1
            
            poolId=row[0]
            if row[-1]=="Err":
                swapFeePercentage=0
            else:
                swapFeePercentage=float(row[-1])/1e18
            # max: 100000000000000000
            # min: 1000000000000
            pairMap[poolId]={"swapFeePercentage":swapFeePercentage}
            
    return pairMap

def BalancerV2():
    pairMap=readPool()
    output_dict="/mnt/sde1/peilin_defi/data/protocolProfit/dict/BalancerV2.map"    
    timeMap={}
                
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
        blockNumber=row[0]
        timestamp=int(row[1])
        timestamp_removeHour=timestamp_removeHour_reduce(timestamp)
        timestamp_removeDay=timestamp_removeDay_reduce(timestamp)
        poolId=row[3]
        type=row[4]
        tokenIn_address=row[7]
        amountIn=None2Float(row[8])
        tokenOut_address=row[9]
        amountOut=None2Float(row[10])
        tokenAddress=row[11]
        amount=None2Float(row[12])
        paidProtocolSwapFeeAmount=None2Float(row[13])
        feeAmount=None2Float(row[14])
        
        if type=="externaltx" or type=="internaltx":
            oneLine = theCSV.readline().decode("utf-8").strip()
            continue
        
        if type=="flashloan":
            resVolume=TOKEN_PRICE_CENTER.swap(tokenAddress, timestamp_removeHour,feeAmount)
            
        elif type=="liquidity":
            resVolume=TOKEN_PRICE_CENTER.swap(tokenAddress, timestamp_removeHour,paidProtocolSwapFeeAmount)
        
        elif type=="swap":
            swapFeePercentage=pairMap[poolId]["swapFeePercentage"]
            volume0=TOKEN_PRICE_CENTER.swap(tokenIn_address, timestamp_removeHour,amountIn)*swapFeePercentage
            volume1=TOKEN_PRICE_CENTER.swap(tokenOut_address, timestamp_removeHour,amountOut)*swapFeePercentage
            resVolume=max([abs(volume0),abs(volume1)])
        
        try:
            timeMap[timestamp_removeDay]+=abs(resVolume)
        except:
            timeMap[timestamp_removeDay]=abs(resVolume)

        oneLine = theCSV.readline().decode("utf-8").strip()

    with open(output_dict, "wb") as tf:
        pickle.dump(timeMap,tf) 
            
if __name__ == '__main__':
    BalancerV2()