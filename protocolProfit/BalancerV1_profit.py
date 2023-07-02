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
    
    with open("/mnt/sde1/peilin_defi/defi_1650w/BalancerV1_PoolInfo.csv",'r', encoding="UTF8") as f:
        reader = csv.reader(f)
        header_row=next(reader)
        
        i=0
        for row in reader:
            if i%10000==0:
                print(i)
            i+=1
            
            poolAddress=row[0]
            swapFee=float(row[-1])/1e18
            # max: 100000000000000000
            # min: 1000000000000
            pairMap[poolAddress]={"swapFee":swapFee}
            
    return pairMap

def BalancerV1():
    pairMap=readPool()
    output_dict="/mnt/sde1/peilin_defi/data/protocolProfit/dict/BalancerV1.map"    
    timeMap={}
                
    theZIP = zipfile.ZipFile("/mnt/sde1/peilin_defi/defi_1650w/BalancerV1.zip", 'r')
    theCSV = theZIP.open("BalancerV1_Transaction.csv")	
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
        swapFee=pairMap[poolAddress]["swapFee"]
        
        if type!="swap":
            oneLine = theCSV.readline().decode("utf-8").strip()
            continue
        
        volume0=TOKEN_PRICE_CENTER.swap(tokenIn_address, timestamp_removeHour,amountIn)*swapFee
        volume1=TOKEN_PRICE_CENTER.swap(tokenOut_address, timestamp_removeHour,amountOut)*swapFee
            
        resVolume=max([abs(volume0),abs(volume1)])
        
        if resVolume==0.0:
            oneLine = theCSV.readline().decode("utf-8").strip()
            continue
        
        try:
            timeMap[timestamp_removeDay]+=abs(resVolume)
        except:
            timeMap[timestamp_removeDay]=abs(resVolume)

        oneLine = theCSV.readline().decode("utf-8").strip()

    with open(output_dict, "wb") as tf:
        pickle.dump(timeMap,tf) 
            
if __name__ == '__main__':
    BalancerV1()