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
        fee=None2Float(oneArray[3])/1000000

        pairMap[poolAddress]={"tokenAddress0":tokenAddress0,"tokenAddress1":tokenAddress1,"fee":fee}
        
        oneLine = theCSV.readline().decode("utf-8").strip()	
    
    theCSV.close()
    theZIP.close()
    
    return pairMap


def UniswapV3():
    pairMap=readPool()
    output_dict="/mnt/sde1/peilin_defi/data/protocolProfit/dict/UniswapV3.map"    
    timeMap={}
                
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
        blockNumber=row[0]
        timestamp=int(row[1])
        timestamp_removeHour=timestamp_removeHour_reduce(timestamp)
        timestamp_removeDay=timestamp_removeDay_reduce(timestamp)
        transactionHash=row[2]
        poolAddress=row[3]
        type=row[4]
        amount0=None2Float(row[7])
        amount1=None2Float(row[9])
        
        tokenAddress0=pairMap[poolAddress]["tokenAddress0"]
        tokenAddress1=pairMap[poolAddress]["tokenAddress1"]
        fee=pairMap[poolAddress]["fee"]
        
        if type=="add" or type=="remove" or type=="collect":
            oneLine = theCSV.readline().decode("utf-8").strip()
            continue
        
        volume0=0.0
        volume1=0.0
        
        if amount0!=0.0:
            volume0=TOKEN_PRICE_CENTER.swap(tokenAddress0, timestamp_removeHour,amount0)*fee
            
        if amount1!=0.0:
            volume1=TOKEN_PRICE_CENTER.swap(tokenAddress1, timestamp_removeHour,amount1)*fee
            
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
    UniswapV3()