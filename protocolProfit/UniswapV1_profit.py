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
    pairMap=readPool()
    output_dict="/mnt/sde1/peilin_defi/data/protocolProfit/dict/UniswapV1.map"    
    timeMap={}
                
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
        fee=0.003
        
        if type=="add" or type=="remove":
            oneLine = theCSV.readline().decode("utf-8").strip()
            continue

        volume0=0.0
        volume1=0.0
        
        if ethAmount!=0.0:
            volume0=TOKEN_PRICE_CENTER.swap(wethAddress, timestamp_removeHour,ethAmount)*fee
            
        if tokenAmount!=0.0:
            volume1=TOKEN_PRICE_CENTER.swap(tokenAddress, timestamp_removeHour,tokenAmount)*fee
            
        resVolume=max([abs(volume0),abs(volume1)])
        
        try:
            timeMap[timestamp_removeDay]+=abs(resVolume)
        except:
            timeMap[timestamp_removeDay]=abs(resVolume)

        oneLine = theCSV.readline().decode("utf-8").strip()


    with open(output_dict, "wb") as tf:
        pickle.dump(timeMap,tf) 
            
if __name__ == '__main__':
    UniswapV1()