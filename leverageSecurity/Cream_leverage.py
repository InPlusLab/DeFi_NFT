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


timeMap={}
reserves={}
reserves_borrow={}
last_timestamp_removeHour = 0


def getTVLtoTimeMap(tempTimestamp):
    tvl = 0
    borrow_tvl = 0
    for tokenAddress in reserves:
        if float(reserves[tokenAddress])<0:
            continue
        tvl += TOKEN_PRICE_CENTER.swap(tokenAddress, tempTimestamp,float(reserves[tokenAddress]))
        
    for borrowKey in reserves_borrow:
        tokenAddress=borrowKey.split("_")[1]
        if float(reserves_borrow[borrowKey])<0:
            continue
        borrow_tvl += TOKEN_PRICE_CENTER.swap(tokenAddress, tempTimestamp,float(reserves_borrow[borrowKey]))

    
    # timeMap[tempTimestamp] = tvl/borrow_tvl
    timeMap[tempTimestamp] = borrow_tvl/tvl
    print(timestamp_2_date(tempTimestamp), tvl,borrow_tvl,borrow_tvl/tvl, flush=True)
    
def readCreamCrtokens():
    cTokenMap={}
    with open("/mnt/sde1/peilin_defi/defi_1650w/Cream_crTokens.json", "r", encoding="utf-8") as f:
        content = json.load(f)
        for item in content:
            cTokenAddress=item["token_address"].lower()
            underlyingAddress=item["underlying"].lower()
            
            cTokenMap[cTokenAddress]=underlyingAddress
            
    
    cTokenMap["0xd06527d5e56a3495252a528c4987003b712860ee"]="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"

    return cTokenMap   


def Cream():
    cTokenMap=readCreamCrtokens()
    global last_timestamp_removeHour
    output_dict="/mnt/sde1/peilin_defi/data/leverageSecurity/dict/Cream.map"
    theZIP = zipfile.ZipFile("/mnt/sde1/peilin_defi/defi_1650w/Cream.zip", 'r')
    theCSV = theZIP.open("Cream_Transaction.csv")	
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

        if last_timestamp_removeHour == 0:
            last_timestamp_removeHour = timestamp_removeHour
        else:
            if last_timestamp_removeHour < timestamp_removeHour:
                getTVLtoTimeMap(last_timestamp_removeHour)
                last_timestamp_removeHour = timestamp_removeHour
        
        cTokenAddress=row[3]
        type=row[4]
        amount=None2Float(row[5])
        cTokens=None2Float(row[7])
        borrower=row[8]
        
        underlyingAddress=cTokenMap[cTokenAddress]
        
        if type=="add":
            if underlyingAddress not in reserves:
                reserves[underlyingAddress]=0
            reserves[underlyingAddress] += amount
            
        elif type=="remove":
            if underlyingAddress not in reserves:
                reserves[underlyingAddress]=0
            reserves[underlyingAddress] -= amount            

        elif type=="borrow":
            if underlyingAddress not in reserves:
                reserves[underlyingAddress]=0
            reserves[underlyingAddress] -= amount
            
            borrowKey=borrower+'_'+underlyingAddress
            if borrower not in reserves_borrow:
                reserves_borrow[borrowKey]=0
            reserves_borrow[borrowKey]+=amount
            
        elif type=="repay":
            if underlyingAddress not in reserves:
                reserves[underlyingAddress]=0
            reserves[underlyingAddress] += amount
            
            
            borrowKey=borrower+'_'+underlyingAddress
            if borrower not in reserves_borrow:
                reserves_borrow[borrowKey]=0
            reserves_borrow[borrowKey]-=amount
            if reserves_borrow[borrowKey]<0:
                reserves_borrow[borrowKey]=0
                

        elif type=="liquidate":
            if underlyingAddress not in reserves:
                reserves[underlyingAddress]=0
            reserves[underlyingAddress] += amount
            
            borrowKey=borrower+'_'+underlyingAddress
            if borrower not in reserves_borrow:
                reserves_borrow[borrowKey]=0
            reserves_borrow[borrowKey]-=amount
            if reserves_borrow[borrowKey]<0:
                reserves_borrow[borrowKey]=0
                
        oneLine = theCSV.readline().decode("utf-8").strip()
            

    # 最后一天的
    getTVLtoTimeMap(last_timestamp_removeHour)    

    with open(output_dict, "wb") as tf:
        pickle.dump(timeMap,tf) 
            
if __name__ == '__main__':
    Cream()