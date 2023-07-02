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

def readMakerDAOCollateralInfo():
    idMap={}
    with open("/mnt/sde1/peilin_defi/defi_1650w/MakerDAO_CollateralInfo.json", "r", encoding="utf-8") as f:
        content = json.load(f)
        for key,value in content.items():
            if "id" not in value or "underlying" not in value:
                continue
            
            id=value["id"].lower()
            underlyingAddress=value["underlying"].lower()
            
            idMap[id]=underlyingAddress

    return idMap     


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
    print(timestamp_2_date(tempTimestamp), tvl,borrow_tvl, flush=True)
    

def MakerDAO():
    tokenInfoMap=readERC20TokenInfo()
    idMap=readMakerDAOCollateralInfo()
    
    global last_timestamp_removeHour
    output_dict="/mnt/sde1/peilin_defi/data/leverageSecurity/dict/MakerDAO.map"
    theZIP = zipfile.ZipFile("/mnt/sde1/peilin_defi/defi_1650w/MakerDAO.zip", 'r')
    theCSV = theZIP.open("MakerDAO_Transaction.csv")	
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
        collateralId=row[4]
        collateralOwner=row[6]
        collateralAmount=None2Float(row[8])
        daiAmount=None2Float(row[9])

        
        if last_timestamp_removeHour == 0:
            last_timestamp_removeHour = timestamp_removeHour
        else:
            if last_timestamp_removeHour < timestamp_removeHour:
                getTVLtoTimeMap(last_timestamp_removeHour)
                last_timestamp_removeHour = timestamp_removeHour
        
        if collateralId not in idMap:
            oneLine = theCSV.readline().decode("utf-8").strip()
            continue
        
        underlyingAddress=idMap[collateralId]
        decimal=int(tokenInfoMap[underlyingAddress]["decimal"])
        if underlyingAddress not in reserves:
            reserves[underlyingAddress]=0
        reserves[underlyingAddress] += collateralAmount/pow(10,(18-decimal))
        
        
        daiAddress="0x6b175474e89094c44da98b954eedeac495271d0f"
        decimalDai=int(tokenInfoMap[daiAddress]["decimal"])
        borrowKey=collateralOwner+'_'+daiAddress
        if daiAddress not in reserves_borrow:
            reserves_borrow[borrowKey]=0
        reserves_borrow[borrowKey] += daiAmount/pow(10,(18-decimalDai))
        if reserves_borrow[borrowKey]<0:
            reserves_borrow[borrowKey]=0
        
        oneLine = theCSV.readline().decode("utf-8").strip()
            

    # 最后一天的
    getTVLtoTimeMap(last_timestamp_removeHour)    

    with open(output_dict, "wb") as tf:
        pickle.dump(timeMap,tf) 
            
if __name__ == '__main__':
    MakerDAO()