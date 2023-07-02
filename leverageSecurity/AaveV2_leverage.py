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
    print(timestamp_2_date(tempTimestamp), tvl,borrow_tvl, flush=True)
    

def AaveV2():
    global last_timestamp_removeHour
    output_dict="/mnt/sde1/peilin_defi/data/leverageSecurity/dict/AaveV2.map"
    theZIP = zipfile.ZipFile("/mnt/sde1/peilin_defi/defi_1650w/AaveV2.zip", 'r')
    theCSV = theZIP.open("AaveV2_Transaction.csv")	
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
        
        type=row[4]
        collateralAddress=row[6]
        amount=None2Float(row[7])
        borrower=row[9]
        premium=None2Float(row[16])
        liquidatedCollateralAddress=row[18]
        debtToCover=None2Float(row[19])
        liquidatedCollateralAmount=None2Float(row[20])
        
        
        if type=="add":
            if collateralAddress not in reserves:
                reserves[collateralAddress]=0
            reserves[collateralAddress] += amount
            
        elif type=="remove":
            if collateralAddress not in reserves:
                reserves[collateralAddress]=0
            reserves[collateralAddress] -= amount
            
        elif type=="borrow":
            if collateralAddress not in reserves:
                reserves[collateralAddress]=0
            reserves[collateralAddress] -= amount
            
            
            borrowKey=borrower+'_'+collateralAddress
            if borrower not in reserves_borrow:
                reserves_borrow[borrowKey]=0
            reserves_borrow[borrowKey]+=amount
            
            
        elif type=="repay":
            if collateralAddress not in reserves:
                reserves[collateralAddress]=0
            reserves[collateralAddress] += amount
            
            
            borrowKey=borrower+'_'+collateralAddress
            if borrower not in reserves_borrow:
                reserves_borrow[borrowKey]=0
            reserves_borrow[borrowKey]-=amount
            if reserves_borrow[borrowKey]<0:
                reserves_borrow[borrowKey]=0
            
            
        elif type=="flashloan":
            if collateralAddress not in reserves:
                reserves[collateralAddress]=0
            reserves[collateralAddress] += premium
            
        elif type=="liquidate":
            if collateralAddress not in reserves:
                reserves[collateralAddress]=0
            reserves[collateralAddress] += debtToCover
            
            if liquidatedCollateralAddress not in reserves:
                reserves[liquidatedCollateralAddress]=0
            reserves[liquidatedCollateralAddress] -= liquidatedCollateralAmount
            
            
            borrowKey=borrower+'_'+collateralAddress
            if borrower not in reserves_borrow:
                reserves_borrow[borrowKey]=0
            reserves_borrow[borrowKey]-=debtToCover
            if reserves_borrow[borrowKey]<0:
                reserves_borrow[borrowKey]=0
                
        oneLine = theCSV.readline().decode("utf-8").strip()
            

    # 最后一天的
    getTVLtoTimeMap(last_timestamp_removeHour)    

    with open(output_dict, "wb") as tf:
        pickle.dump(timeMap,tf) 
            
if __name__ == '__main__':
    AaveV2()