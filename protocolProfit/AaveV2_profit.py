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

user2token2debt = {}


def AaveV2():
    output_dict="/mnt/sde1/peilin_defi/data/protocolProfit/dict/AaveV2.map"    
    timeMap={}
                
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
        timestamp_removeDay=timestamp_removeDay_reduce(timestamp)
        type=row[4]
        collateralAddress=row[6]
        amount=None2Float(row[7])
        borrower = row[9]
        onBehalfOf = row[10]
        premium=None2Float(row[16])
        liquidatedCollateralAddress=row[18]
        debtToCover=None2Float(row[19])
        liquidatedCollateralAmount=None2Float(row[20])

        if borrower != "None" and onBehalfOf != "None":
            borrower = onBehalfOf
        # TODO supplyer

        profit = 0
        
        if type=="add":
            oneLine = theCSV.readline().decode("utf-8").strip()
            continue
            
        elif type=="remove":
            oneLine = theCSV.readline().decode("utf-8").strip()
            continue
            
        elif type=="borrow":
            # print(oneLine)
            if borrower not in user2token2debt:
                user2token2debt[borrower] = {}
            if collateralAddress not in user2token2debt[borrower]:
                user2token2debt[borrower][collateralAddress] = 0
            user2token2debt[borrower][collateralAddress] -= amount         
            
        elif type=="repay":
            # print(oneLine)
            user2token2debt[borrower][collateralAddress] += amount
            if user2token2debt[borrower][collateralAddress] > 0:
                profit=TOKEN_PRICE_CENTER.swap(collateralAddress, timestamp_removeHour, user2token2debt[borrower][collateralAddress])
                user2token2debt[borrower][collateralAddress] = 0
            
        elif type=="flashloan":
            profit=TOKEN_PRICE_CENTER.swap(collateralAddress, timestamp_removeHour,premium)
            
        elif type=="liquidate":            
            user2token2debt[borrower][collateralAddress] += debtToCover
            if user2token2debt[borrower][collateralAddress] > 0:
                profit=TOKEN_PRICE_CENTER.swap(collateralAddress, timestamp_removeHour, user2token2debt[borrower][collateralAddress])
                user2token2debt[borrower][collateralAddress] = 0
        
        try:
            timeMap[timestamp_removeDay]+=abs(profit)
        except:
            timeMap[timestamp_removeDay]=abs(profit)

        oneLine = theCSV.readline().decode("utf-8").strip()


    timestamps = []   
    for i in timeMap:
        timestamps.append(i)
    timestamps.sort()
    for timestamp in timestamps:
        print(timestamp_2_date(timestamp), timeMap[timestamp])

    with open(output_dict, "wb") as tf:
        pickle.dump(timeMap,tf) 
            
if __name__ == '__main__':
    AaveV2()