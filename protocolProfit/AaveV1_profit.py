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

def AaveV1():
    output_dict="/mnt/sde1/peilin_defi/data/protocolProfit/dict/AaveV1.map"    
    timeMap={}
                
    theZIP = zipfile.ZipFile("/mnt/sde1/peilin_defi/defi_1650w/AaveV1.zip", 'r')
    theCSV = theZIP.open("AaveV1_Transaction.csv")	
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
        type=row[4]
        collateralAddress=row[6]
        amount=None2Float(row[7])
        amountMinusFees=None2Float(row[15])
        fees=None2Float(row[16])
        totalFee=None2Float(row[17])
        liquidatedCollateralAddress=row[20]
        purchaseAmount=None2Float(row[21])
        liquidatedCollateralAmount=None2Float(row[22])
        accruedBorrowInterest=None2Float(row[23])
        resVolume=0.0
        
        if type=="add" or type=="remove":
            oneLine = theCSV.readline().decode("utf-8").strip()
            continue
            
        elif type=="borrow":
            oneLine = theCSV.readline().decode("utf-8").strip()
            continue
            
        elif type=="repay":
            resVolume=TOKEN_PRICE_CENTER.swap(collateralAddress, timestamp_removeHour,fees)

        elif type=="flashloan":                
            resVolume=TOKEN_PRICE_CENTER.swap(collateralAddress, timestamp_removeHour,totalFee)

        elif type=="liquidate":                
            resVolume=TOKEN_PRICE_CENTER.swap(collateralAddress, timestamp_removeHour,accruedBorrowInterest)
        
        try:
            timeMap[timestamp_removeDay]+=abs(resVolume)
        except:
            timeMap[timestamp_removeDay]=abs(resVolume)

        oneLine = theCSV.readline().decode("utf-8").strip()

    with open(output_dict, "wb") as tf:
        pickle.dump(timeMap,tf) 
            
if __name__ == '__main__':
    AaveV1()