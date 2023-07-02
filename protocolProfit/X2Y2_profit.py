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


def X2Y2():    
    output_dict="/mnt/sde1/peilin_defi/data/protocolProfit/dict/X2Y2.map"    
    timeMap={}
                
    theZIP = zipfile.ZipFile("/mnt/sde1/peilin_defi/nft_1650w/X2Y2.zip", 'r')
    theCSV = theZIP.open("X2Y2_Transaction.csv")	
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
        currency=row[10]
        itemPrice=None2Float(row[12])
        fees=row[-1]
        if fees=="":
            oneLine = theCSV.readline().decode("utf-8").strip()
            continue
        feeList=fees.split(";")
        resFee=0.0   
        
        for fee in feeList:
            feeAmount=float(fee.split(":")[0])/1000000*itemPrice
            resFee+=TOKEN_PRICE_CENTER.swap(currency, timestamp_removeHour,feeAmount)
        
        try:
            timeMap[timestamp_removeDay]+=abs(resFee)
        except:
            timeMap[timestamp_removeDay]=abs(resFee)

        oneLine = theCSV.readline().decode("utf-8").strip()

    with open(output_dict, "wb") as tf:
        pickle.dump(timeMap,tf) 
            
if __name__ == '__main__':
    X2Y2()