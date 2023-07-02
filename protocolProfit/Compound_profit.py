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

def readCompoundCtokens():
    cTokenMap={}
    with open("/mnt/sde1/peilin_defi/defi_1650w/Compound_cTokens.json", "r", encoding="utf-8") as f:
        content = json.load(f)
        for key,value in content.items():
            cTokenAddress=value["address"].lower()
            underlyingAddress=value["underlying"].lower()
            
            cTokenMap[cTokenAddress]=underlyingAddress
            
    
    cTokenMap["0x4ddc2d193948926d02f9b1fe9e1daa0718270ed5"]="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"

    return cTokenMap   


user2token2debt = {}

def Compound():
    cTokenMap=readCompoundCtokens()
    output_dict="/mnt/sde1/peilin_defi/data/protocolProfit/dict/Compound.map"    
    timeMap={}
                
    theZIP = zipfile.ZipFile("/mnt/sde1/peilin_defi/defi_1650w/Compound.zip", 'r')
    theCSV = theZIP.open("Compound_Transaction.csv")	
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
        cTokenAddress=row[3]
        type=row[4]
        amount=None2Float(row[5])
        borrower=row[8]
        underlyingAddress=cTokenMap[cTokenAddress]
        profit=0
        
        if type=="add" or type=="remove" or amount==0:
            oneLine = theCSV.readline().decode("utf-8").strip()
            continue
            
        elif type=="borrow":                
            if borrower not in user2token2debt:
                user2token2debt[borrower] = {}
            if underlyingAddress not in user2token2debt[borrower]:
                user2token2debt[borrower][underlyingAddress] = 0
            user2token2debt[borrower][underlyingAddress] -= amount   
            
        elif type=="repay":
            user2token2debt[borrower][underlyingAddress] += amount
            if user2token2debt[borrower][underlyingAddress] > 0:
                profit=TOKEN_PRICE_CENTER.swap(underlyingAddress, timestamp_removeHour, user2token2debt[borrower][underlyingAddress])
                user2token2debt[borrower][underlyingAddress] = 0
                
        elif type=="liquidate":                
            user2token2debt[borrower][underlyingAddress] += amount
            if user2token2debt[borrower][underlyingAddress] > 0:
                profit=TOKEN_PRICE_CENTER.swap(underlyingAddress, timestamp_removeHour, user2token2debt[borrower][underlyingAddress])
                user2token2debt[borrower][underlyingAddress] = 0
            
        try:
            timeMap[timestamp_removeDay]+=abs(profit)
        except:
            timeMap[timestamp_removeDay]=abs(profit)

        oneLine = theCSV.readline().decode("utf-8").strip()

    with open(output_dict, "wb") as tf:
        pickle.dump(timeMap,tf) 
            
if __name__ == '__main__':
    Compound()