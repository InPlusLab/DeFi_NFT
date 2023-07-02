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

tokenInfoMap=readERC20TokenInfo()
cTokenMap=readCreamCrtokens()
def Cream():    
    input_csv="/mnt/sde1/peilin_defi/defi_1650w/Cream_Transaction.csv"
    output_dict="/mnt/sde1/peilin_defi/data/volume_line/dict/timeWithVolume/Cream.map"

    
    timeMap={}
    
    with open(input_csv,'r', encoding="UTF8") as f:
        reader = csv.reader(f)
        header_row=next(reader)
        
        i=0
        for row in reader:
            if i%10000==0:
                print(i)
            i+=1
            
            timestamp=int(row[1])
            timestamp_removeHour=timestamp_removeHour_reduce(timestamp)
            timestamp_removeDay=timestamp_removeDay_reduce(timestamp)
            cTokenAddress=row[3]
            type=row[4]
            amount=None2Float(row[5])
            cTokens=None2Float(row[7])
            underlyingAddress=cTokenMap[cTokenAddress]
            resVolume=0
            
            if type=="add" or type=="remove":
                continue
                
            elif type=="borrow":                
                resVolume=TOKEN_PRICE_CENTER.swap(underlyingAddress, timestamp_removeHour,amount)

            elif type=="repay":                
                resVolume=TOKEN_PRICE_CENTER.swap(underlyingAddress, timestamp_removeHour,amount)

            elif type=="liquidate":                
                resVolume=TOKEN_PRICE_CENTER.swap(underlyingAddress, timestamp_removeHour,amount)
            
            try:
                timeMap[timestamp_removeDay]+=abs(resVolume)
            except:
                timeMap[timestamp_removeDay]=abs(resVolume)
                        
            
    with open(output_dict, "wb") as tf:
        pickle.dump(timeMap,tf) 
            
if __name__ == '__main__':
    Cream()