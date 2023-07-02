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


def MakerDAO():
    idMap=readMakerDAOCollateralInfo()
    tokenInfoMap=readERC20TokenInfo()
    user2token2debt = {}
    
    output_dict="/mnt/sde1/peilin_defi/data/protocolProfit/dict/MakerDAO.map"    
    timeMap={}
                
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
        daiAddress="0x6b175474e89094c44da98b954eedeac495271d0f"
        decimalDai=int(tokenInfoMap[daiAddress]["decimal"])
        resProfit=0.0 
        
        
        if collateralOwner not in user2token2debt:
            user2token2debt[collateralOwner] = 0
        # daiAmount>0: 用户借出dai
        # daiAmount>0: 用户归还dai
        user2token2debt[collateralOwner]+=daiAmount
        
        if user2token2debt[collateralOwner]< 0:
            resProfit=TOKEN_PRICE_CENTER.swap(daiAddress, timestamp_removeHour, abs(user2token2debt[collateralOwner])/pow(10,(18-decimalDai)))
            user2token2debt[collateralOwner] = 0    
    
        
        try:
            timeMap[timestamp_removeDay]+=abs(resProfit)
        except:
            timeMap[timestamp_removeDay]=abs(resProfit)

        oneLine = theCSV.readline().decode("utf-8").strip()


    # timestamps = []   
    # for i in timeMap:
    #     timestamps.append(i)
    # timestamps.sort()
    # for timestamp in timestamps:
    #     print(timestamp_2_date(timestamp), timeMap[timestamp])

    with open(output_dict, "wb") as tf:
        pickle.dump(timeMap,tf) 
            
if __name__ == '__main__':
    MakerDAO()