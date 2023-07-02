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


def convertToDecimal(tempStr):
    # delete prefix "0"
    tempStr=tempStr.lstrip("0x")
    tempStr=tempStr.lstrip("0")
    if tempStr=="":
        tempStr="0"
        return tempStr
    convertion = int(tempStr, 16)
    return str(convertion)

def OpenSea():    
    nftMap={}
                
    theZIP = zipfile.ZipFile("/mnt/sde1/peilin_defi/nft_1650w/OpenSea.zip", 'r')
    theCSV = theZIP.open("OpenSea_Transaction.csv")	
    head = theCSV.readline()
    oneLine = theCSV.readline().decode("utf-8").strip()
    
    howToCall1Map={}
    i=0
    while (oneLine!=""):
        if i%10000==0:
            print(i, flush=True)
        i+=1
        row = oneLine.split(",")
        blockNumber=int(row[0])
        timestamp=int(row[1])
        timeString = time.strftime("%Y-%m-%d", time.gmtime(timestamp))
        timestamp_removeHour=int(timestamp_removeHour_reduce(timestamp))
        transactionHash=row[2]
        calldata1=row[16]
        howToCall1=row[15]
        paymentToken1=row[17]
        basePrice1=None2Float(row[18])/1e18
        
        howToCall1Map[howToCall1]=1
        
        if paymentToken1!="0x0000000000000000000000000000000000000000":
            oneLine = theCSV.readline().decode("utf-8").strip()
            continue
        
        if howToCall1=="DelegateCall":
            tokenAddress="0x"+calldata1[160:200]
            tokenId=convertToDecimal(calldata1[200:264])
        elif howToCall1=="Call":
            target1=row[14]
            tokenAddress=target1
            tokenId=convertToDecimal(calldata1[136:200])

        if tokenAddress not in nftMap:
            nftMap[tokenAddress]={}
        
        if tokenId not in nftMap[tokenAddress]:
            nftMap[tokenAddress][tokenId]={}
        
        nftMap[tokenAddress][tokenId][blockNumber]=basePrice1
        
        oneLine = theCSV.readline().decode("utf-8").strip()
        
    print("howToCall1Map",howToCall1Map)

    output_dict="/mnt/sde1/peilin_defi/data/nftPrice/dict/OpenSea.map"
    with open(output_dict, "wb") as tf:
        pickle.dump(nftMap,tf) 
            
if __name__ == '__main__':
    OpenSea()