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


def OpenSea():    
    output_dict="/mnt/sde1/peilin_defi/data/volume_line/dict/timeWithVolume/OpenSea.map"    
    timeMap={}
                
    theZIP = zipfile.ZipFile("/mnt/sde1/peilin_defi/nft_1650w/OpenSea.zip", 'r')
    theCSV = theZIP.open("OpenSea_Transaction.csv")	
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
        saleKind1=row[13]
        paymentToken1=row[17]

        if paymentToken1 == "0x0000000000000000000000000000000000000000":
            paymentToken1 = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"

        basePrice1=None2Float(row[18])
        saleKind2=row[32]
        basePrice2=None2Float(row[37])
        
        resVolume=TOKEN_PRICE_CENTER.swap(paymentToken1, timestamp_removeHour,basePrice1)
        
        try:
            timeMap[timestamp_removeDay]+=abs(resVolume)
        except:
            timeMap[timestamp_removeDay]=abs(resVolume)

        oneLine = theCSV.readline().decode("utf-8").strip()
    
    for timestamp in timeMap:
        print(timestamp, timeMap[timestamp])

    with open(output_dict, "wb") as tf:
        pickle.dump(timeMap,tf) 
            
if __name__ == '__main__':
    OpenSea()