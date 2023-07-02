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


def LooksRare():
    output_dict="/mnt/sde1/peilin_defi/data/protocolProfit/dict/LooksRare.map"    
    timeMap={}
    
    # key: strategy
    protocolFeeMap={
        "0x56244bb70cbd3ea9dc8007399f61dfc065190031":0.02,
        "0x86f909f70813cdb1bc733f4d97dc6b03b8e7e8f3":0.02,
        "0x579af6fd30bf83a5ac0d636bc619f98dbdeb930c":0.015,
        "0x09f93623019049c76209c26517acc2af9d49c69b":0.015,
        "0x58d83536d3efedb9f7f2a1ec3bdaad2b1a4dd98c":0
        
    }
                
    theZIP = zipfile.ZipFile("/mnt/sde1/peilin_defi/nft_1650w/LooksRare.zip", 'r')
    theCSV = theZIP.open("LooksRare_Transaction.csv")	
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
        strategy=row[8]
        currency=row[9]
        price=None2Float(row[13])
        protocolFeeRate=protocolFeeMap[strategy]
        
        resProfit=TOKEN_PRICE_CENTER.swap(currency, timestamp_removeHour,price*protocolFeeRate)
        
        try:
            timeMap[timestamp_removeDay]+=abs(resProfit)
        except:
            timeMap[timestamp_removeDay]=abs(resProfit)

        oneLine = theCSV.readline().decode("utf-8").strip()

    with open(output_dict, "wb") as tf:
        pickle.dump(timeMap,tf) 
            
if __name__ == '__main__':
    LooksRare()