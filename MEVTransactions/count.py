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


def getCount(marketMap,filePath,flashbotsTxMap):
    with open(filePath,'r', encoding="UTF8") as f:
        reader = csv.reader(f)
        header_row=next(reader)
        
        i=0
        for row in reader:
            if i%10000==0:
                print(i)
            i+=1
            timestamp=int(row[1])
            timestamp_removeDay=timestamp_removeDay_reduce(timestamp)
            transactionHash=row[2]
            if transactionHash not in flashbotsTxMap:
                continue
            marketTypeList=row[-1].split("_")
            if len(marketTypeList)==1:
                marketType=marketTypeList[0]
                if marketType not in marketMap:
                    marketMap_v={}
                else:
                    marketMap_v=marketMap[marketType]
                
                if timestamp_removeDay not in marketMap_v:
                    marketMap_v_v=0
                else:
                    marketMap_v_v=marketMap_v[timestamp_removeDay]
                
                marketMap_v_v+=1
                marketMap_v[timestamp_removeDay]=marketMap_v_v
                marketMap[marketType]=marketMap_v
                
            elif len(marketTypeList)>1:
                marketType="Multiple"
                if marketType not in marketMap:
                    marketMap_v={}
                else:
                    marketMap_v=marketMap[marketType]
                
                if timestamp_removeDay not in marketMap_v:
                    marketMap_v_v=0
                else:
                    marketMap_v_v=marketMap_v[timestamp_removeDay]
                
                marketMap_v_v+=1
                marketMap_v[timestamp_removeDay]=marketMap_v_v
                marketMap[marketType]=marketMap_v
            
            
def defiNftCount():
    flashbotsTxMap={}
    with open("/mnt/sde1/peilin_defi/data/flashbots/dict/flashbotsTxMap.data", "rb") as tf:
        flashbotsTxMap=pickle.load(tf)
    
    normalTxDir= "/mnt/sde1/peilin_defi/data/acrossXblock/normalTransaction/defiNft/"
    outputDir="/mnt/sde1/peilin_defi/data/MEVTransactions/dict/flashbotsCount/"
    marketMap={}
    for fileName in os.listdir(normalTxDir):
        filePath=normalTxDir+fileName
        # if fileName not in ["15500000to15749999_BlockTransaction.csv","15750000to15999999_BlockTransaction.csv",
        #                     "16000000to16249999_BlockTransaction.csv","16250000to16499999_BlockTransaction.csv"]:
        #     continue
        print(filePath)
        getCount(marketMap,filePath,flashbotsTxMap)
        
    for key,value in marketMap.items():
        outputPath_dict=outputDir+key+".map"
                
        with open(outputPath_dict, "wb") as tf:
            pickle.dump(value,tf) 
            
    print("flashbotsTxMap",len(flashbotsTxMap))
        
if __name__ == '__main__':    
    defiNftCount()