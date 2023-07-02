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

def getFeeFromNormalTx(filePath,marketMap,outputPath_dict):
    with open(filePath,'r', encoding="UTF8") as f:
        reader = csv.reader(f)
        header_row=next(reader)
        i=0
        for row in reader:
            if i%10000==0:
                print(i)
            i+=1
            
            transactionHash=row[2]
            gasPrice=row[10]
            gasUsed=row[11]
            transactionFee=(float(gasPrice)*float(gasUsed))
            marketTypeList=row[-1].split("_")
            
            for marketType in marketTypeList:
                if marketType not in marketMap:
                    marketMap_value={"count":0,"fee":0,"averageFee":0}
                else:
                    marketMap_value=marketMap[marketType]
                
                marketMap_value["count"]+=1
                marketMap_value["fee"]+=transactionFee
                marketMap_value["averageFee"]+=(transactionFee/len(marketTypeList))
                marketMap[marketType]=marketMap_value
                
                
    with open(outputPath_dict, "wb") as tf:
        pickle.dump(marketMap,tf) 
                    
            
def defiFee():
    pathDir= "/mnt/sde1/peilin_defi/data/acrossXblock/normalTransaction/defi/"
    outputPath_dict="/mnt/sde1/peilin_defi/data/ethereumProfit_v1/dict/defiFee.map"
    marketMap={}
    for fileName in os.listdir(pathDir):
        filePath=pathDir+fileName
        getFeeFromNormalTx(filePath,marketMap,outputPath_dict)
        
        
def nftFee():
    pathDir= "/mnt/sde1/peilin_defi/data/acrossXblock/normalTransaction/nft/"
    outputPath_dict="/mnt/sde1/peilin_defi/data/ethereumProfit_v1/dict/nftFee.map"
    marketMap={}
    for fileName in os.listdir(pathDir):
        filePath=pathDir+fileName
        getFeeFromNormalTx(filePath,marketMap,outputPath_dict)
        
        
def defiNftFee():
    pathDir= "/mnt/sde1/peilin_defi/data/acrossXblock/normalTransaction/defiNft/"
    outputPath_dict="/mnt/sde1/peilin_defi/data/ethereumProfit_v1/dict/defiNftFee.map"
    marketMap={}
    for fileName in os.listdir(pathDir):
        filePath=pathDir+fileName
        getFeeFromNormalTx(filePath,marketMap,outputPath_dict)
        
        
if __name__ == '__main__':
    # defiFee()
    # nftFee()
    
    defiNftFee()

        
        
