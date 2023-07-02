import numpy as np
import csv    #加载csv包便于读取csv文件
import zipfile
import os
import json
import pickle

import datetime
import time
from datetime import timedelta


def date_2_timestamp(year,mon,day):
    tempString=str(year)+"-"+str(mon)+"-"+str(day)
    tempTime=time.strptime(tempString, "%Y-%m-%d")
    return time.mktime(tempTime)

def timestamp_2_date(un_time):
    return datetime.datetime.fromtimestamp(un_time)

def timestamp_removeDay_reduce(temp_timestamp):
    temp_date=timestamp_2_date(temp_timestamp)
    temp_timestamp=date_2_timestamp(temp_date.year,temp_date.month,1)
    return int(temp_timestamp)


def getCsv1_sub(txMap,filePath_zip,outputPath_dict):
    theZIP = zipfile.ZipFile(filePath_zip, 'r')
    
    targetFileName=None
    for fileName in theZIP.namelist():
        if "Transaction" in fileName:
            targetFileName=fileName

    theCSV = theZIP.open(targetFileName);
    head = theCSV.readline().decode("utf-8").strip();
    oneLine = theCSV.readline().decode("utf-8").strip();
    i=0
    
    timeMap={}
    while (oneLine!=""):
        if i%1000000==0:
            print(i)
        i+=1
        oneArray = oneLine.split(",")
        
        timestamp=oneArray[1]
        timestamp_withoutDay=timestamp_removeDay_reduce(int(timestamp))
        transactionHash=oneArray[2]
        fromAddr=txMap[transactionHash]
        
        # print("timestamp_withoutDay",timestamp_withoutDay)
        try:
            timeMap[timestamp_withoutDay].add(fromAddr)
        except:
            timeMap[timestamp_withoutDay]=set(fromAddr)
        
        oneLine = theCSV.readline().decode("utf-8").strip(); 
        
        
    newTimeMap={}
    for key,value in timeMap.items():
        newTimeMap[key]=len(value)

    with open(outputPath_dict, "wb") as tf:
        pickle.dump(newTimeMap,tf) 

# timestamp timestamp_withoutDay transactionHash from
def getCsv1():
    inputPath_dict="/mnt/sde1/peilin_defi/data/dict/txFrom_nft.map"
    txMap={}
    with open(inputPath_dict, "rb") as tf:
        txMap=pickle.load(tf)
        
    dir= "/mnt/sde1/peilin_defi/nft_1650w/"
    for fileName in os.listdir(dir):
        defiType=fileName.split(".")[0]
        filePath=dir+fileName
        if ".zip" not in filePath:
            continue
        
        print(defiType)
        outputPath_dict="/mnt/sde1/peilin_defi/data/activeUsers_line/dict/timeWithFromCount/"+defiType+".map"
        getCsv1_sub(txMap,filePath,outputPath_dict)


#····························································
def combineMap(resMap,tempMap):
    for key,value in tempMap.items():
        try:
            _=resMap[key]
            resMap[key]+=value
        except:
            resMap[key]=value
        
def combineDefiMap():
    exchangeList=["UniswapV1","UniswapV2","UniswapV3", "BalancerV1","BalancerV2", "Curve", "SushiSwap", "ShibaSwap"]
    lendingList=["AaveV1","AaveV2", "MakerDAO", 'Compound', "Cream"]
    
    exchangeMap={}
    lendingMap={}
    
    dir="/mnt/sde1/peilin_defi/data/activeUsers_line/dict/timeWithFromCount/"
    
    for item in exchangeList:
        tempPath=dir+item+".map"
        tempMap={}
        with open(tempPath, "rb") as tf:
            tempMap=pickle.load(tf)
            
        combineMap(exchangeMap,tempMap)
        
    with open("/mnt/sde1/peilin_defi/data/activeUsers_line/dict/timeWithFromCount/exchange_defi.map", "wb") as tf:
        pickle.dump(exchangeMap,tf) 
        
        
    for item in lendingList:
        tempPath=dir+item+".map"
        tempMap={}
        with open(tempPath, "rb") as tf:
            tempMap=pickle.load(tf)
            
        combineMap(lendingMap,tempMap)
    
    with open("/mnt/sde1/peilin_defi/data/activeUsers_line/dict/timeWithFromCount/lending_defi.map", "wb") as tf:
        pickle.dump(lendingMap,tf) 
        

if __name__ == '__main__':
    getCsv1()
    
    # combineDefiMap()