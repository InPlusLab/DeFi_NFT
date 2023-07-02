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


def combine():
    nftMap0={}
    with open("/mnt/sde1/peilin_defi/data/nftPrice/dict/OpenSea.map", "rb") as tf:
        nftMap0=pickle.load(tf) 
        
        
    nftMap1={}
    with open("/mnt/sde1/peilin_defi/data/nftPrice/dict/LooksRare.map", "rb") as tf:
        nftMap1=pickle.load(tf) 
        
        
    for tokenAddress,value in nftMap1.items():
        if tokenAddress not in nftMap0:
            nftMap0[tokenAddress]=value
            continue
            
        for tokenId, timestampMap in value.items():
            if tokenId not in nftMap0[tokenAddress]:
                nftMap0[tokenAddress][tokenId]=timestampMap
                continue
                
            for timestamp,price in timestampMap.items():
                nftMap0[tokenAddress][tokenId][timestamp]=price
                
                
    output_dict="/mnt/sde1/peilin_defi/data/nftPrice/dict/Combine.map"
    with open(output_dict, "wb") as tf:
        pickle.dump(nftMap0,tf) 
        
# ······················································································
def getKey(x):
    return int(x[0])


def getFloorPrice(priceList):
    if len(priceList)==0:
        return 0
    if len(priceList)==1:
        return priceList[0]
    
    if len(priceList)>=2:   
        if priceList[0]!=0 and priceList[1]/priceList[0]>100:
            return priceList[1]
        if priceList[0]==0:
            return priceList[1]
        
        return priceList[0] 
            
def map2List():
    nftMap={}
    with open("/mnt/sde1/peilin_defi/data/nftPrice/dict/Combine.map", "rb") as tf:
        nftMap=pickle.load(tf) 
    
    for tokenAddress,value in nftMap.items():
        floorPrice=0
        priceSet=set()
        for tokenId, timestampMap in value.items():
            tempList=[]
            for timestamp,price in timestampMap.items():
                tempList.append((timestamp,price))
                
                if floorPrice==0 and price!=0:
                    floorPrice=price
                if floorPrice>price and price!=0:
                    floorPrice=price
                    
                priceSet.add(price)
                
            tempList.sort(key=getKey)
            value[tokenId]["list"]=tempList
            
        priceList=list(priceSet)
        priceList.sort()        
        nftMap[tokenAddress]["floorPrice"]=getFloorPrice(priceList)
        nftMap[tokenAddress]=value
        
    output_dict="/mnt/sde1/peilin_defi/data/nftPrice/dict/Combine_f.map"
    with open(output_dict, "wb") as tf:
        pickle.dump(nftMap,tf) 
            
            
combine()      
map2List()
        