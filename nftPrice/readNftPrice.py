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



class TokenPriceCenter_NFT():
    def __init__(self):
        self.nftMap={}
        with open("/mnt/sde1/peilin_defi/data/nftPrice/dict/Combine_f.map", "rb") as tf:
            self.nftMap=pickle.load(tf)

    def swap(self, tokenAddress,tokenId, blockNumber):
        try:
            return self.nftMap[tokenAddress][tokenId][blockNumber]
        except:
            pass
        
        if tokenAddress not in self.nftMap:
            return 0
        
        if tokenId not in self.nftMap[tokenAddress]:
            return self.nftMap[tokenAddress]["floorPrice"]
        
        priceList=self.nftMap[tokenAddress][tokenId]["list"]
        
        # for i in range(len(priceList)-1,-1,-1):
        #     curItem=priceList[i]
        #     if curItem[0]<=blockNumber:
        #         return curItem[1]
            
        # return self.nftMap[tokenAddress]["floorPrice"]
        
        minDis=0
        minDisItem=None
        for item in priceList:
            if minDisItem==None:
                minDisItem=item
                minDis=abs(blockNumber-item[0])
                continue
            
            if abs(blockNumber-item[0])<minDis:
                minDisItem=item
                minDis=abs(blockNumber-item[0])
                
        if minDisItem==None:
            return self.nftMap[tokenAddress]["floorPrice"]
        else:
            return minDisItem[1]
            
            
        # for item in priceList:
        #     if blockNumber<item[0]:
        #         return item[1]
            
        # return self.nftMap[tokenAddress]["floorPrice"]

