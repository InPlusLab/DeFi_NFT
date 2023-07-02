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


import sys
sys.path.append("/mnt/sde1/peilin_defi/code/interfaceTool")
from readTotal import *


class Pool():    
    def __init__(self,exchangeAddress):
        self.exchangeAddress=exchangeAddress
        self.ethAmount=0.0
        self.tokenAmount=0.0
        self.poolTimeMap={}
        
    def add(self,timestamp_removeHour,ethAmount):
        self.ethAmount+=ethAmount

        self.poolTimeMap[timestamp_removeHour]={"ethAmount":self.ethAmount}
        
    def remove(self,timestamp_removeHour,ethAmount):
        self.ethAmount-=ethAmount
        
        self.poolTimeMap[timestamp_removeHour]={"ethAmount":self.ethAmount}
        
    def swapTokenToEth(self,timestamp_removeHour,ethAmount):
        self.ethAmount-=ethAmount
        
        self.poolTimeMap[timestamp_removeHour]={"ethAmount":self.ethAmount}
        
    def swapEthToToken(self,timestamp_removeHour,ethAmount):
        self.ethAmount+=ethAmount
        
        self.poolTimeMap[timestamp_removeHour]={"ethAmount":self.ethAmount}
    

def UniswapV1():
    poolMap={}
    
    
    wethAddr="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    wbtcAddr="0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"
    
    print("read UniswapV1_ExchangeInfo")
    exchangeMap=readUniswapV1ExchangeInfo()

    
    print("tokePrice")
    binanceTokenPriceMap=readBinanceTokenPrice()
    
    input_csv="/mnt/sde1/peilin_defi/defi_1650w/UniswapV1_Transaction.csv"
    
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
            exchangeAddress=row[3]
            type=row[4]
            ethAmount=float(row[6])
                
            try:
                poolMap_value=poolMap[exchangeAddress]
            except:
                poolMap_value=Pool(exchangeAddress)
                poolMap[exchangeAddress]=poolMap_value

            if type=="add":
                poolMap_value.add(timestamp_removeHour,ethAmount)
            elif type=="remove":
                poolMap_value.remove(timestamp_removeHour,ethAmount)
            elif type=="swapTokenToEth":
                poolMap_value.swapTokenToEth(timestamp_removeHour,ethAmount)
            else:
                poolMap_value.swapEthToToken(timestamp_removeHour,ethAmount)
            
            poolMap[exchangeAddress]=poolMap_value
             
    # gather all poolTimeMap
    timeMap={}
    resMap={}
    
    for exchangeAddress,pool in poolMap.items():
        for tempTimestamp, value in pool.poolTimeMap.items():
            ethAmount=value["ethAmount"]
            usdAmount=binanceTokenPriceMap[wethAddr].swap(tempTimestamp,float(ethAmount)/pow(10,18))
            try:
                timeMap[tempTimestamp]+=usdAmount*2
            except:
                timeMap[tempTimestamp]=usdAmount*2
                
    # for key,value in timeMap.items():
    #     if int(key)==int(timestamp_removeDay_reduce(key)):
    #         resMap[key]=value
                
    # print("len resMap",len(resMap))
    
    with open("/mnt/sde1/peilin_defi/data/totalValueLocked_line/dict/timeWithTVL/UniswapV1.map", "wb") as tf:
        pickle.dump(timeMap,tf) 


UniswapV1()
                
# if __name__ == '__main__':
    # UniswapV1()
    # test()