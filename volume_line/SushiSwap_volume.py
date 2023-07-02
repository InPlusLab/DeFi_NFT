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


def readSushiSwapPairInfo():
    with open("/mnt/sde1/peilin_defi/data/interfaceTool/dict/SushiSwap_PairInfo.map", "rb") as tf:
        tempMap=pickle.load(tf) 
        return tempMap
    

def SushiSwap():
    rivetTokenList=['0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2','0x2260fac5e5542a773aa44fbcfedf7c193bc2c599',
                    '0x6b175474e89094c44da98b954eedeac495271d0f','0xdac17f958d2ee523a2206206994597c13d831ec7',
                    '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48']
    
    pairInfoMap=readSushiSwapPairInfo()
    
    input_csv="/mnt/sde1/peilin_defi/defi_1650w/SushiSwap_Transaction.csv"
    output_dict="/mnt/sde1/peilin_defi/data/volume_line/dict/timeWithVolume/SushiSwap.map"

    
    timeMap={}
    
    with open(input_csv,'r', encoding="UTF8") as f:
        reader = csv.reader(f)
        header_row=next(reader)
        
        i=0
        for row in reader:
            if i%10000==0:
                print(i)
            i+=1
            
            blockNumber=row[0]
            timestamp=int(row[1])
            timestamp_removeHour=timestamp_removeHour_reduce(timestamp)
            timestamp_removeDay=timestamp_removeDay_reduce(timestamp)
            transactionHash=row[2]
            pairAddress=row[3]
            type=row[4]
            amount0In=None2Float(row[7])
            amount1In=None2Float(row[8])
            amount0Out=None2Float(row[9])
            amount1Out=None2Float(row[10])
            
            tokenAddress0=pairInfoMap[pairAddress]["tokenAddress0"]
            tokenAddress1=pairInfoMap[pairAddress]["tokenAddress1"]
    
            if type=="add" or type=="remove":
                continue
            
            if tokenAddress0 not in rivetTokenList and tokenAddress1 not in rivetTokenList:
                continue
            
            volume0In=0.0
            volume1In=0.0
            volume0Out=0.0
            volume1Out=0.0
            
            if amount0In!=0.0 and tokenAddress0 in rivetTokenList:                
                volume0In=TOKEN_PRICE_CENTER.swap(tokenAddress0, timestamp_removeHour,amount0In)
                
            if amount1In!=0.0 and tokenAddress1 in rivetTokenList:
                volume1In=TOKEN_PRICE_CENTER.swap(tokenAddress1, timestamp_removeHour,amount1In)
                
            if amount0Out!=0.0 and tokenAddress0 in rivetTokenList:                
                volume0Out=TOKEN_PRICE_CENTER.swap(tokenAddress0, timestamp_removeHour,amount0Out)
                
            if amount1Out!=0.0 and tokenAddress1 in rivetTokenList:
                volume1Out=TOKEN_PRICE_CENTER.swap(tokenAddress1, timestamp_removeHour,amount1Out)
            
            resVolume=max([volume0In,volume1In,volume0Out,volume1Out])                    
            if resVolume==0.0:
                continue
            
            try:
                timeMap[timestamp_removeDay]+=resVolume
            except:
                timeMap[timestamp_removeDay]=resVolume
                        
            
    with open(output_dict, "wb") as tf:
        pickle.dump(timeMap,tf) 
            
if __name__ == '__main__':
    SushiSwap()
    # test()