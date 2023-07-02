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
from readTotal import *


def UniswapV3():
    rivetTokenList=['0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2','0x2260fac5e5542a773aa44fbcfedf7c193bc2c599',
                    '0x6b175474e89094c44da98b954eedeac495271d0f','0xdac17f958d2ee523a2206206994597c13d831ec7',
                    '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48']
    
    print("read tokenInfo")
    tokenInfoMap=readERC20TokenInfo()
    
    print("read UniswapV3_PoolInfo")
    pairInfoMap=readUniswapV3PoolInfo()
    
    print("tokePrice")
    tokenPriceMap=readRivetTokenPrice()
    
    input_csv="/mnt/sde1/peilin_defi/defi_1650w/UniswapV3_Transaction.csv"
    output_dict="/mnt/sde1/peilin_defi/data/volume_line/dict/timeWithVolume/UniswapV3.map"
    
    
    jumpCount=0
    i=0
    timeMap={}
    with open(input_csv,'r', encoding="UTF8") as f:
        reader = csv.reader(f)
        header_row=next(reader)
        
        
        for row in reader:
            if i%10000==0:
                print(i)
            i+=1
            
            blockNumber=row[0]
            timestamp=int(row[1])
            timestamp_removeHour=timestamp_removeHour_reduce(timestamp)
            timestamp_removeDay=timestamp_removeDay_reduce(timestamp)
            transactionHash=row[2]
            poolAddress=row[3]
            type=row[4]
            amount0=None2Float(row[7])
            amount1=None2Float(row[9])
            
            tokenAddress0=pairInfoMap[poolAddress]["tokenAddress0"]
            tokenAddress1=pairInfoMap[poolAddress]["tokenAddress1"]
    
            if tokenAddress0 not in rivetTokenList and tokenAddress1 not in rivetTokenList:
                jumpCount+=1
                continue
            
            if type=="add" or type=="remove" or type=="collect":
                jumpCount+=1
                continue
            
            volume0=0.0
            volume1=0.0
            
            if amount0!=0.0 and tokenAddress0 in rivetTokenList:
                amount0_true=float(amount0)/pow(10,int(tokenInfoMap[tokenAddress0]["decimal"]))
                volume0=tokenPriceMap[tokenAddress0].swap(timestamp_removeHour,amount0_true)
                
            if amount1!=0.0 and tokenAddress1 in rivetTokenList:
                amount1_true=float(amount1)/pow(10,int(tokenInfoMap[tokenAddress1]["decimal"]))
                volume1=tokenPriceMap[tokenAddress1].swap(timestamp_removeHour,amount1_true)
                
            resVolume=max([abs(volume0),abs(volume1)])
            if resVolume==0.0:
                continue
            
            try:
                timeMap[timestamp_removeDay]+=resVolume
            except:
                timeMap[timestamp_removeDay]=resVolume
            
    print("jumpCount",jumpCount)
    print("total",i)
            
    with open(output_dict, "wb") as tf:
        pickle.dump(timeMap,tf) 
                
if __name__ == '__main__':
    UniswapV3()
    
    # test()