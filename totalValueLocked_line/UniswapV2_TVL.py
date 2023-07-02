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


def UniswapV2():
    rivetTokenList=['0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2','0x2260fac5e5542a773aa44fbcfedf7c193bc2c599',
                    '0x6b175474e89094c44da98b954eedeac495271d0f','0xdac17f958d2ee523a2206206994597c13d831ec7',
                    '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48']
    
    print("read tokenInfo")
    tokenInfoMap=readERC20TokenInfo()
    
    print("read UniswapV2_PairInfo")
    pairInfoMap=readUniswapV2PairInfo()
    
    print("tokePrice")
    tokenPriceMap=readRivetTokenPrice()
    
    input_csv="/mnt/sde1/peilin_defi/defi_1650w/UniswapV2_Transaction.csv"
    output_dict="/mnt/sde1/peilin_defi/data/totalValueLocked_line/dict/timeWithTVL/UniswapV2.map"

    
    timeMap={}
    
    pairMap={}
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
            transactionHash=row[2]
            pairAddress=row[3]
            type=row[4]
            
            reserve0=float(row[11])
            reserve1=float(row[12])
            
            tokenAddress0=pairInfoMap[pairAddress]["tokenAddress0"]
            tokenAddress1=pairInfoMap[pairAddress]["tokenAddress1"]
            
            if tokenAddress0 not in rivetTokenList and tokenAddress1 not in rivetTokenList:
                continue
            
            if tokenAddress0 in rivetTokenList:
                rivetTokenAddres=tokenAddress0
                rivetAmount=reserve0
            else:
                rivetTokenAddres=tokenAddress1
                rivetAmount=reserve1
                
            rivetUsd=tokenPriceMap[rivetTokenAddres].swap(timestamp_removeHour,float(rivetAmount)/pow(10,tokenInfoMap[rivetTokenAddres]["decimal"]))
            
            pairMap[pairAddress]={timestamp_removeHour:rivetUsd*2}
            
            
    #         if pairAddress=="0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc":
    #             print(row)
                
    #         if i==100:break
            
            
    # return 

    for pairAddress,value in pairMap.items():
        for tempTimestamp, rivetUsd in value.items():
            try:
                timeMap[tempTimestamp]+=rivetUsd
            except:
                timeMap[tempTimestamp]=rivetUsd
                        
            
    with open(output_dict, "wb") as tf:
        pickle.dump(timeMap,tf) 

    
            
            
if __name__ == '__main__':
    UniswapV2()
    # test()