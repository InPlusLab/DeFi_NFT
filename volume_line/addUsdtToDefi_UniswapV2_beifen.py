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
    wethAddr="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    wbtcAddr="0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"
    
    print("read tokenInfo")
    tokenInfoMap=readERC20TokenInfo()
    
    print("read UniswapV2_PairInfo")
    pairInfoMap=readUniswapV2PairInfo()
    
    print("tokePrice")
    tokenPriceMap=readTokenPrice()
    
    input_csv="/mnt/sde1/peilin_defi/defi_1650w/UniswapV2_Transaction.csv"
    output_csv="/mnt/sde1/peilin_defi/data/volume_line/defi/UniswapV2_Transaction.csv"
    
    f = open(output_csv,'w')
    writer = csv.writer(f)
    writer.writerow(["blockNumber","timestamp_removeHour","transactionHash","tradeVolume"])
    
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
            reserve0=row[11]
            reserve1=row[12]
            tradeVolume=0.0
            
            tokenAddress0=pairInfoMap[pairAddress]["tokenAddress0"]
            tokenAddress1=pairInfoMap[pairAddress]["tokenAddress1"]
            
            if tokenAddress0 not in tokenPriceMap.keys() and tokenAddress1 not in tokenPriceMap.keys():
                continue
            
            try:
                reserve0_true=float(reserve0)/pow(10,int(tokenInfoMap[tokenAddress0]["decimal"]))
                reserve1_true=float(reserve1)/pow(10,int(tokenInfoMap[tokenAddress1]["decimal"]))
            except:
                if tokenAddress0 not in rivetTokenList and tokenAddress1 not in rivetTokenList:
                    continue

            if type=="add" or type=="remove":
                if tokenAddress0 in rivetTokenList or tokenAddress1 in rivetTokenList:
                    if tokenAddress0 in rivetTokenList:
                        tempTokenAddress=tokenAddress0
                        tempReserve=reserve0_true
                    else:
                        tempTokenAddress=tokenAddress1
                        tempReserve=reserve1_true
                    
                    tradeVolume_temp=tokenPriceMap[tempTokenAddress].swapToken(timestamp_removeHour,tempReserve)
                    tradeVolume=tradeVolume_temp*2
                    
                else:
                    if tokenAddress0 in tokenPriceMap.keys():
                        tradeVolume_temp=tokenPriceMap[tokenAddress0].swapToken(timestamp_removeHour,reserve0_true)
                    else:
                        tradeVolume_temp=tokenPriceMap[tokenAddress1].swapToken(timestamp_removeHour,reserve1_true)
                        
                    tradeVolume=tradeVolume_temp*2
                 
            else:
                if tokenAddress0 in rivetTokenList or tokenAddress1 in rivetTokenList:
                    if tokenAddress0 in rivetTokenList:
                        tempTokenAddress=tokenAddress0
                        tempReserve=reserve0_true
                    else:
                        tempTokenAddress=tokenAddress1
                        tempReserve=reserve1_true
                    
                    tradeVolume_temp=tokenPriceMap[tempTokenAddress].swapToken(timestamp_removeHour,tempReserve)
                    tradeVolume=tradeVolume_temp
                    
                else:
                    if tokenAddress0 in tokenPriceMap.keys():
                        tradeVolume_temp=tokenPriceMap[tokenAddress0].swapToken(timestamp_removeHour,reserve0_true)
                    else:
                        tradeVolume_temp=tokenPriceMap[tokenAddress1].swapToken(timestamp_removeHour,reserve1_true)
                        
                    tradeVolume=tradeVolume_temp
            
            writer.writerow([blockNumber,timestamp_removeHour,transactionHash,tradeVolume])

def test(): 
    # blockNumber,timestamp,transactionHash,pairAddress,type,from,to,
    # amount0In,amount1In,amount0Out,amount1Out,reserve0,reserve1,LPTokens
    input_csv="/mnt/sde1/peilin_defi/defi_1650w/UniswapV2_Transaction.csv"
    
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
            reserve0=row[11]
            reserve1=row[12]
            if transactionHash=="0x2ef96febd1777e0403768e45e46dbd677f21079ba5f88297b500806b6fef23cb":
                print(row)
            if transactionHash=="0x932cb88306450d481a0e43365a3ed832625b68f036e9887684ef6da594891366":
                print(row)
                return
                
if __name__ == '__main__':
    UniswapV2()
    # test()