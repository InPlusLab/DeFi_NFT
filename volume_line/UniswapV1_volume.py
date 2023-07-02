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



def UniswapV1():
    wethAddress="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    wbtcAddr="0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"
    
    print("read UniswapV1_ExchangeInfo")
    exchangeMap=readUniswapV1ExchangeInfo()
    
    print("tokePrice")
    binanceTokenPriceMap=readBinanceTokenPrice()
    
    
    input_csv="/mnt/sde1/peilin_defi/defi_1650w/UniswapV1_Transaction.csv"
    output_dict="/mnt/sde1/peilin_defi/data/volume_line/dict/timeWithVolume/UniswapV1.map"

    
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
            exchangeAddress=row[3]
            tokenAddress=exchangeMap[exchangeAddress]
            type=row[4]
            ethAmount=float(row[6])
            ethAmount_true=float(ethAmount)/pow(10,18)
            tokenAmount=float(row[7])
            
            tradeVolume_eth=binanceTokenPriceMap[wethAddress].swap(timestamp_removeHour,ethAmount_true)
            if type=="add" or type=="remove":
                continue
                        
            try:
                timeMap[timestamp_removeDay]+=tradeVolume_eth
            except:
                timeMap[timestamp_removeDay]=tradeVolume_eth
                
                
    with open(output_dict, "wb") as tf:
        pickle.dump(timeMap,tf) 
            
                
                
if __name__ == '__main__':
    UniswapV1()
    # test()