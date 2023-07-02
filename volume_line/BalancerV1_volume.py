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

def BalancerV1():
    rivetTokenList=['0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2','0x2260fac5e5542a773aa44fbcfedf7c193bc2c599',
                    '0x6b175474e89094c44da98b954eedeac495271d0f','0xdac17f958d2ee523a2206206994597c13d831ec7',
                    '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48']
    
    input_csv="/mnt/sde1/peilin_defi/defi_1650w/BalancerV1_Transaction.csv"
    output_dict="/mnt/sde1/peilin_defi/data/volume_line/dict/timeWithVolume/BalancerV1.map"

    
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
            type=row[4]
            
            tokenIn_address=row[6]
            amountIn=None2Float(row[7])
            tokenOut_address=row[8]
            amountOut=None2Float(row[9])
    
            if type!="swap":
                continue
            
            if tokenIn_address not in rivetTokenList and tokenOut_address not in rivetTokenList:
                continue
            
            volumeIn=0.0
            volumeOut=0.0
            
            if amountIn!=0.0 and tokenIn_address in rivetTokenList:                
                volumeIn=TOKEN_PRICE_CENTER.swap(tokenIn_address, timestamp_removeHour,amountIn)
                
            if amountOut!=0.0 and tokenOut_address in rivetTokenList:
                volumeOut=TOKEN_PRICE_CENTER.swap(tokenOut_address, timestamp_removeHour,amountOut)
            
            resVolume=max([volumeIn,volumeOut])                    
            if resVolume==0.0:
                continue
            
            try:
                timeMap[timestamp_removeDay]+=resVolume
            except:
                timeMap[timestamp_removeDay]=resVolume
                        
            
    with open(output_dict, "wb") as tf:
        pickle.dump(timeMap,tf) 
            
if __name__ == '__main__':
    BalancerV1()