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

def readPool():
    theZIP = zipfile.ZipFile("/mnt/sde1/peilin_defi/defi_1650w/Curve.zip", 'r')
    theJson = theZIP.open("Curve_PoolInfo.json")	

    poolInfo=json.load(theJson)
    
    theJson.close()
    theZIP.close()
    
    return poolInfo


def Curve():
    poolInfo=readPool()
    output_dict="/mnt/sde1/peilin_defi/data/protocolProfit/dict/Curve.map"    
    timeMap={}
                
    theZIP = zipfile.ZipFile("/mnt/sde1/peilin_defi/defi_1650w/Curve.zip", 'r')
    theCSV = theZIP.open("Curve_Transaction.csv")	
    head = theCSV.readline()
    oneLine = theCSV.readline().decode("utf-8").strip()
    i=0
    while (oneLine!=""):
        if i%10000==0:
            print(i, flush=True)
        i+=1
        row = oneLine.split(",")
        blockNumber=row[0]
        timestamp=int(row[1])
        timestamp_removeHour=timestamp_removeHour_reduce(timestamp)
        timestamp_removeDay=timestamp_removeDay_reduce(timestamp)
        transactionHash=row[2]
        poolAddress=row[3]
        type=row[4]
        tokenIn_address=row[6]
        amountIn=None2Float(row[7])
        tokenOut_address=row[8]
        amountOut=None2Float(row[9])
        fees=None2Float(row[10])
        resVolume=0.0

        # TODO 手续费点数
        # 0.04%
        swapFee=0.0004
        if type=="swap":
            volumeIn=0.0
            volumeOut=0.0
                        
            if amountIn!=0.0:
                volumeIn=TOKEN_PRICE_CENTER.swap(tokenIn_address, timestamp_removeHour,amountIn)*swapFee
            if amountOut!=0.0:
                volumeOut=TOKEN_PRICE_CENTER.swap(tokenOut_address, timestamp_removeHour,amountOut)*swapFee
                
            resVolume=max(volumeIn,volumeOut)
            
        # elif type == "remove" and tokenOut_address == "None" and amountOut!=0:
        #     is_erc20_remove = True
        #     if "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee" in poolInfo[poolAddress]["coins"]:
        #         if previous_row[4] != "erc20" or previous_row[8] not in poolInfo[poolAddress]["coins"]:
        #             is_erc20_remove = False
        #         for row_index in range(len(row)):
        #             # the same, except for type, tokenOut_address, amountOut
        #             if row_index != 4 and row_index != 8 and row_index != 9 and  previous_row[row_index] != row[row_index]:
        #                 is_erc20_remove = False
        #     if is_erc20_remove:
        #         tokenOut_address = previous_row[8]
        #     else:
        #         tokenOut_address = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
        #     resVolume = TOKEN_PRICE_CENTER.swap(tokenOut_address, timestamp_removeHour,amountOut)
        

        # TODO 用FeeAmount算这个手续费
        elif type == "remove" and tokenOut_address == "None":
            resVolume=TOKEN_PRICE_CENTER.swap(tokenOut_address, timestamp_removeHour,fees)
            
        elif type == "add" and tokenIn_address == "None":
            resVolume=TOKEN_PRICE_CENTER.swap(tokenIn_address, timestamp_removeHour,fees)


        try:
            timeMap[timestamp_removeDay]+=abs(resVolume)
        except:
            timeMap[timestamp_removeDay]=abs(resVolume)

        oneLine = theCSV.readline().decode("utf-8").strip()

    with open(output_dict, "wb") as tf:
        pickle.dump(timeMap,tf) 
            
if __name__ == '__main__':
    Curve()