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

def AaveV2():    
    input_csv="/mnt/sde1/peilin_defi/defi_1650w/AaveV2_Transaction.csv"
    output_dict="/mnt/sde1/peilin_defi/data/volume_line/dict/timeWithVolume/AaveV2.map"

    
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
            collateralAddress=row[6]
            amount=None2Float(row[7])

            premium=None2Float(row[16])
            liquidatedCollateralAddress=row[18]
            debtToCover=None2Float(row[19])
            liquidatedCollateralAmount=None2Float(row[20])
            
    
    
            resVolume=0
            
            if type=="add" or type=="remove":
                continue
                
            elif type=="borrow":
                resVolume=TOKEN_PRICE_CENTER.swap(collateralAddress, timestamp_removeHour,amount)
                
            elif type=="repay":
                resVolume=TOKEN_PRICE_CENTER.swap(collateralAddress, timestamp_removeHour,amount)

            elif type=="flashloan":                
                resVolume=TOKEN_PRICE_CENTER.swap(collateralAddress, timestamp_removeHour,amount+premium)

            elif type=="liquidate":                
                resVolume1=TOKEN_PRICE_CENTER.swap(collateralAddress, timestamp_removeHour,debtToCover)
                resVolume2=TOKEN_PRICE_CENTER.swap(liquidatedCollateralAddress, timestamp_removeHour,liquidatedCollateralAmount)

                resVolume=max(resVolume1,resVolume2)
            
            try:
                timeMap[timestamp_removeDay]+=resVolume
            except:
                timeMap[timestamp_removeDay]=resVolume
                        
            
    with open(output_dict, "wb") as tf:
        pickle.dump(timeMap,tf) 
            
if __name__ == '__main__':
    AaveV2()