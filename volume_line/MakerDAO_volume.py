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


def readCompoundCtokens():
    idMap={}
    with open("/mnt/sde1/peilin_defi/defi_1650w/MakerDAO_CollateralInfo.json", "r", encoding="utf-8") as f:
        content = json.load(f)
        for key,value in content.items():
            if "id" not in value or "underlying" not in value:
                continue
            
            id=value["id"].lower()
            underlyingAddress=value["underlying"].lower()
            
            idMap[id]=underlyingAddress

    return idMap     

tokenInfoMap=readERC20TokenInfo()
idMap=readCompoundCtokens()
def MakerDAO():    
    input_csv="/mnt/sde1/peilin_defi/defi_1650w/MakerDAO_Transaction.csv"
    output_dict="/mnt/sde1/peilin_defi/data/volume_line/dict/timeWithVolume/MakerDAO.map"

    
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
            collateralId=row[4]
            collateralAmount=None2Float(row[8])
            
            if collateralId not in idMap:
                continue
            
            underlyingAddress=idMap[collateralId]
            decimal=int(tokenInfoMap[underlyingAddress]["decimal"])
                        
            resVolume=0
            resVolume=TOKEN_PRICE_CENTER.swap(underlyingAddress, timestamp_removeHour,collateralAmount/pow(10,(18-decimal)))
            
            try:
                timeMap[timestamp_removeDay]+=abs(resVolume)
            except:
                timeMap[timestamp_removeDay]=abs(resVolume)
                        
            
    with open(output_dict, "wb") as tf:
        pickle.dump(timeMap,tf) 
            
if __name__ == '__main__':
    MakerDAO()