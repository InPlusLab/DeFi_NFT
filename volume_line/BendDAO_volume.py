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


def BendDAO():    
    output_dict="/mnt/sde1/peilin_defi/data/volume_line/dict/timeWithVolume/BendDAO.map"    
    timeMap={}
                
    theZIP = zipfile.ZipFile("/mnt/sde1/peilin_defi/nft_1650w/BendDAO.zip", 'r')
    theCSV = theZIP.open("BendDAO_Transaction.csv")	
    head = theCSV.readline()
    oneLine = theCSV.readline().decode("utf-8").strip()
    i=0
    while (oneLine!=""):
        if i%10000==0:
            print(i)
        i+=1
        row = oneLine.split(",")
        timestamp=int(row[1])
        timestamp_removeHour=timestamp_removeHour_reduce(timestamp)
        timestamp_removeDay=timestamp_removeDay_reduce(timestamp)
        type=row[3]
        reserve=row[5]
        amount=None2Float(row[6])
        bidPrice=None2Float(row[15])
        borrowAmount=None2Float(row[15])
        fineAmount=None2Float(row[15])
        repayAmount=None2Float(row[18])
        remainAmount=None2Float(row[19])
        
        resVolume=0.0
        if type=="borrow":
            resVolume=TOKEN_PRICE_CENTER.swap(reserve, timestamp_removeHour,amount)
            
        elif type=="repay":
            resVolume=TOKEN_PRICE_CENTER.swap(reserve, timestamp_removeHour,amount)
            
        elif type=="withdraw":
            resVolume=TOKEN_PRICE_CENTER.swap(reserve, timestamp_removeHour,amount)
            
        elif type=="auction":
            resVolume=TOKEN_PRICE_CENTER.swap(reserve, timestamp_removeHour,bidPrice)
            
        elif type=="liquidate":
            oneLine = theCSV.readline().decode("utf-8").strip()
            continue
            resVolume=TOKEN_PRICE_CENTER.swap(reserve, timestamp_removeHour,repayAmount+remainAmount)
            
        elif type=="redeem":
            resVolume=TOKEN_PRICE_CENTER.swap(reserve, timestamp_removeHour,repayAmount+remainAmount)
            
        
        try:
            timeMap[timestamp_removeDay]+=abs(resVolume)
        except:
            timeMap[timestamp_removeDay]=abs(resVolume)

        oneLine = theCSV.readline().decode("utf-8").strip()
        
    with open(output_dict, "wb") as tf:
        pickle.dump(timeMap,tf) 
            
if __name__ == '__main__':
    BendDAO()