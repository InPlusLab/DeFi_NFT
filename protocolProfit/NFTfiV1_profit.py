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


def NFTfiV1():    
    output_dict="/mnt/sde1/peilin_defi/data/protocolProfit/dict/NFTfiV1.map"    
    timeMap={}
                
    theZIP = zipfile.ZipFile("/mnt/sde1/peilin_defi/nft_1650w/NFTfiV1.zip", 'r')
    theCSV = theZIP.open("NFTfiV1_Transaction.csv")	
    head = theCSV.readline()
    oneLine = theCSV.readline().decode("utf-8").strip()
    i=0
    while (oneLine!=""):
        if i%10000==0:
            print(i, flush=True)
        i+=1
        row = oneLine.split(",")
        timestamp=int(row[1])
        timestamp_removeHour=timestamp_removeHour_reduce(timestamp)
        timestamp_removeDay=timestamp_removeDay_reduce(timestamp)
        type=row[3]
        loanId=row[4]
        loanPrincipalAmount=None2Float(row[7])
        loanERC20Denomination=row[14]
        amountPaidToLender=None2Float(row[16])
        adminFee=None2Float(row[17])
        
        if type=="start":
            oneLine = theCSV.readline().decode("utf-8").strip()
            continue
            
        elif type=="repay":
            resProfit=TOKEN_PRICE_CENTER.swap(loanERC20Denomination, timestamp_removeHour,amountPaidToLender+adminFee-loanPrincipalAmount)
            
        elif type=="liquidate":
            oneLine = theCSV.readline().decode("utf-8").strip()
            continue
        
        try:
            timeMap[timestamp_removeDay]+=abs(resProfit)
        except:
            timeMap[timestamp_removeDay]=abs(resProfit)

        oneLine = theCSV.readline().decode("utf-8").strip()

    with open(output_dict, "wb") as tf:
        pickle.dump(timeMap,tf) 
            
if __name__ == '__main__':
    NFTfiV1()