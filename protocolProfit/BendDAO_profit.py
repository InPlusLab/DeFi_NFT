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
    output_dict="/mnt/sde1/peilin_defi/data/protocolProfit/dict/BendDAO.map"    
    timeMap={}
    loanIdMap={}
    user2token2debt = {}
                
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
        loanId=row[13]
        bidPrice=None2Float(row[15])
        borrowAmount=None2Float(row[15])
        fineAmount=None2Float(row[15])
        repayAmount=None2Float(row[18])
        remainAmount=None2Float(row[19])
        
        resFee=0.0
        if type=="borrow":
            if loanId not in loanIdMap:
                loanIdMap[loanId]=0
            loanIdMap[loanId]-=amount
            
        elif type=="repay":
            loanIdMap[loanId]+=amount
            if loanIdMap[loanId]>0:
                resFee=TOKEN_PRICE_CENTER.swap(reserve, timestamp_removeHour,loanIdMap[loanId])
                loanIdMap[loanId]=0

        elif type=="withdraw":
            oneLine = theCSV.readline().decode("utf-8").strip()
            continue
            
        elif type=="auction":
            oneLine = theCSV.readline().decode("utf-8").strip()
            continue
            
        elif type=="liquidate":
            loanIdMap[loanId]+=repayAmount
            if loanIdMap[loanId]>0:
                resFee=TOKEN_PRICE_CENTER.swap(reserve, timestamp_removeHour,loanIdMap[loanId])
                loanIdMap[loanId]=0
            
        elif type=="redeem":
            loanIdMap[loanId]+=repayAmount
            if loanIdMap[loanId]>0:
                resFee=TOKEN_PRICE_CENTER.swap(reserve, timestamp_removeHour,loanIdMap[loanId])
                loanIdMap[loanId]=0        
        
        try:
            timeMap[timestamp_removeDay]+=abs(resFee)
        except:
            timeMap[timestamp_removeDay]=abs(resFee)

        oneLine = theCSV.readline().decode("utf-8").strip()
        
    with open(output_dict, "wb") as tf:
        pickle.dump(timeMap,tf) 
            
if __name__ == '__main__':
    BendDAO()