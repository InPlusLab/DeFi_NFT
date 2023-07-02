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

def calReturnAmount(principal,interestRate):
    # principal + principal * interestRate / INTEREST_RATE_DENOMINATOR / BASIS_POINTS_DENOMINATOR;
    principal=float(principal)
    interestRate=float(interestRate)
    return principal+principal*interestRate/1e18/10000

def Arcade():
    output_dict="/mnt/sde1/peilin_defi/data/protocolProfit/dict/Arcade.map"    
    timeMap={}
    loanIdMap={}
                
    theZIP = zipfile.ZipFile("/mnt/sde1/peilin_defi/nft_1650w/Arcade.zip", 'r')
    theCSV = theZIP.open("Arcade_Transaction.csv")	
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
        interestRate=None2Float(row[8])
        principal=None2Float(row[9])
        payableCurrency=row[12]
        settledAmount=None2Float(row[15])
        amountToOldLender=None2Float(row[16])
        amountToLender=None2Float(row[17])
        amountToBorrower=None2Float(row[18])
        oldLoanId=row[19]
        newLoanId=row[20]
        resFee=0.0
        
        # 借款人抵押NFT, 放贷人同意借钱，该函数由放贷人调用
        if type=="start":
            loanIdMap[loanId]={"principal":principal,"returnAmount":calReturnAmount(principal,interestRate),"payableCurrency":payableCurrency}
            
        elif type=="rollover":
            loanIdMap[newLoanId]={"principal":principal,"returnAmount":calReturnAmount(principal,interestRate),"payableCurrency":payableCurrency}
            resFee=TOKEN_PRICE_CENTER.swap(payableCurrency, timestamp_removeHour,settledAmount-amountToOldLender-amountToLender-amountToBorrower)

        elif type=="repay":
            principal=loanIdMap[loanId]["principal"]
            returnAmount=loanIdMap[loanId]["returnAmount"]
            payableCurrency=loanIdMap[loanId]["payableCurrency"]
            resFee=TOKEN_PRICE_CENTER.swap(payableCurrency, timestamp_removeHour,returnAmount-principal)
        else:
            # claim
            oneLine = theCSV.readline().decode("utf-8").strip()
            continue
        
        try:
            timeMap[timestamp_removeDay]+=abs(resFee)
        except:
            timeMap[timestamp_removeDay]=abs(resFee)

        oneLine = theCSV.readline().decode("utf-8").strip()

    with open(output_dict, "wb") as tf:
        pickle.dump(timeMap,tf) 
            
if __name__ == '__main__':
    Arcade()