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

def getTxs_fromDefi():
    dir= "/mnt/sde1/peilin_defi/defi_1650w/"
    for fileName in os.listdir(dir):
        filePath=dir+fileName
        if ".zip" not in filePath:
            continue
        
        makertType=fileName.split(".")[0]
        outputPath_dict="/mnt/sde1/peilin_defi/data/acrossXblock/dict/"+makertType+".map"
        
        getTxs_fromZip(filePath,outputPath_dict)
        
        
def getTxs_fromNFT():
    dir= "/mnt/sde1/peilin_defi/nft_1650w/"
    for fileName in os.listdir(dir):
        filePath=dir+fileName
        if ".zip" not in filePath:
            continue
        
        makertType=fileName.split(".")[0]
        outputPath_dict="/mnt/sde1/peilin_defi/data/acrossXblock/dict/"+makertType+".map"
        
        getTxs_fromZip(filePath,outputPath_dict)
        
        
def getTxs_fromZip(filePath_zip,outputPath_dict):
    txMap={}
    print(filePath_zip)
    theZIP = zipfile.ZipFile(filePath_zip, 'r')
    for fileName in theZIP.namelist():
        if "Transaction" not in fileName:
            continue
        
        theCSV = theZIP.open(fileName);

        head = theCSV.readline().decode("utf-8").strip();
        oneLine = theCSV.readline().decode("utf-8").strip();
        i=0
        while (oneLine!=""):
            if i%1000000==0:
                print(i)
            i+=1
            oneArray = oneLine.split(",")
            transactionHash=oneArray[2]
            
            txMap[transactionHash]=1
                
            oneLine = theCSV.readline().decode("utf-8").strip();
            
        with open(outputPath_dict, "wb") as tf:
            pickle.dump(txMap,tf)    
            
            

if __name__ == '__main__':
    getTxs_fromDefi()
    getTxs_fromNFT()