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


def LooksRare():
    nftMap={}
                
    theZIP = zipfile.ZipFile("/mnt/sde1/peilin_defi/nft_1650w/LooksRare.zip", 'r')
    theCSV = theZIP.open("LooksRare_Transaction.csv")	
    head = theCSV.readline()
    oneLine = theCSV.readline().decode("utf-8").strip()
    i=0
    while (oneLine!=""):
        if i%10000==0:
            print(i, flush=True)
        i+=1
        row = oneLine.split(",")
        blockNumber=int(row[0])
        timestamp=int(row[1])
        timeString = time.strftime("%Y-%m-%d", time.gmtime(timestamp))
        timestamp_removeHour=int(timestamp_removeHour_reduce(timestamp))
        collection=row[10]
        tokenId=row[11]
        price=None2Float(row[13])/1e18
        
        if collection not in nftMap:
            nftMap[collection]={}
        
        if tokenId not in nftMap[collection]:
            nftMap[collection][tokenId]={}
        
        nftMap[collection][tokenId][blockNumber]=price
                

        oneLine = theCSV.readline().decode("utf-8").strip()

    output_dict="/mnt/sde1/peilin_defi/data/nftPrice/dict/LooksRare.map"
    with open(output_dict, "wb") as tf:
        pickle.dump(nftMap,tf) 
            
if __name__ == '__main__':
    LooksRare()