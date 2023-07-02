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

tokenInfoMap={}

theZIP = zipfile.ZipFile("/mnt/sde1/peilin_defi/tokenInfo/ERC20TokenInfo.zip", 'r')
theCSV = theZIP.open("ERC20TokenInfo.csv")	
head = theCSV.readline()	

oneLine = theCSV.readline().decode("utf-8").strip()
i=0
while (oneLine!=""):
    if i%10000==0:
        print(i)
    i+=1
    
    oneArray = oneLine.split(",")	
    address	 = oneArray[0]	
    name		= oneArray[1]	
    symbol	  = oneArray[2]	

    totalSupply = oneArray[3]
    decimal	 = oneArray[4]
    
    if decimal=="None":
        oneLine = theCSV.readline().decode("utf-8").strip()	
        continue
        

    tokenInfoMap[address]={"name":name,"symbol":symbol,"totalSupply":totalSupply,"decimal":int(decimal)}
    oneLine = theCSV.readline().decode("utf-8").strip()	
    

theCSV.close()
theZIP.close()


with open("/mnt/sde1/peilin_defi/data/interfaceTool/dict/ERC20TokenInfo.map", "wb") as tf:
    pickle.dump(tokenInfoMap,tf) 