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

# max: 16499999, min:6628280

blockLimit={"AaveV1":[9241323,16497431],"AaveV2":[11363052,16499995],"BalancerV1":[9665369,16499993],
            "BalancerV2":[12286258,16499998],"Compound":[7710833,16499977],"Cream":[10580902,15240089],
            "Curve":[9457457,16499999],"MakerDAO":[8928674,16499969],"ShibaSwap":[12771829,16499997],
            "SushiSwap":[10794352,16499999],"UniswapV1":[6628280,16499804],"UniswapV2":[10008555,16499999],
            "UniswapV3":[12369739,16499999],"Arcade":[15118144,16499561],"BendDAO":[14431264,16499924],
            "Blur":[15779873,16499999],"LooksRare":[13899842,16499997],"NFTfiV1":[10073259,16060420],
            "NFTfiV2":[14487024,16499914],"OpenSea":[14232877,15257106],"X2Y2":[14140203,16499998]}

def getTxsData_fromXblock():
    
    mapDir= "/mnt/sde1/peilin_defi/data/acrossXblock/dict/"
    for fileName in os.listdir(mapDir):
        inputPath_dict=mapDir+fileName
        marketType=fileName.split(".")[0]
        outputPath_csv="/mnt/sde1/peilin_defi/data/csv/defi/normalTransaction/"+marketType+".csv"
        
        txMap={}
        with open(inputPath_dict, "rb") as tf:
            txMap=pickle.load(tf)

        # cross with xblock normaltransaction
        normalTxDir= "/mnt/sda1/bowei/sbw/xblock/normalTransaction/"
        for fileName in os.listdir(normalTxDir):
            filePath=normalTxDir+fileName
            if "BlockTransaction.zip" not in filePath:
                continue
            
            getTxsData_fromZip(txMap,outputPath_csv,filePath)
        
        
def getTxsData_fromZip(txMap,outputPath_csv,filePath_zip):
    print("getNormalTransactionFromXblock")
    
    fNew = open(outputPath_csv,'w')
    writerNew = csv.writer(fNew)
    
    theZIP = zipfile.ZipFile(filePath_zip, 'r');
    for fileName in theZIP.namelist():
        if "csv" not in fileName:
            continue    

        theCSV = theZIP.open(fileName);

        head = theCSV.readline().decode("utf-8").strip();
        oneLine = theCSV.readline().decode("utf-8").strip();
        
        writerNew.writerow(head.split(","))
        
        i=0
        while (oneLine!=""):
            if i%1000000==0:
                print(i)
            i+=1
            oneArray = oneLine.split(",")
            transactionHash=oneArray[2]
            
            try:
                _=txMap[transactionHash]
                writerNew.writerow(oneArray)
            except:
                pass

            oneLine = theCSV.readline().decode("utf-8").strip();
        theZIP.close()
        
        
if __name__ == '__main__':
    getTxsData_fromXblock()
    