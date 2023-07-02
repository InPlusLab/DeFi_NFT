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

def csvToMap(inputPath_csv,outputPath_dict):
    
    timeMap={}
    
    with open(inputPath_csv,'r', encoding="UTF8") as f:
        reader = csv.reader(f)
        header_row=next(reader)
        
        i=0
        for row in reader:
            if i%10000==0:
                print(i)
            i+=1
            
            timestamp_removeHour=int(row[1])
            tradeVolume=float(row[3])
            
            try:
                timeMap[timestamp_removeHour]+=tradeVolume
            except:
                timeMap[timestamp_removeHour]=tradeVolume
                
    with open(outputPath_dict, "wb") as tf:
        pickle.dump(timeMap,tf) 
    

def getVolumeMap():
    input_dir="/mnt/sde1/peilin_defi/data/volume_line/defi/"
    output_dir="/mnt/sde1/peilin_defi/data/volume_line/dict/timeWithVolume/"
    for fileName in os.listdir(input_dir):
        defiType=fileName.split(".")[0].split("_")[0]
        inputPath_csv=input_dir+fileName
        if "UniswapV1_Transaction.csv" not in fileName:
            continue
        # if "UniswapV1_Transaction.csv" in fileName:
        #     continue
        
        outputPath_dict=output_dir+defiType+"_test.map"
        csvToMap(inputPath_csv,outputPath_dict)
        
        
if __name__ == '__main__':
    getVolumeMap()