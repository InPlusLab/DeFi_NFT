import numpy as np
import csv    #加载csv包便于读取csv文件
import zipfile
import os
import json
import pickle


def getTxsFrom():
    dir="/mnt/sde1/peilin_defi/data/acrossXblock/normalTransaction/defiNft/"
    outputPath_csv="/mnt/sde1/peilin_defi/data/activeUsers_bar/csv/fromData.csv"
    fromMap={}
    for fileName in os.listdir(dir):
        filePath=dir+fileName
        print(fileName)
        
        getTxsFrom_sub(fromMap,filePath,outputPath_csv)
        
        
def getTxsFrom_sub(fromMap,filePath,outputPath_csv):
    totalMarketTypeList=["AaveV1","AaveV2","BalancerV1","BalancerV2","Compound","Cream","Curve","MakerDAO","ShibaSwap","SushiSwap","UniswapV1","UniswapV2","UniswapV3","Arcade","BendDAO","Blur","LooksRare","NFTfiV1","NFTfiV2","OpenSea","X2Y2"]
    with open(filePath,'r', encoding="UTF8") as f:
        reader = csv.reader(f)
        header_row=next(reader)
        i=0
        for row in reader:
            if i%10000==0:
                print(i)
            i+=1
            transactionHash=row[2]
            fromAddr=row[3]
            
            try:
                fromMap_value=fromMap[fromAddr]
            except:
                fromMap_value={"AaveV1":0,"AaveV2":0,"BalancerV1":0,"BalancerV2":0,"Compound":0,"Cream":0,"Curve":0,"MakerDAO":0,"ShibaSwap":0,"SushiSwap":0,"UniswapV1":0,"UniswapV2":0,"UniswapV3":0,"Arcade":0,"BendDAO":0,"Blur":0,"LooksRare":0,"NFTfiV1":0,"NFTfiV2":0,"OpenSea":0,"X2Y2":0,"marketCount":0}
                  
            marketTypeList=row[-1].split("_")
            for marketType in marketTypeList:
                fromMap_value[marketType]+=1
                
            marketCount=0
            for marketType in totalMarketTypeList:
                if fromMap_value[marketType]>0:
                    marketCount+=1
                    
            fromMap_value["marketCount"]=marketCount
            fromMap[fromAddr]=fromMap_value
                
    ouputCsv(fromMap,outputPath_csv)
                

def ouputCsv(fromMap,outputPath_csv):
    f = open(outputPath_csv,'w')
    writer = csv.writer(f)
    writer.writerow(["from","AaveV1","AaveV2","BalancerV1","BalancerV2","Compound","Cream","Curve","MakerDAO","ShibaSwap","SushiSwap","UniswapV1","UniswapV2","UniswapV3","Arcade","BendDAO","Blur","LooksRare","NFTfiV1","NFTfiV2","OpenSea","X2Y2","marketCount"])

    for key,value in fromMap.items():
        row=[key]
        row.append(value["AaveV1"])
        row.append(value["AaveV2"])
        row.append(value["BalancerV1"])
        row.append(value["BalancerV2"])
        row.append(value["Compound"])
        row.append(value["Cream"])
        row.append(value["Curve"])
        row.append(value["MakerDAO"])
        row.append(value["ShibaSwap"])
        row.append(value["SushiSwap"])
        row.append(value["UniswapV1"])
        row.append(value["UniswapV2"])
        row.append(value["UniswapV3"])
        row.append(value["Arcade"])
        row.append(value["BendDAO"])
        row.append(value["Blur"])
        row.append(value["LooksRare"])
        row.append(value["NFTfiV1"])
        row.append(value["NFTfiV2"])
        row.append(value["OpenSea"])
        row.append(value["X2Y2"])
        row.append(value["marketCount"])

        writer.writerow(row)
                
                
if __name__ == '__main__':
    getTxsFrom()