import numpy as np
import csv    #加载csv包便于读取csv文件
import zipfile
import os
import json
import pickle



#··········································································
# get all txs from defi data
def getTxs_fromDefi():
    dir= "/mnt/sde1/peilin_defi/defi_1650w/"
    outputPath_dict="/mnt/sde1/peilin_defi/data/acrossXblock/txTypeMap/tx_defi.map"
    txMap={}
    for fileName in os.listdir(dir):
        filePath=dir+fileName
        if ".zip" not in filePath:
            continue
        marketType=fileName.split(".")[0]
        getTxs_fromZip(txMap,filePath,outputPath_dict,marketType)
        
        
def getTxs_fromZip(txMap,filePath_zip,outputPath_dict,marketType):
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
            
            if transactionHash not in txMap:
                txMap_value=[]
            txMap_value.append(marketType)
            txMap[transactionHash]=txMap_value
                
            oneLine = theCSV.readline().decode("utf-8").strip();
            
            
        for key,value in txMap.items():
            txMap[key]=list(set(value))
            
        with open(outputPath_dict, "wb") as tf:
            pickle.dump(txMap,tf)    

            
#··········································································
# get all txs data from xblock data
def getTxsData_fromXblock():
    inputPath_dict="/mnt/sde1/peilin_defi/data/acrossXblock/txTypeMap/tx_defi.map"
    txMap={}
    with open(inputPath_dict, "rb") as tf:
        txMap=pickle.load(tf)

    dir= "/mnt/sda1/bowei/sbw/xblock/normalTransaction/"
    
    for fileName in os.listdir(dir):
        filePath=dir+fileName
        if "BlockTransaction.zip" not in filePath:
            continue
        
        outputPath_csv="/mnt/sde1/peilin_defi/data/acrossXblock/normalTransaction/defi/"+fileName.split(".")[0]+".csv"
        
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
        
        newHead=head.split(",")
        newHead.append("marketType")
        writerNew.writerow(newHead)
        
        i=0
        while (oneLine!=""):
            if i%1000000==0:
                print(i)
            i+=1
            oneArray = oneLine.split(",")
            transactionHash=oneArray[2]
            
            try:
                txMap_value=txMap[transactionHash]
                tempList=list(set(txMap_value))
                typeString='_'.join(tempList)
                oneArray.append(typeString)
                writerNew.writerow(oneArray)
            except:
                pass

            oneLine = theCSV.readline().decode("utf-8").strip();
        theZIP.close()
    
        
def main():
    getTxs_fromDefi()
    getTxsData_fromXblock()



if __name__ == '__main__':
    main()