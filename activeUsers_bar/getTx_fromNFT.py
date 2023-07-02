import numpy as np
import csv    #加载csv包便于读取csv文件
import zipfile
import os
import json
import pickle



#··········································································
# get all txs from defi data
def getTxs_fromNFT():
    dir= "/mnt/sde1/peilin_defi/nft_1650w/"
    outputPath_dict="/mnt/sde1/peilin_defi/data/dict/tx_NFT.map"
    txMap={}
    for fileName in os.listdir(dir):
        filePath=dir+fileName
        if ".zip" not in filePath:
            continue
        
        getTxs_fromZip(txMap,filePath,outputPath_dict)
        
        
def getTxs_fromZip(txMap,filePath_zip,outputPath_dict):
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

            
#··········································································
# get all txs data from xblock data
def getTxsData_fromXblock():
    inputPath_dict="/mnt/sde1/peilin_defi/data/dict/tx_NFT.map"
    txMap={}
    with open(inputPath_dict, "rb") as tf:
        txMap=pickle.load(tf)

    dir= "/mnt/sda1/bowei/sbw/xblock/normalTransaction/"
    
    for fileName in os.listdir(dir):
        filePath=dir+fileName
        if "BlockTransaction.zip" not in filePath:
            continue
        
        outputPath_csv="/mnt/sde1/peilin_defi/data/csv/nft/normalTransaction/"+fileName.split(".")[0]+".csv"
        
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
    
        
#··········································································
def getTxsFrom():
    dir="/mnt/sde1/peilin_defi/data/csv/nft/normalTransaction/"
    outputPath_dict="/mnt/sde1/peilin_defi/data/dict/txFrom_nft.map"
    txMap={}
    for fileName in os.listdir(dir):
        filePath=dir+fileName
        print(fileName)
        
        getTxsFrom_sub(txMap,filePath,outputPath_dict)
        
        
def getTxsFrom_sub(txMap,filePath,outputPath_dict):
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
            txMap[transactionHash]=fromAddr
            
    with open(outputPath_dict, "wb") as tf:
        pickle.dump(txMap,tf)    
        
        
#··········································································
def calFromData():
    fromMap={}
    
    inputPath_dict="/mnt/sde1/peilin_defi/data/dict/txFrom_nft.map"
    txMap={}
    with open(inputPath_dict, "rb") as tf:
        txMap=pickle.load(tf)
        
    outputPath_csv="/mnt/sde1/peilin_defi/data/csv/nft/fromData.csv"
    dir= "/mnt/sde1/peilin_defi/nft_1650w/"
    for fileName in os.listdir(dir):
        defiType=fileName.split(".")[0]
        filePath=dir+fileName
        if ".zip" not in filePath:
            continue
        
        print(defiType)
        calFromData_sub(defiType,txMap,fromMap,filePath,outputPath_csv)
        
def calFromData_sub(defiType,txMap,fromMap,filePath_zip,outputPath_csv):
    theZIP = zipfile.ZipFile(filePath_zip, 'r')
    
    targetFileName=None
    for fileName in theZIP.namelist():
        if "Transaction" in fileName:
            targetFileName=fileName
            
        
    theCSV = theZIP.open(targetFileName);
    head = theCSV.readline().decode("utf-8").strip();
    oneLine = theCSV.readline().decode("utf-8").strip();
    i=0
    while (oneLine!=""):
        if i%1000000==0:
            print(i)
        i+=1
        oneArray = oneLine.split(",")
        transactionHash=oneArray[2]
        
        fromAddr=txMap[transactionHash]
        
        try:
            fromMap_value=fromMap[fromAddr]
        except:
            fromMap_value={"Arcade":0,"BendDAO":0,"Blur":0,"LooksRare":0,"NFTfiV1":0,"NFTfiV2":0,"OpenSea":0,"X2Y2":0}
            
        fromMap_value[defiType]+=1
        fromMap[fromAddr]=fromMap_value
        
            
        oneLine = theCSV.readline().decode("utf-8").strip(); 
            
    ouputCsv(fromMap,outputPath_csv)

def ouputCsv(fromMap,outputPath_csv):
    f = open(outputPath_csv,'w')
    writer = csv.writer(f)
    writer.writerow(["from","Arcade","BendDAO","Blur","LooksRare","NFTfiV1","NFTfiV2","OpenSea","X2Y2"])

    for key,value in fromMap.items():
        row=[key]
        row.append(value["Arcade"])
        row.append(value["BendDAO"])
        row.append(value["Blur"])
        row.append(value["LooksRare"])
        row.append(value["NFTfiV1"])
        row.append(value["NFTfiV2"])
        row.append(value["OpenSea"])
        row.append(value["X2Y2"])

        writer.writerow(row)
        
        
#··········································································
def delFromData():
    fnew = open("/mnt/sde1/peilin_defi/data/csv/nft/fromData_plus.csv",'w')
    writer = csv.writer(fnew)
    
    with open("/mnt/sde1/peilin_defi/data/csv/nft/fromData.csv",'r', encoding="UTF8") as f:
        reader = csv.reader(f)
        header_row=next(reader)
        
        header_row.append("marketCount")
        writer.writerow(header_row)
        
        i=0
        for row in reader:
            if i%10000==0:
                print(i)
            i+=1

            marketCount=0
            if int(row[1])!=0:
                marketCount+=1
            if int(row[2])!=0:
                marketCount+=1
            if int(row[3])!=0:
                marketCount+=1
            if int(row[4])!=0:
                marketCount+=1
            if int(row[5])!=0:
                marketCount+=1
            if int(row[6])!=0:
                marketCount+=1
            if int(row[7])!=0:
                marketCount+=1
            if int(row[8])!=0:
                marketCount+=1
                
            row.append(marketCount)
            writer.writerow(row)
                
    
        
def main():
    # getTxs_fromNFT()
    
    # getTxsData_fromXblock()

    # getTxsFrom()
    
    calFromData()
    
    delFromData()



if __name__ == '__main__':
    main()