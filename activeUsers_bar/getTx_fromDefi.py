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
    outputPath_dict="/mnt/sde1/peilin_defi/data/dict/tx_defi.map"
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
    inputPath_dict="/mnt/sde1/peilin_defi/data/dict/tx_defi.map"
    txMap={}
    with open(inputPath_dict, "rb") as tf:
        txMap=pickle.load(tf)

    dir= "/mnt/sda1/bowei/sbw/xblock/normalTransaction/"
    
    for fileName in os.listdir(dir):
        filePath=dir+fileName
        if "BlockTransaction.zip" not in filePath:
            continue
        
        outputPath_csv="/mnt/sde1/peilin_defi/data/csv/defi/normalTransaction/"+fileName.split(".")[0]+".csv"
        
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
    dir="/mnt/sde1/peilin_defi/data/csv/defi/normalTransaction/"
    outputPath_dict="/mnt/sde1/peilin_defi/data/dict/txFrom_defi.map"
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
    
    inputPath_dict="/mnt/sde1/peilin_defi/data/dict/txFrom_defi.map"
    txMap={}
    with open(inputPath_dict, "rb") as tf:
        txMap=pickle.load(tf)
        
    outputPath_csv="/mnt/sde1/peilin_defi/data/csv/defi/fromData.csv"
    dir= "/mnt/sde1/peilin_defi/defi_1650w/"
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
            fromMap_value={"AaveV1":0,"AaveV2":0,"BalancerV1":0,"BalancerV2":0,"Compound":0,"Cream":0,"Curve":0,"MakerDAO":0,"ShibaSwap":0,"SushiSwap":0,"UniswapV1":0,"UniswapV2":0,"UniswapV3":0}
            
        fromMap_value[defiType]+=1
        fromMap[fromAddr]=fromMap_value
        
            
        oneLine = theCSV.readline().decode("utf-8").strip(); 
            
    ouputCsv(fromMap,outputPath_csv)

def ouputCsv(fromMap,outputPath_csv):
    f = open(outputPath_csv,'w')
    writer = csv.writer(f)
    writer.writerow(["from","AaveV1","AaveV2","BalancerV1","BalancerV2","Compound","Cream","Curve","MakerDAO","ShibaSwap","SushiSwap","UniswapV1","UniswapV2","UniswapV3"])

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
          
        writer.writerow(row)
        
        
#··········································································
def delFromData():
    fnew = open("/mnt/sde1/peilin_defi/data/csv/defi/fromData_plus.csv",'w')
    writer = csv.writer(fnew)
    
    with open("/mnt/sde1/peilin_defi/data/csv/defi/fromData.csv",'r', encoding="UTF8") as f:
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
            if int(row[9])!=0:
                marketCount+=1
            if int(row[10])!=0:
                marketCount+=1
            if int(row[11])!=0:
                marketCount+=1
            if int(row[12])!=0:
                marketCount+=1
            if int(row[13])!=0:
                marketCount+=1
                
            row.append(marketCount)
            writer.writerow(row)
                
    
        
def main():
    # getTxs_fromDefi()
    getTxsData_fromXblock()

    getTxsFrom()
    
    calFromData()
    
    delFromData()



if __name__ == '__main__':
    main()