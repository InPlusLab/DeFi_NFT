import numpy as np
import csv    #加载csv包便于读取csv文件
import zipfile
import os
import json
import pickle

def addFlashbotsToCsv(filePath,flashbotsTxMap,outputPath_csv):
    fNew = open(outputPath_csv,'w')
    writerNew = csv.writer(fNew)
    
    with open(filePath,'r', encoding="UTF8") as f:
        reader = csv.reader(f)
        header_row=next(reader)
        writerNew.writerow(header_row)
        
        i=0
        for row in reader:
            if i%10000==0:
                print(i)
            i+=1
            transactionHash=row[2]
            if transactionHash not in flashbotsTxMap:
                continue
            writerNew.writerow(row)
        

def addFlashbotsToDefi():
    flashbotsTxMap={}
    with open("/mnt/sde1/peilin_defi/data/flashbots/dict/flashbotsTxMap.data", "rb") as tf:
        flashbotsTxMap=pickle.load(tf)
    
    inputDir= "/mnt/sde1/peilin_defi/data/acrossXblock/normalTransaction/defi/"
    outputDir="/mnt/sde1/peilin_defi/data/MEVTransactions/normalTransaction/defi/"

    for fileName in os.listdir(inputDir):
        filePath=inputDir+fileName
        outputPath_csv=outputDir+fileName
        addFlashbotsToCsv(filePath,flashbotsTxMap,outputPath_csv)
        
        
def addFlashbotsToNft():
    flashbotsTxMap={}
    with open("/mnt/sde1/peilin_defi/data/flashbots/dict/flashbotsTxMap.data", "rb") as tf:
        flashbotsTxMap=pickle.load(tf)
    
    inputDir= "/mnt/sde1/peilin_defi/data/acrossXblock/normalTransaction/nft/"
    outputDir="/mnt/sde1/peilin_defi/data/MEVTransactions/normalTransaction/nft/"

    for fileName in os.listdir(inputDir):
        filePath=inputDir+fileName
        outputPath_csv=outputDir+fileName
        addFlashbotsToCsv(filePath,flashbotsTxMap,outputPath_csv)
        
        
def addFlashbotsToDefiNft():
    flashbotsTxMap={}
    with open("/mnt/sde1/peilin_defi/data/flashbots/dict/flashbotsTxMap.data", "rb") as tf:
        flashbotsTxMap=pickle.load(tf)
    
    inputDir= "/mnt/sde1/peilin_defi/data/acrossXblock/normalTransaction/defiNft/"
    outputDir="/mnt/sde1/peilin_defi/data/MEVTransactions/normalTransaction/defiNft/"

    for fileName in os.listdir(inputDir):
        filePath=inputDir+fileName
        outputPath_csv=outputDir+fileName
        addFlashbotsToCsv(filePath,flashbotsTxMap,outputPath_csv)
        
        
if __name__ == '__main__':
    # addFlashbotsToDefi()
    # addFlashbotsToNft()
    
    addFlashbotsToDefiNft()