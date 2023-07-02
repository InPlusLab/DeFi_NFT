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
csv.field_size_limit(500 * 1024 * 1024)

class PriceItem():
    def __init__(self,timestamp_removeHour,usdExchangeRate):
        self.timestamp_removeHour=timestamp_removeHour
        self.usdExchangeRate=usdExchangeRate
        
class NormalTokenPrice():    
    def __init__(self,priceMap,priceList,tokenAddress):
        self.tokenAddress=tokenAddress
        # (timestamp_removeHour,exchangeRate)
        self.priceList=priceList
        self.priceMap=priceMap
        
    # def getKey(self,x):
    #     return int(x.timestamp_removeHour)

    # def mapToList(self):
    #     for timestamp_removeHour, usdExchangeRate in self.priceMap.items():
    #         priceItem=PriceItem(timestamp_removeHour,usdExchangeRate)
    #         self.priceItemList.append(priceItem)
    #         self.priceItemList.sort(key=self.getKey)
                
    def search(self,targetTime):
        # maxTime=0
        # for item in self.priceList:
        #     if maxTime<item[0]:
        #         maxTime=item[0]
        
        # if int(self.priceList[0][0])>int(targetTime):
            # print("curTime",targetTime)
            # print("return 0")
            # print("!!!")
        #     return 0
        
        # lastItem=None
        # for item in self.priceList:
        #     if item[0]==targetTime:
        #         return item[1]
            
        #     if lastItem!=None and int(lastItem[0])<int(targetTime) and int(item[0])>int(targetTime):
        #         return  lastItem[1]
            
        #     lastItem=item
            
        for i in range(len(self.priceList)-1,-1,-1):
            curItem=self.priceList[i]
            if curItem[0]<=targetTime:
                return curItem[1]
            
        return 0
            
        return self.priceList[len(self.priceList)-1][1]
            
    def swap(self,timestamp_removeHour,inputAmount):
        try:
            # print("tempPrice",self.priceMap[timestamp_removeHour])
            outputAmount=float(inputAmount)* self.priceMap[timestamp_removeHour]
        except:
            # print("tempPrice",self.search(timestamp_removeHour))
            outputAmount=float(inputAmount) * self.search(timestamp_removeHour)
        
        # print("outputAmount",outputAmount)
        return outputAmount
    

class UsdTokenPrice():
    # usdTokenList=['0x6b175474e89094c44da98b954eedeac495271d0f','0xdac17f958d2ee523a2206206994597c13d831ec7',
    #                 '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48']
    decimalMap={"0x6b175474e89094c44da98b954eedeac495271d0f":18,"0xdac17f958d2ee523a2206206994597c13d831ec7":6,
                "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48":6}

    def __init__(self,tokenAddress):
        self.tokenAddress=tokenAddress
    
    def swap(self,timestamp_removeHour,inputAmount):
        return float(inputAmount)/pow(10,self.decimalMap[self.tokenAddress])
    

class BinanceTokenPrice():
    decimalMap={"0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2":18,"0x2260fac5e5542a773aa44fbcfedf7c193bc2c599":8}
    
    def __init__(self,path,tokenAddress):
        self.priceMap={}
        self.tokenAddress=tokenAddress
        self.readCsv(path)
            
    def readCsv(self,path):
        with open(path,'r', encoding="UTF8") as f:
            reader = csv.reader(f)
            header_row=next(reader)
            
            for row in reader:                
                timestamp=int(row[0])
                closeExchangeRate=float(row[1])
                
                self.priceMap[timestamp]=closeExchangeRate
    
    def swap(self,timestamp_removeHour,inputAmount):
        outputAmount=float(inputAmount)* self.priceMap[timestamp_removeHour] / pow(10,self.decimalMap[self.tokenAddress])
        
        return outputAmount
    
    
def readBinanceTokenPrice():
    wethAddress="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    wbtcAddress="0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"

    ethPath="/mnt/sde1/peilin_defi/data/tokenPrice/binance/ethusdt_edi/ethUsdt_exchangeRate.csv"
    btcPath="/mnt/sde1/peilin_defi/data/tokenPrice/binance/btcusdt_edi/btcUsdt_exchangeRate.csv"
    
    binanceTokenPriceMap={}
    binanceTokenPriceMap[wbtcAddress]=BinanceTokenPrice(btcPath,wbtcAddress)
    binanceTokenPriceMap[wethAddress]=BinanceTokenPrice(ethPath,wethAddress)
    
    
    return binanceTokenPriceMap


def readRivetTokenPrice():
    tokenPriceMap={}
    
    # weth and wbtc
    wethAddress="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    wbtcAddress="0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"

    ethPath="/mnt/sde1/peilin_defi/data/tokenPrice/binance/ethusdt_edi/ethUsdt_exchangeRate.csv"
    btcPath="/mnt/sde1/peilin_defi/data/tokenPrice/binance/btcusdt_edi/btcUsdt_exchangeRate.csv"
    
    tokenPriceMap[wethAddress]=BinanceTokenPrice(ethPath,wethAddress)
    
    tokenPriceMap[wbtcAddress]=BinanceTokenPrice(btcPath,wbtcAddress)


    # usd token
    usdTokenList=['0x6b175474e89094c44da98b954eedeac495271d0f','0xdac17f958d2ee523a2206206994597c13d831ec7',
                    '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48']
    
    for tokenAddress in usdTokenList:
        tokenPriceMap[tokenAddress]=UsdTokenPrice(tokenAddress)
        
    return tokenPriceMap
    
    

def readTokenPrice():
    tokenPriceMap={}
    tempPriceMap={}
    with open("/mnt/sde1/peilin_defi/data/tokenPrice/dict/normalTokenPrice_plus.map", "rb") as tf:
        tempPriceMap=pickle.load(tf) 
    for tokenAddress,value in tempPriceMap.items():
        tokenPrice=NormalTokenPrice(value["map"],value["list"],tokenAddress)
        tokenPriceMap[tokenAddress]=tokenPrice
    
    # weth and wbtc
    wethAddress="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    wbtcAddress="0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"

    ethPath="/mnt/sde1/peilin_defi/data/tokenPrice/binance/ethusdt_edi/ethUsdt_exchangeRate.csv"
    btcPath="/mnt/sde1/peilin_defi/data/tokenPrice/binance/btcusdt_edi/btcUsdt_exchangeRate.csv"
    
    tokenPriceMap[wethAddress]=BinanceTokenPrice(ethPath,wethAddress)
    tokenPriceMap[wbtcAddress]=BinanceTokenPrice(btcPath,wbtcAddress)
    
    # usd token
    usdTokenList=['0x6b175474e89094c44da98b954eedeac495271d0f','0xdac17f958d2ee523a2206206994597c13d831ec7',
                    '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48']
    
    for tokenAddress in usdTokenList:
        tokenPriceMap[tokenAddress]=UsdTokenPrice(tokenAddress)
        
    return tokenPriceMap
    
    
    
# ·············································································
def date_2_timestamp(year,mon,day):
    tempString=str(year)+"-"+str(mon)+"-"+str(day)
    tempTime=time.strptime(tempString, "%Y-%m-%d")
    return time.mktime(tempTime)

def timestamp_2_date(un_time):
    return datetime.datetime.fromtimestamp(un_time)


def timestamp_removeHour_reduce(temp_timestamp):
    temp_date=timestamp_2_date(int(temp_timestamp))
    temp_timestamp=date_2_timestamp(temp_date.year,temp_date.month,temp_date.day)
    return int(temp_timestamp)

def timestamp_removeDay_reduce(temp_timestamp):
    temp_date=timestamp_2_date(int(temp_timestamp))
    temp_timestamp=date_2_timestamp(temp_date.year,temp_date.month,1)
    return int(temp_timestamp)
# ·············································································
def readERC20TokenInfo():
    with open("/mnt/sde1/peilin_defi/data/interfaceTool/dict/ERC20TokenInfo.map", "rb") as tf:
        tokenInfoMap=pickle.load(tf) 
        return tokenInfoMap


def readUniswapV1ExchangeInfo():
    with open("/mnt/sde1/peilin_defi/data/interfaceTool/dict/UniswapV1_ExchangeInfo.map", "rb") as tf:
        tempMap=pickle.load(tf)
        return tempMap

def readUniswapV2PairInfo():
    with open("/mnt/sde1/peilin_defi/data/interfaceTool/dict/UniswapV2_PairInfo.map", "rb") as tf:
        tempMap=pickle.load(tf) 
        return tempMap

def readUniswapV3PoolInfo():
    with open("/mnt/sde1/peilin_defi/data/interfaceTool/dict/UniswapV3_PoolInfo.map", "rb") as tf:
        tempMap=pickle.load(tf) 
        return tempMap
    
    
    
# WETH,WBTC,DAI,USDT,USDC
# rivetTokenList=['0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2','0x2260fac5e5542a773aa44fbcfedf7c193bc2c599',
#                 '0x6b175474e89094c44da98b954eedeac495271d0f','0xdac17f958d2ee523a2206206994597c13d831ec7',
#                 '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48']

# rivetTokenList_ethBtc=["0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2","0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"]
# rivetTokenList1_usd=['0x6b175474e89094c44da98b954eedeac495271d0f','0xdac17f958d2ee523a2206206994597c13d831ec7',
#                 '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48']

# wethAddress="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"



def None2Float(temp):
    if temp=="None":
        return 0.0
    else:
        return float(temp)
    
    