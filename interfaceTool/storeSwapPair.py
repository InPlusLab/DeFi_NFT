import zipfile
import pickle

def UniswapV1():
    pairMap={}
    
    theZIP = zipfile.ZipFile("/mnt/sde1/peilin_defi/defi_1650w/UniswapV1.zip", 'r')
    theCSV = theZIP.open("UniswapV1_ExchangeInfo.csv")	
    head = theCSV.readline()	

    oneLine = theCSV.readline().decode("utf-8").strip()
    i=0
    while (oneLine!=""):
        if i%10000==0:
            print(i)
        i+=1
        oneArray = oneLine.split(",")	

        # address,name,symbol,totalSupply,decimal
        exchangeAddress	 = oneArray[0]	
        tokenAddress = oneArray[1]	

        pairMap[exchangeAddress]=tokenAddress
        
        oneLine = theCSV.readline().decode("utf-8").strip()	
    
    theCSV.close()
    theZIP.close()
    
    with open("/mnt/sde1/peilin_defi/data/interfaceTool/dict/UniswapV1_ExchangeInfo.map", "wb") as tf:
        pickle.dump(pairMap,tf) 
        
        
def UniswapV2():
    pairMap={}
    
    theZIP = zipfile.ZipFile("/mnt/sde1/peilin_defi/defi_1650w/UniswapV2.zip", 'r')
    theCSV = theZIP.open("UniswapV2_PairInfo.csv")	
    head = theCSV.readline()	

    oneLine = theCSV.readline().decode("utf-8").strip()
    i=0
    while (oneLine!=""):
        if i%10000==0:
            print(i)
        i+=1
        oneArray = oneLine.split(",")	

        # address,name,symbol,totalSupply,decimal
        pairAddress	 = oneArray[0]	
        tokenAddress0 = oneArray[1]
        tokenAddress1 = oneArray[2]	

        pairMap[pairAddress]={"tokenAddress0":tokenAddress0,"tokenAddress1":tokenAddress1}
        
        oneLine = theCSV.readline().decode("utf-8").strip()	
    
    theCSV.close()
    theZIP.close()
    
    with open("/mnt/sde1/peilin_defi/data/interfaceTool/dict/UniswapV2_PairInfo.map", "wb") as tf:
        pickle.dump(pairMap,tf) 
        
        
def UniswapV3():
    pairMap={}
    
    theZIP = zipfile.ZipFile("/mnt/sde1/peilin_defi/defi_1650w/UniswapV3.zip", 'r')
    theCSV = theZIP.open("UniswapV3_PoolInfo.csv")	
    head = theCSV.readline()	

    oneLine = theCSV.readline().decode("utf-8").strip()
    i=0
    while (oneLine!=""):
        if i%10000==0:
            print(i)
        i+=1
        oneArray = oneLine.split(",")	

        # address,name,symbol,totalSupply,decimal
        poolAddress	 = oneArray[0]	
        tokenAddress0 = oneArray[1]
        tokenAddress1 = oneArray[2]	

        pairMap[poolAddress]={"tokenAddress0":tokenAddress0,"tokenAddress1":tokenAddress1}
        
        oneLine = theCSV.readline().decode("utf-8").strip()	
    
    theCSV.close()
    theZIP.close()
    
    with open("/mnt/sde1/peilin_defi/data/interfaceTool/dict/UniswapV3_PoolInfo.map", "wb") as tf:
        pickle.dump(pairMap,tf) 

def ShibaSwap():
    pairMap={}
    
    theZIP = zipfile.ZipFile("/mnt/sde1/peilin_defi/defi_1650w/ShibaSwap.zip", 'r')
    theCSV = theZIP.open("ShibaSwap_PairInfo.csv")	
    head = theCSV.readline()	

    oneLine = theCSV.readline().decode("utf-8").strip()
    i=0
    while (oneLine!=""):
        if i%10000==0:
            print(i)
        i+=1
        oneArray = oneLine.split(",")	

        # address,name,symbol,totalSupply,decimal
        poolAddress	 = oneArray[0]	
        tokenAddress0 = oneArray[1]
        tokenAddress1 = oneArray[2]	

        pairMap[poolAddress]={"tokenAddress0":tokenAddress0,"tokenAddress1":tokenAddress1}
        
        oneLine = theCSV.readline().decode("utf-8").strip()	
    
    theCSV.close()
    theZIP.close()
    
    with open("/mnt/sde1/peilin_defi/data/interfaceTool/dict/ShibaSwap_PairInfo.map", "wb") as tf:
        pickle.dump(pairMap,tf) 
        
        
def SushiSwap():
    pairMap={}
    
    theZIP = zipfile.ZipFile("/mnt/sde1/peilin_defi/defi_1650w/SushiSwap.zip", 'r')
    theCSV = theZIP.open("SushiSwap_PairInfo.csv")	
    head = theCSV.readline()	

    oneLine = theCSV.readline().decode("utf-8").strip()
    i=0
    while (oneLine!=""):
        if i%10000==0:
            print(i)
        i+=1
        oneArray = oneLine.split(",")	

        # address,name,symbol,totalSupply,decimal
        poolAddress	 = oneArray[0]	
        tokenAddress0 = oneArray[1]
        tokenAddress1 = oneArray[2]	

        pairMap[poolAddress]={"tokenAddress0":tokenAddress0,"tokenAddress1":tokenAddress1}
        
        oneLine = theCSV.readline().decode("utf-8").strip()	
    
    theCSV.close()
    theZIP.close()
    
    with open("/mnt/sde1/peilin_defi/data/interfaceTool/dict/SushiSwap_PairInfo.map", "wb") as tf:
        pickle.dump(pairMap,tf) 
        
        
if __name__ == '__main__':
    # UniswapV1()
    # UniswapV2()
    # UniswapV3()
    ShibaSwap()
    SushiSwap()