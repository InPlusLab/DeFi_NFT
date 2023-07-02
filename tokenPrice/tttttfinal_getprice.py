import time
import zipfile

fileDir = "/mnt/sde1/peilin_defi/defi_1650w/"

reservesOf = {}
datesOf = {}
tokensOf = {}
txcountOf = {}
userOf = {}
datesAll = [""]

# UniswapV2
file = "UniswapV2"
theZIP = zipfile.ZipFile(fileDir+file+".zip", 'r')

# UniswapV2_PairInfo
theCSV = theZIP.open("UniswapV2_PairInfo.csv")    
head = theCSV.readline()    
oneLine = theCSV.readline().decode("utf-8").strip()
while (oneLine!=""):
    oneArray = oneLine.split(",")    
    pairAddress                = oneArray[0]
    tokenAddress0            = oneArray[1]
    tokenAddress1            = oneArray[2]

    reservesOf[pairAddress] = []
    datesOf[pairAddress] = [""]
    tokensOf[pairAddress]    = [tokenAddress0, tokenAddress1]
    txcountOf[pairAddress] = 0
    userOf[pairAddress] = set()

    oneLine = theCSV.readline().decode("utf-8").strip()
theCSV.close()

print("finish pair")


theCSV = theZIP.open("UniswapV2_Transaction.csv")    

head = theCSV.readline()
oneLine = theCSV.readline().decode("utf-8").strip()

while (oneLine!=""):
    oneArray = oneLine.split(",")
    oneLine = theCSV.readline().decode("utf-8").strip()
    blockNumber            = int(oneArray[0])
    timestamp            = int(oneArray[1])
    pairAddress            = oneArray[3]
    fromAddress                        = oneArray[5]
    toAddress                        = oneArray[6]
    reserve0            = int(oneArray[11])
    reserve1            = int(oneArray[12])

    t = time.strftime("%Y-%m-%d", time.gmtime(timestamp))
    dateTemp = time.strptime(t, "%Y-%m-%d")

    if datesAll[-1] != dateTemp:
        datesAll.append(dateTemp)
        print(blockNumber, theCSV.tell(), t, flush=True)
    if datesOf[pairAddress][-1] != dateTemp:
        datesOf[pairAddress].append(dateTemp)
        reservesOf[pairAddress].append([])

    reservesOf[pairAddress][-1] = [reserve0, reserve1]
    txcountOf[pairAddress] += 1
    # userOf[pairAddress].add(fromAddress)
    # userOf[pairAddress].add(toAddress)

theCSV.close()

datesAll = datesAll[1:]
for i in datesOf:
    datesOf[i] = datesOf[i][1:]
print("finish read")

def getReserve(dateTarget, pairAddress):
    dates = datesOf[pairAddress]
    for i in range(len(dates)-1, -1 ,-1):
        if dates[i] <= dateTarget:
            return reservesOf[pairAddress][i]
    return None


U_ADDR = "0xdac17f958d2ee523a2206206994597c13d831ec7"
MIN_U = 1000*1000000
MIN_TX = 1000
MIN_USER = 1000

date2token2price = {}

for dateOne in datesAll:
    date2token2price[dateOne] = {}
    remain = set()
    dateStr = time.strftime("%Y-%m-%d", dateOne )

    tvlOf = {}
    # one-hop
    for pairAddress in tokensOf:
        if txcountOf[pairAddress] < MIN_TX:
            continue
        token0, token1 = tokensOf[pairAddress]
        if token0 != U_ADDR and token1 != U_ADDR:
            remain.add(pairAddress)
            continue

        reserves = getReserve(dateOne, pairAddress)
        if reserves==None:
            continue
        
        # target = "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9"
        # if token0 == target or token1 == target:
        #     print(dateStr, "1hop", token0, reserves[0], token1, reserves[1])

        if token0 == U_ADDR:
            if reserves[0] > MIN_U:
                date2token2price[dateOne][token1] = reserves[0]/reserves[1]
        elif token1 == U_ADDR:
            if reserves[1] > MIN_U:
                date2token2price[dateOne][token0] = reserves[1]/reserves[0]
    
    # MLGB
    # continue
    
    # two-hop
    for pairAddress in remain:
        if txcountOf[pairAddress] < MIN_TX:
            continue
        token0, token1 = tokensOf[pairAddress]
        if token0 not in date2token2price[dateOne] and token1 not in date2token2price[dateOne]:
            continue

        reserves = getReserve(dateOne, pairAddress)
        if reserves==None:
            continue

        # target = "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9"
        # if token0 == target or token1 == target:
        #     print(dateStr, "2hop", token0, reserves[0], token1, reserves[1])

        if token0 in date2token2price[dateOne] and token1 in date2token2price[dateOne]:
            pass
        elif token0 in date2token2price[dateOne]:
            if reserves[0]*date2token2price[dateOne][token0] > MIN_U:
                date2token2price[dateOne][token1] = reserves[0]*date2token2price[dateOne][token0]/reserves[1]
        elif token1 in date2token2price[dateOne]:
            if reserves[1]*date2token2price[dateOne][token1] > MIN_U:
                date2token2price[dateOne][token0] = reserves[1]*date2token2price[dateOne][token1]/reserves[0]
    


    print(dateStr, "prices", len(date2token2price[dateOne]))

theZIP.close()

import pickle
with open("/mnt/sde1/peilin_defi/data/tokenPrice/dict/date2token2price_v2.pkl", "wb") as f:
    pickle.dump(date2token2price, f)

print("finish")