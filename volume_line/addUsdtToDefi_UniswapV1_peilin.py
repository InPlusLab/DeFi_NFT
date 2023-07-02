import time
import json
import zipfile

month2volume = {}
month2tvl = {}

# UniswapV1
file = "/mnt/sde1/peilin_defi/defi_1650w/UniswapV1"
theZIP = zipfile.ZipFile(file+".zip", 'r')

theCSV = theZIP.open("UniswapV1_Transaction.csv")	

head = theCSV.readline()
print(head)
# exit()
oneLine = theCSV.readline().decode("utf-8").strip()

i=0
while (oneLine!=""):
    if i%10000==0:
        print(i)
    i+=1
    oneArray = oneLine.split(",")
    # blockNumber,timestamp,transactionHash,exchangeAddress,type,user,ethAmount,tokenAmount,LPTokens
    timestamp			= int(oneArray[1])
    type = oneArray[4]
    ethAmount= int(oneArray[6])
    if type == "swapEthToToken" or type == "swapTokenToEth":
        month = time.strftime("%Y-%m", time.gmtime(timestamp))
        if month not in month2volume:
            month2volume[month] = 0
        month2volume[month] += ethAmount

    oneLine = theCSV.readline().decode("utf-8").strip()

print("total line: ",i)


for month in month2volume:
    print(month, month2volume[month]/1000000000000000000)