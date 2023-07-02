import time
import requests
import os

def download(localFile):
	srcUrl = "https://zhengpeilin.com/download.php?file="+localFile
	if os.path.exists(localFile+".temp"):
		os.remove(localFile+".temp")
	if os.path.exists(localFile):
		print(localFile+"exist!\n")
		return
	print("------------------------------------------------------------")
	print('Downloading %s' % localFile, end='\r')
	try:
		with requests.get(srcUrl, stream=True) as r:
			if r.status_code != 200:
				print("retrying", srcUrl, r.status_code)
				time.sleep(10)
				return download(localFile)
			contentLength = int(r.headers['content-length'])
			print('Downloading %s %.2f MB' % (localFile, contentLength/1024/1024))
			downSize = 0
			startTime = time.time()
			with open(localFile+".temp", 'wb') as f:
				for chunk in r.iter_content(8192):
					if chunk:
						f.write(chunk)
					downSize += len(chunk)
					line = '%.1f%% - %.2f MB/s - %.2f MB          '
					line = line % (downSize/contentLength*100, downSize/1024/1024/(time.time()-startTime), downSize/1024/1024)
					print(line, end='\r')
					if downSize >= contentLength:
						break
			os.rename(localFile+".temp", localFile)
			print()
	except:
		print("exception wait 180s\n")
		time.sleep(180)
		print("retry\n")
		return download(localFile)


DeFi_Exchange_Files = ["UniswapV1.zip", "UniswapV2.zip", "UniswapV3.zip", "SushiSwap.zip",
                       "BalancerV1.zip", "BalancerV2.zip", "Curve.zip", "ShibaSwap.zip"]

DeFi_Lending_Files = ["AaveV1.zip", "AaveV2.zip", "Cream.zip", "Compound.zip", "MakerDAO.zip"]

NFT_Exchange_Files = ["OpenSea.zip", "X2Y2.zip", "LooksRare.zip", "Blur.zip"]

NFT_Lending_Files = ["NFTfiV1.zip" ,"NFTfiV2.zip", "Arcade.zip" ,"BendDAO.zip"]

All = [DeFi_Exchange_Files, DeFi_Lending_Files, NFT_Exchange_Files, NFT_Lending_Files]

def start():
	print("Select the datasets to download:")
	print("0. All")
	print("1. DeFi-Exchange")
	print("2. DeFi-Lending")
	print("3. NFT-Exchange")
	print("4. NFT-Lending")
	select = input("Please input a number (0~4): ")

	if select == "0":
		for Files in All:
			for localFile in Files:
				download(localFile)
	elif select == "1":
		for localFile in DeFi_Exchange_Files:
			download(localFile)
	elif select == "2":
		for localFile in DeFi_Lending_Files:
			download(localFile)
	elif select == "3":
		for localFile in NFT_Exchange_Files:
			download(localFile)
	elif select == "4":
		for localFile in NFT_Lending_Files:
			download(localFile)
	else:
		return start()

start()
print("finish")
