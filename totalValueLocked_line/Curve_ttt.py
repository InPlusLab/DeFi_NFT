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

import sys


import sys
sys.path.append("/mnt/sde1/peilin_defi/code/interfaceTool")
from readTotal2 import *


timeMap={}
reserves={}

input_csv="/mnt/sde1/peilin_defi/defi_1650w/Curve_Transaction.csv"

last_timestamp_removeHour = 0


def getTVLtoTimeMap(tempTimestamp):
    tvl = 0
    for tokenAddress in reserves:
        # if float(reserves[tokenAddress])<0:
        #     continue
        
        tvl += TOKEN_PRICE_CENTER.swap(tokenAddress, tempTimestamp,float(reserves[tokenAddress]))

    timeMap[tempTimestamp] = tvl
    print(timestamp_2_date(tempTimestamp), tvl, flush=True)

type2tx = {}

with open(input_csv,'r', encoding="UTF8") as f:
    reader = csv.reader(f)
    header_row=next(reader)
    
    i=0
    for row in reader:
        if i%10000==0:
            print(i)
        i+=1
        
        
        txHash=row[2]
        type=row[4]
        if type not in type2tx:
            type2tx[type] = set()
        type2tx[type].add(txHash)

all_tx = set()
dup_tx = set()
for ori_type in type2tx:
    for tx in type2tx[ori_type]:
        all_tx.add(tx)
        for new_type in type2tx:
            if tx in type2tx[new_type] and new_type != ori_type:
                dup_tx.add(tx)
                break

print(len(all_tx))
print(len(dup_tx))

