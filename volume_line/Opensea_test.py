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
sys.path.append("/mnt/sde1/peilin_defi/code/interfaceTool")
from readTotal2 import *


def OpenSea():    
    input_csv="/mnt/sde1/peilin_defi/nft_1650w/OpenSea_Transaction.csv"
    
    with open(input_csv,'r', encoding="UTF8") as f:
        reader = csv.reader(f)
        header_row=next(reader)
        
        i=0
        for row in reader:
            # if i%10000==0:
            #     print(i)
            i+=1            
            saleKind1=row[13]
            basePrice1=None2Float(row[18])
            saleKind2=row[32]
            basePrice2=None2Float(row[37])
            
            # if saleKind1=="DutchAuction" and basePrice1!=basePrice2:
            #     print(row)
            #     break
            if basePrice1<basePrice2:
                print(row)
                break

                        
            
if __name__ == '__main__':
    OpenSea()