import pandas as pd
import numpy as np
import datetime
import os

def cutDstoRange(dS,t1,t2):
    #t1 = '%s %s'%(date,t1)
    #t2 = '%s %s'%(date,t2)
    dS = dS[dS.index <= t2]
    dS = dS[dS.index >= t1]
    return dS

def selCdfTimes(dF,t1,t2):
    #t1 = '%s %s'%(date,t1)
    #t2 = '%s %s'%(date,t2)
    timeArr = dF.varget('epoch')
    startId = np.where(timeArr>=datetime.datetime.strptime(t1,'%Y-%m-%d %H:%M:%S'))[0][0]
    endId = np.where(timeArr<=datetime.datetime.strptime(t2,'%Y-%m-%d %H:%M:%S'))[0][-1] + 1
    return startId,endId

def getBasePath():
    with open('path','r') as fl:
        d = fl.readlines()
    basePath = d[0].strip()
    dataPath = d[1].strip()
    symPath = basePath + '/Data/sym' 
    if not os.path.exists(symPath):
        os.symlink(dataPath,symPath)
    return basePath