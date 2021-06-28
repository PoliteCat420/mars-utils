import pandas as pd
import numpy as np
import datetime

def cutDstoRange(dS,date,t1,t2):
    t1 = '%s %s'%(date,t1)
    t2 = '%s %s'%(date,t2)
    dS = dS[dS.index <= t2]
    dS = dS[dS.index >= t1]
    return dS

def selCdfTimes(dF,date,t1,t2):
    t1 = '%s %s'%(date,t1)
    t2 = '%s %s'%(date,t2)
    timeArr = dF['epoch'][:]
    startId = np.where(timeArr>=datetime.datetime.strptime(t1,'%Y-%m-%d %H:%M:%S'))[0][0]
    endId = np.where(timeArr<=datetime.datetime.strptime(t2,'%Y-%m-%d %H:%M:%S'))[0][-1] + 1
    return startId,endId