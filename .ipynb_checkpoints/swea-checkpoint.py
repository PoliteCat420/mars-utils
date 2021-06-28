from astropy.io import fits
import numpy as np
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
import requests
import re
import glob
import datetime
import time
import random
import os
import utils
import re
import pandas as pd

instr = 'swea'
instrPath = '../Data/maven/data/sci/%s/'%instr[:3]
def getDataLinks(dataCls='svy_3d'):
    def loadDf():
        dF = pd.read_csv(writePath1 + 'paths.csv',names=['Date','Download_Url'])
        dF = dF.set_index('Date')
        dF.index = pd.to_datetime(dF.index,format='%Y%m%d')
        return dF
    
    baseUrl = 'https://pds-ppi.igpp.ucla.edu/search/view/?f=yes&id=pds://PPI/maven.%s.calibrated/data/'%instr
    dwUrl = 'https://pds-ppi.igpp.ucla.edu/ditdos/download?id=pds://PPI/maven.%s.calibrated/data/'%instr
    dataPath = '../Data/'
    writePath1 = dataPath + '%s/'%instr + '%s/'%dataCls 
        
    if not os.path.exists(writePath1 + 'paths.csv'):
        if not os.path.exists(writePath1):
            os.makedirs(writePath1)
        dataCls += '/'
        urlPref = baseUrl + dataCls 
        req = Request(urlPref)
        html_page = urlopen(req)
        soup = BeautifulSoup(html_page, "lxml")
        yrLinks = [re.search(r'[2]\d{3}$',item.get('href')).group(0) for item in soup.findAll('a') if re.search(r'[2]\d{3}$',item.get('href'))]
        yrLinks = list(set(yrLinks))
        print(yrLinks)
        for yr in yrLinks:
            print(dataCls,yr)
            yrUrl = baseUrl + dataCls + yr 
            req = Request(yrUrl)
            html_page = urlopen(req)
            soup = BeautifulSoup(html_page, "lxml")
            mtLinks = [re.search(r'[2]\d{3}/\d{2}$',item.get('href')).group(0).split('/')[-1] for item in soup.findAll('a') if re.search(r'[2]\d{3}/\d{2}$',item.get('href'))]
            mtLinks = list(set(mtLinks))
            mtLinks = ['/'+x for x in mtLinks]
            for mt in mtLinks:
                mtUrl = baseUrl + dataCls + yr + mt
                req = Request(mtUrl)
                html_page = urlopen(req)
                soup = BeautifulSoup(html_page, "lxml")
                fdLinks = [item.get('href').split('/')[-1] for item in soup.findAll('a') if re.search(r'&o=1\b',item.get('href'))]
                fdLinks = list(set(fdLinks))
                dwLinks = [dwUrl+dataCls+yr+mt+'/'+x[:-4]+'.cdf' for x in fdLinks]
                for dwLink in dwLinks:
                    dateS = dwLink.split('_')[-3]
                    with open(writePath1 + 'paths.csv','a') as fl:
                        fl.write('%s,%s\n'%(dateS,dwLink))
    return loadDf()


