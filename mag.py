from astropy.io import fits
import numpy as np
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
import os
#os.environ["CDF_LIB"] = '/home/ghost/cdf38_0-dist/lib'
#from spacepy import pycdf
import requests
import glob
import datetime
import time
import random
import os
import re
import pandas as pd
import utils
from scipy.integrate import simpson
import math
#import pydivide
import calendar
import builtins
from dateutil.parser import parse
from _collections import OrderedDict

instr = 'mag'
projectPath = utils.getBasePath()
instrPath = projectPath + '/Data/sym/maven/data/sci/%s/'%instr
degToRad = math.pi/180.0

def loadUrls():
    pathsPath = instrPath + 'paths.csv'
    if not os.path.exists(pathsPath):
        getDataUrls(toLoadUrls=False)
    dF = pd.read_csv(pathsPath,names=['Date','Url'])
    dF = dF.set_index('Date')
    dF.index = pd.to_datetime(dF.index,format='%Y%m%d')
    return dF

def download(date):
    urlDf = loadUrls()
    dateUrl = urlDf.loc[date]['Url'].tolist()[0]
    year = date[:4]
    month = date[5:7]
    day = date[8:10]
    filePath = instrPath + '%s/%s/'%(year,month)
    if not os.path.exists(filePath):
        os.makedirs(filePath)
    fileName = dateUrl.split('/')[-1]
    downloadPath = filePath + fileName
    if not os.path.exists(downloadPath):
        print('Downloading %s %s'%(instr,date))
        r = requests.get(dateUrl,allow_redirects=True)
        with open(downloadPath,'wb') as fl:
            fl.write(r.content)
    return downloadPath

def writeStartIndex(dates):
    with open(instrPath + 'startIndices.csv','a') as fl:
        LEN = len(dates)
        for i,date in enumerate(dates):
            print('calclulating index %d/%d'%(i+1,LEN))
            downloadPath = download(date)
            with open(downloadPath,'r') as fp:
                d = fp.readlines()
            d = [x.strip().split() for x in d]
            startI = 140
            while len(d[startI]) != 18:
                startI += 1
            fl.write('%s,%d\n'%(date,startI))
    return startI
              
def getData(date):
    #date - yyyy-mm-dd
    downloadPath = download(date)
    with open(downloadPath,'r') as fl:
        thisD = fl.readlines()
    siPath = instrPath + 'startIndices.csv'
    dF = pd.read_csv(siPath,names=['Date','Index'])
    dF['Index'] = dF['Index'].astype(np.int)
    dF = dF.set_index('Date')
    dF.index = pd.to_datetime(dF.index,format='%Y-%m-%d')
    if date in dF.index:
        startI = dF.loc[date]['Index'].tolist()[0]
    else:
        writeStartIndex([date])
        siPath = instrPath + 'startIndices.csv'
        dF = pd.read_csv(siPath,names=['Date','Index'])
        dF['Index'] = dF['Index'].astype(np.int)
        dF = dF.set_index('Date')
        dF.index = pd.to_datetime(dF.index,format='%Y-%m-%d')
        startI = dF.loc[date]['Index'].tolist()[0]
    return thisD[startI:]

def getDataUrls(toLoadUrls=False):
    #Retrives all data urls from maven data repository
    #baseUrl = 'https://pds-ppi.igpp.ucla.edu/search/view/?f=yes&id=pds://PPI/maven.insitu.calibrated/data/'
    baseUrl = 'https://pds-ppi.igpp.ucla.edu/search/view/?f=yes&id=pds://PPI/maven.mag.calibrated/data/ss/highres/'
    #dwUrl = 'https://pds-ppi.igpp.ucla.edu/ditdos/download?id=pds://PPI/maven.insitu.calibrated/data/'
    dwUrl = 'https://pds-ppi.igpp.ucla.edu/ditdos/download?id=pds://PPI/maven.mag.calibrated/data/ss/highres/'
            #/2016/01/mvn_mag_l2_2016001ss_20160101_v01_r01.sts
    urlsPath = instrPath + 'paths.csv'
        
    if not os.path.exists(instrPath):
        os.makedirs(instrPath)
    if not os.path.exists(urlsPath):
        print('Retriving urls for %s'%(instr))
        urlPref = baseUrl
        req = Request(urlPref)
        html_page = urlopen(req)
        soup = BeautifulSoup(html_page, "lxml")
        yrLinks = [re.search(r'[2]\d{3}$',item.get('href')).group(0) for item in soup.findAll('a') if re.search(r'[2]\d{3}$',item.get('href'))]
        yrLinks = list(set(yrLinks))
        print(yrLinks)
        for yr in yrLinks:
            yrUrl = baseUrl + yr 
            req = Request(yrUrl)
            html_page = urlopen(req)
            soup = BeautifulSoup(html_page, "lxml")
            mtLinks = [re.search(r'[2]\d{3}/\d{2}$',item.get('href')).group(0).split('/')[-1] for item in soup.findAll('a') if re.search(r'[2]\d{3}/\d{2}$',item.get('href'))]
            mtLinks = list(set(mtLinks))
            mtLinks = ['/'+x for x in mtLinks]
            for mt in mtLinks:
                mtUrl = baseUrl + yr + mt
                req = Request(mtUrl)
                html_page = urlopen(req)
                soup = BeautifulSoup(html_page, "lxml")
                fdLinks = [item.get('href').split('/')[-1] for item in soup.findAll('a') if re.search(r'&o=1\b',item.get('href'))]
                fdLinks = list(set(fdLinks))
                dwLinks = [dwUrl+yr+mt+'/'+x[:-4]+'.sts' for x in fdLinks]
                for dwLink in dwLinks:
                    dateS = dwLink.split('_')[-3]
                    with open(urlsPath,'a') as fl:
                        fl.write('%s,%s\n'%(dateS,dwLink))
    urlDf = None
    if toLoadUrls:
        urlDf = loadUrls()
    return urlDf

