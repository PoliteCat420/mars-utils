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

instr = 'iuvs'
projectPath = utils.getBasePath()
instrPath = projectPath + '/Data/sym/maven/data/sci/%s/'%instr[:3] 
def loadUrls(dataLevel,dataCls):
    pathsPath = instrPath + '%s_%s_paths.csv'%(dataLevel,dataCls)
    if not os.path.exists(pathsPath):
        getDataUrls(dataLevel,dataCls,loadUrls=False)
    dF = pd.read_csv(pathsPath,names=['Date','Orbit','Url'])
    dF['Date'] = pd.to_datetime(dF['Date'].str.split('T').str[0],format='%Y%m%d')
    dF = dF.set_index(['Date'])
    return dF

def getData(date,dataLevel,dataCls):
    urlDf = loadUrls(dataLevel,dataCls)
    dateUrls = urlDf.loc[date]['Url'].tolist()
    dateOrbits = urlDf.loc[date]['Orbit'].tolist() 
    year = date[:4]
    month = date[5:7]
    day = date[8:10]
    filePath = instrPath + '%s/%s/'%(year,month)
    if not os.path.exists(filePath):
        os.makedirs(filePath)
    fileNames = [x.split('/')[-1] for x in dateUrls]
    downloadPaths = [filePath + x for x in fileNames]
    fitFiles = {}
    for i,dP in enumerate(downloadPaths):
        if not os.path.exists(dP):
            print('Downloading %s %s %s %s orbit=%s'%(instr,dataLevel,dataCls,date,dateOrbits[i]))
            thisFits = fits.open(dateUrls[i],cache=False)
            thisFits.verify('fix')
            thisFits.writeto(dP)
        else:
            thisFits = fits.open(dP)
        fitFiles[dateOrbits[i]] = thisFits
    return fitFiles

def getDataUrls(dataLevel,dataCls,loadUrls=False):
    #Retrives all data urls from maven data repository
    #dataLevel - select from 'processed','callibrated','derived'
    #dataCls - select from 'corona','limb','disk'
    levelDict = {'processed':'l1c','calibrated':'l1b','derived':'l2'}
    dataLevelC = levelDict[dataLevel]
    baseUrl = 'https://atmos.nmsu.edu/PDS/data/PDS4/MAVEN/iuvs_%s_bundle/%s/%s/'%(dataLevel,dataLevelC,dataCls)
    urlsPath = instrPath + '%s_%s_paths.csv'%(dataLevel,dataCls)
    if not os.path.exists(instrPath):
        os.makedirs(instrPath)
    if not os.path.exists(urlsPath):
        print('Retriving urls for %s %s %s'%(instr,dataLevel,dataCls))
        req = Request(baseUrl)
        html_page = urlopen(req)
        soup = BeautifulSoup(html_page, "lxml")
        yrLinks = [re.search(r'[2]\d{3}[/]',item.get('href')).group(0) for item in soup.findAll('a') if re.search(r'[2]\d{3}[/]',item.get('href'))]
        yrLinks = list(set(yrLinks))
        for yr in yrLinks:
            yrUrl = baseUrl + yr
            req = Request(yrUrl)
            html_page = urlopen(req)
            soup = BeautifulSoup(html_page, "lxml")
            mtLinks = [re.search(r'\d{2}[/]$',item.get('href')).group(0) for item in soup.findAll('a') if re.search(r'\d{2}[/]$',item.get('href'))]
            mtLinks = list(set(mtLinks))
            for mt in mtLinks:
                mtUrl = baseUrl + yr + mt
                req = Request(mtUrl)
                html_page = urlopen(req)
                soup = BeautifulSoup(html_page, "lxml")
                dwLinks = [mtUrl + item.get('href') for item in soup.findAll('a') if re.search(r'.fits\b',item.get('href'))] 
                dwLinks = list(set(dwLinks))
                for dwLink in dwLinks:
                    splits = dwLink.split('/')[-1].split('_')
                    dateS = splits[-3]
                    orbit = splits[-4].split('-')[1][5:10]
                    with open(urlsPath,'a') as fl:
                        fl.write('%s,%s,%s\n'%(dateS,orbit,dwLink))
    urlDf = None
    if loadUrls:
        urlDf = loadUrls(dataLevel,dataCls)
    return urlDf