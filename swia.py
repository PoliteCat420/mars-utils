from astropy.io import fits
import numpy as np
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
import os
os.environ["CDF_LIB"] = '/home/ghost/cdf38_0-dist/lib'
from spacepy import pycdf
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

instr = 'swia'
instrPath = '../Data/maven/data/sci/%s/'%instr[:3]
degToRad = math.pi/180.0
def loadUrls(dataCls='coarse_svy_3d'):
    pathsPath = instrPath + '%s_paths.csv'%dataCls
    if not os.path.exists(pathsPath):
        getDataUrls(dataCls,loadUrls=False)
    dF = pd.read_csv(pathsPath,names=['Date','Url'])
    dF = dF.set_index('Date')
    dF.index = pd.to_datetime(dF.index,format='%Y%m%d')
    return dF

def download(date,dataCls='coarse_svy_3d'):
    urlDf = loadUrls(dataCls)
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
        print('Downloading %s %s %s'%(instr,dataCls,date))
        r = requests.get(dateUrl,allow_redirects=True)
        with open(downloadPath,'wb') as fl:
            fl.write(r.content)
    return downloadPath
              
def getData(date,dataCls='coarse_svy_3d'):
    #date - yyyy-mm-dd
    downloadPath = download(date,dataCls)
    thisCdf = pycdf.CDF(downloadPath)    
    return thisCdf

def getDataUrls(dataCls='coarse_svy_3d',loadUrls=False):
    #Retrives all data urls from maven data repository
    #dataCls - select from 'coarse-svy-3d','fine-svy-3d','coarse-arc-3d','fine-arc-3d','onboard-svy-mom'
    baseUrl = 'https://pds-ppi.igpp.ucla.edu/search/view/?f=yes&id=pds://PPI/maven.%s.calibrated/data/'%instr
    dwUrl = 'https://pds-ppi.igpp.ucla.edu/ditdos/download?id=pds://PPI/maven.%s.calibrated/data/'%instr
    urlsPath = instrPath + '%s_paths.csv'%dataCls
        
    if not os.path.exists(instrPath):
        os.makedirs(instrPath)
    if not os.path.exists(urlsPath):
        print('Retriving urls for %s %s'%(instr,dataCls))
        dataCls += '/'
        urlPref = baseUrl + dataCls 
        req = Request(urlPref)
        html_page = urlopen(req)
        soup = BeautifulSoup(html_page, "lxml")
        yrLinks = [re.search(r'[2]\d{3}$',item.get('href')).group(0) for item in soup.findAll('a') if re.search(r'[2]\d{3}$',item.get('href'))]
        yrLinks = list(set(yrLinks))
        print(yrLinks)
        for yr in yrLinks:
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
                    with open(urlsPath,'a') as fl:
                        fl.write('%s,%s\n'%(dateS,dwLink))
    urlDf = None
    if loadUrls:
        urlDf = loadUrls(dataCls)
    return urlDf

def reduceCoarseData(dF,date,t1,t2):
    #Average proton flux observations over area for each energy bin for coarse data
    tyP = 'coarse'
    startId, endId = utils.selCdfTimes(dF,date,t1,t2)
    timeArr = dF['epoch'][:][startId:endId]
    fluxArr = dF['diff_en_fluxes'][:][startId:endId,:,:,:]
    energyArr = dF['energy_%s'%tyP][:]
    forwardFluxes = np.zeros((len(energyArr),len(timeArr)),dtype=np.float) #within phi 45
    backwardFluxes = np.zeros((len(energyArr),len(timeArr)),dtype=np.float) #outside phi 45
    forwardIndices = [0,13,14,15]
    backwardIndices = np.arange(1,13).tolist() 
    forwardFluxes = fluxArr[:,forwardIndices,:,:].mean(axis=(1,2))
    backwardFluxes = fluxArr[:,backwardIndices,:,:].mean(axis=(1,2)) 
    
    forwardFluxes[forwardFluxes == 0.0] = 1.0
    backwardFluxes[backwardFluxes == 0.0] = 1.0
    forwardFluxes = np.log10(forwardFluxes)
    backwardFluxes = np.log10(backwardFluxes)
    forwardFluxes = np.transpose(forwardFluxes)
    backwardFluxes = np.transpose(backwardFluxes)
    return forwardFluxes,backwardFluxes,energyArr,timeArr

def getSAIntegratedFlux(dF,tyP,phI,phiIn,date,t1,t2):
    startId, endId = utils.selCdfTimes(dF,date,t1,t2)
    fluX = dF['diff_en_fluxes'][:][startId:endId]
    timE = dF['epoch'][:][startId:endId]
    thetA = (dF['theta_%s'%tyP][...]+90.0)*degToRad 
    sinTheta = np.sin(thetA)
    eStarts = np.zeros(len(timE),dtype=np.int)
    dStarts = np.zeros(len(timE),dtype=np.int)
    if tyP == 'fine':
        eStarts = dF['estep_first']
        dStarts = dF['dstep_first']
    fluxArr = np.empty((len(timE),48))
    for iT in range(len(timE)):
        eII = 0
        for eI in range(eStarts[iT],eStarts[iT]+48):
            thetAs = thetA[dStarts[iT]:dStarts[iT]+12,eI]
            intTheta = np.empty(len(phI))
            for iP in range(len(phI)):
                intTheta[iP] = simpson(fluX[iT,phiIn[iP],:,eII]*np.sin(thetAs),thetAs)
            fluxArr[iT,eII] = simpson(intTheta,phI)
            eII += 1
    return fluxArr, timE