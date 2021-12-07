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

instr = 'kp/insitu'
projectPath = utils.getBasePath()
instrPath = projectPath + '/Data/sym/maven/data/sci/%s/'%instr
degToRad = math.pi/180.0

def loadUrls():
    pathsPath = instrPath + 'paths.csv'
    if not os.path.exists(pathsPath):
        getDataUrls(loadUrls=False)
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
    fileName = dateUrl.split('/')[-1].split('_')[:-2]
    fileName = '_'.join(fileName) + '.tab'
    downloadPath = filePath + fileName
    if not os.path.exists(downloadPath):
        print('Downloading %s %s'%(instr,date))
        r = requests.get(dateUrl,allow_redirects=True)
        with open(downloadPath,'wb') as fl:
            fl.write(r.content)
    return downloadPath

def downloadAll():
    urlDf = loadUrls()
    for index,row in urlDf.iterrows():
        dateUrl = row['Url']
        date = str(index).split(' ')[0]
        print(date)
        year = date[:4]
        month = date[5:7]
        day = date[8:10]
        filePath = instrPath + '%s/%s/'%(year,month)
        if not os.path.exists(filePath):
            os.makedirs(filePath)
        fileName = dateUrl.split('/')[-1].split('_')[:-2]
        fileName = '_'.join(fileName) + '.tab'
        downloadPath = filePath + fileName
        if not os.path.exists(downloadPath):
            print('Downloading %s %s'%(instr,date))
            r = requests.get(dateUrl,allow_redirects=True)
            with open(downloadPath,'wb') as fl:
                fl.write(r.content)
    return
              
def getData(date):
    #date - yyyy-mm-dd
    downloadPath = download(date)
    #thisDf = pydivide.read(date,insitu_only=True)
    thisDf = readData(downloadPath,insitu_only=True)
    return thisDf

def getDataUrls(loadUrls=False,update=False):
    #Retrives all data urls from maven data repository
    baseUrl = 'https://pds-ppi.igpp.ucla.edu/search/view/?f=yes&id=pds://PPI/maven.insitu.calibrated/data/'
    dwUrl = 'https://pds-ppi.igpp.ucla.edu/ditdos/download?id=pds://PPI/maven.insitu.calibrated/data/'
    urlsPath = instrPath + 'paths.csv'
        
    if not os.path.exists(instrPath):
        os.makedirs(instrPath)
    if not os.path.exists(urlsPath) or update==True:
        with open(urlsPath,'w') as fl:
            print('Retriving urls for %s'%(instr))
            urlPref = baseUrl
            req = Request(urlPref)
            html_page = urlopen(req)
            soup = BeautifulSoup(html_page, "lxml")
            yrLinks = [re.search(r'[2]\d{3}$',item.get('href')).group(0) for item in soup.findAll('a') if re.search(r'[2]\d{3}$',item.get('href'))]
            yrLinks = list(set(yrLinks))
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
                    dwLinks = [dwUrl+yr+mt+'/'+x[:-4]+'.tab' for x in fdLinks]
                    for dwLink in dwLinks:
                        dateS = dwLink.split('_')[-3]
                        fl.write('%s,%s\n'%(dateS,dwLink))
    urlDf = None
    if loadUrls:
        urlDf = loadUrls()
    return urlDf

def readData(filename=None,input_time=None, instruments=None, insitu_only=False):
    filenames = []

    if instruments is not None:
        if not isinstance(instruments, builtins.list):
            instruments = [instruments]

    if filename is None and input_time is None:
        print('You must specify either a set of filenames to read in, or a time frame in which '
              'you want to search for downloaded files.')

    if filename is not None:
        if not isinstance(filename, builtins.list):
            filename = [filename]

        dates = []
        for file in filename:
            date = re.findall(r'_(\d{8})', file)[0]
            dates.append(date)
            if 'iuvs' in file:
                iuvs_filenames.append(file)
            else:
                filenames.append(file)

        # To keep the rest of the code consistent, if someone gave a files, or files, to load, but no input_time,
        # go ahead and create an 'input_time'
        if input_time is None:
            if len(dates) == 1:
                input_time = str(dates[0][:4]) + '-' + str(dates[0][4:6]) + '-' + str(dates[0][6:])

            else:
                beg_date = min(dates)
                end_date = max(dates)
                input_time = [str(dates[0][:4]) + '-' + str(dates[0][4:6]) + '-' + str(dates[0][6:]),
                              str(dates[1][:4]) + '-' + str(dates[1][4:6]) + '-' + str(dates[1][6:])]

    # Check for orbit num rather than time string
    if isinstance(input_time, builtins.list):
        if isinstance(input_time[0], int):
            input_time = orbit_time(input_time[0], input_time[1])
    elif isinstance(input_time, int):
        input_time = orbit_time(input_time)

    # Turn string input into datetime objects
    if isinstance(input_time, list):
        if len(input_time[0]) <= 10:
            input_time[0] = input_time[0] + ' 00:00:00'
        if len(input_time[1]) <= 10:
            input_time[1] = input_time[1] + ' 00:00:00'
        date1 = parse(input_time[0])
        date2 = parse(input_time[1])
    else:
        if len(input_time) <= 10:
            input_time += ' 00:00:00'
        date1 = parse(input_time)
        date2 = date1 + datetime.timedelta(days=1)

    date1_unix = calendar.timegm(date1.timetuple())
    date2_unix = calendar.timegm(date2.timetuple())

    ## Grab insitu and iuvs files for the specified/created date ranges
    #date_range_filenames = get_latest_files_from_date_range(date1, date2)
    #date_range_iuvs_filenames = get_latest_iuvs_files_from_date_range(date1, date2)
#
    ## Add date range files to respective file lists if desired
    #if not specified_files_only:
    #    filenames.extend(date_range_filenames)
    #    iuvs_filenames.extend(date_range_iuvs_filenames)
#
    #if not date_range_filenames and not date_range_iuvs_filenames:
    #    if not filenames and not iuvs_filenames:
    #        print("No files found for the input date range, and no specific filenames were given. Exiting.")
    #        return

    # Going to look for files between time frames, but as we might have already specified
    # certain files to load in, we don't want to load them in 2x... so doing a check for that here
    filenames = list(set(filenames))
    
    kp_insitu = []
    if filenames:
        names, inst = [], []
        crus_name, crus_inst = [], []
        c_found = False
        r_found = False
    
#         kp_pattern = (r'^mvn_(?P<{0}>kp)_'
#                       '(?P<{1}>insitu|iuvs)'
#                       '(?P<{2}>|_[a-zA-Z0-9\-]+)_'
#                       '(?P<{3}>[0-9]{{4}})'
#                       '(?P<{4}>[0-9]{{2}})'
#                       '(?P<{5}>[0-9]{{2}})'
#                       '(?P<{6}>|[t|T][0-9]{{6}})_'
#                       'v(?P<{7}>[0-9]+)_r(?P<{8}>[0-9]+)\.'
#                       '(?P<{9}>tab)'
#                       '(?P<{10}>\.gz)*').format('instrument',
#                                                 'level',
#                                                 'description',
#                                                 'year',
#                                                 'month',
#                                                 'day',
#                                                 'time',
#                                                 'version',
#                                                 'revision',
#                                                 'extension',
#                                                 'gz')
        kp_pattern = (r'^mvn_(?P<{0}>kp)_'
                      '(?P<{1}>insitu|iuvs)'
                      '(?P<{2}>|_[a-zA-Z0-9\-]+)_'
                      '(?P<{3}>[0-9]{{4}})'
                      '(?P<{4}>[0-9]{{2}})'
                      '(?P<{5}>[0-9]{{2}})'
                      '(?P<{6}>|[t|T][0-9]{{6}})\.'
                      '(?P<{7}>tab)'
                      '(?P<{8}>\.gz)*').format('instrument',
                                                'level',
                                                'description',
                                                'year',
                                                'month',
                                                'day',
                                                'time',
                                                'extension',
                                                'gz')
        kp_regex = re.compile(kp_pattern)
    
        
        for f in filenames:
            if kp_regex.match(os.path.basename(f)).group('description') == '_crustal' and not c_found:
                name, inss = get_header_info(f)
                # Strip off the first name for now (Time), and use that as the dataframe index.
                # Seems to make sense for now, but will it always?
                crus_name.extend(name[1:])
                crus_inst.extend(inss[1:])
                c_found = True
            elif kp_regex.match(os.path.basename(f)).group('description') == '' and not r_found:
                name, ins = get_header_info(f)
                # Strip off the first name for now (Time), and use that as the dataframe index.
                # Seems to make sense for now, but will it always?
                names.extend(name[1:])
                inst.extend(ins[1:])
                r_found = True
        all_names = names + crus_name
        all_inst = inst + crus_inst
    
        # Break up dictionary into instrument groups
        lpw_group, euv_group, swe_group, swi_group, sta_group, sep_group, mag_group, ngi_group, app_group, sc_group, \
            crus_group = [], [], [], [], [], [], [], [], [], [], []
    
        for i, j in zip(all_inst, all_names):
            if re.match('^LPW$', i.strip()):
                lpw_group.append(j)
            elif re.match('^LPW-EUV$', i.strip()):
                euv_group.append(j)
            elif re.match('^SWEA$', i.strip()):
                swe_group.append(j)
            elif re.match('^SWIA$', i.strip()):
                swi_group.append(j)
            elif re.match('^STATIC$', i.strip()):
                sta_group.append(j)
            elif re.match('^SEP$', i.strip()):
                sep_group.append(j)
            elif re.match('^MAG$', i.strip()):
                mag_group.append(j)
            elif re.match('^NGIMS$', i.strip()):
                ngi_group.append(j)
            elif re.match('^MODELED_MAG$', i.strip()):
                crus_group.append(j)
            elif re.match('^SPICE$', i.strip()):
                # NB Need to split into APP and SPACECRAFT
                if re.match('(.+)APP(.+)', j):
                    app_group.append(j)
                else:  # Everything not APP is SC in SPICE
                    # But do not include Orbit Num, or IO Flag
                    # Could probably stand to clean this line up a bit
                    if not re.match('(.+)(Orbit Number|Inbound Outbound Flag)', j):
                        sc_group.append(j)
            else:
                pass
    
        delete_groups = []
        if instruments is not None:
            if 'LPW' not in instruments and 'lpw' not in instruments:
                delete_groups += lpw_group
            if 'MAG' not in instruments and 'mag' not in instruments:
                delete_groups += mag_group
            if 'EUV' not in instruments and 'euv' not in instruments:
                delete_groups += euv_group
            if 'SWI' not in instruments and 'swi' not in instruments:
                delete_groups += swi_group
            if 'SWE' not in instruments and 'swe' not in instruments:
                delete_groups += swe_group
            if 'NGI' not in instruments and 'ngi' not in instruments:
                delete_groups += ngi_group
            if 'SEP' not in instruments and 'sep' not in instruments:
                delete_groups += sep_group
            if 'STA' not in instruments and 'sta' not in instruments:
                delete_groups += sta_group
            if 'MODELED_MAG' not in instruments and 'modeled_mag' not in instruments:
                delete_groups += crus_group
    
        # Read in all relavent data into a pandas dataframe called "temp"
        temp_data = []
        filenames.sort()
        for filename in filenames:
            # Determine number of header lines
            nheader = 0
            with open(filename) as f:
                for line in f:
                    if line.startswith('#'):
                        nheader += 1
                if kp_regex.match(os.path.basename(filename)).group('description') == '_crustal':
                    temp_data.append(pd.read_fwf(filename, skiprows=nheader, index_col=0,
                                                 widths=[19] + len(crus_name) * [16], names=crus_name))
                else:
                    temp_data.append(pd.read_fwf(filename, skiprows=nheader, index_col=0,
                                                 widths=[19] + len(names) * [16], names=names))
                for i in delete_groups:
                    del temp_data[-1][i]
    
        temp_unconverted = pd.concat(temp_data, axis=0)
    
        # Need to convert columns
        # This is kind of a hack, but I can't figure out a better way for now
    
        if 'SWEA.Electron Spectrum Shape' in temp_unconverted and 'NGIMS.Density NO' in temp_unconverted:
            temp = temp_unconverted.astype(dtype={'SWEA.Electron Spectrum Shape': np.float64,
                                                  'NGIMS.Density NO': np.float64})
        elif 'SWEA.Electron Spectrum Shape' in temp_unconverted and 'NGIMS.Density NO' not in temp_unconverted:
            temp = temp_unconverted.astype(dtype={'SWEA.Electron Spectrum Shape': np.float64})
        elif 'SWEA.Electron Spectrum Shape' not in temp_unconverted and 'NGIMS.Density NO' in temp_unconverted:
            temp = temp_unconverted.astype(dtype={'NGIMS.Density NO': np.float64})
        else:
            temp = temp_unconverted
    
        # Cut out the times not included in the date range
        time_unix = [calendar.timegm(datetime.datetime.strptime(i, '%Y-%m-%dT%H:%M:%S').timetuple()) for i in temp.index]
        start_index = 0
        for t in time_unix:
            if t >= date1_unix:
                break
            start_index += 1
        end_index = 0
        for t in time_unix:
            if t >= date2_unix:
                break
            end_index += 1
    
        # Assign the first-level only tags
        time_unix = time_unix[start_index:end_index]
        temp = temp[start_index:end_index]
        time = temp.index
        time_unix = pd.Series(time_unix)  # convert into Series for consistency
        time_unix.index = temp.index
    
        if 'SPICE.Orbit Number' in list(temp):
            orbit = temp['SPICE.Orbit Number']
        else:
            orbit = None
        if 'SPICE.Inbound Outbound Flag' in list(temp):
            io_flag = temp['SPICE.Inbound Outbound Flag']
        else:
            io_flag = None
    
        # Build the sub-level DataFrames for the larger dictionary/structure
        app = temp[app_group]
        spacecraft = temp[sc_group]
        if instruments is not None:
            if 'LPW' in instruments or 'lpw' in instruments:
                lpw = temp[lpw_group]
            else:
                lpw = None
            if 'MAG' in instruments or 'mag' in instruments:
                mag = temp[mag_group]
            else:
                mag = None
            if 'EUV' in instruments or 'euv' in instruments:
                euv = temp[euv_group]
            else:
                euv = None
            if 'SWE' in instruments or 'swe' in instruments:
                swea = temp[swe_group]
            else:
                swea = None
            if 'SWI' in instruments or 'swi' in instruments:
                swia = temp[swi_group]
            else:
                swia = None
            if 'NGI' in instruments or 'ngi' in instruments:
                ngims = temp[ngi_group]
            else:
                ngims = None
            if 'SEP' in instruments or 'sep' in instruments:
                sep = temp[sep_group]
            else:
                sep = None
            if 'STA' in instruments or 'sta' in instruments:
                static = temp[sta_group]
            else:
                static = None
            if 'MODELED_MAG' in instruments or 'modeled_mag' in instruments:
                crus = temp[crus_group]
            else:
                crus = None
        else:
            lpw = temp[lpw_group]
            euv = temp[euv_group]
            swea = temp[swe_group]
            swia = temp[swi_group]
            static = temp[sta_group]
            sep = temp[sep_group]
            mag = temp[mag_group]
            ngims = temp[ngi_group]
            crus = temp[crus_group]
    
        # Strip out the duplicated instrument part of the column names
        # (this is a bit hardwired and can be improved)
        for i in [lpw, euv, swea, swia, sep, static, ngims, mag, crus, app, spacecraft]:
            if i is not None:
                i.columns = remove_inst_tag(i)
    
        if lpw is not None:
            lpw = lpw.rename(index=str, columns=param_dict)
        if euv is not None:
            euv = euv.rename(index=str, columns=param_dict)
        if swea is not None:
            swea = swea.rename(index=str, columns=param_dict)
        if swia is not None:
            swia = swia.rename(index=str, columns=param_dict)
        if sep is not None:
            sep = sep.rename(index=str, columns=param_dict)
        if static is not None:
            static = static.rename(index=str, columns=param_dict)
        if ngims is not None:
            ngims = ngims.rename(index=str, columns=param_dict)
        if mag is not None:
            mag = mag.rename(index=str, columns=param_dict)
        if crus is not None:
            crus = crus.rename(index=str, columns=param_dict)
        if app is not None:
            app = app.rename(index=str, columns=param_dict)
        if spacecraft is not None:
            spacecraft = spacecraft.rename(index=str, columns=param_dict)
    
        if orbit is not None and io_flag is not None:
            # Do not forget to save units
            # Define the list of first level tag names
            tag_names = ['TimeString', 'Time', 'Orbit', 'IOflag',
                         'LPW', 'EUV', 'SWEA', 'SWIA', 'STATIC',
                         'SEP', 'MAG', 'NGIMS', 'MODELED_MAG',
                         'APP', 'SPACECRAFT']
    
            # Define list of first level data structures
            data_tags = [time, time_unix, orbit, io_flag,
                         lpw, euv, swea, swia, static,
                         sep, mag, ngims, crus, app, spacecraft]
        else:
            # Do not forget to save units
            # Define the list of first level tag names
            tag_names = ['TimeString', 'Time', 'LPW', 'EUV',
                         'SWEA', 'SWIA', 'STATIC', 'SEP',
                         'MAG', 'NGIMS', 'MODELED_MAG',
                         'APP', 'SPACECRAFT']
    
            # Define list of first level data structures
            data_tags = [time, time_unix, lpw, euv,
                         swea, swia, static, sep, 
                         mag, ngims, crus, app,
                         spacecraft]
        kp_insitu = OrderedDict(zip(tag_names, data_tags))
    return kp_insitu

def get_header_info(filename):
    # Determine number of header lines    
    nheader = 0
    with open(filename) as f:
        for line in f:
            if line.startswith('#'):
                nheader += 1

    # Parse the header (still needs special case work)
    read_param_list = False
    start_temp = False
    index_list = []
    with open(filename) as fin:
        icol = -2  # Counting header lines detailing column names
        iname = 1  # for counting seven lines with name info
        ncol = -1  # Dummy value to allow reading of early headerlines?
        col_regex = '#\s(.{16}){%3d}' % ncol  # needed for column names
        crustal = False
        if 'crustal' in filename:
            crustal = True
        for iline in range(nheader):
            line = fin.readline()
            # Define the proper indices change depending on the file type and row
            i = [2, 2, 1] if crustal else [1, 1, 1]
            if re.search('Number of parameter columns', line):
                ncol = int(re.split("\s{3}", line)[i[0]])
                # needed for column names
                col_regex = '#\s(.{16}){%2d}' % ncol if crustal else '#\s(.{16}){%3d}' % ncol
            elif re.search('Line on which data begins', line):
                nhead_test = int(re.split("\s{3}", line)[i[1]]) - 1
            elif re.search('Number of lines', line):
                ndata = int(re.split("\s{3}", line)[i[2]])
            elif re.search('PARAMETER', line):
                read_param_list = True
                param_head = iline
            elif read_param_list:
                icol += 1
                if icol > ncol:
                    read_param_list = False
            elif re.match(col_regex, line):
                # OK, verified match now get the values
                temp = re.findall('(.{16})', line[3:])
                if temp[0] == '               1':
                    start_temp = True
                if start_temp:
                    # Crustal files do not have as much variable info as other insitu files, need
                    # to modify the lines below
                    if crustal:
                        if iname == 1:
                            index = temp
                        elif iname == 2:
                            obs1 = temp
                        elif iname == 3:
                            obs2 = temp
                        elif iname == 4:
                            unit = temp
                            # crustal files don't come with this field
                            # throwing it in here for consistency with other insitu files
                            inst = ['     MODELED_MAG']*13
                        else:
                            print('More lines in data descriptor than expected.')
                            print('Line %d' % iline)
                    else:
                        if iname == 1:
                            index = temp
                        elif iname == 2:
                            obs1 = temp
                        elif iname == 3:
                            obs2 = temp
                        elif iname == 4:
                            obs3 = temp
                        elif iname == 5:
                            inst = temp
                        elif iname == 6:
                            unit = temp
                        elif iname == 7:
                            format_code = temp
                        else:
                            print('More lines in data descriptor than expected.')
                            print('Line %d' % iline)
                    iname += 1
            else:
                pass

        # Generate the names list.
        # NB, there are special case redundancies in there
        # (e.g., LPW: Electron Density Quality (min and max))
        # ****SWEA FLUX electron QUALITY *****
        first = True
        parallel = None
        names = []
        if crustal:
            for h, i, j in zip(inst, obs1, obs2):
                combo_name = (' '.join([i.strip(), j.strip()])).strip()
                # Add inst to names to avoid ambiguity
                # Will need to remove these after splitting
                names.append('.'.join([h.strip(), combo_name]))
                names[0] = 'Time'
        else:
            for h, i, j, k in zip(inst, obs1, obs2, obs3):
                combo_name = (' '.join([i.strip(), j.strip(), k.strip()])).strip()
                if re.match('^LPW$', h.strip()):
                    # Max and min error bars use same name in column
                    # SIS says first entry is min and second is max
                    if re.match('(Electron|Spacecraft)(.+)Quality', combo_name):
                        if first:
                            combo_name = combo_name + ' Min'
                            first = False
                        else:
                            combo_name = combo_name + ' Max'
                            first = True
                elif re.match('^SWEA$', h.strip()):
                    # electron flux qual flags do not indicate whether parallel or anti
                    # From context it is clear; but we need to specify in name
                    if re.match('.+Parallel.+', combo_name):
                        parallel = True
                    elif re.match('.+Anti-par', combo_name):
                        parallel = False
                    else:
                        pass
                    if re.match('Flux, e-(.+)Quality', combo_name):
                        if parallel:
                            p = re.compile('Flux, e- ')
                            combo_name = p.sub('Flux, e- Parallel ', combo_name)
                        else:
                            p = re.compile('Flux, e- ')
                            combo_name = p.sub('Flux, e- Anti-par ', combo_name)
                    if re.match('Electron eflux (.+)Quality', combo_name):
                        if parallel:
                            p = re.compile('Electron eflux ')
                            combo_name = p.sub('Electron eflux  Parallel ', combo_name)
                        else:
                            p = re.compile('Electron eflux ')
                            combo_name = p.sub('Electron eflux  Anti-par ', combo_name)
                # Add inst to names to avoid ambiguity
                # Will need to remove these after splitting
                names.append('.'.join([h.strip(), combo_name]))
                names[0] = 'Time'
    
    return names, inst

def remove_inst_tag(df):
    '''
    Remove the leading part of the column name that includes the instrument
    identifier for use in creating the parameter names for the toolkit.
    Input:
        A DataFrame produced from the insitu KP data
    Output:
        A new set of column names
    '''

    newcol = []
    for i in df.columns:
        if len(i.split('.')) >= 2:
            j = i.split('.')
            newcol.append('.'.join(j[1:]))

    return newcol


param_dict = {'Electron Density': 'ELECTRON_DENSITY',
              'Electron Density Quality Min': 'ELECTRON_DENSITY_QUAL_MIN',
              'Electron Density Quality Max': 'ELECTRON_DENSITY_QUAL_MAX',
              'Electron Temperature': 'ELECTRON_TEMPERATURE',
              'Electron Temperature Quality Min': 'ELECTRON_TEMPERATURE_QUAL_MIN',
              'Electron Temperature Quality Max': 'ELECTRON_TEMPERATURE_QUAL_MAX',
              'Spacecraft Potential': 'SPACECRAFT_POTENTIAL',
              'Spacecraft Potential Quality Min':  'SPACECRAFT_POTENTIAL_QUAL_MIN',
              'Spacecraft Potential Quality Max':  'SPACECRAFT_POTENTIAL_QUAL_MAX',
              'E-field Power 2-100 Hz':  'EWAVE_LOW_FREQ',
              'E-field 2-100 Hz Quality':  'EWAVE_LOW_FREQ_QUAL_QUAL',
              'E-field Power 100-800 Hz':  'EWAVE_MID_FREQ',
              'E-field 100-800 Hz Quality':  'EWAVE_MID_FREQ_QUAL_QUAL',
              'E-field Power 0.8-1.0 Mhz':  'EWAVE_HIGH_FREQ',
              'E-field 0.8-1.0 Mhz Quality':  'EWAVE_HIGH_FREQ_QUAL_QUAL',
              'EUV Irradiance 0.1-7.0 nm':  'IRRADIANCE_LOW',
              'Irradiance 0.1-7.0 nm Quality':  'IRRADIANCE_LOW_QUAL',
              'EUV Irradiance 17-22 nm':  'IRRADIANCE_MID',
              'Irradiance 17-22 nm Quality':  'IRRADIANCE_MID_QUAL',
              'EUV Irradiance Lyman-alpha':  'IRRADIANCE_LYMAN',
              'Irradiance Lyman-alpha Quality':  'IRRADIANCE_LYMAN_QUAL',
              'Solar Wind Electron Density':  'SOLAR_WIND_ELECTRON_DENSITY',
              'Solar Wind E- Density Quality':  'SOLAR_WIND_ELECTRON_DENSITY_QUAL',
              'Solar Wind Electron Temperature':  'SOLAR_WIND_ELECTRON_TEMPERATURE',
              'Solar Wind E- Temperature Quality':  'SOLAR_WIND_ELECTRON_TEMPERATURE_QUAL',
              'Flux, e- Parallel (5-100 ev)':  'ELECTRON_PARALLEL_FLUX_LOW',
              'Flux, e- Parallel (5-100 ev) Quality':  'ELECTRON_PARALLEL_FLUX_LOW_QUAL',
              'Flux, e- Parallel (100-500 ev)':  'ELECTRON_PARALLEL_FLUX_MID',
              'Flux, e- Parallel (100-500 ev) Quality':  'ELECTRON_PARALLEL_FLUX_MID_QUAL',
              'Flux, e- Parallel (500-1000 ev)':  'ELECTRON_PARALLEL_FLUX_HIGH',
              'Flux, e- Parallel (500-1000 ev) Quality':  'ELECTRON_PARALLEL_FLUX_HIGH_QUAL',
              'Flux, e- Anti-par (5-100 ev)':  'ELECTRON_ANTI_PARALLEL_FLUX_LOW',
              'Flux, e- Anti-par (5-100 ev) Quality':  'ELECTRON_ANTI_PARALLEL_FLUX_LOW_QUAL',
              'Flux, e- Anti-par (100-500 ev)':  'ELECTRON_ANTI_PARALLEL_FLUX_MID',
              'Flux, e- Anti-par (100-500 ev) Quality':  'ELECTRON_ANTI_PARALLEL_FLUX_MID_QUAL',
              'Flux, e- Anti-par (500-1000 ev)':  'ELECTRON_ANTI_PARALLEL_FLUX_HIGH',
              'Flux, e- Anti-par (500-1000 ev) Quality':  'ELECTRON_ANTI_PARALLEL_FLUX_HIGH_QUAL',
              'Electron eflux Parallel (5-100 ev)':  'ELECTRON_PARALLEL_FLUX_LOW',
              'Electron eflux Parallel (5-100 ev) Quality':  'ELECTRON_PARALLEL_FLUX_LOW_QUAL',
              'Electron eflux Parallel (100-500 ev)':  'ELECTRON_PARALLEL_FLUX_MID',
              'Electron eflux Parallel (100-500 ev) Quality':  'ELECTRON_PARALLEL_FLUX_MID_QUAL',
              'Electron eflux Parallel (500-1000 ev)':  'ELECTRON_PARALLEL_FLUX_HIGH',
              'Electron eflux Parallel (500-1000 ev) Quality':  'ELECTRON_PARALLEL_FLUX_HIGH_QUAL',
              'Electron eflux Anti-par (5-100 ev)':  'ELECTRON_ANTI_PARALLEL_FLUX_LOW',
              'Electron eflux Anti-par (5-100 ev) Quality':  'ELECTRON_ANTI_PARALLEL_FLUX_LOW_QUAL',
              'Electron eflux Anti-par (100-500 ev)':  'ELECTRON_ANTI_PARALLEL_FLUX_MID',
              'Electron eflux Anti-par (100-500 ev) Quality':  'ELECTRON_ANTI_PARALLEL_FLUX_MID_QUAL',
              'Electron eflux Anti-par (500-1000 ev)':  'ELECTRON_ANTI_PARALLEL_FLUX_HIGH',
              'Electron eflux Anti-par (500-1000 ev) Quality':  'ELECTRON_ANTI_PARALLEL_FLUX_HIGH_QUAL',
              'Electron Spectrum Shape':  'ELECTRON_SPECTRUM_SHAPE_PARAMETER',
              'Spectrum Shape Quality':  'ELECTRON_SPECTRUM_SHAPE_PARAMETER_QUAL',
              'H+ Density':  'HPLUS_DENSITY',
              'H+ Density Quality':  'HPLUS_DENSITY_QUAL',
              'H+ Flow Velocity MSO X':  'HPLUS_FLOW_VELOCITY_MSO_X',
              'H+ Flow MSO X Quality':  'HPLUS_FLOW_VELOCITY_MSO_X_QUAL',
              'H+ Flow Velocity MSO Y':  'HPLUS_FLOW_VELOCITY_MSO_Y',
              'H+ Flow MSO Y Quality':  'HPLUS_FLOW_VELOCITY_MSO_Y_QUAL',
              'H+ Flow Velocity MSO Z':  'HPLUS_FLOW_VELOCITY_MSO_Z',
              'H+ Flow MSO Z Quality':  'HPLUS_FLOW_VELOCITY_MSO_Z_QUAL',
              'H+ Temperature':  'HPLUS_TEMPERATURE',
              'H+ Temperature Quality':  'HPLUS_TEMPERATURE_QUAL',
              'Solar Wind Dynamic Pressure':  'SOLAR_WIND_DYNAMIC_PRESSURE',
              'Solar Wind Pressure Quality':  'SOLAR_WIND_DYNAMIC_PRESSURE_QUAL',
              'STATIC Quality Flag':  'STATIC_QUALITY_FLAG',
              'H+ Density':  'HPLUS_DENSITY',
              'H+ Density Quality':  'HPLUS_DENSITY_QUAL',
              'O+ Density':  'OPLUS_DENSITY',
              'O+ Density Quality':  'OPLUS_DENSITY_QUAL',
              'O2+ Density':  'O2PLUS_DENSITY',
              'O2+ Density Quality':  'O2PLUS_DENSITY_QUAL',
              'H+ Temperature':  'HPLUS_TEMPERATURE',
              'H+ Temperature Quality':  'HPLUS_TEMPERATURE_QUAL',
              'O+ Temperature':  'OPLUS_TEMPERATURE',
              'O+ Temperature Quality':  'OPLUS_TEMPERATURE_QUAL',
              'O2+ Temperature':  'O2PLUS_TEMPERATURE',
              'O2+ Temperature Quality':  'O2PLUS_TEMPERATURE_QUAL',
              'O2+ Flow Velocity MAVEN_APP X':  'O2PLUS_FLOW_VELOCITY_MAVEN_APP_X',
              'O2+ Flow MAVEN_APP X Quality':  'O2PLUS_FLOW_VELOCITY_MAVEN_APP_X_QUAL',
              'O2+ Flow Velocity MAVEN_APP Y':  'O2PLUS_FLOW_VELOCITY_MAVEN_APP_Y',
              'O2+ Flow MAVEN_APP Y Quality':  'O2PLUS_FLOW_VELOCITY_MAVEN_APP_Y_QUAL',
              'O2+ Flow Velocity MAVEN_APP Z':  'O2PLUS_FLOW_VELOCITY_MAVEN_APP_Z',
              'O2+ Flow MAVEN_APP Z Quality':  'O2PLUS_FLOW_VELOCITY_MAVEN_APP_Z_QUAL',
              'O2+ Flow Velocity MSO X':  'O2PLUS_FLOW_VELOCITY_MSO_X',
              'O2+ Flow MSO X Quality':  'O2PLUS_FLOW_VELOCITY_MSO_X_QUAL',
              'O2+ Flow Velocity MSO Y':  'O2PLUS_FLOW_VELOCITY_MSO_Y',
              'O2+ Flow MSO Y Quality':  'O2PLUS_FLOW_VELOCITY_MSO_Y_QUAL',
              'O2+ Flow Velocity MSO Z':  'O2PLUS_FLOW_VELOCITY_MSO_Z',
              'O2+ Flow MSO Z Quality':  'O2PLUS_FLOW_VELOCITY_MSO_Z_QUAL',
              'H+ Omni Flux':  'HPLUS_OMNI_DIRECTIONAL_FLUX',
              'H+ Energy':  'HPLUS_CHARACTERISTIC_ENERGY',
              'H+ Energy Quality':  'HPLUS_CHARACTERISTIC_ENERGY_QUAL',
              'He++ Omni Flux':  'HEPLUS_OMNI_DIRECTIONAL_FLUX',
              'He++ Energy':  'HEPLUS_CHARACTERISTIC_ENERGY',
              'He++ Energy Quality':  'HEPLUS_CHARACTERISTIC_ENERGY_QUAL',
              'O+ Omni Flux':  'OPLUS_OMNI_DIRECTIONAL_FLUX',
              'O+ Energy':  'OPLUS_CHARACTERISTIC_ENERGY',
              'O+ Energy Quality':  'OPLUS_CHARACTERISTIC_ENERGY_QUAL',
              'O2+ Omni Flux':  'O2PLUS_OMNI_DIRECTIONAL_FLUX',
              'O2+ Energy':  'O2PLUS_CHARACTERISTIC_ENERGY',
              'O2+ Energy Quality':  'O2PLUS_CHARACTERISTIC_ENERGY_QUAL',
              'H+ Direction MSO X':  'HPLUS_CHARACTERISTIC_DIRECTION_MSO_X',
              'H+ Direction MSO Y':  'HPLUS_CHARACTERISTIC_DIRECTION_MSO_Y',
              'H+ Direction MSO Z':  'HPLUS_CHARACTERISTIC_DIRECTION_MSO_Z',
              'H+ Angular Width':  'HPLUS_CHARACTERISTIC_ANGULAR_WIDTH',
              'H+ Width Quality':  'HPLUS_CHARACTERISTIC_ANGULAR_WIDTH_QUAL',
              'Pickup Ion Direction MSO X':  'DOMINANT_PICKUP_ION_CHARACTERISTIC_DIRECTION_MSO_X',
              'Pickup Ion Direction MSO Y':  'DOMINANT_PICKUP_ION_CHARACTERISTIC_DIRECTION_MSO_Y',
              'Pickup Ion Direction MSO Z':  'DOMINANT_PICKUP_ION_CHARACTERISTIC_DIRECTION_MSO_Z',
              'Pickup Ion Angular Width':  'DOMINANT_PICKUP_ION_CHARACTERISTIC_ANGULAR_WIDTH',
              'Pickup Ion Width Quality':  'DOMINANT_PICKUP_ION_CHARACTERISTIC_ANGULAR_WIDTH_QUAL',
              'Ion Flux FOV 1 F':  'ION_ENERGY_FLUX__FOV_1_F',
              'Ion Flux FOV 1F Quality':  'ION_ENERGY_FLUX__FOV_1_F_QUAL',
              'Ion Flux FOV 1 R':  'ION_ENERGY_FLUX__FOV_1_R',
              'Ion Flux FOV 1R Quality':  'ION_ENERGY_FLUX__FOV_1_R_QUAL',
              'Ion Flux FOV 2 F':  'ION_ENERGY_FLUX__FOV_2_F',
              'Ion Flux FOV 2F Quality':  'ION_ENERGY_FLUX__FOV_2_F_QUAL',
              'Ion Flux FOV 2 R':  'ION_ENERGY_FLUX__FOV_2_R',
              'Ion Flux FOV 2R Quality':  'ION_ENERGY_FLUX__FOV_2_R_QUAL',
              'Electron Flux FOV 1 F':  'ELECTRON_ENERGY_FLUX___FOV_1_F',
              'Electron Flux FOV 1F Quality':  'ELECTRON_ENERGY_FLUX___FOV_1_F_QUAL',
              'Electron Flux FOV 1 R':  'ELECTRON_ENERGY_FLUX___FOV_1_R',
              'Electron Flux FOV 1R Quality':  'ELECTRON_ENERGY_FLUX___FOV_1_R_QUAL',
              'Electron Flux FOV 2 F':  'ELECTRON_ENERGY_FLUX___FOV_2_F',
              'Electron Flux FOV 2F Quality':  'ELECTRON_ENERGY_FLUX___FOV_2_F_QUAL',
              'Electron Flux FOV 2 R':  'ELECTRON_ENERGY_FLUX___FOV_2_R',
              'Electron Flux FOV 2R Quality':  'ELECTRON_ENERGY_FLUX___FOV_2_R_QUAL',
              'Look Direction 1-F MSO X':  'LOOK_DIRECTION_1_F_MSO_X',
              'Look Direction 1-F MSO Y':  'LOOK_DIRECTION_1_F_MSO_Y',
              'Look Direction 1-F MSO Z':  'LOOK_DIRECTION_1_F_MSO_Z',
              'Look Direction 1-R MSO X':  'LOOK_DIRECTION_1_R_MSO_X',
              'Look Direction 1-R MSO Y':  'LOOK_DIRECTION_1_R_MSO_Y',
              'Look Direction 1-R MSO Z':  'LOOK_DIRECTION_1_R_MSO_Z',
              'Look Direction 2-F MSO X':  'LOOK_DIRECTION_2_F_MSO_X',
              'Look Direction 2-F MSO Y':  'LOOK_DIRECTION_2_F_MSO_Y',
              'Look Direction 2-F MSO Z':  'LOOK_DIRECTION_2_F_MSO_Z',
              'Look Direction 2-R MSO X':  'LOOK_DIRECTION_2_R_MSO_X',
              'Look Direction 2-R MSO Y':  'LOOK_DIRECTION_2_R_MSO_Y',
              'Look Direction 2-R MSO Z':  'LOOK_DIRECTION_2_R_MSO_Z',
              'Magnetic Field MSO X':  'MSO_X',
              'Magnetic MSO X Quality':  'MSO_X_QUAL',
              'Magnetic Field MSO Y':  'MSO_Y',
              'Magnetic MSO Y Quality':  'MSO_Y_QUAL',
              'Magnetic Field MSO Z':  'MSO_Z',
              'Magnetic MSO Z Quality':  'MSO_Z_QUAL',
              'Magnetic Field GEO X':  'GEO_X',
              'Magnetic GEO X Quality':  'GEO_X_QUAL',
              'Magnetic Field GEO Y':  'GEO_Y',
              'Magnetic GEO Y Quality':  'GEO_Y_QUAL',
              'Magnetic Field GEO Z':  'GEO_Z',
              'Magnetic GEO Z Quality':  'GEO_Z_QUAL',
              'Magnetic Field RMS Dev':  'RMS_DEVIATION',
              'Magnetic RMS Quality':  'RMS_DEVIATION_QUAL',
              'Density He':  'HE_DENSITY',
              'Density He Precision':  'HE_DENSITY_PRECISION',
              'Density He Quality':  'HE_DENSITY_QUAL',
              'Density O':  'O_DENSITY',
              'Density O Precision':  'O_DENSITY_PRECISION',
              'Density O Quality':  'O_DENSITY_QUAL',
              'Density CO':  'CO_DENSITY',
              'Density CO Precision':  'CO_DENSITY_PRECISION',
              'Density CO Quality':  'CO_DENSITY_QUAL',
              'Density N2':  'N2_DENSITY',
              'Density N2 Precision':  'N2_DENSITY_PRECISION',
              'Density N2 Quality':  'N2_DENSITY_QUAL',
              'Density NO':  'NO_DENSITY',
              'Density NO Precision':  'NO_DENSITY_PRECISION',
              'Density NO Quality':  'NO_DENSITY_QUAL',
              'Density Ar':  'AR_DENSITY',
              'Density Ar Precision':  'AR_DENSITY_PRECISION',
              'Density Ar Quality':  'AR_DENSITY_QUAL',
              'Density CO2':  'CO2_DENSITY',
              'Density CO2 Precision':  'CO2_DENSITY_PRECISION',
              'Density CO2 Quality':  'CO2_DENSITY_QUAL',
              'Density 32+':  'O2PLUS_DENSITY',
              'Density 32+ Precision':  'O2PLUS_DENSITY_PRECISION',
              'Density 32+ Quality':  'O2PLUS_DENSITY_QUAL',
              'Density 44+':  'CO2PLUS_DENSITY',
              'Density 44+ Precision':  'CO2PLUS_DENSITY_PRECISION',
              'Density 44+ Quality':  'CO2PLUS_DENSITY_QUAL',
              'Density 30+':  'NOPLUS_DENSITY',
              'Density 30+ Precision':  'NOPLUS_DENSITY_PRECISION',
              'Density 30+ Quality':  'NOPLUS_DENSITY_QUAL',
              'Density 16+':  'OPLUS_DENSITY',
              'Density 16+ Precision':  'OPLUS_DENSITY_PRECISION',
              'Density 16+ Quality':  'OPLUS_DENSITY_QUAL',
              'Density 28+':  'CO2PLUS_N2PLUS_DENSITY',
              'Density 28+ Precision':  'CO2PLUS_N2PLUS_DENSITY_PRECISION',
              'Density 28+ Quality':  'CO2PLUS_N2PLUS_DENSITY_QUAL',
              'Density 12+':  'CPLUS_DENSITY',
              'Density 12+ Precision':  'CPLUS_DENSITY_PRECISION',
              'Density 12+ Quality':  'CPLUS_DENSITY_QUAL',
              'Density 17+':  'OHPLUS_DENSITY',
              'Density 17+ Precision':  'OHPLUS_DENSITY_PRECISION',
              'Density 17+ Quality':  'OHPLUS_DENSITY_QUAL',
              'Density 14+':  'NPLUS_DENSITY',
              'Density 14+ Precision':  'NPLUS_DENSITY_PRECISION',
              'Density 14+ Quality':  'NPLUS_DENSITY_QUAL',
              'APP Attitude GEO X':  'ATTITUDE_GEO_X',
              'APP Attitude GEO Y':  'ATTITUDE_GEO_Y',
              'APP Attitude GEO Z':  'ATTITUDE_GEO_Z',
              'APP Attitude MSO X':  'ATTITUDE_MSO_X',
              'APP Attitude MSO Y':  'ATTITUDE_MSO_Y',
              'APP Attitude MSO Z':  'ATTITUDE_MSO_Z',
              'Spacecraft GEO X':  'GEO_X',
              'Spacecraft GEO Y':  'GEO_Y',
              'Spacecraft GEO Z':  'GEO_Z',
              'Spacecraft MSO X':  'MSO_X',
              'Spacecraft MSO Y':  'MSO_Y',
              'Spacecraft MSO Z':  'MSO_Z',
              'Spacecraft GEO Longitude':  'SUB_SC_LONGITUDE',
              'Spacecraft GEO Latitude':  'SUB_SC_LATITUDE',
              'Spacecraft Solar Zenith Angle':  'SZA',
              'Spacecraft Local Time':  'LOCAL_TIME',
              'Spacecraft Altitude Aeroid':  'ALTITUDE',
              'Spacecraft Attitude GEO X':  'ATTITUDE_GEO_X',
              'Spacecraft Attitude GEO Y':  'ATTITUDE_GEO_Y',
              'Spacecraft Attitude GEO Z':  'ATTITUDE_GEO_Z',
              'Spacecraft Attitude MSO X':  'ATTITUDE_MSO_X',
              'Spacecraft Attitude MSO Y':  'ATTITUDE_MSO_Y',
              'Spacecraft Attitude MSO Z':  'ATTITUDE_MSO_Z',
              'Mars Season (Ls)':  'MARS_SEASON',
              'Mars-Sun Distance':  'MARS_SUN_DISTANCE',
              'Subsolar Point GEO Longitude':  'SUBSOLAR_POINT_GEO_LONGITUDE',
              'Subsolar Point GEO Latitude':  'SUBSOLAR_POINT_GEO_LATITUDE',
              'Sub-Mars Point on the Sun Longitude':  'SUBMARS_POINT_SOLAR_LONGITUDE',
              'Sub-Mars Point on the Sun Latitude':  'SUBMARS_POINT_SOLAR_LATITUDE',
              'Rot matrix MARS -> MSO Row 1, Col 1':  'T11',
              'Rot matrix MARS -> MSO Row 1, Col 2':  'T12',
              'Rot matrix MARS -> MSO Row 1, Col 3':  'T13',
              'Rot matrix MARS -> MSO Row 2, Col 1':  'T21',
              'Rot matrix MARS -> MSO Row 2, Col 2':  'T22',
              'Rot matrix MARS -> MSO Row 2, Col 3':  'T23',
              'Rot matrix MARS -> MSO Row 3, Col 1':  'T31',
              'Rot matrix MARS -> MSO Row 3, Col 2':  'T32',
              'Rot matrix MARS -> MSO Row 3, Col 3':  'T33',
              'Rot matrix SPCCRFT -> MSO Row 1, Col 1':  'SPACECRAFT_T11',
              'Rot matrix SPCCRFT -> MSO Row 1, Col 2':  'SPACECRAFT_T12',
              'Rot matrix SPCCRFT -> MSO Row 1, Col 3':  'SPACECRAFT_T13',
              'Rot matrix SPCCRFT -> MSO Row 2, Col 1':  'SPACECRAFT_T21',
              'Rot matrix SPCCRFT -> MSO Row 2, Col 2':  'SPACECRAFT_T22',
              'Rot matrix SPCCRFT -> MSO Row 2, Col 3':  'SPACECRAFT_T23',
              'Rot matrix SPCCRFT -> MSO Row 3, Col 1':  'SPACECRAFT_T31',
              'Rot matrix SPCCRFT -> MSO Row 3, Col 2':  'SPACECRAFT_T32',
              'Rot matrix SPCCRFT -> MSO Row 3, Col 3':  'SPACECRAFT_T33'}