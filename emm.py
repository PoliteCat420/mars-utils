#This is a wrapper around emm_api.py provided by the EMM team
#More resources are available here - https://sdc.emiratesmarsmission.ae/
import emm_api
import utils

projectPath = utils.getBasePath()
dataPath = projectPath + '/Data/sym/' 

#Available instruments and levels
instrDict = {}
instrDict['exi'] =['l2a']
instrDict['emr'] = ['l2','l2ql']
instrDict['emu'] = ['l2a']

#for instr in instrDict:
print(emm_api._get_user_token())
instr = 'exi'
for level in instrDict[instr]:
    emm_api.emm_search_and_download(download_dir=dataPath,instrument = instr,level = level)
