import emm_api
diR = 'EMIRS/'
instr = 'exi'
level = 'l2a'

emm_api.emm_search_and_download(download_dir=diR,
                        instrument = instr,
                        level = level)

