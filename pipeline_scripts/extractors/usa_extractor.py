# Script that extracts the cases for the USA (All states used this data)


# Imports all the necesary functions

import fb_functions as fb

from usa_functions import *

# Other imports
import os, sys


import pandas as pd
import shutil
import extraction_functions as ext_fun
from pathlib import Path


#Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')

ident = '         '


# Creates the folders if the don't exist
cases_folder = os.path.join(data_dir, 'data_stages/usa/raw/cases/')
if not os.path.exists(cases_folder):
	os.makedirs(cases_folder)


# repository folder
print(ident + 'Extracts Cases for USA')


print(ident + '   Extracting:')


# Updates the repository
ext_fun.update_git_repository(repository_name)


print(ident + '   Checking Integrity')


new_cases = pd.read_csv(cases_in_respository)

cols = ['UID','iso2','iso3','code3','FIPS','Admin2','Province_State','Country_Region','Lat','Long_','Combined_Key']

ok = True
for col in cols:
	if col not in new_cases.columns:
		print(ident + '      {} not in new cases file.'.format(col))


if ok:
	print(ident + '      Integrity OK')
	print(ident + '   Moving File')
	# Moves the file
	shutil.copy(cases_in_respository, jh_time_series_location)
	print(ident + 'Done!:')

else:
	print(ident + 'Integrity Failed. Will Nor Move File')
	print(ident + 'Please Check File Manually!:')

