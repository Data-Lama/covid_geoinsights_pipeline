# Script that extracts the cases for the USA (All states used this data)


# Imports all the necesary functions

import fb_functions as fb

from italy_functions import *

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
cases_folder = os.path.join(data_dir, 'data_stages/italy/raw/cases/')
if not os.path.exists(cases_folder):
	os.makedirs(cases_folder)

# repository folder
print(ident + 'Extracts Cases for Italy')


print(ident + '   Extracting:')


# Updates the repository
ext_fun.update_git_repository(repository_name)


print(ident + '   Checking Integrity')

print(ident + '   No Integrity Scheme Implemented')

ok = True

if ok:
	print(ident + '      Integrity OK')
	print(ident + '   Moving Files')
	# Moves the files
	# Iterates over the files in folder
	for file in os.listdir(repository_cases_folder):
		org_file = os.path.join(repository_cases_folder, file)
		fin_file = os.path.join(cases_folder, file)

		shutil.copy(org_file, fin_file)
	print(ident + 'Done!')

else:
	print(ident + 'Integrity Failed. Will Nor Move File')
	print(ident + 'Please Check File Manually!:')

