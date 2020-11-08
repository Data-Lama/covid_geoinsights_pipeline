# Script that cleans the data from the raw data. Only facebook movement


# Imports all the necesary functions


# Other imports
import os, sys
from pathlib import Path
# Generic file download
# Must be a .csv

import pandas as pd
import shutil
import extraction_functions as ext_fun
from pathlib import Path

#Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')


# Reads the parameters from excecution
location_name  = sys.argv[1] # Location name
location_folder_name = sys.argv[2] # location folder name

ident = '         '

# Location Folder
location_folder = os.path.join(data_dir, 'data_stages', location_folder_name)


# Extracts the description
df_description = pd.read_csv(os.path.join(location_folder, 'description.csv'), index_col = 0)


url = df_description.loc['cases_url','value']

sep = ","
if "cases_sep" in df_description.index:
	sep = df_description.loc['cases_sep','value']
	print(ident + f'Separator: {sep} detected')





# Creates the folders if the don't exist
cases_folder = os.path.join(data_dir, 'data_stages', location_folder_name, 'raw','cases')
if not os.path.exists(cases_folder):
	os.makedirs(cases_folder)

cases_file_location = os.path.join(cases_folder, 'cases_raw.csv')

# New Cases file and location
new_cases_file_name = 'cases_{}_raw.csv'.format(location_folder_name)
new_cases_location = os.path.join(ext_fun.downloads_location, new_cases_file_name)

# repository folder
print(ident + 'Extracts Cases for {}'.format(location_name))


print(ident + '   Extracting:')

# Downloads the file
ext_fun.download_file(url, new_cases_file_name)

try:
	new_cases = pd.read_csv(new_cases_location, low_memory=False, sep = sep)
except:
	new_cases = pd.read_csv(new_cases_location, low_memory=False, encoding = 'latin-1', sep = sep)

print(ident + '   Checking Integrity')

if os.path.exists(cases_file_location) and os.path.isfile(cases_file_location):

	try:
		old_cases = pd.read_csv(cases_file_location, low_memory=False)
	except:
		old_cases = pd.read_csv(cases_file_location, low_memory=False, encoding = 'latin-1')

	ok = True

	if not old_cases.columns.equals(new_cases.columns):

		print(ident + '      The structure of the cases file has changed:')

		for col in old_cases.columns.difference(new_cases.columns):
			print(ident + '         {}: missing'.format(col))

		for col in new_cases.columns.difference(old_cases.columns):
			print(ident + '         {}: new'.format(col))

		ok = False
else:
	print(ident + '      No previous cases file found. Skipping integrity check.')
	ok = True



# Copies Files

if ok:
	print(ident + '      Integrity OK')
	print(ident + '   Moving File')
	# Moves the file
	new_cases.to_csv(cases_file_location, index = False, encoding = 'utf-8')
	#shutil.copy(new_cases_location, cases_file_location)

	print(ident + 'Done')

else:
	print(ident + 'Integrity Failed. Will Not Move File')
	print(ident + 'Please Check File Manually!')
	raise ValueError('Integrity Check Failed. Check File Manually')