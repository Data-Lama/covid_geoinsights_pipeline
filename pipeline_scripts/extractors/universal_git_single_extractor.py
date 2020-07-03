# Script that extracts the cases and geo from a git repository (they must be both single files)


# Imports all the necesary functions
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


location_name  = sys.argv[1] # Location name
location_folder_name = sys.argv[2] # location folder name

# Location Folder
location_folder = os.path.join(data_dir, 'data_stages/', location_folder_name)


# Extracts the description
df_description = pd.read_csv(os.path.join(location_folder, 'description.csv'), index_col = 0)

# Repository folder
respository_folder = os.path.join(data_dir, 'git_repositories/{}/'.format(df_description.loc['repository_name','value']))

# repository folder
print(ident + 'Extracts Files for {}'.format(location_name))


print(ident + '   Extracting:')


# Updates the repository
ext_fun.update_git_repository(df_description.loc['repository_name','value'])



print(ident + '   Moving Files')

# Cases
if 'cases_file_in_repository' in df_description.index:

	print(ident + '      Cases File')
	# Creates the folders if the don't exist
	cases_folder = os.path.join(location_folder, 'raw/cases/')
	if not os.path.exists(cases_folder):
		os.makedirs(cases_folder)


	cases_in_repository = os.path.join(respository_folder, df_description.loc['cases_file_in_repository','value'])
	cases_file_location = os.path.join(cases_folder, df_description.loc['cases_file_name','value'])

	shutil.copy(cases_in_repository, cases_file_location)

# Geo
if 'geo_file_in_repository' in df_description.index:

	print(ident + '      Geo File')

	# Creates the folders if the don't exist
	geo_folder = os.path.join(location_folder, 'raw/geo/')
	if not os.path.exists(geo_folder):
		os.makedirs(geo_folder)


	geo_in_repository = os.path.join(respository_folder, df_description.loc['geo_file_in_repository','value'])
	geo_file_location = os.path.join(geo_folder, df_description.loc['geo_file_name','value'])

	shutil.copy(geo_in_repository, geo_file_location)

print(ident + 'Done!')

