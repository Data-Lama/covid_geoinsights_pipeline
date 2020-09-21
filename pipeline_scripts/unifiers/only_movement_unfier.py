# Script that cleans the data from the raw data. Only facebook movement


# Imports all the necesary functions

import fb_functions as fb


# Other imports
import os, sys
from pathlib import Path


#Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')


# Reads the parameters from excecution
location_name  = sys.argv[1] # Location name
location_folder_name  = sys.argv[2] # Slocation folder name


ident = '         '


# Creates the folders if the don't exist
unified_folder = os.path.join(data_dir, 'data_stages', location_folder_name, 'unified/')
if not os.path.exists(unified_folder):
	os.makedirs(unified_folder)


print(ident + 'Unifies for {}'.format(location_name))



print(ident + 'Builds Datasets:')

# -------------------
# ---- Movement -----
# -------------------
print(ident + '   Movement')
df_movement = fb.build_movement(location_folder_name)

# Extracts date
movement_date = df_movement.date_time.max()

# Saves
df_movement.to_csv(os.path.join(unified_folder, 'movement.csv'), index = False)


print(ident + 'Saving Dates')

#Saves the dates
with open(os.path.join(data_dir, 'data_stages',location_folder_name, 'unified/README.txt'), 'w') as file:

	file.write('Current max dates for databases:' + '\n')
	file.write('   Movement: {}'.format(movement_date) + '\n')

print(ident + 'Done! Data copied to: {}/unified'.format(location_folder_name))
print(ident + '')