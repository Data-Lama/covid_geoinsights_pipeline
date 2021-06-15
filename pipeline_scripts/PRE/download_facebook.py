# Script that doelioads the facebook movement data and moves it to the corresponding folders
# Imports all the necesary functions
 
import fb_functions as fb
# Other imports
import os, sys
from datetime import datetime, timedelta
from pathlib import Path
import numpy as np
import pandas as pd
import time
import shutil
import zipfile

#Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')


# Dowloads all the facebook data
ident = '         '

print(ident + 'Downloading Facebook Movement Data')
# Sets the location

temp_folder = os.path.join(data_dir, 'downloads/facebook_movement_{}/'.format(datetime.now().strftime('%Y_%m_%d_%H_%M_%S')))
#temp_folder = os.path.join('pipeline_scripts/PRE/temp_folder_{}/'.format(12826453))
configuration_folder = os.path.join('pipeline_scripts/configuration/')

# Creates the temp folder
if not os.path.exists(temp_folder):
	os.makedirs(temp_folder)

# Reads the selected locations
selected_locations_all = pd.read_csv(os.path.join(configuration_folder, 'facebook_extraction.csv'),dtype={'database_id': object}, parse_dates = ['start_date'])
selected_locations = selected_locations_all[selected_locations_all.excecute == 'YES'].copy()



# Creates the current date
end_date = (datetime.today() - timedelta(days = 1)).date()

# If no locations are scheduled
if selected_locations.shape[0] == 0:
	print(ident + 'No Places are scheduled for excecution.')

else:	


	# Boolean that checks if carwling is necesarry
	crawling_necesary = False

	# Results dicitonaries
	missing = []

	print(ident + 'Checking Missing Data for {} Places.'.format(selected_locations.shape[0]))
	# Iterates over the locations
	for ind, row in selected_locations.iterrows():

		_, m = fb.check_movement_integrity(row.location_folder, row.database_id, row.type_data, row.start_date, end_date)
		print(ident + '   {} ({}): {} missing'.format(row.location_name, row.type_data, len(m)))

		if len(m) > 0:
			# Adds them
			d = row.to_dict()
			d['dates'] = m

			# Folder
			destination_folder =  os.path.join(data_dir, 'data_stages/',row.location_folder, 'raw/', fb.get_folder_name_by_type(row.type_data))
			# Makes Directory
			if not os.path.exists(destination_folder):
				os.makedirs(destination_folder)

			d['destination_folder'] = destination_folder
			missing.append(d)

			crawling_necesary = True


	print('')

	print(ident + 'Downloads Missing Data')	
	
	driver = None

	if crawling_necesary:
		driver = fb.get_driver(temp_folder)
		driver = fb.login_driver(driver)

		for row in missing:

			loc = row['location_name']

		
			# Constructs dates batches
			all_dates = np.sort(np.unique([pd.to_datetime(d.strftime("%Y-%m-%d")) for d in row['dates']]))
			max_date = row['dates'][-1]
			days_back = np.array([ (max_date - d).days for d in row['dates']])
			days_back = days_back[days_back < fb.MAX_DAYS_CRAWLING]
			days = np.max(days_back)
					
			print(ident + '   {} ({}): {} days back'.format(row['location_name'], row['type_data'], days))
			
			
			# Dowloads the zip file
			downloaded_file_name = fb.download_zip_movement_interval(driver, temp_folder, row['database_name'], row['database_id'], days = days)

			if downloaded_file_name is None:
				raise ValueError("There was an error downloading the file. Check the scrapper")


			print(ident + '   Finished Downloading')
			print(ident + '   Moves the Downloaded Files')

			
			with zipfile.ZipFile(os.path.join(temp_folder,downloaded_file_name), 'r') as zip_ref:
				zip_ref.extractall(row['destination_folder'])

			# Removes file
			os.remove(os.path.join(temp_folder,downloaded_file_name) )
			
			print(ident + '   Finished.')
			print()


		print(ident + 'Finished Downloading')


		driver.close()

	else:
		print(ident + '   No missing data found')

# Removes Temporal Folder

print()
print(ident + 'Removes Temporal Folder')
if len(os.listdir(temp_folder)) == 0:
	shutil.rmtree(temp_folder)
else:
	print(ident + '   Folder still contains files, will not remove.')

# Prints the final results
print()
print(ident + 'Final Status (Only selected locations)')
# Iterates over the locations
for ind, row in selected_locations.iterrows():

	print(ident + '   {}:'.format(row.location_name))
	destination_folder =  os.path.join(data_dir, 'data_stages/',row.location_folder, 'raw/', fb.get_folder_name_by_type(row.type_data))

	if not os.path.exists(destination_folder):
		print(ident + '      No Folder Detected')
		print()
		continue

	w, m = fb.check_movement_integrity(row.location_folder, row.database_id, row.type_data, row.start_date, end_date)

	# Missing
	print(ident + '      Missing: {}'.format(len(m)))
	for d in m:
		print(ident + '         {}'.format(d))

	# Wrong
	print(ident + '      Wrong Folder: {}'.format(len(w)))
	for wr in w:
		print(ident + '         {}'.format(wr))

	print()



print(ident + 'Done')

	
