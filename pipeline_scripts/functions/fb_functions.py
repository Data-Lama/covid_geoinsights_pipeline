# Facebook unifier

# This class provides simple functions for unifiyng facebook data
# These functions assume that:
# 	- Movement files are in: movement_between_tiles
#   - populations files are in: population_tiles


# Necesary imports
import pandas as pd
import numpy as np
from pathlib import Path
import os
from geopy.geocoders import Nominatim
import time
from sklearn.metrics import pairwise_distances
from datetime import timedelta, datetime
import ot

import time
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException 
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.firefox.options import Options

import geo_functions as geo


#Directories
from global_config import config
data_dir     = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')

# Facebook user and password
user = config.get_property('fb_user')
pwd = config.get_property('fb_pwd')

data_stages_location = os.path.join(data_dir, 'data_stages')


MOVEMENT_COLS = set(['geometry', 'date_time', 'start_polygon_id', 'start_polygon_name','end_polygon_id', 
					 'end_polygon_name', 'length_km', 'tile_size','country', 'level', 'n_crisis', 'n_baseline', 
					 'n_difference','percent_change', 'is_statistically_significant', 'z_score','start_lat', 
					 'start_lon', 'end_lat', 'end_lon', 'start_quadkey','end_quadkey'])


def get_driver(download_dir):
	'''
	Gets the dirver with the automatic download options to the received directory
	'''


	desktop = True

	if desktop:

		fp = webdriver.FirefoxProfile()
		fp.set_preference("browser.download.folderList", 2)
		fp.set_preference("browser.download.manager.showWhenStarting", False)
		fp.set_preference("browser.download.dir", download_dir)
		fp.set_preference("browser.helperApps.neverAsk.saveToDisk", "text/csv")
		driver = webdriver.Firefox(firefox_profile=fp)

	else:

		fp = webdriver.FirefoxProfile()
		fp.set_preference("browser.download.folderList", 2)
		fp.set_preference("browser.download.manager.showWhenStarting", False)
		fp.set_preference("browser.download.dir", download_dir)
		fp.set_preference("browser.helperApps.neverAsk.saveToDisk", "text/csv")

		cap = DesiredCapabilities().FIREFOX
		cap["marionette"] = True
		binary = '/opt/conda/bin/FirefoxApp/firefox' 

		options = Options()
		options.set_headless(headless=True)
		options.binary = binary

		driver = webdriver.Firefox(firefox_options=options, capabilities=cap, firefox_profile=fp)


	return(driver)


def login_driver(driver):
	'''
	Logins to facebook on the driver
	'''

	# Landing page
	landing_page = "https://www.facebook.com/login/?next=https%3A%2F%2Fwww.facebook.com%2Fgeoinsights-portal%2F"

	driver.get(landing_page)

	# Logins

	# Random Sleeps
	time.sleep(np.random.randint(1,3))

	# Accept cookies (if found)
	try:
		driver.find_element_by_xpath("//button[@data-cookiebanner='accept_button']").click()

	except NoSuchElementException:
		# Doe nothig
		pass


	time.sleep(np.random.randint(1,3))

	# User
	inputElement = driver.find_element_by_id("email")
	inputElement.clear()
	inputElement.send_keys(user)

	# Pwd
	inputElement = driver.find_element_by_id("pass")
	inputElement.clear()
	inputElement.send_keys(pwd)

	# Random Sleeps
	time.sleep(np.random.randint(1,3))

	# Enters
	driver.find_element_by_id('loginbutton').click()

	time.sleep(np.random.randint(3,5))

	return(driver)



def download_fb_file(driver, type_data, dataset_id, date, extra_param = None, timeout = 20):
	'''
	Downloads the given date for the dataset
	'''

	string_date = format_date_by_type(type_data, date)
	time.sleep(np.random.randint(1,3))

	url_dir = 'https://www.facebook.com/geoinsights-portal/downloads/vector/?id={}&ds={}'.format(dataset_id, string_date)
	if extra_param is not None:
		url_dir += '&{}'.format(extra_param)

	driver.set_page_load_timeout(timeout)

	try:
		driver.get(url_dir)
	except:
		# The driver freezes after download
		pass

	time.sleep(np.random.randint(2,5))



def check_movement_integrity(folder_name, fb_location_name, type_data, start_date, end_date):
	'''
	Checks the integrity of the movement files
	'''
	

	directory = os.path.join(data_stages_location, folder_name, 'raw/', get_folder_name_by_type(type_data))
	
	if not os.path.exists(directory):
		os.makedirs(directory)

	if type_data == 'movement':

		name_string = '{}'.format(fb_location_name)
		type_data_string = 'Movement between Tiles'
		
		# Wtong files
		wrong_files = []
		
		# Expected dates
		hours = [0,8,16]

		dates = set()

		for d in pd.date_range(start=start_date, end=end_date):
			for h in hours:
				dates.add(d + timedelta(hours = h))
		
		# Iterates over the files in folder
		for file in os.listdir(directory):
			if file.endswith('.csv'):
				if name_string not in file or type_data_string not in file:
					wrong_files.append(file)
				elif os.stat(os.path.join(directory, file)).st_size > 0:

					date_string = file.split('_')[-1]
					date_string = date_string.split('.')[0]
					d = pd.to_datetime(date_string)
					if d in dates:
						dates.remove(d)
				
		
		return(wrong_files, list(dates))


	elif type_data == 'movement_range':

		name_string = '{}'.format(fb_location_name)
		type_data_string = 'Movement Range_'
		
		# Wtong files
		wrong_files = []
		
		dates = set()

		for d in pd.date_range(start=start_date, end=end_date):
			dates.add(d )
		
		# Iterates over the files in folder
		for file in os.listdir(directory):
			if file.endswith('.csv'):
				if name_string not in file or type_data_string not in file:
					wrong_files.append(file)
				else:
					date_string = file.split('_')[-1]
					date_string = date_string.split('.')[0]
					d = pd.to_datetime(date_string)
					if d in dates:
						dates.remove(d)
				
		
		return(wrong_files, list(dates))


	else:
		raise ValueError('Data Type of Extraction not supported: {}'.format(type_data))


def get_folder_name_by_type(type_data):

	if type_data == 'movement':
		return('movement_between_tiles/')

	elif type_data == 'movement_range':
		return('movement_range')

	else:
		raise ValueError('Data Type of Extraction not supported: {}'.format(type_data))


def format_date_by_type(type_data, date):

	if type_data == 'movement':
		return(date.strftime("%Y-%m-%d+%H%M"))

	elif type_data == 'movement_range':
		return(date.strftime("%Y-%m-%d"))

	else:
		raise ValueError('Data Type of Extraction not supported: {}'.format(type_data))


def clean_fb_folders(location_folder_name, stage = 'raw'):
	'''
	Cleans the facebook folders
	'''

	clean_folder(directory = os.path.join(data_stages_location, location_folder_name, stage, 'movement_between_tiles/'))
	clean_folder(directory = os.path.join(data_stages_location, location_folder_name, stage, 'population_tiles/'))



def clean_folder(directory):
	'''
	Unifies the names of each file so it only includes the date. Strips everything else
	'''

	# Cleans folder
	for file in os.listdir(directory):
		if file.endswith('.csv'):
			new_name = file.split('_')[-1]
			os.rename(os.path.join(directory, file) , os.path.join(directory, new_name)  )



def read_single_file(file_name, dropna = False, parse_dates = False):
	'''
	Reads a single file and returns it as a pandas data frame.
	This method is designed for te facebook files
	'''

	try:
		if parse_dates:
			df = pd.read_csv(file_name, parse_dates=["date_time"], date_parser=lambda x: pd.to_datetime(x, format="%Y-%m-%d %H%M"), na_values = ['\\N'])
		else: 
			df = pd.read_csv(file_name, na_values = ['\\N'])

		if dropna:
			df.dropna(inplace = True)

		return(df)

	except pd.errors.EmptyDataError:
		raise ValueError(f'File {file_name} is empty, please download it again.')





def build_dataset_in_directory(directory, dropna = False):
	'''
	Loads all the file sinto a single dataframe.
	This method is designed for te facebook files
	'''


	datasets = []
	for file in os.listdir(directory):
		file_name = os.path.join(directory, file)
		if file_name.endswith('.csv') and os.stat(file_name).st_size > 0:		
			datasets.append(read_single_file(file_name, dropna = dropna))

	df = pd.concat(datasets, ignore_index = True)
	return(df)




def build_movement(location_folder_name, stage = 'raw'):
	'''
	Method that build the movement database
	'''

	# Constructs the directory
	directory = os.path.join(data_stages_location, location_folder_name, stage, 'movement_between_tiles/')

	# Builds the dataset
	df = build_dataset_in_directory(directory)

	# Adds the geometry
	df['start_movement_lon'] = df.geometry.apply(lambda g: geo.extract_lon(g, pos = 1))
	df['start_movement_lat'] = df.geometry.apply(lambda g: geo.extract_lat(g, pos = 1))
	df['end_movement_lon'] = df.geometry.apply(lambda g: geo.extract_lon(g, pos = 2))
	df['end_movement_lat'] = df.geometry.apply(lambda g: geo.extract_lat(g, pos = 2))
	return(df)


def export_movement_batch(final_file_location, location_folder_name, stage = 'raw', max_date = None, dropna = False, ident = '    '):
	'''
	Method that exports all the files in a given location into a single file.

	This method does the same thing that build_movment, but lowering the memory consumption. 

	Returns the max date
	'''

	header = False

	global_max_date = None
	
	# Declares the directory
	directory = os.path.join(data_stages_location, location_folder_name, stage, 'movement_between_tiles/')

	for file in os.listdir(directory):
		file_name = os.path.join(directory, file)
		if file_name.endswith('.csv') and os.stat(file_name).st_size > 0:		
			
			df = read_single_file(file_name, dropna = dropna, parse_dates = True)

			# Adds geometry
			if set(df.columns) != MOVEMENT_COLS:
				print(ident + f"File {file} is corrupted\n"  + ident + "   Will remove.")
				os.remove(file_name)
				continue


			df['start_movement_lon'] = df.geometry.apply(lambda g: geo.extract_lon(g, pos = 1))
			df['start_movement_lat'] = df.geometry.apply(lambda g: geo.extract_lat(g, pos = 1))
			df['end_movement_lon'] = df.geometry.apply(lambda g: geo.extract_lon(g, pos = 2))
			df['end_movement_lat'] = df.geometry.apply(lambda g: geo.extract_lat(g, pos = 2))

			# Checks if max date is given
			df.date_time = pd.to_datetime(df.date_time)
			if max_date is not None:
				df = df[df.date_time < max_date]


			if not header: #overwrite
				df.to_csv(final_file_location, index = False)
				header = True
			else: # Append
				df.to_csv(final_file_location, index = False, mode='a', header=False)

			# Computes global date max
			if global_max_date is None:
				global_max_date = df.date_time.max()
			else:
				global_max_date = max(global_max_date, df.date_time.max())

	return global_max_date

def build_population(location_folder_name, stage = 'raw', dropna = True):
	'''
	Method that build the population database
	'''

	# Constructs the directory
	directory = os.path.join(data_stages_location, location_folder_name, stage, 'population_tiles/')

	return(build_dataset_in_directory(directory, dropna = dropna))

def build_empty_population():
	'''
	Method that returns an empyt dataframe with the population structure
	'''

	cols = ['lat','lon','country','date_time','n_baseline','n_crisis','n_difference','density_baseline','density_crisis','percent_change','clipped_z_score','ds']
	df = pd.DataFrame(columns = cols)

	return(df)


def build_movement_range(location_folder_name, stage = 'raw', dropna = True):
	'''
	Method that build the population database
	'''

	# Constructs the directory
	directory = os.path.join(data_stages_location, location_folder_name, stage, 'movement_range/')

	return(build_dataset_in_directory(directory, dropna = dropna))