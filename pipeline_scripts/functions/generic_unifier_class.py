# Script for Germany Locations



# Necesary imports
import pandas as pd
import numpy as np
from pathlib import Path
import os
import time

import geo_functions as geo
import fb_functions as fb
import extraction_functions as ext_fun


#Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')


class GenericUnifier():

	# Identation variable
	ident = '            '


	def __init__(self, location_name, location_folder_name):

		# Creates the folders
		# Raw
		self.raw_folder = os.path.join(data_dir, 'data_stages', location_folder_name, 'raw')
		# Creates the folder if does not exists
		if not os.path.exists(self.raw_folder):
			os.makedirs(self.raw_folder)

		# Unified
		self.unified_folder = os.path.join(data_dir, 'data_stages', location_folder_name, 'unified')
		# Creates the folder if does not exists
		if not os.path.exists(self.unified_folder):
			os.makedirs(self.unified_folder)


		# Unified
		self.agglomerated_folder = os.path.join(data_dir, 'data_stages', location_folder_name, 'agglomerated')
		# Creates the folder if does not exists
		if not os.path.exists(self.agglomerated_folder):
			os.makedirs(self.agglomerated_folder)

		# Constructed
		self.constructed_folder = os.path.join(data_dir, 'data_stages', location_folder_name, 'constructed')
		# Creates the folder if does not exists
		if not os.path.exists(self.constructed_folder):
			os.makedirs(self.constructed_folder)

		# Other Variables
		self.location_name = location_name
		self.location_folder_name = location_folder_name

		# Description
		self.df_description = pd.read_csv(os.path.join(data_dir, 'data_stages', location_folder_name, 'description.csv'), index_col = 0)


	def get(self, name):
		'''
		Method that gets a characteristic of the description file
		'''
		return(self.df_description.loc[name,'value'])




	def build_cases_geo(self):
		'''
		Method that builds the geolocated cases. 

		Should return a DataFrame with at least the columns:
			- date_time (pd.date_time): time stamp of the case
			- geo_id (str): geographical id of the location
			- location (str): location name
			- lon (float): longitud of the location
			- lat (float): lattitude of the location
			- num_cases (float): the number of cases at that point and time
		'''
		
		pass



	def build_polygons(self):
		'''
		Method that builds the polygons. 

		Should return a DataFrame with at least the columns:
			- poly_id (str): geographical id of the polygon
			- poly_name (str): polygon name name
			- poly_lon (float): longitud of the location
			- poly_lat (float): lattitude of the location

		Generic implemenation builds ploygons from cases
		'''

		df_cases = self.build_cases_geo()
		df_cases = df_cases[['geo_id', 'location','lon', 'lat']].drop_duplicates().rename(columns = {'geo_id':'poly_id', 'location':'poly_name', 'lon':'poly_lon', 'lat':'poly_lat'})
		return(df_cases)


	def build_movement_range_by_polygon(self):
		'''
		Gives the movmeent range by the polygon used in the unifier. By default return None, which does not save anything when excecuted
		'''

		return(None)



	def update_geo(max_tries = 3):
		'''
		Updates the new missing geo_locations
		'''
		
		pass


	


	def generic_build_cases_geo(self, country):
		'''
		Method that builds the geolocated cases by simply corssing geo_id from locations database and cases

		Should return a DataFrame with at least the columns:
			- date_time (pd.date_time): time stamp of the case
			- geo_id (str): geographical id of the location
			- location (str): location name
			- lon (float): longitud of the location
			- lat (float): lattitude of the location
			- num_cases (float): the number of cases at that point and time
		'''
		

		# loads geo 
		df_geo = geo.get_geo(country)
		df_geo['location'] = df_geo.apply(lambda row: '{}-{}'.format(row.city_name, row.state_name), axis = 1)
		
		# Loads cases
		cases = self.build_cases()
		num_cols = [col for col in cases.columns if 'num_' in col] 

		# Adds the geo locations
		merged = cases.merge(df_geo[['geo_id','lon','lat', 'location']], on = ['geo_id'])
		merged = merged[['date_time', 'geo_id', 'location','lon', 'lat'] + num_cols]
		merged = merged.groupby(['date_time', 'geo_id', 'location','lon', 'lat']).sum().reset_index().sort_values('date_time', ascending = False)

		return(merged)