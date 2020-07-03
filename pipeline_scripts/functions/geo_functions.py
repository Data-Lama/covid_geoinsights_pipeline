# Geo functions



# Necesary imports
import pandas as pd
import numpy as np
from pathlib import Path
import os
from geopy.geocoders import Nominatim
import time
from sklearn.metrics import pairwise_distances
from datetime import timedelta
import ot


#Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')


geo_loc = os.path.join(data_dir,'geo/locations.csv')

km_constant = 110.567


# ----------------------------------------
# ------- Geographical Coordinates -------
# ----------------------------------------

def get_geo(country):
	'''
	Loads the geo dataset
	'''

	location = geo_loc
	df = pd.read_csv(location)

	df = df.loc[df.country_name.apply(lambda s : s.upper()) == country.upper()].copy()

	return(df)

	

def geolocate(country, state, city):
	'''
	Function for geolocating a given place
	
	Returns
	----------
	tuple:
		(lon,lat)
	'''
	
	to_search = city + ', ' + state + ', ' + country
	
	# Starts the geolocator
	geolocator = Nominatim(user_agent="other_app")

	location = geolocator.geocode(to_search)
	lon = location.longitude
	lat = location.latitude
	
	return(lon, lat)



def save_new_location(geo_id, country_id, state_name,city_name,lon,lat):
	'''
	Saves a new location
	'''
	
	location = geo_loc
	with open(location, "a") as file:
		file.write('{},{},{},{},{},{} \n'.format(geo_id, country_id, state_name,city_name,lon,lat))




def extract_lon(poly, pos = 1):
	'''
	Extracts the longitud from the polygon string
	'''
	poly = poly[12:-1]
	poly = poly.replace(', ', ',')
	poly = poly.split(',')[pos - 1]
	resp = poly.split(' ')[0]
	
	return(float(resp))


def extract_lat(poly, pos = 1):
	'''
	Extracts the longitud from the polygon string
	'''
	poly = poly[12:-1]
	poly = poly.replace(', ', ',')
	poly = poly.split(',')[pos - 1]
	resp = poly.split(' ')[1]
	
	return(float(resp))