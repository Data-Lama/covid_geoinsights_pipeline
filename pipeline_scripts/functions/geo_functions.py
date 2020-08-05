# Geo functions



# Necesary imports




import os
import ot
import time
import numpy as np
import pandas as pd
from shapely import wkt
import geopandas as gpd
from pathlib import Path
from datetime import timedelta
from geopy.geocoders import Nominatim
from sklearn.metrics import pairwise_distances


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
	Extracts the latitude from the polygon string
	'''
	poly = poly[12:-1]
	poly = poly.replace(', ', ',')
	poly = poly.split(',')[pos - 1]
	resp = poly.split(' ')[1]
	
	return(float(resp))

# ----------------------------------------
# --- Movement Range to Movement Tile ----
# ----------------------------------------


def get_GADM_polygon(GADM_id, df_GADM=None):
	if df_GADM == None:
		path = os.path.join(data_dir, 'data_stages', 'colombia', 'raw', 'geo', 'gadm36_COL_shp', 'gadm36_COL_2.shp')
		# GADM geodataset originally crs = {epsg:4326}
		df_GADM = gpd.read_file(path)
	df_GADM.set_index("GID_2", inplace=True)
	# polygon = df_GADM[df_GADM['GID_2'] == GADM_id]['geometry']
	# polygons = []
	polygon = df_GADM.at[GADM_id, 'geometry']
	return polygon

def get_GADM_popdensity(GADM_id, df_GADM=None):
	if df_GADM == None:
		path = os.path.join(data_dir, 'data_stages', 'colombia', 'raw', 'geo', 'gadm36_COL_shp', 'gadm36_COL_2_population_density.csv')
		# GADM geodataset originally crs = {epsg:4326}
		df_GADM = pd.read_csv(path)
	df_GADM.set_index("GID_2", inplace=True)
	polygon = df_GADM.at[GADM_id, 'geometry']
	pop_density = df_GADM.at[GADM_id, 'population_density']
	return pop_density