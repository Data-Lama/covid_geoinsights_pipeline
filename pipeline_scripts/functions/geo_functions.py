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
from math import radians, cos, sin, asin, sqrt
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

# Shapefiles map
shape_file_map = {}
shape_file_map['colombia'] = 'colombia'
shape_file_map['gabon'] = 'gadm36_GAB'
shape_file_map['equatorial_guinea'] = 'gadm36_GNQ'
shape_file_map['cameroon'] = 'gadm36_CMR'
shape_file_map['west_africa'] = 'west_africa'




def get_gadm_polygons(location_name, level = 2):
	'''
	Method that gets the  GADM polygons (the ones that facebook uses)
	'''

	if location_name not in shape_file_map:
		raise ValueError(f'No shapefile name found for location: {location_name} please add it!')

	gadm_dir = os.path.join(data_dir, 'geo', 'gadm_polygons', location_name, f"{shape_file_map[location_name]}_{level}.shp")

	if not os.path.exists(gadm_dir):
		raise ValueError('No GADM Polygons found for location: {}. Please save it in: {}'.format(location_name, gadm_dir))

	df_GADM = gpd.read_file(gadm_dir).rename(columns = {f'GID_{level}':'poly_id', f'NAME_{level}':'poly_name'})

	return(df_GADM)


def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    # Radius of earth in kilometers is 6371
    km = 6371* c
    return km


def get_centroids(geometry):
	'''
	Returns the centroids of a given geometry in WS84
	'''

	# Converts to mercator and then back to WS84
	centroids = geometry.to_crs('EPSG:3395').centroid.to_crs("EPSG:4326")
	return(centroids)