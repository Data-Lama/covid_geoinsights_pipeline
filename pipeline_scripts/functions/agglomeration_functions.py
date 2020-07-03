# Agglomerator by radius

# This script offers the functions to agglomerate (create the geographical nodes) by radius, using the
# polygons and geo located samples in the unified data stage.

import numpy as np
import pandas as pd
import os
from sklearn.metrics import pairwise_distances

import geo_functions as geo
km_constant = geo.km_constant


def find_polygons_by_radius(coordinates, final_polygons, radius):
	'''
	Finds the corresponding polygons.
	
	coordinates : pd.DataFrame with columns:
		- lon
		- lat
	'''
	

	distances = pairwise_distances( coordinates, final_polygons[['poly_lon','poly_lat']])
	min_ind = distances.argmin(axis = 1)
	min_values = distances.min(axis = 1)*km_constant

	coordinates['poly_id'] = final_polygons['poly_id'].values[min_ind]
	coordinates['poly_distance'] = min_values

	# Filters out beyond radius
	coordinates = coordinates.loc[coordinates.poly_distance <= radius].copy()
	
	# Merges with population
	coordinates = coordinates.merge(final_polygons, on = ['poly_id'])

	return(coordinates)


def extract_geometry(df):
	'''
	Constructs the geometry of Multipoints, given a dataframe
	'''

	response = 'MULTIPOINT (' + ','.join(df.apply(lambda row: '{} {}'.format(row.lon, row.lat), axis = 1).values) + ')'
	return(response)



def agglomerate_by_radius(polygons, cases, movement, population, radius):
	'''
	Function that aglomerates the cases, movement, population and polygons into geographical nodes

	Returns
	---------
	Four elements
		- Polygons: pd.DataFrame with the final polygons
		- Cases: pd.DataFrame with the cases asociated eith the final polygons
		- Movement:
		- Population:

	'''
	
	# Constructs the polygons as follows.
	# Creates a list of cases_locations sorted by most cases to least cases
	# Takes the first elelenet of the list, Searches for the closest polygon,
	# If the closest polygon is closer than two times the radius to another one already in the assigned ones, 
	# the polygon is discarted and the location is asociated with the that one.


	cases_locations  = cases[['location','lon','lat','num_cases']].groupby(['location','lon','lat']).sum().reset_index().sort_values('num_cases', ascending = False)
	cases_locations['poly_id'] = None
	cases_locations['poly_name'] = None
	cases_locations['poly_lon'] = None
	cases_locations['poly_lat'] = None
	cases_locations['poly_distance'] = None
	cases_locations['poly_merged'] = False


	while cases_locations.poly_id.isna().sum() > 0:

		#Current
		current = cases_locations.loc[cases_locations.poly_id.isna()].iloc[0]
		current_index = current.name

		#Extracted
		extracted = cases_locations.loc[~cases_locations.poly_id.isna()]

		# Checks for the closest polygon
		res = pairwise_distances( current[['lon','lat']].values.reshape(1, -1), polygons[['poly_lon','poly_lat']])
		min_poly_index = res.argmin(axis = 1)[0]
		min_poly_value = res.min(axis = 1)[0]*(km_constant)

		current_poly = polygons.iloc[[min_poly_index]][['poly_id','poly_name','poly_lon','poly_lat']]


		# Checks is closes polygon is already in the range of any of the extracted values
		if extracted.shape[0] > 0:

			# Checks if there are any already assigned
			res = pairwise_distances( current_poly[['poly_lon','poly_lat']], extracted[['poly_lon','poly_lat']])
			min_val = res.min()*(km_constant)
			min_ind = res.argmin()

		else:
			min_val = np.inf

		if min_val < 2*radius:

			# Polygon alrady exists
			cases_locations.loc[[current_index], ['poly_id', 'poly_name', 'poly_lon', 'poly_lat']] = extracted.iloc[[min_ind]][['poly_id', 'poly_name', 'poly_lon', 'poly_lat']].values

			# Final Min
			final_min = pairwise_distances( current[['lon','lat']].values.reshape(1, -1), extracted.iloc[[min_ind]][['poly_lon','poly_lat']].values.reshape(1, -1)).min()
			cases_locations.loc[current_index, 'poly_distance'] = final_min
			cases_locations.loc[current_index, 'poly_merged'] = True

		else:

			cases_locations.loc[[current_index], ['poly_id', 'poly_name', 'poly_lon', 'poly_lat']] = current_poly.values
			cases_locations.loc[current_index, 'poly_distance'] = min_poly_value


	# Sets the final polygons
	final_polygons = cases_locations[['poly_id', 'poly_name', 'poly_lon', 'poly_lat', 'num_cases']].groupby(['poly_id', 'poly_name', 'poly_lon', 'poly_lat']).sum().reset_index().sort_values('num_cases', ascending = False)

	# Creates the geometry
	geometry = cases_locations.groupby('poly_id').apply(extract_geometry).reset_index().rename(columns = {0:'geometry'})

	final_polygons = final_polygons.merge(geometry, on = 'poly_id')

	# Adds the polygon construction to the cases
	final_cases = cases.merge(cases_locations[['location','poly_id','poly_name','poly_lon','poly_lat', 'poly_distance','poly_merged']], on = 'location')

	# Agglomerates the movements and  population by the radius
	# Populations
	# ---------------------
	coordinates = population[['lon','lat']].drop_duplicates()
	coordinates = find_polygons_by_radius(coordinates, final_polygons, radius)
	final_population = population.merge(coordinates, on = ['lon','lat'])



	# Movements
	# ---------------------

	column_dict = {'poly_id':'start_poly_id', 'poly_distance':'start_poly_distance', 'poly_name':'start_poly_name', 'poly_lon':'start_poly_lon', 'poly_lat':'start_poly_lat'}

	# Start
	coordinates = movement[['start_movement_lon','start_movement_lat']].drop_duplicates()

	coordinates.rename(columns = {'start_movement_lon':'lon', 'start_movement_lat':'lat'}, inplace = True)
	coordinates = find_polygons_by_radius(coordinates, final_polygons, radius)
	coordinates.rename(columns = {'lon':'start_movement_lon', 'lat':'start_movement_lat'}, inplace = True)

	final_movement = movement.merge(coordinates, on = ['start_movement_lon','start_movement_lat'])
	
	final_movement.rename(columns = column_dict, inplace = True)

	# End
	for k in column_dict:
		column_dict[k] = column_dict[k].replace('start','end')

	coordinates = movement[['end_movement_lon','end_movement_lat']].drop_duplicates()

	coordinates.rename(columns = {'end_movement_lon':'lon', 'end_movement_lat':'lat'}, inplace = True)
	coordinates = find_polygons_by_radius(coordinates, final_polygons, radius)
	coordinates.rename(columns = {'lon':'end_movement_lon', 'lat':'end_movement_lat'}, inplace = True)

	final_movement = final_movement.merge(coordinates, on = ['end_movement_lon','end_movement_lat'])
	final_movement.rename(columns = column_dict, inplace = True)


	
	return(final_polygons, final_cases, final_population, final_movement)
