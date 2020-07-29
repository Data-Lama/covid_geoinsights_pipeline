# Time series functions

from fastdtw import fastdtw
from scipy.spatial.distance import euclidean
import numpy as np
import pandas as pd
import os


#Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')


data_folder = os.path.join(data_dir, 'data_stages')


ident = '   '


def extract_location_cases_distance(location, agglomeration_method, start_day = 0, lag = 0, strech = 1, base_locations = None, verbose = False, smooth_days = 1, types = ['state','country']):
	'''
	Extracts the most similar locations to the given one
	'''
	
	# extracts the possible locations
	if base_locations is None:
		base_locations = list(os.listdir(data_folder))


	current = extract_timeseries_cases(location, agglomeration_method, polygon_id = None, lag = lag, smooth_days = smooth_days)

	if start_day > current.day.max():
		raise ValueError('The time series for {} of {} has only {} days. Cannot start at: {}'.format(polygon_id, location, current.day.max, strat_day))

	current = current[current.day >= start_day]
	
	current_total_cases = current['value'].sum()


	# Extracts end_day
	end_day = current.day.max()


	res = []

	# Extracts the time series of the given location
	for loc in base_locations:

		# Reads Description
		desc = pd.read_csv(os.path.join(data_folder, loc, 'description.csv'), index_col = 0)
		loc_type = desc.loc['type','value']
		loc_name = desc.loc['name','value']
		
		if loc != location and  has_aglomeration(loc, agglomeration_method) and loc_type in types:
			

			if verbose:
				print(ident + loc)

				
			# Extarcts Other
			other = extract_timeseries_cases(loc, agglomeration_method, polygon_id = None, smooth_days = smooth_days)
			other_max_day = other.day.max()
				
			# Filters
			other = other[(other.day >= start_day) & (other.day <= end_day)]
			
			
			# Distance
			distance = np.inf
			if np.abs(other.shape[0] - current.shape[0]) <= strech:
				
				distance = distance_between_timeseries(current['value'].values, other['value'].values)/current_total_cases
				
				# Calculates total cases
				total_cases_dif = np.abs(current_total_cases - other['value'].sum())
						
				
				if verbose:
					print(ident + '   {}, {}, {}'.format(poly,other_max_day, distance ))

				res.append({'location':loc, 'dist': distance, 'max_day':other_max_day, 'total_cases_dif':total_cases_dif})
			
	
	df = pd.DataFrame(res).sort_values('dist').reset_index(drop = True)
	
	return(df)




def distance_between_timeseries(series_1, series_2):
	'''
	Distance between time series (using Dynamic Time Warping)
	'''
	
	distance, _ = fastdtw(series_1, series_2, dist=euclidean)
	
	return(distance)


def adjust_timeseries(timeseries, start, end):

	timeseries = timeseries.merge(pd.DataFrame({'day':range(start_day, end_day + 1)}), on = 'day', how = 'right').sort_values('day')
	return(timeseries)


def accumulate_timeseries(timeseries, col):
	timeseries = timeseries.sort_values('day')
	timeseries[col] = timeseries[col].rolling(min_periods=1, window=timeseries.shape[0]).sum()
	return(timeseries)



def has_aglomeration(location, agglomeration_method):

	cases_location = os.path.join(data_folder, location, 'agglomerated', agglomeration_method, 'cases.csv')

	return(os.path.exists(cases_location))
		


def extract_polygon_cases_distance(location, agglomeration_method, polygon_id, start_day = 0, lag = 0, strech = 1, base_locations = None, verbose = False, smooth_days = 1):
	'''
	Extracts the most similar locations to the given one based on the cases distance, using Dynamic Time Warping
	'''
	
	# extracts the possible locations
	if base_locations is None:
		base_locations = list(os.listdir(data_folder))


	current = extract_all_timeseries_cases(locations = [location], polygons_ids = [polygon_id], agglomeration_method = agglomeration_method, lag = lag, smooth_days = smooth_days, verbose = verbose)

	if start_day > current.day.max():
		raise ValueError('The time series for {} of {} has only {} days. Cannot start at: {}'.format(polygon_id, location, current.day.max(), start_day))

	# Filters for the start day
	current = current[current.day >= start_day]
	

	# Extracts the other locations
	df_all_locations = extract_all_timeseries_cases(locations = base_locations,  agglomeration_method = agglomeration_method, lag = lag, smooth_days = smooth_days, verbose = verbose)
	
	# Filters for the start day and end day
	df_all_locations = df_all_locations[(df_all_locations.day >= start_day) & (df_all_locations.day <= current.day.max())]


	df = construct_distance(current, df_all_locations, strech, verbose)

	return(df)



def extract_polygon_movement_distance(location, agglomeration_method, polygon_id, movement_type = "BOTH", start_day = 0, lag = 0, strech = 1, base_locations = None, verbose = False, smooth_days = 1):
	'''
	Extracts the most similar locations to the given one based on the movement distance, using Dynamic Time Warping
	'''
	
	# extracts the possible locations
	if base_locations is None:
		base_locations = list(os.listdir(data_folder))


	current = extract_all_timeseries_movement(locations = [location], polygons_ids = [polygon_id],  movement_type = movement_type,  agglomeration_method = agglomeration_method, lag = lag, smooth_days = smooth_days, verbose = verbose)

	if start_day > current.day.max():
		raise ValueError('The time series for {} of {} has only {} days. Cannot start at: {}'.format(polygon_id, location, current.day.max(), start_day))

	# Filters for the start day
	current = current[current.day >= start_day]
	

	# Extracts the other locations
	df_all_locations = extract_all_timeseries_movement(locations = base_locations, movement_type = movement_type, agglomeration_method = agglomeration_method, lag = lag, smooth_days = smooth_days, verbose = verbose)
	
	# Filters for the start day and end day
	df_all_locations = df_all_locations[(df_all_locations.day >= start_day) & (df_all_locations.day <= current.day.max())]


	df = construct_distance(current, df_all_locations, strech, verbose)

	return(df)	




def construct_distance(current, df_all_locations, strech, verbose = False):
	'''
	Computes the distance between the current and all locations
	'''

	current_location = current.location_id.values[0]
	current_polygon_id = current.polygon_id.values[0]

	# Total value
	current_total = current['value'].sum()

	res = []

	# Extracts the time series of the given location
	for loc in df_all_locations.location_id.unique():
		
		if verbose:
			print(ident + loc)


		df_location = df_all_locations[df_all_locations.location_id == loc].copy()
		df_location.index = df_location.polygon_id

		all_polygons = df_location.polygon_id.unique()

		i = 0
		for poly in all_polygons:

			i += 1
			
			# Extracts name
			poly_name = df_location.loc[df_location.polygon_id == poly, 'polygon_name'].values[0]

			# Extarcts Other
			df_other = df_location[df_location.polygon_id == poly]
			other_max_day = df_other.day.max()
			

			# Distance
			distance = np.inf

			if np.abs(df_other.shape[0] - current.shape[0]) <= strech:					
				distance = distance_between_timeseries(current['value'].values, df_other['value'].values)/current_total
				
			# Calculates total cases
			total_dif = np.abs(current_total - df_other['value'].sum())
					
			
			if verbose:
				print(ident + f'   {poly} ({i} of {len(all_polygons)}). Max Day: {other_max_day}, Distance: {distance}')

			res.append({'location':loc, 'polygon_id':poly, 'polygon_name': poly_name,'dist': distance, 'max_day':other_max_day, 'total_dif':total_dif})
		
	
	df = pd.DataFrame(res)
	df = df[(df.loc != current_location) & (df.polygon_id != current_polygon_id)].sort_values('dist').reset_index(drop = True)
	
	return(df)


def extract_timeseries_cases(location, agglomeration_method, polygon_id, lag = 0, accum = False, smooth_days = 1):
	'''
	Extrtacts the time series of cases for a single place
	if polygon id is none, will return the entire place
	'''

	if polygon_id is None:
		df = extract_all_timeseries_cases(locations = [location], polygons_ids = None, agglomeration_method = agglomeration_method, lag = lag, smooth_days = smooth_days,  accum = accum)
		df = df[['location_id','day', 'value']].groupby(['location_id', 'day']).sum().reset_index()

		return(df)

	else:
		return(extract_all_timeseries_cases(locations = [location], polygons_ids = [polygon_id], agglomeration_method = agglomeration_method, lag = lag, smooth_days = smooth_days,  accum = accum))




def extract_timeseries_movement(location, agglomeration_method, polygon_id, movement_type = "BOTH", lag = 0, accum = False, smooth_days = 1):
	'''
	Extrtacts the time series of cases for a single place
	'''

	if polygon_id is None:
		df = extract_all_timeseries_movement(locations = [location], polygons_ids = None,  movement_type = movement_type,  agglomeration_method = agglomeration_method, lag = lag, smooth_days = smooth_days, accum = accum)
		df = df[['location_id','day', 'value']].groupby(['location_id', 'day']).sum().reset_index()

		return(df)

	return(extract_all_timeseries_movement(locations = [location], polygons_ids = [polygon_id],  movement_type = movement_type,  agglomeration_method = agglomeration_method, lag = lag, smooth_days = smooth_days, accum = accum))



def extract_all_timeseries_cases(locations, agglomeration_method, lag = 0, accum = False, smooth_days = 1, verbose = False, polygons_ids = None):
	'''
	Extract all the time series for the number cases of the given locations. Results are stored in a data frame with the following
	structure:
		- location_id (str): Location ID
		- polygon_id (str): The polygonstring id
		- day (int): the day
		- value (float) The coases on that day

	If polygons ids is none will compute all of them but if list is received will only compute the ones in the list
	'''

	all_dfs = []

	for location in locations:


		cases_location = os.path.join(data_folder, location, 'agglomerated',agglomeration_method,'cases.csv')
		poly_location = os.path.join(data_folder, location, 'agglomerated',agglomeration_method,'polygons.csv')	

		if not has_aglomeration(location, agglomeration_method):
			if verbose:
				print('No Agglomerated Cases for {}'.format(location))
			continue

		# Reads Cases
		df_cases_all = pd.read_csv(cases_location, parse_dates = ['date_time'])
		df_polygons = pd.read_csv(poly_location)
		df_polygons.index = df_polygons.poly_id

		all_polygons = df_cases_all.poly_id.unique()

		if polygons_ids is not None:
			all_polygons = set(all_polygons).intersection(polygons_ids)


		for polygon_id in all_polygons:

			
			df_cases = df_cases_all.loc[df_cases_all.poly_id == polygon_id,['date_time','num_cases']].groupby('date_time').sum().reset_index().sort_values('date_time')

	
			# Adds lag
			df_cases = df_cases.iloc[lag:]
			
			# Creates day
			df_cases['day'] = (df_cases.date_time - df_cases.date_time.min()).apply(lambda dif: dif.days)

			# Fills in the blanks
			df_cases = df_cases[['day','num_cases']].merge(pd.DataFrame({'day':range(df_cases.day.max() + 1)}), on = 'day', how = 'right').fillna(0).sort_values('day')
			
			# Smoothes
			df_cases['num_cases'] = df_cases.num_cases.rolling(smooth_days, min_periods=1).mean()

			if accum:
				df_cases = accumulate_timeseries(df_cases, 'num_cases')


			df_cases['location_id'] = location
			df_cases['polygon_id'] = polygon_id
			df_cases['polygon_name'] = df_polygons.loc[polygon_id,'poly_name']

			df_cases.rename(columns = {'num_cases':'value'}, inplace = True)			

			# Adds It
			all_dfs.append(df_cases)
	

	return(pd.concat(all_dfs, ignore_index = True))	




def extract_all_timeseries_movement(locations, agglomeration_method, movement_type = "BOTH", lag = 0, accum = False, smooth_days = 1, verbose = False, polygons_ids = None):
	'''
	Extract all the time series for the movement of the given locations. Resultds are stores in a data frame with the following
	structure:
		- location_id (str): Location ID
		- polygon_id (str): The polygonstring id
		- day (int): the day
		- value (float) The movement on that day
	'''

	all_dfs = []

	for location in locations:

		if not has_aglomeration(location, agglomeration_method):
			if verbose:
				print('No Agglomerated movement for {}'.format(location))
			continue
				
		mov_location = os.path.join(data_folder, location, 'agglomerated',agglomeration_method,'movement.csv')		
		poly_location = os.path.join(data_folder, location, 'agglomerated',agglomeration_method,'polygons.csv')	

		# Reads Movement
		df_mov_all = pd.read_csv(mov_location, parse_dates = ['date_time'])
		df_polygons = pd.read_csv(poly_location)

		df_polygons.index = df_polygons.poly_id


		all_polygons = set(df_mov_all.start_poly_id.unique()).union(set(df_mov_all.end_poly_id.unique()))
		
		if polygons_ids is not None:
			all_polygons = all_polygons.intersection(polygons_ids)


		for polygon_id in all_polygons:

			# Iterates over the type
			if movement_type.upper() == "INTERNAL":
				df_mov = df_mov_all[(df_mov_all.start_poly_id == polygon_id) & (df_mov_all.end_poly_id == polygon_id)]

			elif movement_type.upper() == "EXTERNAL":
				df_mov = df_mov_all[(df_mov_all.start_poly_id == polygon_id) | (df_mov_all.end_poly_id == polygon_id)]
				df_mov = df_mov[df_mov.start_poly_id != df_mov.end_poly_id ]

			elif movement_type.upper() == "BOTH":
				df_mov = df_mov_all[(df_mov_all.start_poly_id == polygon_id) | (df_mov_all.end_poly_id == polygon_id)]

			else:
				raise ValueError(f'No Support for movement type: {movement_type}')

		
			df_mov = df_mov[['date_time','movement']].groupby('date_time').sum().reset_index().sort_values('date_time')


			# Adds lag
			df_mov = df_mov.iloc[lag:]
			
			# Creates day
			df_mov['day'] = (df_mov.date_time - df_mov.date_time.min()).apply(lambda dif: dif.days)

			# Fills in the blanks
			df_mov = df_mov[['day','movement']].merge(pd.DataFrame({'day':range(df_mov.day.max() + 1)}), on = 'day', how = 'right').fillna(0).sort_values('day')
			
			# Smoothes
			df_mov['movement'] = df_mov.movement.rolling(smooth_days, min_periods=1).mean()

			if accum:
				df_mov = accumulate_timeseries(df_mov, 'movement')


			df_mov['location_id'] = location
			df_mov['polygon_id'] = polygon_id
			df_mov['polygon_name'] = df_polygons.loc[polygon_id,'poly_name']

			df_mov.rename(columns = {'movement':'value'}, inplace = True)

			# Adds It
			all_dfs.append(df_mov)
	

	return(pd.concat(all_dfs, ignore_index = True))		




def get_closest_neighbors(location, agglomeration_method, polygon_id, k = None, start_day = 0, lag = 0, strech = 3, base_locations = None, verbose = False, smooth_days = 1, types = ['state','country']):
	



	if polygon_id != None:
		df_mov = extract_polygon_movement_distance(location = location,
									  agglomeration_method = agglomeration_method,
									  movement_type = "BOTH",
									  polygon_id = polygon_id, 
									  start_day = start_day, 
									  lag = lag, 
									  strech = strech, 
									  base_locations = base_locations, 
									  verbose = False, 									  
									  smooth_days = smooth_days)

		df_cases = extract_polygon_cases_distance(location = location,
									  agglomeration_method = agglomeration_method,
									  polygon_id = polygon_id, 
									  start_day = start_day, 
									  lag = lag, 
									  strech = strech, 
									  base_locations = base_locations, 
									  verbose = False, 									  
									  smooth_days = smooth_days)

		df_mov.rename(columns = {'dist':'mov_dist', 'total_dif':'total_dif_mov', 'max_day':'max_day_mov'}, inplace = True)
		df_cases.rename(columns = {'dist':'cases_dist', 'total_dif':'total_dif_cases',  'max_day':'max_day_cases'}, inplace = True)


		df = df_cases.merge(df_mov, on = ['location','polygon_id','polygon_name'])
		df['dist'] = df['mov_dist'] + df['cases_dist']
		df.sort_values('dist', inplace = True)

	else:


		df = extract_location_cases_distance(location = location, 
										agglomeration_method = agglomeration_method, 
										start_day = start_day, 
										lag = lag, 
										strech = strech, 
										base_locations = base_locations, 
										verbose = verbose, 
										smooth_days = smooth_days, 
										types = types)
	
	# Extract only the k closest (if k is given)
	if k != None and k > 0:
		return(df.head(min(df.shape[0],k)))
	else:
		return(df)


