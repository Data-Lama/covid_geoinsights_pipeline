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


def extract_polygon_distance(location, agglomeration_method, polygon_id, start_day = 0, lag = 0, strech = 1, base_locations = None, verbose = False, accum = False, smooth_days = 1):
	'''
	Extracts the most similar locations to the given one
	'''
	
	# extracts the possible locations
	if base_locations is None:
		base_locations = list(os.listdir(data_folder))


	current = extract_timeseries(location, agglomeration_method, polygon_id, lag = lag, smooth_days = smooth_days)

	if start_day > current.day.max():
		raise ValueError('The time series for {} of {} has only {} days. Cannot start at: {}'.format(polygon_id, location, current.day.max, strat_day))

	current = current[current.day >= start_day]
	
	if accum:
		current_total_cases = current.num_cases.max()
	else:
		current_total_cases = current.num_cases.sum()

	# Extracts end_day
	end_day = current.day.max()


	res = []

	# Extracts the time series of the given location
	for loc in base_locations:
		if has_aglomeration(loc, agglomeration_method):

			if verbose:
				print(ident + loc)
			polygons_location = os.path.join(data_folder, loc, 'agglomerated',agglomeration_method, 'polygons.csv')
			polys = pd.read_csv(polygons_location)

			for ind, row in polys.iterrows():

				poly = row.poly_id
				
				# Extarcts Other
				other = extract_timeseries(loc, agglomeration_method, poly, smooth_days = smooth_days)
				other_max_day = other.day.max()
				
				# Filters
				other = other[(other.day >= start_day) & (other.day <= end_day)]
				
				
				# Distance
				distance = np.inf

				if np.abs(other.shape[0] - current.shape[0]) <= strech:					
					distance = distance_between_timeseries(current, other)
					
				# Calculates total cases
				if accum:
					total_cases_dif = np.abs(current_total_cases - other.num_cases.max())
				else:
					total_cases_dif = np.abs(current_total_cases - other.num_cases.sum())
						
				
				if verbose:
					print(ident + '   {}, {}, {}'.format(poly,other_max_day, distance ))

				res.append({'location':loc, 'polygon_id':poly, 'polygon_name':row.poly_name, 'dist': distance, 'max_day':other_max_day, 'total_cases_dif':total_cases_dif})
			
	
	df = pd.DataFrame(res)
	df = df[(df.loc != location) & (df.polygon_id != polygon_id)].sort_values('dist').reset_index()
	
	return(df)


def extract_location_distance(location, agglomeration_method, start_day = 0, lag = 0, strech = 1, base_locations = None, verbose = False, accum = False, smooth_days = 1, types = ['state','country']):
	'''
	Extracts the most similar locations to the given one
	'''
	
	# extracts the possible locations
	if base_locations is None:
		base_locations = list(os.listdir(data_folder))


	current = extract_timeseries(location, agglomeration_method, polygon_id = None, lag = lag, smooth_days = smooth_days)

	if start_day > current.day.max():
		raise ValueError('The time series for {} of {} has only {} days. Cannot start at: {}'.format(polygon_id, location, current.day.max, strat_day))

	current = current[current.day >= start_day]
	
	if accum:
		current_total_cases = current.num_cases.max()
	else:
		current_total_cases = current.num_cases.sum()

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
			other = extract_timeseries(loc, agglomeration_method, polygon_id = None, smooth_days = smooth_days)
			other_max_day = other.day.max()
				
			# Filters
			other = other[(other.day >= start_day) & (other.day <= end_day)]
			
			
			# Distance
			distance = np.inf
			if np.abs(other.shape[0] - current.shape[0]) <= strech:
				
				distance = distance_between_timeseries(current, other)
				
				# Calculates total cases
				if accum:
					total_cases_dif = np.abs(current_total_cases - other.num_cases.max())
				else:
					total_cases_dif = np.abs(current_total_cases - other.num_cases.sum())
						
				
				if verbose:
					print(ident + '   {}, {}, {}'.format(poly,other_max_day, distance ))

				res.append({'location':loc, 'dist': distance, 'max_day':other_max_day, 'total_cases_dif':total_cases_dif})
			
	
	df = pd.DataFrame(res).sort_values('dist').reset_index()
	
	return(df)


def distance_between_timeseries(series_1, series_2):
	'''
	Distance between time series (using Dynamic Time Warping)
	'''
	
	x = series_1.dropna().num_cases.values
	y = series_2.dropna().num_cases.values

	distance, path = fastdtw(x, y, dist=euclidean)
	
	return(distance)

def adjust_timeseries(timeseries, start, end):

	timeseries = timeseries.merge(pd.DataFrame({'day':range(start_day, end_day + 1)}), on = 'day', how = 'right').sort_values('day')
	return(timeseries)

def accumulate_timeseries(timeseries):
	timeseries = timeseries.sort_values('day')
	timeseries['num_cases'] = timeseries['num_cases'].rolling(min_periods=1, window=timeseries.shape[0]).sum()
	return(timeseries)



def has_aglomeration(location, agglomeration_method):

	cases_location = os.path.join(data_folder, location, 'agglomerated', agglomeration_method, 'cases.csv')

	return(os.path.exists(cases_location))
		

	
# Extracts the time series for a given polygon
def extract_timeseries(location, agglomeration_method, polygon_id, lag = 0, accum = False, smooth_days = 1):
	'''
	If Polygon ID is None, will construct the  time serires for the entire location
	'''

	desc = pd.read_csv(os.path.join(data_folder, location, 'description.csv'), index_col = 0)
	location_type = desc.loc['type','value']
	location_name = desc.loc['name','value']

	cases_location = os.path.join(data_folder, location, 'agglomerated',agglomeration_method,'cases.csv')

	if not has_aglomeration(location, agglomeration_method):
		raise ValueError('No Agglomerated cases for {}'.format(cases_location))

	# Reads Cases
	df_cases = pd.read_csv(cases_location, parse_dates = ['date_time'])

	if polygon_id is not None and polygon_id not in df_cases.poly_id.unique():
		raise ValueError('No polygon with id {} in {}'.format(polygon_id, location))

	if polygon_id is not None:
		df_cases = df_cases.loc[df_cases.poly_id == polygon_id,['date_time','num_cases']].groupby('date_time').sum().reset_index().sort_values('date_time')

	else: # Entire location
		df_cases = df_cases[['date_time','num_cases']].groupby('date_time').sum().reset_index().sort_values('date_time')

	# Adds lag
	df_cases = df_cases.iloc[lag:]
	
	# Creates day
	df_cases['day'] = (df_cases.date_time - df_cases.date_time.min()).apply(lambda dif: dif.days)

	# Fills in the blanks
	df_cases = df_cases[['day','num_cases']].merge(pd.DataFrame({'day':range(df_cases.day.max() + 1)}), on = 'day', how = 'right').fillna(0).sort_values('day')
	
	# Smoothes
	df_cases['num_cases'] = df_cases.num_cases.rolling(smooth_days, min_periods=1).mean()

	if accum:
		df_cases = accumulate_timeseries(df_cases)

	# Assings name
	if polygon_id != None:
		df_cases['location'] = polygon_id
	else:
		df_cases['location'] = location_name
	
	return(df_cases)

	


def extract_timeseries_no_smooth(location, agglomeration_method, polygon_id, lag = 0, accum = False):
	'''
	If Polygon ID is None, will construct the  time serires for the entire location
	'''

	desc = pd.read_csv(os.path.join(data_folder, location, 'description.csv'), index_col = 0)
	location_type = desc.loc['type','value']
	location_name = desc.loc['name','value']


	cases_location = os.path.join(data_folder, location, 'agglomerated', agglomeration_method, 'cases.csv')

	if not has_aglomeration(location, agglomeration_method):
		raise ValueError('No Agglomerated cases for {}'.format(cases_location))

	# Reads Cases
	df_cases = pd.read_csv(cases_location, parse_dates = ['date_time'])

	if polygon_id is not None  and polygon_id not in df_cases.poly_id.unique():
		raise ValueError('No polygon with id {} in {}'.format(polygon_id, location))

	if polygon_id is not None:
		df_cases = df_cases.loc[df_cases.poly_id == polygon_id,['date_time','num_cases']].groupby('date_time').sum().reset_index().sort_values('date_time')
	else: # Entire location
		df_cases = df_cases[['date_time','num_cases']].groupby('date_time').sum().reset_index().sort_values('date_time')

	# Adds lag
	df_cases = df_cases.iloc[lag:]
	
	# Creates day
	df_cases['day'] = (df_cases.date_time - df_cases.date_time.min()).apply(lambda dif: dif.days)

	# Fills in the blanks
	df_cases = df_cases[['day','num_cases']].merge(pd.DataFrame({'day':range(df_cases.day.max() + 1)}), on = 'day', how = 'right').fillna(0).sort_values('day')
	

	if accum:
		df_cases = accumulate_timeseries(df_cases)


	if polygon_id != None:
		df_cases['location'] = polygon_id
	else:
		df_cases['location'] = location_name

	
	return(df_cases)


def get_closest_neighbors(location, agglomeration_method, polygon_id, k, start_day = 0, lag = 0, strech = 3, base_locations = None, verbose = False, accum = False, smooth_days = 1, types = ['state','country']):
	
	if polygon_id != None:
		df = extract_polygon_distance(location = location,
									  agglomeration_method = agglomeration_method,
									  polygon_id = polygon_id, 
									  start_day = start_day, 
									  lag = lag, 
									  strech = strech, 
									  base_locations = base_locations, 
									  verbose = verbose, 
									  accum = accum,
									  smooth_days = smooth_days)
	else:


		df = extract_location_distance(location = location, 
										agglomeration_method = agglomeration_method, 
										start_day = start_day, 
										lag = lag, 
										strech = strech, 
										base_locations = base_locations, 
										verbose = verbose, 
										accum = accum, 
										smooth_days = smooth_days, 
										types = types)
	
	return(df.head(min(df.shape[0],k)))


