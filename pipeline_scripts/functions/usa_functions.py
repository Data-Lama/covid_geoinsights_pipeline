# USA Individual State Function
# Different methods for USA related data


# Necesary imports
import pandas as pd
import numpy as np
from pathlib import Path
import os
import time
from sklearn.metrics import pairwise_distances
from datetime import timedelta
import ot
import shutil

import geo_functions as geo

import fb_functions as fb

import extraction_functions as ext_fun

import attr_agglomeration_functions as agg_fun

#Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')


# Km constant for convetting coordinates to kilometers
km_constant = geo.km_constant


# Repository
repository_name = 'COVID-19-USA'
respository_folder = os.path.join(data_dir, 'git_repositories/{}/'.format(repository_name))

# Cases in repository
cases_in_respository = os.path.join(respository_folder, 'csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv')


# Constant for name of file
jh_time_series_location = os.path.join(data_dir, 'data_stages/usa/raw/cases/time_series_covid19_confirmed_US.csv')




def build_cases_from_timeline(state_name, state_folder_name):
	'''
	Loads the cases downloaded from the jhon hopkins repository
	'''
	
	# Loads the time series
	time_series = pd.read_csv(jh_time_series_location)

	# Filters out
	# By state
	time_series = time_series[time_series.Province_State.apply(lambda s: s.upper()) == state_name.upper()]
	# By missing coordinates
	time_series = time_series[time_series.Lat != 0]
	# By columns
	selcted_columns = ['FIPS','Lat','Long_','Combined_Key'] + time_series.columns.values[11:].tolist()
	time_series = time_series[selcted_columns].rename(columns = {'Lat':'lat', 'Long_':'lon', 'Combined_Key':'location', 'FIPS':'geo_id'})

	# Adjusts the columns so it's not total cases but cases in the day
	last_col = time_series.shape[1] - 1
	for i in range(last_col - 4):
		time_series.iloc[:, last_col - i] = time_series.iloc[:,last_col - i] - time_series.iloc[:,last_col - (i+1)]
		
	# Melts
	df_cases = pd.melt(time_series, id_vars= ['geo_id','lat','lon', 'location'], value_vars = list(time_series.columns[4:]), var_name='date_time', value_name='num_cases')

	# Removes zeros
	df_cases = df_cases.loc[df_cases.num_cases > 0,['date_time','geo_id','location','lon','lat','num_cases']]

	# Adjusts the date
	df_cases.date_time = pd.to_datetime(df_cases.date_time)

	# Adjusts the geo_id
	df_cases.geo_id = df_cases.geo_id.astype(int)

	df_cases.sort_values('date_time', inplace = True)


	return(df_cases)


def build_polygons(state_name, state_folder_name):
	'''
	Current implementation of polygons
	'''

	df_cases = build_cases_from_timeline(state_name, state_folder_name)
	df_poly = df_cases[['geo_id', 'location','lon', 'lat']].drop_duplicates().rename(columns = {'geo_id':'poly_id', 'location':'poly_name', 'lon':'poly_lon', 'lat':'poly_lat'})

	return(df_poly)


def attr_agglomeration_scheme():
	'''
	Default agglomeration Scheme
	'''
	return(agg_fun.get_generic_attr_agglomeration_scheme())