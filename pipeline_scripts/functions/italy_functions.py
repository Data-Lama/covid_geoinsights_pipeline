# USA Individual State Function
# Different methods for USA related data


# Necesary imports
import pandas as pd
import numpy as np
import os

from pathlib import Path
import geo_functions as geo

import fb_functions as fb

#Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')


# Km constant for convetting coordinates to kilometers
km_constant = geo.km_constant



# Repository and cases folders
repository_name = 'COVID-19-ITALY'
respository_folder = os.path.join(data_dir, 'git_repositories/{}/'.format(repository_name))

# Cases in repository
repository_cases_folder = os.path.join(respository_folder, 'dati-province')




def is_number(s):
	'''
	If is number
	'''
	
	try:
		int(s)
		return(True)
	except:
		return(False)
	

def build_geo_cases():

	col_dict = {'data':'date_time','codice_provincia':'geo_id', 'denominazione_provincia':'location' , 'long':'lon', 'lat':'lat', 'totale_casi':'num_cases'}


	# Cleans folder
	dfs = []
	directory = os.path.join(data_dir, 'data_stages/italy/raw/cases/')
	for file in os.listdir(directory):
		if file.endswith('.csv') and is_number(file.split('-')[-1].replace('.csv','')):
			dfs.append(pd.read_csv(os.path.join(directory, file), encoding = "ISO-8859-1", parse_dates = ['data']))


	df_cases = pd.concat(dfs, ignore_index = True)

	# Filters out
	df_cases = df_cases[df_cases.long != 0]


	df_cases = df_cases[['data','codice_provincia','denominazione_provincia','long','lat','totale_casi']].rename(columns = col_dict)

	df_cases.sort_values(['date_time'], inplace = True)

	# Cases are cumulative, restores them

	# corrects wrong values
	# Flattens!
	for geo_id in df_cases.geo_id.unique():

		val = df_cases[df_cases.geo_id == geo_id].num_cases.values
		

		finished = False		
		while not finished:
			finished = True
			for i in range(1,len(val) - 1):
				if val[i] > val[i+1]:
					val[i] = int(np.floor((val[i-1] + val[i+1])/2))
					finished = False



		df_cases.loc[df_cases.geo_id == geo_id, 'num_cases'] = val


	# Restores to non cumulative
	for geo_id in df_cases.geo_id.unique():
		
		val = df_cases[df_cases.geo_id == geo_id].num_cases.values
		new_val = val - np.roll(val, 1)
		new_val[0] = val[0]
		df_cases.loc[df_cases.geo_id == geo_id,'num_cases'] = new_val

	return(df_cases)

	

def build_polygons():
	
	poly_dict = {'geo_id':'poly_id','location':'poly_name','lon':'poly_lon','lat':'poly_lat'}
		
	df = build_geo_cases()
	df = df[['geo_id','location','lon','lat']].rename(columns = poly_dict).drop_duplicates()
	
	return(df)

