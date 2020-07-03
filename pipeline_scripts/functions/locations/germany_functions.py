# Script for Germany 

# Necesary imports
import pandas as pd
import numpy as np
import os
import geo_functions as geo
import json
import time 

# Generic Unifier
from generic_unifier_class import GenericUnifier



# Cleaning functions for the city and the state
def clean_name(name):
	'''
	Removes non essential characters from string
	'''

	dic = {}
	dic['LK '] = ''
	dic['SK '] = ''

	
	for key in dic:
		name = name.replace(key, dic[key])
		name = name.replace(key.upper(), dic[key].upper())
	
	name = name.strip()
	return(name)





class Unifier(GenericUnifier):
	'''
	Unifier class
	'''

	def __init__(self):
		# Initilizes
		GenericUnifier.__init__(self, 'Germany', 'germany')


	def build_cases(self):

		file_name = os.path.join(self.raw_folder, 'cases', self.get('cases_file_name'))

		df = pd.read_csv(file_name, parse_dates = ['time_iso8601'])
		df = df.rename(columns = {'time_iso8601':'date_time'})

		df_cases = pd.melt(df, id_vars = ['date_time'], value_vars = df.columns[1:-1])
		df_cases.columns = ['date_time','geo_id','num_cases']
		df_cases.date_time = pd.to_datetime(df_cases.date_time.dt.round(freq = 'D').apply(lambda x: x.strftime('%Y-%m-%d')))

		df_cases.sort_values(['date_time'], inplace = True)

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
		
		df_cases = df_cases[df_cases.num_cases > 0]
        
		return(df_cases)


	def build_cases_geo(self):

		
		return(self.generic_build_cases_geo('Germany'))




	def update_geo(self, max_tries = 3):
		'''
		Updates the new missing geo_locations
		'''
		
		country = 'Germany'
		geo_file_location = os.path.join(self.raw_folder, 'geo', self.get('geo_file_name'))

		# loads geo 
		df_geo = geo.get_geo(country)

		# Loads cases
		cases = self.build_cases()

		# Finds missing locations
		merged = cases.merge(df_geo, on = ['geo_id'], how = 'left')
		missing = merged.loc[merged.lon.isna(), ['geo_id']].drop_duplicates()

		# Reads the JSON
		with open(geo_file_location) as json_file:
			geo_names = json.load(json_file)

		df_geo_names = pd.DataFrame.from_dict(geo_names, orient = 'index').reset_index().drop(['note'], axis = 1).rename(columns = {'index':'geo_id','name':'geo_name'})
		df_geo_names.geo_name = df_geo_names.geo_name.apply(clean_name)

		# merges
		missing = missing.merge(df_geo_names, on = 'geo_id')

		#Updates
		if missing.shape[0] == 0:
			print(self.ident + 'Found zero new places.')
		else:
			print(self.ident + 'Found {} new places. Updating'.format(missing.shape[0]))
			print('')


			for ind, row in missing.iterrows():

				print(self.ident + '   Finding: {}, {}, {} ({})'.format(country, row.state, row.geo_name, row.geo_id))
				tries = 0
				while tries < max_tries:
					try:
						lon, lat = geo.geolocate(country, row.state, row.geo_name)
						print('      Done! Location: {},{}'.format(lat,lon))
						geo.save_new_location(row.geo_id, country, row.state, row.geo_name, lon, lat)
						break
					except:
						time.sleep(15)

					tries += 1

				if tries == max_tries:
					print(self.ident + '      Could not find location. Please enter it manually')

				print()
