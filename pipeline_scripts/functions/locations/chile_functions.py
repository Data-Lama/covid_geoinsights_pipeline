# Script for Chile 

# Necesary imports
import pandas as pd
import numpy as np
import os
import geo_functions as geo
import json
import time 

# Generic Unifier
from generic_unifier_class import GenericUnifier



class Unifier(GenericUnifier):
	'''
	Unifier class
	'''

	def __init__(self):
		# Initilizes
		GenericUnifier.__init__(self, 'Chile', 'chile')


	def build_cases(self):


		columns = {}
		columns['Region'] = 'region'
		columns['Codigo region'] = 'state_code'
		columns['Comuna'] = 'comune'
		columns['Codigo comuna'] = 'geo_id'
		columns['Poblacion'] = 'population'
		columns['Fecha'] = 'date_time'
		columns['Casos confirmados'] = 'num_cases'


						

		file_name = os.path.join(self.raw_folder, 'cases', self.get('cases_file_name'))

		df = pd.read_csv(file_name, parse_dates = ['Fecha'], dtype={'Codigo comuna':'str'})
		df = df.rename(columns = columns)

		df_cases = df.loc[df.num_cases > 0, ['date_time','geo_id','region','comune','num_cases']].copy()
		df_cases = df_cases.dropna(subset = ['geo_id'])
		df_cases.geo_id = df_cases.geo_id.astype(str)


		df_cases.sort_values(['date_time'], inplace = True)

		# Corrects wrong values
		# Flattens!
		for geo_id in df_cases.geo_id.unique():
			val = df_cases[df_cases.geo_id == geo_id].num_cases.values

			finished = False		
			while not finished:
				finished = True
				for i in range(1,len(val) - 1):
					if val[i] > val[i+1]:
						cur = val[i]
						val[i] = int(np.floor((val[i-1] + val[i+1])/2))
						if val[i] == cur:
							val[i] =  val[i+1]

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

		
		return(self.generic_build_cases_geo('Chile'))




	def update_geo(self, max_tries = 3):
		'''
		Updates the new missing geo_locations
		'''
		
		country = 'Chile'

		# Loads cases
		cases = self.build_cases()

		# loads geo 
		df_geo = geo.get_geo(country)

		# Finds missing locations
		merged = cases.merge(df_geo, on = ['geo_id'], how = 'left')
		missing = merged.loc[merged.lon.isna(), ['geo_id', 'region', 'comune']].drop_duplicates()


		#Updates
		if missing.shape[0] == 0:
			print(self.ident + 'Found zero new places.')
		else:
			print(self.ident + 'Found {} new places. Updating'.format(missing.shape[0]))
			print('')


			for ind, row in missing.iterrows():

				print(self.ident + '   Finding: {}, {}, {} ({})'.format(country, row.region, row.comune, row.geo_id))
				tries = 0
				while tries < max_tries:
					try:
						lon, lat = geo.geolocate(country, row.region, row.comune)
						print(self.ident + '      Done! Location: {},{}'.format(lat,lon))
						geo.save_new_location(row.geo_id, country, row.region, row.comune, lon, lat)
						break
					except:
						time.sleep(15)

					tries += 1

				if tries == max_tries:
					print(self.ident + '      Could not find location. Please enter it manually')

				print()
