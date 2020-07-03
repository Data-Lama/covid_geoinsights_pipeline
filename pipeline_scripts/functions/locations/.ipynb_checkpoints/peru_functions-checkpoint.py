# Script for Peru 

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
		GenericUnifier.__init__(self, 'Peru', 'peru')


	def build_cases(self):

UUID	DEPARTAMENTO	PROVINCIA	DISTRITO	METODODX	EDAD	SEXO	FECHA_RESULTADO

		columns = {}
		columns['UUID'] = 'ID'
		columns['DEPARTAMENTO'] = 'departement'
		columns['PROVINCIA'] = 'province'
		columns['DISTRITO'] = 'district'
		columns['METODODX'] = 'method'
		columns['EDAD'] = 'age'
		columns['SEXO'] = 'sex'
		columns['FECHA_RESULTADO'] = 'date_time'


						

		file_name = os.path.join(self.raw_folder, 'cases', self.get('cases_file_name'))

		df = pd.read_csv(file_name, parse_dates = ['FECHA_RESULTADO'], dayfirst = True, encoding = 'latin-1')
		df = df.rename(columns = columns)

		df_cases = df[['date_time','departement', 'province']].copy()
		df_cases['num_cases'] = 1
		df_cases = df_cases.groupby(['date_time','departement', 'province']).sum().reset_index()

		df_geo = df_cases[['province','district']].drop_duplicates()
		df_geo['geo_id'] = df_geo.apply(lambda row: '{}-{}'.format(row.departement[0:2].upper(), row.province[0:3].upper()))

		df_cases.sort_values(['date_time'], inplace = True)

		df_cases = df_cases.merge(df_geo, on = ['departement', 'province'])

        
		return(df_cases)


	def build_cases_geo(self):

		
		return(self.generic_build_cases_geo('Peru'))




	def update_geo(self, max_tries = 3):
		'''
		Updates the new missing geo_locations
		'''
		
		country = 'Peru'

		# Loads cases
		cases = self.build_cases()

		# loads geo 
		df_geo = geo.get_geo(country)

		# Finds missing locations
		merged = cases.merge(df_geo, on = ['geo_id'], how = 'left')
		missing = merged.loc[merged.lon.isna(), ['geo_id','departement', 'province']].drop_duplicates()


		#Updates
		if missing.shape[0] == 0:
			print('Found zero new places.')
		else:
			print('Found {} new places. Updating'.format(missing.shape[0]))
			print('')


			for ind, row in missing.iterrows():

				print('   Finding: {}, {}, {} ({})'.format(country, row.departement, row.province, row.geo_id))
				tries = 0
				while tries < max_tries:
					try:
						lon, lat = geo.geolocate(country, row.departement, row.province)
						print('      Done! Location: {},{}'.format(lat,lon))
						geo.save_new_location(row.geo_id, country, row.departement, row.province, lon, lat)
						break
					except:
						time.sleep(15)

					tries += 1

				if tries == max_tries:
					print('      Could not find location. Please enter it manually')

				print()
