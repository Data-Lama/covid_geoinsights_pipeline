# Script for New South Wales Australia 

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
	dic['(A)'] = ''
	dic['(C)'] = ''
	dic['(NSW'] = ''
	dic['('] = ''
	dic[')'] = ''
	


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
		GenericUnifier.__init__(self, 'NewSouthWales', 'new_south_wales')


	def build_cases(self):

		file_name = os.path.join(self.raw_folder, 'cases', self.get('cases_file_name'))

		cols = {}
		cols['notification_date'] = 'date_time' 
		cols['lga_name19'] = 'geo_name'


		df = pd.read_csv(file_name, parse_dates = ['notification_date'])
		
		df = df.rename(columns=cols).dropna(subset = ['geo_name'])


		df['geo_name'] = df['geo_name'].apply(clean_name)

		# Invented Geo Id
		df['geo_id'] = df['geo_name'].apply(lambda name : '{}-{}-{}'.format(name[0:3], len(name), name[-1]))
		

		df = df[['date_time', 'geo_id', 'geo_name']].copy()
		df['num_cases'] = 1
        
		return(df)



	def build_cases_geo(self):

		return(self.generic_build_cases_geo('Australia'))


	def update_geo(self, max_tries = 3):
		'''
		Updates the new missing geo_locations
		'''
	
	
		country = 'Australia'
		dep_name = 'New South Wales'

		# loads geo 
		df_geo = geo.get_geo(country)
		
		# Loads cases
		cases = self.build_cases()
		
		# Finds missing locations
		merged = cases.merge(df_geo, on = ['geo_id'], how = 'left')
		geo_ids = merged.loc[merged.lon.isna(), ['geo_id','geo_name']].drop_duplicates()

		
		if geo_ids.shape[0] == 0:
			print(self.ident +  'Found zero new places.')
		else:
			print(self.ident +  'Found {} new places. Updating'.format(geo_ids.shape[0]))
			print('')
		

			for ind, row in geo_ids.iterrows():


				print(self.ident + '   Finding: {}, {} ({})'.format(row.geo_name, dep_name, row.geo_id))
				tries = 0
				while tries < max_tries:
					try:
						lon, lat = geo.geolocate(country, dep_name, row.geo_name)
						print('      Done! Location: {},{}'.format(lat,lon))
						geo.save_new_location(row.geo_id, country, dep_name, row.geo_name, lon, lat)
						break
					except:
						time.sleep(15)
						
					tries += 1
				
				if tries == max_tries:
					print(self.ident + '      Could not find location. Please enter it manually')
				
				print()





