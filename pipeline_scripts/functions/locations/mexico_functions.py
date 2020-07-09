import os
import time
import pandas as pd
import geo_functions as geo


# Generic Unifier
from generic_unifier_class import GenericUnifier

class Unifier(GenericUnifier):
	'''
	Unifier class
	'''

	def __init__(self):
		# Initilizes
		GenericUnifier.__init__(self, 'Mexico', 'mexico')

	def build_cases_geo(self):
		
		return(self.generic_build_cases_geo('Mexico'))

	def update_geo(self, max_tries = 3):
		'''
		Updates the new missing geo_locations
		'''

		country = 'Mexico'
		geo_file_location = os.path.join(self.raw_folder, 'geo', self.get('geo_file_name'))

		# loads geo 
		df_geo = geo.get_geo(country)

		# Loads cases
		cases = self.build_cases()

		# Finds missing locations
		merged = cases.merge(df_geo, on = ['geo_id'], how = 'left')
		missing = merged.loc[merged.lon.isna(), ['geo_id']].drop_duplicates()

		# Reads the CSV
		df_geo_names = pd.read_csv(geo_file_location)
		df_geo_names.set_index(['geo_id'])

		# merges
		missing = missing.merge(df_geo_names, on = 'geo_id').rename(columns = {'name':'geo_name'})
		print(missing)

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

	def build_cases(self):
		print("DEBUGGING")
		file_name = os.path.join(self.raw_folder, 'cases', self.get('cases_file_name'))

		df = pd.read_csv(file_name)
		df.rename(columns={"cve_ent": "geo_id"}, inplace=True)
		df.drop(['poblacion', 'nombre'], inplace=True, axis=1)
		df = df.set_index('geo_id').transpose()
		df.reset_index(inplace=True)
		df = pd.melt(df, id_vars=['index'], value_vars = df.columns[1:])

		return(df)