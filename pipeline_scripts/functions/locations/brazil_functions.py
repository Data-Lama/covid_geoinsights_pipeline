# Script for Brazil 

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


	name = name.split("/")[0]
	return(name)



class Unifier(GenericUnifier):
	'''
	Unifier class
	'''

	def __init__(self):
		# Initilizes
		GenericUnifier.__init__(self, 'Brazil', 'brazil')




	def build_cases_geo(self):

		file_name = os.path.join(self.raw_folder, 'cases', self.get('cases_file_name'))
		geo_file_location = os.path.join(self.raw_folder, 'geo', self.get('geo_file_name'))

		df = pd.read_csv(file_name, parse_dates = ["date"])
		df = df[["date","newCases", "ibgeID"]].rename(columns = {'date':'date_time', 'newCases':'num_cases', "ibgeID":'geo_id'})

		# Removes fake ids and negative
		df = df[df.geo_id.apply(lambda geo: len(str(geo)) > 3)].copy()
		df = df[df.num_cases > 0].copy()

		# Loads Geo
		df_geo = pd.read_csv(geo_file_location)
		df_geo = df_geo[['ibgeID','id','lon','lat']].rename(columns = {'ibgeID':'geo_id','id':'location'})
		df_geo.location = df_geo.location.apply(clean_name)

		df_final = df.merge(df_geo, on = 'geo_id').sort_values('date_time')
		
		return(df_final)



