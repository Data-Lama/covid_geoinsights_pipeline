
# Script for Colombia 

# Necesary imports
import pandas as pd
import numpy as np
import os
import geo_functions as geo
import geopandas as geopandas
from shapely import wkt
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
		GenericUnifier.__init__(self, 'Colombia', 'colombia')



	def build_cases_geo(self):

		'''
		Loads the cases downloaded from: https://www.datos.gov.co/
		'''
		
		file_name = os.path.join(self.raw_folder, 'cases', self.get('cases_file_name'))
		
		# Columns names for convertion
		cols = {}
		cols['ID de caso'] = 'ID' 
		cols['Fecha de notificación'] = 'notification_time'
		cols['Código DIVIPOLA'] = 'geo_id'
		cols['Ciudad de ubicación'] = 'city'
		cols['Departamento o Distrito '] = 'state'
		cols['atención'] = 'attention'
		cols['Edad'] = 'age'
		cols['Sexo'] = 'sex'
		cols['Tipo'] = 'type'
		cols['Estado'] = 'status'
		cols['País de procedencia'] = 'country'
		cols['FIS'] = 'FIS'
		cols['Fecha de muerte'] = 'date_death'
		cols['Fecha diagnostico'] = 'date_time'
		cols['Fecha recuperado'] = 'date_recovered'
		cols['Fecha reporte web'] = 'date_reported_web'

		df = pd.read_csv(file_name, parse_dates = ['Fecha diagnostico'], date_parser = lambda x: pd.to_datetime(x, errors="coerce"), low_memory = False)
		df = df.rename(columns=cols)

		df.dropna(subset = ['date_time', 'attention'], inplace = True)
		df.geo_id = df.geo_id.apply(str).astype(str)

		df['num_cases'] = 1
		df.loc[df.attention == 'Fallecido', 'num_diseased'] = 1
		df.loc[df.attention == 'Recuperado', 'num_recovered'] = 1
		df.loc[(df.attention == 'Hospital') | (df.attention == 'Hospital UCI'), 'num_infected_in_hospital'] = 1
		df.loc[df.attention == 'Casa', 'num_infected_in_house'] = 1
		df.fillna(0, inplace = True)
		df['num_infected'] = df.num_infected_in_hospital + df.num_infected_in_house


		# Selects columns
		df = df[['date_time', 'geo_id','num_cases','num_diseased', 'num_recovered', 'num_infected', 'num_infected_in_hospital', 'num_infected_in_house']].copy()
		df = df.groupby(['date_time', 'geo_id']).sum().reset_index()
		df

		# Adds lat and lon from the polyfons of the shapefile
		polygons_final = self.build_polygons()
		polygons_final = polygons_final[['poly_id', 'poly_lon', 'poly_lat', 'poly_name']].rename(columns = {'poly_id':'geo_id', 'poly_lon':'lon', 'poly_lat':'lat', 'poly_name':'location'})

		df = df.merge(polygons_final, on = 'geo_id')
		df = df[['date_time','geo_id','location','lon','lat', 'num_cases', 'num_diseased', 'num_recovered', 'num_infected', 'num_infected_in_hospital', 'num_infected_in_house']]

		return(df)	



	def build_polygons(self):

		# Loads the data
		shape_file = os.path.join(self.raw_folder, 'geo', self.get('shape_file_name'))
		shape_file_info = os.path.join(self.raw_folder, 'geo', self.get('geo_file_name'))

		polygons = geopandas.read_file(shape_file)
		polygons_info = pd.read_csv(shape_file_info)

		# Polygons
		polygons = polygons[['Codigo_Dan','Shape_Area','geometry','Total_2018']].rename(columns = {'Codigo_Dan':'poly_id','Shape_Area':'attr_area', 'Total_2018': 'attr_population' })
		polygons.poly_id = polygons.poly_id.astype(int)


		# Polygon Info
		polygons_info['poly_name'] = polygons_info.apply(lambda row: '{}-{}'.format(row.muni_name, row.dep_name), axis = 1)
		polygons_info = polygons_info[['muni_id','poly_name']].rename(columns = {'muni_id':'poly_id'})
		polygons_final = polygons.merge(polygons_info, on = 'poly_id')

		# Extracts the center
		polygons_final['poly_lon'] = polygons_final.geometry.centroid.to_crs('epsg:4326').x
		polygons_final['poly_lat'] = polygons_final.geometry.centroid.to_crs('epsg:4326').y

		# Adjusts geometry  to latiude and longitud
		polygons_final = polygons_final.to_crs('epsg:4326')


		# Converts to string
		polygons_final['poly_id'] = polygons_final['poly_id'].astype(str)

		# Manually adjusts adjusts Bogota
		polygons_final.loc[polygons_final.poly_id == '11001', 'poly_lon'] = -74.0939301
		polygons_final.loc[polygons_final.poly_id == '11001', 'poly_lat'] = 4.6576632

		return(polygons_final)




