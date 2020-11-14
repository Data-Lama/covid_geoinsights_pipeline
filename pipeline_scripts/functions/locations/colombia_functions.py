
# Script for Colombia 

# Necesary imports
import os
import json
import time 
import datetime
import numpy as np
import pandas as pd
from shapely import wkt
import geo_functions as geo
import geopandas as geopandas

# Generic Unifier
from generic_unifier_class import GenericUnifier
import attr_agglomeration_functions as attr_agg


# Constants
FIS_COLUMN = 'Fecha de inicio de síntomas'
DATE_DEATH_COLUMN = 'Fecha de muerte'
DIAG_COLUMN = 'Fecha de diagnóstico'
DATE_RECOVERED_COLUMN = 'Fecha de recuperación'
DATE_REPORTED_WEB = 'fecha reporte web'

# Dates Start with day
dayfirst = True


class Unifier(GenericUnifier):
	'''
	Unifier class
	'''

	def __init__(self):
		# Initilizes
		GenericUnifier.__init__(self, 'Colombia', 'colombia')


	# def build_cases_geo(self):

	# 	'''
	# 	Loads the cases downloaded from: https://www.datos.gov.co/
	# 	https://www.datos.gov.co/api/views/gt2j-8ykr/rows.csv?accessType=DOWNLOAD
	# 	'''
		
	# 	file_name = os.path.join(self.raw_folder, 'cases', self.get('cases_file_name'))
		
	# 	# Columns names for convertion
	# 	cols = {}
	# 	cols['ID de caso'] = 'ID' 
	# 	cols['Fecha de notificación'] = 'notification_time'
	# 	cols['Código DIVIPOLA'] = 'geo_id'
	# 	cols['Ciudad de ubicación'] = 'city'
	# 	cols['Departamento o Distrito '] = 'state'
	# 	cols['atención'] = 'attention'
	# 	cols['Edad'] = 'age'
	# 	cols['Sexo'] = 'sex'
	# 	cols['Tipo'] = 'type'
	# 	cols['Estado'] = 'status'
	# 	cols['País de procedencia'] = 'country'
	# 	cols['FIS'] = 'FIS'
	# 	cols['Fecha de muerte'] = 'date_death'
	# 	cols['Fecha diagnostico'] = 'DIAG'
	# 	cols['Fecha recuperado'] = 'date_recovered'
	# 	cols['Fecha reporte web'] = 'date_reported_web'

	# 	df = pd.read_csv(file_name, parse_dates = ['Fecha diagnostico','FIS','Fecha de muerte','Fecha recuperado'], date_parser = lambda x: pd.to_datetime(x, errors="coerce"), low_memory = False)
	# 	df = df.rename(columns=cols)

	# 	df.dropna(subset = ['DIAG', 'attention', 'FIS'], inplace = True)

	# 	# Rounds to day
	# 	df['date_time'] = df['DIAG'].dt.round('D')
	# 	df.geo_id = df.geo_id.apply(str).astype(str)

	# 	df['num_cases'] = 1
	# 	df.loc[df.attention == 'Fallecido', 'num_diseased'] = 1
	# 	df.loc[df.attention == 'Recuperado', 'num_recovered'] = 1
	# 	df.loc[(df.attention == 'Hospital') | (df.attention == 'Hospital UCI'), 'num_infected_in_hospital'] = 1
	# 	df.loc[df.attention == 'Casa', 'num_infected_in_house'] = 1
	# 	df.fillna(0, inplace = True)
	# 	df['num_infected'] = df.num_infected_in_hospital + df.num_infected_in_house


	# 	# Selects columns
	# 	df = df[['date_time', 'geo_id','num_cases','num_diseased', 'num_recovered', 'num_infected', 'num_infected_in_hospital', 'num_infected_in_house']].copy()
	# 	df = df.groupby(['date_time', 'geo_id']).sum().reset_index()

	# 	# Adds lat and lon from the polyfons of the shapefile
	# 	polygons_final = self.build_polygons()
	# 	polygons_final = polygons_final[['poly_id', 'poly_lon', 'poly_lat', 'poly_name']].rename(columns = {'poly_id':'geo_id', 'poly_lon':'lon', 'poly_lat':'lat', 'poly_name':'location'})

	# 	df = df.merge(polygons_final, on = 'geo_id', how = 'right')
	# 	df.loc[df.date_time.isna(), 'date_time'] = df.date_time.min()
	# 	df.fillna(0, inplace = True)
	# 	df = df[['date_time','geo_id','location','lon','lat', 'num_cases', 'num_diseased', 'num_recovered', 'num_infected', 'num_infected_in_hospital', 'num_infected_in_house']]

	# 	return(df)	

	def build_cases_geo(self):

		'''
		Loads the cases downloaded from: https://www.datos.gov.co/
		https://www.datos.gov.co/api/views/gt2j-8ykr/rows.csv?accessType=DOWNLOAD
		'''

		file_name = os.path.join(self.raw_folder, 'cases', self.get('cases_file_name'))

		cols = {}
		cols['ID de caso'] = 'ID' 
		cols['Código DIVIPOLA municipio'] = 'geo_id'		
		cols['Ubicación del caso'] = 'attention'
		cols[FIS_COLUMN] = 'FIS'
		cols[DATE_DEATH_COLUMN] = 'date_death'
		cols[DIAG_COLUMN] = 'DIAG'
		cols[DATE_RECOVERED_COLUMN] = 'date_recovered'
		cols[DATE_REPORTED_WEB] = 'date_reported_web'
        
        
		df = pd.read_csv(file_name, parse_dates = ['Fecha de diagnóstico','Fecha de inicio de síntomas','Fecha de muerte','Fecha de recuperación'], date_parser = lambda x: pd.to_datetime(x, errors="coerce", dayfirst = dayfirst), low_memory = False)
		df = df.rename(columns=cols)

		# Rounds to day
		df['date_time'] = df['DIAG'].dt.round('D')
		df.geo_id = df.geo_id.apply(str).astype(str)
		df.set_index(["date_time", "geo_id"], inplace=True)

		df['num_cases'] = 1
		df.loc[df.attention == 'Fallecido', 'num_diseased'] = 1
		df.loc[df.attention == 'Recuperado', 'num_recovered'] = 1
		df.loc[(df.attention == 'Hospital') | (df.attention == 'Hospital UCI'), 'num_infected_in_hospital'] = 1
		df.loc[df.attention == 'Casa', 'num_infected_in_house'] = 1
		df.fillna(0, inplace = True)
		df['num_infected'] = df.num_infected_in_hospital + df.num_infected_in_house

		# Unifies

		# Groups by date and geoi_id to save space
		df.reset_index(inplace=True)
		df = df[['date_time', 'geo_id','num_cases','num_diseased', 'num_recovered', 'num_infected', 'num_infected_in_hospital', 'num_infected_in_house']].copy()				
		df = df.groupby(['date_time', 'geo_id']).sum().reset_index()

		# Adds lat and lon from the polyfons of the shapefile
		polygons_final = self.build_polygons()
		polygons_final = polygons_final[['poly_id', 'poly_lon', 'poly_lat', 'poly_name']].rename(columns = {'poly_id':'geo_id', 'poly_lon':'lon', 'poly_lat':'lat', 'poly_name':'location'})
		df = df.merge(polygons_final, on = 'geo_id', how = 'right')
		df.loc[df.date_time.isna(), 'date_time'] = df.date_time.min()
		df.fillna(0, inplace = True)
		df = df[['date_time','geo_id','location','lon','lat', 'num_cases', 'num_diseased', 'num_recovered', 'num_infected', 'num_infected_in_hospital', 'num_infected_in_house']]

		return(df)	



	def build_polygons(self):

		# Load cases and agglomeration scheme to build time delay per polygon
		'''
		Loads the cases downloaded from: https://www.datos.gov.co/
		https://www.datos.gov.co/api/views/gt2j-8ykr/rows.csv?accessType=DOWNLOAD
		'''
		aggl_scheme = pd.read_csv(os.path.join(self.unified_folder, "aggl_scheme.csv"))
		file_name = os.path.join(self.raw_folder, 'cases', self.get('cases_file_name'))

		cols = {}
		cols['ID de caso'] = 'ID' 
		cols['Código DIVIPOLA municipio'] = 'geo_id'		
		cols['Ubicación del caso'] = 'attention'
		cols[FIS_COLUMN] = 'FIS'
		cols[DATE_DEATH_COLUMN] = 'date_death'
		cols[DIAG_COLUMN] = 'DIAG'
		cols[DATE_RECOVERED_COLUMN] = 'date_recovered'
		cols[DATE_REPORTED_WEB] = 'date_reported_web'
        
		df = pd.read_csv(file_name, parse_dates = ['Fecha de diagnóstico','Fecha de inicio de síntomas','Fecha de muerte','Fecha de recuperación'], date_parser = lambda x: pd.to_datetime(x, errors="coerce", dayfirst = dayfirst), low_memory = False)
		df = df.rename(columns=cols)

		# Adds delay
		df["attr_time_delay"] = df["DIAG"] - df["FIS"]
		df.dropna(subset = ['DIAG', 'attention', 'attr_time_delay'], inplace = True)
		df["attr_time_delay"] = df["attr_time_delay"].astype('timedelta64[D]').astype(int)
		df = df.loc[(df["attr_time_delay"] > 0) & (df["attr_time_delay"] <= 60)].copy()

		# Calculates attr_time-delay_union
		groupby_cols = ['geo_id']
		agglomerate_cols = ['attr_time_delay']

		df_aggr = attr_agg.agglomerate(df, aggl_scheme, groupby_cols, agglomerate_cols)
		df_aggr.rename(columns={"attr_time_delay": "attr_time-delay_dist_mix"}, inplace=True)

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
		centroids = geo.get_centroids(polygons_final.geometry) 
		polygons_final['poly_lon'] = centroids.x
		polygons_final['poly_lat'] = centroids.y

		# Adjusts geometry  to latiude and longitud
		polygons_final = polygons_final.to_crs('epsg:4326')

		# Converts to string
		polygons_final['poly_id'] = polygons_final['poly_id'].astype(str)

		# Adds time-delay to polygons_final
		df_aggr['geo_id'] = df_aggr['geo_id'].astype(str)
		polygons_final = polygons_final.merge(df_aggr[['geo_id', 'attr_time-delay_dist_mix']], left_on="poly_id", right_on="geo_id", how="outer")
		polygons_final.dropna(subset=["poly_id"], inplace=True)
		polygons_final.drop(columns="geo_id", inplace=True)

		# Manually adjusts adjusts Bogota
		polygons_final.loc[polygons_final.poly_id == '11001', 'poly_lon'] = -74.0939301
		polygons_final.loc[polygons_final.poly_id == '11001', 'poly_lat'] = 4.6576632

		# Manually adjusts adjusts Cucuta
		polygons_final.loc[polygons_final.poly_id == '54001', 'poly_lon'] = -72.495447
		polygons_final.loc[polygons_final.poly_id == '54001', 'poly_lat'] = 7.890634	

		# Manually adjusts Valledupar
		polygons_final.loc[polygons_final.poly_id == '20001', 'poly_lon'] = -73.2548254
		polygons_final.loc[polygons_final.poly_id == '20001', 'poly_lat'] = 10.4686143			

		return(polygons_final)

	def attr_agglomeration_scheme(self):
		aggl_scheme = attr_agg.get_generic_attr_agglomeration_scheme()
 
		aggl_scheme["^attr.*wvg-pop$"] = ["attr_weighted_average", "attr_population",""]
		aggl_scheme["^attr.*time_delay$"] = ["estimate_gamma_delay", "", ""]
		aggl_scheme["attr_area"] = ["attr_addition", "", ""]
		aggl_scheme["^attr.*dist_mix$"] = ["attr_dist_mix", "", "sep=|"]
		aggl_scheme["geometry"] = ["merge_geometry", "", ""]
		aggl_scheme["poly_name"] = ["attr_with_max", "attr_population",""]
		aggl_scheme["poly_lat"] = ["attr_with_max", "attr_population",""]
		aggl_scheme["poly_lon"] = ["attr_with_max", "attr_population",""]

		return aggl_scheme


		

