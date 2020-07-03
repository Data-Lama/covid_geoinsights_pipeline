# Script that agglomerates by geometry



# Other imports
import os, sys

from pathlib import Path
import pandas as pd
from shapely import wkt
import geopandas as geopandas

# Direcotries
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')


# Method Name
method_name = 'geometry'

# Reads the parameters from excecution
location_name  = sys.argv[1] # location namme
location_folder_name  = sys.argv[2] # location folder namme


# Sets the location
location_folder = os.path.join(data_dir, 'data_stages', location_folder_name)


# Creates the folders if the don't exist
# constructed
agglomeration_folder = os.path.join(location_folder, 'agglomerated/', method_name)
if not os.path.exists(agglomeration_folder):
	os.makedirs(agglomeration_folder)



ident = '         '

print(ident + 'Agglomerates for {}'.format(location_name))
print()

print(ident + 'Builds Datasets by Geometry')

# Loads Data
print()
print(ident + '   Loads Data:')

print(ident + '      Polygons')
polygons = pd.read_csv(os.path.join(location_folder, 'unified/polygons.csv'))

print(ident + '      Cases')
cases = pd.read_csv(os.path.join(location_folder, 'unified/cases.csv'))

print(ident + '      Movement')
movement = pd.read_csv(os.path.join(location_folder,  'unified/movement.csv'))

print(ident + '      Population')
population = pd.read_csv(os.path.join(location_folder,  'unified/population.csv'))

print()
print(ident + '   Agglomerates')

print(ident + '      Checks Data Structure')

if 'geometry' not in polygons.columns or 'POLYGON' not in polygons.geometry.values[0].upper():
	raise ValueError('To agglomerate by geometry, polygons must have a geometry column populated with POLYGON geometry')

print(ident + '      Converts to GeoPandas')

# Converts the datasets into geometry
# Polygons
polygons['geometry'] = polygons['geometry'].apply(wkt.loads)
polygons = geopandas.GeoDataFrame(polygons, geometry = 'geometry')

# Cases
cases = geopandas.GeoDataFrame(cases, geometry= geopandas.points_from_xy(cases.lon, cases.lat))
cases_cols = [col for col in cases.columns if 'num_' in col]


print(ident + '      Agglomerates Cases')
# Creates the agglomerated cases
agg_cases = geopandas.sjoin(cases, polygons, how = 'inner', op = 'within')
agg_cases = agg_cases[['date_time','location','poly_id'] + cases_cols]


print(ident + '      Agglomerates Polygons')

# Creates the agglomerated polygons
agg_polygons = polygons.merge(agg_cases[['poly_id'] + cases_cols].groupby('poly_id').sum().reset_index(), on = 'poly_id')
agg_polygons.sort_values('num_cases', ascending = False)


print(ident + '      Agglomerates Movement')
# Creates the agglomerated movement
agg_movement = movement[['date_time','start_movement_lon','start_movement_lat','end_movement_lon','end_movement_lat','n_crisis']]

# First Start
agg_movement = geopandas.sjoin(geopandas.GeoDataFrame(agg_movement, geometry= geopandas.points_from_xy(agg_movement.start_movement_lon, agg_movement.start_movement_lat)), 
						   polygons[['poly_id','geometry']], how = 'inner', op = 'within').rename(columns = {'poly_id':'start_poly_id'}).drop(['index_right'], axis = 1)
# Then End
agg_movement = geopandas.sjoin(geopandas.GeoDataFrame(agg_movement, geometry= geopandas.points_from_xy(agg_movement.end_movement_lon, agg_movement.end_movement_lat)), 
						   polygons[['poly_id','geometry']], how = 'inner', op = 'within').rename(columns = {'poly_id':'end_poly_id'})

# Filters and renames
agg_movement = agg_movement[['date_time','start_poly_id','end_poly_id','n_crisis']].rename(columns = {'n_crisis':'movement'})

# Filters out geo_id not in polyong
agg_movement = agg_movement[(agg_movement.start_poly_id.isin( agg_polygons.poly_id.unique())) & (agg_movement.end_poly_id.isin( agg_polygons.poly_id.unique()))].copy()

# Rounds and Groups by
agg_movement.date_time = pd.to_datetime(agg_movement.date_time)
agg_movement.date_time = agg_movement.date_time.dt.round(freq = 'D')
agg_movement = agg_movement.groupby(['date_time','start_poly_id','end_poly_id']).sum().reset_index()


# Mock
# TODO: fix population
agg_population = pd.DataFrame(columns = ['date_time','poly_id','population'])

print()
print(ident + '   Saves Data:')

print(ident + '      Cases')
cases_date_max = agg_cases.date_time.max()
cases_date_min = agg_cases.date_time.min()
agg_cases.to_csv(os.path.join(agglomeration_folder, 'cases.csv'), index = False)



print(ident + '      Movement')
movement_date_max = agg_movement.date_time.max()
movement_date_min = agg_movement.date_time.min()
agg_movement.to_csv(os.path.join(agglomeration_folder, 'movement.csv'), index = False)



print(ident + '      Polygons')
agg_polygons.to_csv(os.path.join(agglomeration_folder, 'polygons.csv'), index = False)

print(ident + '      Population')
agg_population.to_csv(os.path.join(agglomeration_folder, 'population.csv'), index = False)
agg_population

print(ident + '   Saves Statistics:')

#Saves the dates
with open(os.path.join(agglomeration_folder, 'README.txt'), 'w') as file:

	file.write('Parameters For Geometry Agglomeration:' + '\n')
	file.write('   None' + '\n')
	file.write('Current dates for databases:' + '\n')
	file.write('   Cases: Min: {}, Max: {}'.format(cases_date_min, cases_date_max) + '\n')
	file.write('   Movement: Min: {}, Max: {}'.format(movement_date_min, movement_date_max) + '\n')
	
print(ident + 'Done! Data copied to: {}/agglomerated/{}'.format(location_folder_name, method_name))
print('')