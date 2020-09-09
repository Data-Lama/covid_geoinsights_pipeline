# Johns Hopkins Unifier

# Imports all the necesary functions

import os
import sys
import numpy as np
import pandas as pd
import geopandas as geo


import fb_functions as fb
import geo_functions as geo_fun
import constants as con

#Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')


ident = '         '


# Reads the parameters from excecution
global_location = sys.argv[1] 
all_locations_names  = sys.argv[2:] # locations



unified_folder = os.path.join(data_dir, 'data_stages', global_location, 'unified')

if not os.path.exists(unified_folder):
	os.makedirs(unified_folder)

print(ident + 'Unifies Several Locations')

print(ident + '   Gathers unified info for: ' + " ".join(all_locations_names))


cases = []
movement = []
polygons = []
population = []
movement_range = []

for location in all_locations_names:

	print(ident + f'      Loads: {location}')

	local_unified = os.path.join(data_dir, 'data_stages', location, 'unified')

	# cases
	cases.append(pd.read_csv(os.path.join(local_unified, 'cases.csv')))

	# movement
	movement.append(pd.read_csv(os.path.join(local_unified, 'movement.csv')))

	# polygons
	polygons.append(pd.read_csv(os.path.join(local_unified, 'polygons.csv')))

	# population
	population.append(pd.read_csv(os.path.join(local_unified, 'population.csv')))

	# movement_range
	mov_range_file = os.path.join(local_unified, 'movement_range.csv')
	if os.path.exists(mov_range_file):
		movement_range.append(pd.read_csv(mov_range_file))


# Merges 
print(ident + '   Merges')

mov_range = True
if len(movement_range) == 0:
	print(ident + '      No Movement Range Found. Skipping')
	mov_range = False

elif len(movement_range) < len(all_locations_names):
	print(ident + '      Not all locations have movement range. Skipping')
	mov_range = False


# cases
df_cases = pd.concat(cases, ignore_index = True)
df_cases.drop_duplicates(inplace = True) 
cases_date = df_cases.date_time.max()

# Movement
df_movement = pd.concat(movement, ignore_index = True)
df_movement.drop_duplicates(inplace = True) 
movement_date = df_movement.date_time.max()

# Polygons
df_polygons = pd.concat(polygons, ignore_index = True)
df_polygons.drop_duplicates(inplace = True) 

# Population
df_population = pd.concat(population, ignore_index = True)
df_population.drop_duplicates(inplace = True) 
population_date = df_population.date_time.max()

# Movement range
if mov_range:
	df_movement_range = pd.concat(movement_range, ignore_index = True)
	df_movement_range.drop_duplicates(inplace = True)
	movement_range_date = df_movement_range.ds.max()


print(ident + '   Saves')

# Cases
df_cases.to_csv(os.path.join(unified_folder, "cases.csv"), index=False)


# Movement
df_movement.to_csv(os.path.join(unified_folder, "movement.csv"), index=False)

# Polygons
df_polygons.to_csv(os.path.join(unified_folder, "polygons.csv"), index=False)

# Population
df_population.to_csv(os.path.join(unified_folder, "population.csv"), index=False)

# Movement range
if mov_range:
	df_movement_range.to_csv(os.path.join(unified_folder, "movement_range.csv"), index=False)	




print(ident + 'Saving Dates')

#Saves the dates
with open(os.path.join(unified_folder, 'README.txt'), 'w') as file:

	file.write('Current max dates for databases:' + '\n')
	file.write('   Cases: {}'.format(cases_date) + '\n')
	file.write('   Movement: {}'.format(movement_date) + '\n')
	file.write('   Population: {}'.format(population_date) + '\n')
	if mov_range:
		file.write('   Population: {}'.format(movement_range_date) + '\n')

print(ident + 'Done! Data copied to: {}/unified'.format(global_location))
print(ident + '')

print(ident + '   Done')



