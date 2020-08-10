# Script that cleans the data from the raw data for a given state of the US


# Imports all the necesary functions

import fb_functions as fb

from italy_functions import *

# Other imports
import os, sys

#Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')


country = 'italy'

ident = '         '


# Creates the folders if the don't exist
unified_folder = os.path.join(data_dir, 'data_stages', country, 'unified/')
if not os.path.exists(unified_folder):
	os.makedirs(unified_folder)

raw_folder = os.path.join(data_dir, 'data_stages', country, 'raw/')

country_folder = os.path.join(data_dir, 'data_stages', country)

print(ident + 'Unifies for {}'.format(country))



print(ident + 'Builds Datasets:')
# ----------------
# ---- Cases -----
# ----------------
print(ident + '   Cases')
df_cases = build_geo_cases()

# Extracts date
cases_date = df_cases.date_time.max()

# Saves
df_cases.to_csv(os.path.join(unified_folder, 'cases.csv'), index = False)


# -------------------
# ---- Movement -----
# -------------------
print(ident + '   Movement')
df_movement = fb.build_movement(country_folder)

# Extracts date
movement_date = df_movement.date_time.max()

# Saves
df_movement.to_csv(os.path.join(unified_folder, 'movement.csv'), index = False)

# -------------------
# ---- Population -----
# -------------------
print(ident + '   Population')
df_population = fb.build_population(country_folder)

# Extracts date
population_date = df_population.date_time.max()

# Saves
df_population.to_csv(os.path.join(unified_folder, 'population.csv'), index = False)



# -------------------
# ---- Polygons -----
# -------------------
print(ident + '   Polygons')
df_poly = build_polygons()

# Extracts date
poly_date = cases_date

# Saves
df_poly.to_csv(os.path.join(unified_folder, 'polygons.csv'), index = False)



print(ident + 'Saving Dates')

#Saves the dates
with open(os.path.join(data_dir, 'data_stages', country, 'unified/README.txt'), 'w') as file:

	file.write('Current max dates for databases:' + '\n')
	file.write('   Cases: {}'.format(cases_date) + '\n')
	file.write('   Movement: {}'.format(movement_date) + '\n')
	file.write('   Population: {}'.format(population_date) + '\n')
	file.write('   Polygons: {}'.format(poly_date) + '\n')

print(ident + 'Done! Data copied to: {}/unified'.format(country_folder))
print(ident + '')