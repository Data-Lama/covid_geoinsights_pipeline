# Script that cleans the data from the raw data for a given state of the US


# Imports all the necesary functions

import fb_functions as fb

from usa_functions import *

# Other imports
import os, sys

#Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')


# Reads the parameters from excecution
state_name  = sys.argv[1] # State namme
state_folder_name  = sys.argv[2] # State folder namme


ident = '         '


# Creates the folders if the don't exist
unified_folder = os.path.join(data_dir, 'data_stages', state_folder_name, 'unified/')
if not os.path.exists(unified_folder):
	os.makedirs(unified_folder)


print(ident + 'Unifies for {} USA'.format(state_name))



print(ident + 'Builds Datasets:')

# ----------------
# -- Agg_scheme --
# ----------------
print(ident + '   Agglomeration scheme')
aggl_scheme = attr_agglomeration_scheme()


# Writes aggl_scheme
df_aggl_scheme = pd.DataFrame.from_dict(aggl_scheme, orient="index", columns=["aggl_function", "secondary_attr", "aggl_parameters"]).reset_index()
df_aggl_scheme.rename(columns={"index":"attr_name"}, inplace=True)
df_aggl_scheme.to_csv(os.path.join(unified_folder, 'aggl_scheme.csv'), index = False)

# ----------------
# ---- Cases -----
# ----------------
print(ident + '   Cases')
df_cases = build_cases_from_timeline(state_name, state_folder_name)

# Extracts date
cases_date = df_cases.date_time.max()

# Saves
df_cases.to_csv(os.path.join(unified_folder, 'cases.csv'), index = False)


# -------------------
# ---- Movement -----
# -------------------
print(ident + '   Movement')
df_movement = fb.build_movement(state_folder_name)

# Extracts date
movement_date = df_movement.date_time.max()

# Saves
df_movement.to_csv(os.path.join(unified_folder, 'movement.csv'), index = False)

# -------------------
# ---- Population -----
# -------------------
print(ident + '   Population')

#Checks if Population exists:
if os.path.exists(os.path.join(unified_folder,'raw/population_tiles/')):
	df_population = fb.build_population(state_folder_name)
else:
	# USA Population Tiles (MOCK)
	df_population = fb.build_population('usa')

# Extracts 'date'
population_date = df_population.date_time.max()

# Saves
df_population.to_csv(os.path.join(unified_folder, 'population.csv'), index = False)


# -------------------
# ---- Polygons -----
# -------------------
print(ident + '   Polygons')
df_poly = build_polygons(state_name, state_folder_name)

# Extracts date
poly_date = cases_date

# Saves
df_poly.to_csv(os.path.join(unified_folder, 'polygons.csv'), index = False)



print(ident + 'Saving Dates')

#Saves the dates
with open(os.path.join(data_dir, 'data_stages', state_folder_name, 'unified/README.txt'), 'w') as file:

	file.write('Current max dates for databases:' + '\n')
	file.write('   Cases: {}'.format(cases_date) + '\n')
	file.write('   Movement: {}'.format(movement_date) + '\n')
	file.write('   Population: {}'.format(population_date) + '\n')
	file.write('   Polygons: {}'.format(poly_date) + '\n')

print(ident + 'Done! Data copied to: {}/unified'.format(state_folder_name))
print(ident + '')