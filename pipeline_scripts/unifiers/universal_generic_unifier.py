# Script that cleans the data from the raw data


# Imports all the necesary functions
import fb_functions as fb
import general_functions as gf


# Other imports
import os, sys
from datetime import datetime, timedelta
from pathlib import Path
import constants as con
import pandas as pd

#Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')
key_string = config.get_property('key_string') 

ident = '         '

# Unifies Facebook data
location_name  = sys.argv[1] # Location name
location_folder_name = sys.argv[2] # location folder name

# If a third parameter is given is the max_date
max_date = None
if len(sys.argv) > 3:
	max_date = pd.to_datetime( sys.argv[3])

	# Adjusts max date (so that it can include the given date)
	max_date = max_date + timedelta(days = 1) - timedelta(seconds = 1)
	print(ident + f'   Max Date established: {max_date}')



# Checks if its encrypted
encrypted = gf.is_encrypted(location_folder_name)

# Loads the unifier class
unifier = con.get_unifier_class(location_folder_name)



unified_dir = unifier.unified_folder


print(ident + 'Unifies for {}'.format(location_name))
print()


# Updates Geo
print(ident + 'Updates Geo')
unifier.update_geo()
print(ident + 'Done!')
print()


print(ident + 'Builds Datasets:')

# ----------------
# ---- Cases -----
# ----------------
print(ident + '   Cases')
df_cases = unifier.build_cases_geo()

# Checks if max date is given
if max_date is not None:
	df_cases = df_cases[df_cases.date_time < max_date]


# Extracts date
cases_date = df_cases.date_time.max()

# Saves
if not encrypted:
    df_cases.to_csv(os.path.join(unified_dir, 'cases.csv'), index = False)
else:
    gf.encrypt_df(df_cases, os.path.join(unified_dir, 'cases.csv'), key_string)


# -------------------
# ---- Movement -----
# -------------------
print(ident + '   Movement')
df_movement = fb.build_movement(location_folder_name)

# Checks if max date is given
if max_date is not None:
	df_movement.date_time = pd.to_datetime(df_movement.date_time)
	df_movement = df_movement[df_movement.date_time < max_date]

# Extracts date
movement_date = df_movement.date_time.max()

# Saves
df_movement.to_csv(os.path.join(unified_dir, 'movement.csv'), index = False)
#df_movement.loc[df_movement.start_polygon_id != df_movement.end_polygon_id].to_csv(os.path.join(data_dir, 'unified/movement_non_internal.csv'), index = False)

# -------------------
# ---- Population -----
# -------------------
print(ident + '   Population')
df_population = fb.build_population(location_folder_name)


# Checks if max date is given
if max_date is not None:
	df_population.date_time = pd.to_datetime(df_population.date_time)
	df_population = df_population[df_population.date_time < max_date]

# Extracts date
population_date = df_population.date_time.max()


# Saves
df_population.to_csv(os.path.join(unified_dir, 'population.csv'), index = False)


# -------------------
# ---- Polygons -----
# -------------------
print(ident + '   Polygons')
df_poly = unifier.build_polygons()

# Extracts date
poly_date = cases_date

# Saves
df_poly.to_csv(os.path.join(unified_dir, 'polygons.csv'), index = False)



# -------------------
# - Movement Range --
# -------------------
print(ident + '   Movement Range')
# Checks if location has movement range
if os.path.exists( os.path.join(unifier.raw_folder, 'movement_range')):
	
	print(ident + '   Movement Range')
	df_movement_range = fb.build_movement_range(location_folder_name)

	# Checks if max date is given
	if max_date is not None:
		df_movement_range.ds = pd.to_datetime(df_movement_range.ds)
		df_movement_range = df_movement_range[df_movement_range.ds < max_date]

	df_movement_range.to_csv(os.path.join(unified_dir, 'movement_range.csv'), index = False)
else:
	print(ident + '      No Movement Range Found Skipping...')


print(ident + 'Saving Dates')

#Saves the dates
with open(os.path.join(unified_dir, 'README.txt'), 'w') as file:

	file.write('Current max dates for databases:' + '\n')
	file.write('   Cases: {}'.format(cases_date) + '\n')
	file.write('   Movement: {}'.format(movement_date) + '\n')
	file.write('   Population: {}'.format(population_date) + '\n')
	file.write('   Polygons: {}'.format(poly_date) + '\n')

print(ident + 'Done! Data copied to: {}/unified'.format(location_folder_name))
print(ident + '')