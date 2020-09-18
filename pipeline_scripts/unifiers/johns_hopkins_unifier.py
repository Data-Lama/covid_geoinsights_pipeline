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


# Reads the parameters from excecution
location_name  = sys.argv[1] # Location name
location_folder_name = sys.argv[2] # location folder name
geo_level = int(sys.argv[3]) # GADM geographic level

# If a fourth parameter is given is the max_date
max_date = None
if len(sys.argv) > 4:
	max_date = pd.to_datetime( sys.argv[4])

	# Adjusts max date (so that it can include the given date)
	max_date = max_date + timedelta(days = 1) - timedelta(seconds = 1)
	print(ident + f'   Max Date established: {max_date}')

	

ident = '         '


hopkins_cases_folder = 'csse_covid_19_time_series/time_series_covid19_confirmed_global.csv'


description_file = os.path.join(data_dir, 'data_stages', location_folder_name, "description.csv")

try:
    df_description = pd.read_csv(description_file,index_col = 0)
except FileNotFoundError:
    raise Exception("Please add description file for {}".format(location_folder_name))



# Creates the folders if the don't exist
hopkins_data = os.path.join(data_dir, "git_repositories", "JOHNS-HOPKINS-COVID-19", "csse_covid_19_data", hopkins_cases_folder)
unified_folder = os.path.join(data_dir, 'data_stages', location_folder_name, 'unified')
raw_folder = os.path.join(data_dir, 'data_stages', location_folder_name, 'raw')
raw_cases_folder = os.path.join(raw_folder, "cases")

if not os.path.exists(unified_folder):
	os.makedirs(unified_folder)

if not os.path.exists(raw_folder):
	os.makedirs(raw_folder)

if not os.path.exists(raw_cases_folder):
	os.makedirs(raw_cases_folder)

print(ident + "Recovers country specific cases for {}".format(location_folder_name))
df_cases = pd.read_csv(os.path.join(hopkins_data))
df_cases_country = df_cases[df_cases["Country/Region"] == con.hopkins_country_names[location_folder_name]].copy()
df_cases_country.to_csv(os.path.join(raw_cases_folder, "cases.csv"), index=False)

print(ident + 'Unifies for {} from John Hopkins Repository'.format(location_folder_name))



print(ident + 'Builds Datasets:')

# ----------------
# ---- Cases -----
# ----------------
def get_geo_id(province_state, country):
    if pd.isna(province_state) and pd.isna(country):
        return np.nan
    elif pd.isna(province_state):
        return("-".join(country.upper().split(" ")))
    else:
        return "-".join(province_state.upper().split(" "))

def get_location(province_state, country):
    if pd.isna(province_state) and pd.isna(country):
        return np.nan
    elif pd.isna(province_state):
        return(country)
    else:
        return(province_state)

print(ident + '   Cases')
df_cases_country = pd.read_csv(os.path.join(raw_cases_folder, "cases.csv"))
df_cases = pd.DataFrame(columns=["date_time", "num_cases", "geo_id", "location", "lat", "lon"])

dates = df_cases_country.columns[4:]
for i in list(df_cases_country.index):
    df_cases_sm = pd.DataFrame()
    geo_id = get_geo_id(df_cases_country.at[i, "Province/State"], df_cases_country.at[i, "Country/Region"])
    location = get_location(df_cases_country.at[i, "Province/State"], df_cases_country.at[i, "Country/Region"])

    lat = df_cases_country.at[i, "Lat"]
    lon = df_cases_country.at[i, "Long"]
    df_cases_by_date = df_cases_country.iloc[i, 4:].copy().to_numpy()
    df_cases_by_date_tmp = df_cases_country.iloc[i, 4:-1].copy().to_numpy()
    df_cases_by_date_tmp = np.insert(df_cases_by_date_tmp, 0, 0, axis=0)
    df_cases_by_date = np.subtract(df_cases_by_date, df_cases_by_date_tmp)
    df_cases_sm["date_time"] = dates
    df_cases_sm["num_cases"] = df_cases_by_date
    df_cases_sm["geo_id"] = geo_id
    df_cases_sm["location"] = location
    df_cases_sm["lat"] = lat
    df_cases_sm["lon"] = lon
    df_cases = df_cases.append(df_cases_sm)


# Removes the starting zeros
index = np.arange(df_cases.shape[0])
starting = np.min(index[df_cases.num_cases > 0])
df_cases = df_cases.iloc[starting:].copy()


# Parse the dates
df_cases['date_time'] = pd.to_datetime(df_cases['date_time']).apply(lambda s: s.strftime("%Y-%m-%d"))


# Checks if max date is given
if max_date is not None:
	df_cases = df_cases[df_cases.date_time < max_date]

# Extracts date
cases_date = df_cases.date_time.max()


df_cases.to_csv(os.path.join(unified_folder, "cases.csv"), index=False)



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
df_movement.to_csv(os.path.join(unified_folder, 'movement.csv'), index = False)




# -------------------
# ---- Population -----
# -------------------
print(ident + '   Population')
# Mock
df_population = fb.build_empty_population()

# Checks if max date is given
if max_date is not None:
	df_population.date_time = pd.to_datetime(df_population.date_time)
	df_population = df_population[df_population.date_time < max_date]


# Extracts date
population_date = df_population.date_time.max()
df_population.to_csv(os.path.join(unified_folder, 'population.csv'), index = False)



# -------------------
# ---- Polygons -----
# -------------------
print(ident + '   Polygons')


# Extracts geometry
polygons = geo_fun.get_gadm_polygons(location_folder_name, geo_level)

# Extracts the center
centroids = geo_fun.get_centroids(polygons.geometry)
polygons['poly_lon'] = centroids.x
polygons['poly_lat'] = centroids.y

polygons = polygons[['poly_id', 'poly_name', 'geometry', 'poly_lon', 'poly_lat']].copy()

polygons.to_csv(os.path.join(unified_folder, 'polygons.csv'), index = False)


# -------------------
# - Movement Range --
# -------------------
print(ident + '   Movement Range')
# Checks if location has movement range
if os.path.exists( os.path.join(raw_folder, 'movement_range')):
	
	print(ident + '   Movement Range')
	df_movement_range = fb.build_movement_range(location_folder_name)

	# Checks if max date is given
	if max_date is not None:
		df_movement_range.ds = pd.to_datetime(df_movement_range.ds)
		df_movement_range = df_movement_range[df_movement_range.ds < max_date]

	df_movement_range.to_csv(os.path.join(unified_folder, 'movement_range.csv'), index = False)
else:
	print(ident + '      No Movement Range Found Skipping...')


print(ident + 'Saving Dates')

#Saves the dates
with open(os.path.join(unified_folder, 'README.txt'), 'w') as file:

	file.write('Current max dates for databases:' + '\n')
	file.write('   Cases: {}'.format(cases_date) + '\n')
	file.write('   Movement: {}'.format(movement_date) + '\n')
	file.write('   Population: {}'.format(population_date) + '\n')

print(ident + 'Done! Data copied to: {}/unified'.format(location_folder_name))
print(ident + '')
