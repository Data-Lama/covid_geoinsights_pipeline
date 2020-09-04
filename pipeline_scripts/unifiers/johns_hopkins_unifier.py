# Johns Hopkins Unifier

# TODO:
# Unificador que extrae los datos del repositorio de JOHNS Hopkins. Ya se encuentra actualizado y esta en data/data_repo/git_repositories/JOHNS-HOPKINS-COVID-19.

# La idea es que uno le da como parametro el nombre del lugar y el unifica los datos.
# Tambien ausume que los poligonos son una sola linea con geometria, el nombre del archivo de la geometria debe estar guardado en la descripcion.

# Imports all the necesary functions

import os
import sys
import numpy as np
import pandas as pd


import fb_functions as fb

#Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')


# Reads the parameters from excecution
country  = sys.argv[1] # location name

ident = '         '
hopkins_country_names = {"equatorial_guinea":"Equatorial Guinea",
                        "cameroon":"Cameroon",
                        "gabon":"Gabon"}


description_file = os.path.join(data_dir, 'data_stages', country, "description.csv")
try:
    df_description = pd.read_csv(description_file,index_col = 0)
except FileNotFoundError:
    raise Exception("Please add description file for {}".format(country))

try:
    hopkins_cases_folder = df_description.loc["john_hopkins_path",'value']
except KeyError:
    raise Exception("The descriptor file for country {} must specify which John's Hopkins dataset to use.")

# Creates the folders if the don't exist
hopkins_data = os.path.join(data_dir, "git_repositories", "JOHNS-HOPKINS-COVID-19", "csse_covid_19_data", hopkins_cases_folder)
unified_folder = os.path.join(data_dir, 'data_stages', country, 'unified')
raw_folder = os.path.join(data_dir, 'data_stages', country, 'raw')
raw_cases_folder = os.path.join(raw_folder, "cases")

if not os.path.exists(unified_folder):
	os.makedirs(unified_folder)

if not os.path.exists(raw_folder):
	os.makedirs(raw_folder)

if not os.path.exists(raw_cases_folder):
	os.makedirs(raw_cases_folder)

print(ident + "Recovers country specific cases for {}".format(country))
df_cases = pd.read_csv(os.path.join(hopkins_data))
df_cases_country = df_cases[df_cases["Country/Region"] == hopkins_country_names[country]].copy()
df_cases_country.to_csv(os.path.join(raw_cases_folder, "cases.csv"), index=False)

print(ident + 'Unifies for {} from John Hopkins Repository'.format(country))
print(ident + 'Builds Datasets:')
# ----------------
# ---- Cases -----
# ----------------
def get_geo_id(province_state):
    if(province_state):
        return np.nan
    else:
        return "-".join(province_state.upper().split(" "))

def get_location(province_state):
    if(province_state):
        return np.nan
    else:
        return "-".join(province_state.split(" "))

print(ident + '   Cases')
df_cases_country = pd.read_csv(os.path.join(raw_cases_folder, "cases.csv"))
df_cases = pd.DataFrame(columns=["date_time", "num_cases", "geo_id", "location", "lat", "lon"])

dates = df_cases_country.columns[4:]
for i in list(df_cases_country.index):
    df_cases_sm = pd.DataFrame()
    geo_id = get_geo_id(df_cases_country.at[i, "Province/State"])
    location = get_location(df_cases_country.at[i, "Province/State"])
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

# Extracts date
cases_date = df_cases.date_time.max()
df_cases.to_csv(os.path.join(unified_folder, "cases.csv"), index=False)

# -------------------
# ---- Movement -----
# -------------------
print(ident + '   Movement')


# -------------------
# ---- Polygons -----
# -------------------
print(ident + '   Polygons')