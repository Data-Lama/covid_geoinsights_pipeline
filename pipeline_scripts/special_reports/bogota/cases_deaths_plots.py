import os
import sys
import math
import numpy as np
import pandas as pd
import networkx as nx
from shapely import wkt

import geopandas as gpd
import matplotlib.pyplot as plt

# Local imports
import bogota_constants as cons

sys.path.insert(0,'../../..')

from global_config import config

data_dir    = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')

# Reads the parameters from excecution
location_name        =  'bogota'   # sys.argv[1] # location name # bogota
agglomeration_folder =  'geometry' # sys.argv[2] # agglomeration name #

polygonspop_path = os.path.join(data_dir, "data_stages", location_name, "agglomerated", agglomeration_folder, "polygons_pop.csv")
movement_path = os.path.join(data_dir, "data_stages", location_name, "agglomerated", agglomeration_folder, "movement.csv")
polygons_path = os.path.join(data_dir, "data_stages", location_name, "agglomerated", agglomeration_folder, "polygons.csv")
cases_path    = os.path.join(data_dir, "data_stages", location_name, "agglomerated", agglomeration_folder, "cases.csv")

out_folder = os.path.join(analysis_dir, location_name, agglomeration_folder, "cases_deaths_plots")

# Check if folder exists
if not os.path.isdir(out_folder):
        os.makedirs(out_folder)

dict_correct = {'Los Martires': 'Los Mártires', 'Fontibon': 'Fontibón', 'Engativa': 'Engativá',
                            'San Cristobal': 'San Cristóbal', 'Usaquen': 'Usaquén',
                            'Ciudad Bolivar': 'Ciudad Bolívar', 'Candelaria': 'La Candelaria'}

# Load Polygons
df_polygons = pd.read_csv(polygons_path)
df_polygons_pop = pd.read_csv(polygonspop_path).set_index('poly_id')

df_polygons["deaths_per10000"] = df_polygons.apply(lambda x: x["num_diseased"] * 10000 / df_polygons_pop.loc[x.poly_id]["population"], axis=1)
df_polygons = df_polygons.sort_values(by='deaths_per10000')
df_polygons["poly_name"] = df_polygons["poly_id"].map(lambda x: ' '.join([w.capitalize() for w in x.replace('colombia_bogota_localidad_', '').split('_')])).replace(dict_correct)

# Load cases
df_cases = pd.read_csv(cases_path, parse_dates=["date_time"])
df_cases = df_cases.groupby(['poly_id', 'date_time'] ).sum().unstack([0]).resample('W-Sun').agg('sum').stack()[["num_cases", "num_diseased"]].reset_index().set_index('date_time')
df_cases["poly_name"] = df_cases["poly_id"].map(lambda x: ' '.join([w.capitalize() for w in x.replace('colombia_bogota_localidad_', '').split('_')])).replace(dict_correct)
df_cases = df_cases[["poly_id", "poly_name", "num_cases", "num_diseased"]]
df_cases["cases_per10000"]  = df_cases.apply(lambda x: x["num_cases"] * 10000 / df_polygons_pop.loc[x.poly_id]["population"], axis=1)
df_cases["deaths_per10000"] = df_cases.apply(lambda x: x["num_diseased"] * 10000 / df_polygons_pop.loc[x.poly_id]["population"], axis=1)

df_cases_hm = pd.pivot(df_cases.reset_index(), index='poly_name', columns='date_time', values='cases_per10000'); df_cases_hm = df_cases_hm.loc[df_polygons.poly_name]
df_deaths_hm = pd.pivot(df_cases.reset_index(), index='poly_name', columns='date_time', values='deaths_per10000'); df_deaths_hm = df_deaths_hm.loc[df_polygons.poly_name]

fig, axes = plt.subplots(2, 1, figsize=(15.7, 10), sharex=True)
axes[0].pcolormesh(df_cases_hm.keys(), df_cases_hm.index.values, df_cases_hm.to_numpy())
axes[1].pcolormesh(df_deaths_hm.keys(), df_deaths_hm.index.values, df_deaths_hm.to_numpy(), cmap='afmhot_r')

axes[0].set_title('Casos por 10,000 Hab.')
axes[1].set_title('Fallecidos por 10,000 Hab.')
plt.tight_layout()

plt.show()