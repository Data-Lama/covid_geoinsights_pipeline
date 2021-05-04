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

movement_path = os.path.join(data_dir, "data_stages", location_name, "agglomerated", agglomeration_folder, "movement.csv")
polygons_path = os.path.join(data_dir, "data_stages", location_name, "agglomerated", agglomeration_folder, "polygons.csv")
cases_path    = os.path.join(data_dir, "data_stages", location_name, "agglomerated", agglomeration_folder, "cases.csv")

out_folder = os.path.join(analysis_dir, location_name, agglomeration_folder, "rt")

# Check if folder exists
if not os.path.isdir(out_folder):
        os.makedirs(out_folder)

# Load movement
df_cases = pd.read_csv(cases_path, parse_dates=["date_time"])
df_bog   = df_cases.copy(); df_bog = df_bog.groupby('date_time').sum().resample('D').sum().fillna(0)

# Load geometry
df_polygons = pd.read_csv(polygons_path)
df_polygons["geometry"] = df_polygons["geometry"].apply(wkt.loads)
gdf_polygons = gpd.GeoDataFrame(df_polygons)

from special_functions.rt_estim import Rt_model

estimate = Rt_model()
def estimate_rt(incidence):
    rt_epiestim = estimate.calc_parametric_rt(incidence,
                                        mean_si=4.7,
                                        std_si=2.3,
                                        win_start=0,
                                        win_end=13, mean_prior=2.3, std_prior=2)

    return rt_epiestim

rt  = estimate_rt(df_bog[['num_cases']]).dataframe.set_index('dates').dropna()


fig, ax = plt.subplots(1, 1, figsize=(15.5, 7.2))

ax.plot(rt.index.values, rt["Rt_median"], color='k')
ax.fill_between(rt.index.values, rt["low_ci_025"], rt["high_ci_975"], color='gray', alpha=0.5)
ylim = ax.get_ylim()
ax.set_ylim([ylim[0], 2.2])
ax.axhline(y=1, linestyle='--')
plt.show()