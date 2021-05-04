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

out_folder = os.path.join(analysis_dir, location_name, agglomeration_folder, "cases_deaths_plots")

# Check if folder exists
if not os.path.isdir(out_folder):
        os.makedirs(out_folder)

dict_correct = {'Los Martires': 'Los Mártires', 'Fontibon': 'Fontibón', 'Engativa': 'Engativá',
                            'San Cristobal': 'San Cristóbal', 'Usaquen': 'Usaquén',
                            'Ciudad Bolivar': 'Ciudad Bolívar', 'Candelaria': 'La Candelaria'}

# Load movement
df_cases = pd.read_csv(cases_path, parse_dates=["date_time"])
df_cases = df_cases.groupby(['poly_id', 'date_time'] ).sum().unstack([0]).resample('W-Sun').agg('sum').stack()[["num_cases", "num_diseased"]].reset_index().set_index('date_time')
