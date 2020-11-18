import os
import sys
import unidecode
import pandas as pd
import matplotlib.pyplot as plt

# Direcotries
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')

import general_functions as gf

# import scripts
import pipeline_scripts.functions.   as polygon_socio_economic_analysis

# Import selected polygons
selected_polygons = pd.read_csv('pipeline_scripts/configuration/selected_polygons.csv')

# Get export_files.csv
export_file = os.path.join("pipeline_scripts", "configuration", "export_files.csv")
df_export_files = pd.read_csv(export_file)

# Get countries
countries = list(selected_polygons["location_name"].unique())

# analysis/community/rt/entire_location

# Get cases
df_cases = pd.read_csv( os.path.join( agglomerated_folder, 'cases.csv' ) )

## add time delta
df_polygons   = pd.read_csv( os.path.join( agglomerated_folder ,  "polygons.csv") )
df_polygons["attr_time-delay_dist_mix"] = df_polygons["attr_time-delay_dist_mix"].fillna("")
df_polygons["attr_time_delay"] = df_polygons.apply(lambda x: np.fromstring(x["attr_time-delay_dist_mix"], sep="|"), axis=1)