# Scripts that computes the top superpreading places and the new ones

import os
import numpy as np
import pandas as pd
from google.cloud import bigquery

# Local imports 
import special_functions.utils as butils
import special_functions.geo_functions as gfun
import superspreading_analysis as ss_analysis
import bigquery_functions as bqf

# Global Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')

# Starts the BigQuery Client
client = bigquery.Client(location="US")

# Constants
location_graph_id = "colombia_bogota" 
dataset_id = "edgelists_cities" 
location_folder_name = "bogota" 
location_name = "Bogotá" 




historic_days = 120
new_days = 14

result_folder_name = "edgelist_detection"
location_folder_name = "bogota"

# Historic
historic_folder  = os.path.join(analysis_dir, 
                                location_folder_name, 
                                result_folder_name,
                                "historic_superspreading")

if not os.path.exists(historic_folder):
    os.makedirs(historic_folder)                                  

historic_shape_file = os.path.join(historic_folder,'historic_superspreading.shp')

# Recent
recent_folder  = os.path.join(analysis_dir, 
                                location_folder_name, 
                                result_folder_name,
                                "recent_superspreading")

if not os.path.exists(recent_folder):
    os.makedirs(recent_folder)                                  

recent_shape_file = os.path.join(recent_folder,'recent_superspreading.shp')

# Computes the max date
max_date = bqf.get_date_for_graph(client, location_graph_id)

#max_date = pd.to_datetime("2021-04-30")

# Computes historic
print('Computes Historic Superspreading')
df_historic = ss_analysis.main(location_graph_id = location_graph_id, 
                                dataset_id = dataset_id, 
                                location_folder_name = location_folder_name, 
                                location_name = location_name,
                                max_date = max_date,                            
                                days_go_back = historic_days, 
                                other_geopandas_to_draw = [], 
                                save=False,
                                plot_map=False,
                                include_trace = False)

print('Computes Recent Superspreading')
df_recent = ss_analysis.main(location_graph_id = location_graph_id, 
                                dataset_id = dataset_id, 
                                location_folder_name = location_folder_name, 
                                location_name = location_name,
                                max_date = max_date,                          
                                days_go_back = new_days, 
                                other_geopandas_to_draw = [], 
                                save=False,
                                plot_map=False,
                                include_trace = False)


# Saves
df_historic.to_file(historic_shape_file)
df_recent.to_file(recent_shape_file)

# Appends
butils.add_export_info(os.path.basename(__file__), [historic_shape_file,recent_shape_file])