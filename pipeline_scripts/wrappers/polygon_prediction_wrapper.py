import os
import re
import sys
import unidecode
import pandas as pd
from datetime import datetime, timedelta

# Direcotries
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')
  
# Import scripts
import pipeline_scripts.analysis.neighbors_polygon_extraction as neighbors_polygon_extraction
import pipeline_scripts.analysis.polygon_prediction_dataset_builder as polygon_prediction_dataset_builder
import pipeline_scripts.analysis.polygon_prediction_analysis as polygon_prediction_analysis

import general_functions as gf

# Constants
k = 20

gap_hours = 20

# Import selected polygons
selected_polygons = pd.read_csv('pipeline_scripts/configuration/selected_polygons.csv')

for i in selected_polygons.index:
    poly_id = selected_polygons.at[i, "poly_id"]
    location_name = selected_polygons.at[i, "location_name"]
    agglomeration = selected_polygons.at[i, "agglomeration"]
    
    # Get polygons
    polygons = os.path.join(data_dir, "data_stages", location_name, "agglomerated", agglomeration, "polygons.csv")
    try:
        df_polygons = pd.read_csv(polygons, low_memory=False)
    except:
        df_polygons = pd.read_csv(polygons, low_memory=False, encoding = 'latin-1')

    df_polygons.set_index("poly_id", inplace=True)
    poly_name = df_polygons.at[poly_id, "poly_name"]
    poly_folder_name = gf.create_folder_name(poly_name)


    # Checks if prediction has been excecuted recently
     # Checks if prediction already exists
    current_prediction_folder = os.path.join(analysis_dir, location_name, agglomeration, 'prediction/polygons' poly_folder_name)

    if os.path.exists(current_prediction_folder):
        statistics_location = os.path.join(current_prediction_folder, 'prediction_statistics.csv')

        if os.path.exists(statistics_location):

            df_stat = pd.read_csv(statistics_location)
            df_stat.index = df_stat['name']

            excecution_date = pd.to_datetime(df_stat.loc['date_time','value'])
            hours = (pd.to_datetime(datetime.now()) - excecution_date).total_seconds()/3600

            if  hours < gap_hours:
                print(ident + f'         Prediction data found ({np.round(hours,1)} hours ago. Skipping)')
                continue


    # Run scripts
    neighbors_polygon_extraction.main(location_name, agglomeration, poly_folder_name, poly_id, poly_name, ident = '         ')
    polygon_prediction_dataset_builder.main(location_name, agglomeration, poly_folder_name, poly_id, poly_name, k, ident = '         ')
    polygon_prediction_analysis.main(location_name, agglomeration, poly_folder_name, poly_id, poly_name, ident = '         ')