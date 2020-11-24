# Individual prediction for critical places of a location


# Imports
from general_functions import *



# Other imports
import os, sys
from datetime import timedelta, datetime
from pathlib import Path
import shutil

import pandas as pd
import numpy as np
import constants as con


# Imports the polygons scripts
import analysis.neighbors_polygon_extraction as neighbors_polygon_extraction
import analysis.polygon_prediction_dataset_builder as polygon_prediction_dataset_builder
import analysis.polygon_prediction_analysis as polygon_prediction_analysis
import analysis.polygon_union_prediction as polygon_union_prediction

#Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')



gap_hours = 10

if gap_hours >= 24:
    print(f'WARNING: Gap hours is set to {gap_hours} please check if excecuting in production!!!!')
    
ident = '         '

# Reads the parameters from excecution
location_folder = sys.argv[1] # location folder
agglomeration_method = sys.argv[2] # Aglomeration name
coverage = float(sys.argv[3]) # percentage coverage
k = int(sys.argv[4]) # num neighbors
selected_polygons_name = sys.argv[5]


# The folder name
selected_polygons_folder_name = 'entire_location'
selected_polygons = []

if len(sys.argv) > 6:

    selected_polygons_folder_name =  sys.argv[6]

    i = 7
    while i < len(sys.argv):
        selected_polygons.append(sys.argv[i])
        i += 1

    if len(selected_polygons) == 0:
        raise ValueError('If selected polygos name is given, at least one polygon id must be provided')



# Global location folder
data_folder = os.path.join(data_dir, 'data_stages')

folder_name = 'entire_location'


# Agglomerated folder location
agglomerated_folder_location = os.path.join(data_dir, 'data_stages', location_folder, 'agglomerated', agglomeration_method)

if not os.path.exists(agglomerated_folder_location):
    raise ValueError('No data found for {} Agglomeration'.format(agglomeration_method))



print(ident + 'Excecuting analysis for {} with Agglomeration {}'.format(location_folder, agglomeration_method ))


# Polygon prediction folder
polygons_predictions_folder_location =  os.path.join(analysis_dir, location_folder, agglomeration_method, 'prediction/polygons')

# 
predictions_folder_location =  os.path.join(analysis_dir, location_folder, agglomeration_method, 'prediction/polygon_unions')

# Creates the folder if does not exists
if not os.path.exists(predictions_folder_location):
    os.makedirs(predictions_folder_location)



print(ident + '   Extracts Selected Polygons for Coverage {}%'.format(100*coverage))

# Extract polygons
polygons = pd.read_csv(os.path.join(agglomerated_folder_location, 'polygons.csv'))
polygons.poly_id = polygons.poly_id.astype(str)


if len(selected_polygons) > 0:
    polygons = polygons[polygons.poly_id.isin(selected_polygons)].copy()

polygons = polygons.sort_values('num_cases', ascending = False)

# Defines Coverage
polygons['coverage'] = polygons['num_cases'].rolling(min_periods=1, window=polygons.shape[0]).sum() /  polygons['num_cases'].sum()

# Selects all below and the next one
polygons = pd.concat((polygons[polygons.coverage <= coverage], polygons[polygons.coverage > coverage].iloc[[0]]))


print(ident + f'   Will Construct Predictions for {polygons.shape[0]} Polygons')

print()

i = 0
for ind, row in polygons.iterrows():

    i += 1

    location  = location_folder
    agglomeration_method = agglomeration_method
    polygon_name =  create_folder_name(row.poly_name)
    polygon_id  = row.poly_id
    polygon_display_name = clean_for_publication(row.poly_name)

    print(ident + f'      Polygon: {polygon_display_name} ({i} of {polygons.shape[0]})')

    # Checks if prediction already exists
    current_prediction_folder = os.path.join(polygons_predictions_folder_location, polygon_name)
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

    
    # Neighbors
    neighbors_polygon_extraction.main(location_folder, agglomeration_method, polygon_name, polygon_id, polygon_display_name, ident = ident + '         ')
    # Data Set
    polygon_prediction_dataset_builder.main(location_folder, agglomeration_method, polygon_name, polygon_id, polygon_display_name, k, ident = ident + '         ')
    # Prediction
    polygon_prediction_analysis.main(location_folder, agglomeration_method, polygon_name, polygon_id, polygon_display_name, ident = ident + '         ')
    
    print()

# Plots
print(ident + '   Plots Predictions')
polygon_union_prediction.main(location_folder, agglomeration_method, selected_polygons_name, selected_polygons_folder_name, selected_polygons)

print(ident + '   Done')


