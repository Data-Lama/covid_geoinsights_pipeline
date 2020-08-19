# Individual prediction for critical places of a location


# Imports
from general_functions import *



# Other imports
import os, sys
from datetime import timedelta
from pathlib import Path
import shutil

import pandas as pd
import numpy as np
import constants as con


# Imports the polygons scripts
import analysis.neighbors_polygon_extraction as neighbors_polygon_extraction
import analysis.polygon_prediction_dataset_builder as polygon_prediction_dataset_builder
import analysis.polygon_prediction_analysis as polygon_prediction_analysis

#Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')


ident = '         '

# Reads the parameters from excecution
location_name  = sys.argv[1] # location name
location_folder = sys.argv[2] # location folder
agglomeration_method_parameter = sys.argv[3] # Aglomeration name
coverage = float(sys.argv[4]) # percentage coverage
k = int(sys.argv[5]) # num neighbors


# Global location folder
data_folder = os.path.join(data_dir, 'data_stages')

folder_name = 'entire_location'

# Checks which aglomeration is received
if agglomeration_method_parameter.upper() == 'ALL':
    agglomeration_methods = con.agglomeration_methods
else:
    agglomeration_methods = [agglomeration_method_parameter]


i = 0
for agglomeration_method in agglomeration_methods:

    i = i + 1
    # Agglomerated folder location
    agglomerated_folder_location = os.path.join(data_dir, 'data_stages', location_folder, 'agglomerated', agglomeration_method)

    if not os.path.exists(agglomerated_folder_location):
        print(ident + 'No data found for {} Agglomeration ({} of {}). Skipping'.format(agglomeration_method,i , len(agglomeration_methods)))
        continue


    print(ident + 'Excecuting analysis for {} with Agglomeration {} ({} of {})'.format(location_name, agglomeration_method,i , len(agglomeration_methods)))


    predictions_folder_location =  os.path.join(analysis_dir, location_folder, agglomeration_method, 'prediction')

    # Creates the folder if does not exists
    if not os.path.exists(predictions_folder_location):
        os.makedirs(predictions_folder_location)



    print(ident + '   Removing old predictions.')
    
    # Removes the old predictions
    for folder in os.listdir(predictions_folder_location):      
        temp_folder =  os.path.join(predictions_folder_location, folder)
        if os.path.isdir(temp_folder) and folder != folder_name:            
            shutil.rmtree(temp_folder)

    print(ident + '   Extracts Selected Polygons for Coverage {}%'.format(100*coverage))

    # Extract polygons
    polygons = pd.read_csv(os.path.join(agglomerated_folder_location, 'polygons.csv'))
    polygons.sort_values('num_cases', ascending = False)

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
        
        # Neighbors
        neighbors_polygon_extraction.main(location, agglomeration_method, polygon_name, polygon_id, polygon_display_name, ident = ident + '         ')
        # Data Set
        polygon_prediction_dataset_builder.main(location, agglomeration_method, polygon_name, polygon_id, polygon_display_name, k, ident = ident + '         ')
        # Prediction
        polygon_prediction_analysis.main(location, agglomeration_method, polygon_name, polygon_id, polygon_display_name, ident = ident + '         ')

        print()

    print(ident + '   Done')


print(ident + 'Done')