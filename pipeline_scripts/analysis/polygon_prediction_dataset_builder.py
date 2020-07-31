# Polygon prediction dataser builder

# Imports
from timeseries_functions import *
from prediction_functions import *
from general_functions import *

import constants as cons

# Other imports
import os, sys

from pathlib import Path

import pandas as pd
import numpy as np


import matplotlib.pyplot as plt
import seaborn as sns
sns.set_style("whitegrid")


#Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')



# Is defined inside function for using it directly from python
def main(location, agglomeration_method, polygon_name, polygon_id, polygon_display_name, k, ident = '         '):



    try:
        polygon_id = int(polygon_id)
    except:
        pass


    # Checks if the polygon has neighbors dataset
    neighbors_location = os.path.join(analysis_dir, location, agglomeration_method, 'neighbors', polygon_name)
    if not os.path.exists(neighbors_location):
        raise ValueError(f'No neighbors found for polygon {polygon_name} ({polygon_id}). Please excecute the script: neighbors_polygon_extraction.py, before this one.')

    # Constructs the export
    folder_location = os.path.join(analysis_dir, location, agglomeration_method, 'prediction', polygon_name)


    # Creates the folder if does not exists
    if not os.path.exists(folder_location):
        os.makedirs(folder_location)


    print(ident + 'Excecuting prediction dataset for {} (polygon {} of {})'.format(polygon_display_name, polygon_id, location))

    # Global variables
    # Prediction build
    days_back = cons.days_back
    days_ahead = cons.days_ahead
    smooth_days = cons.smooth_days


    # Extracts Neighbors
    print(ident + '   Extracts the {} closest neighbors'.format(k))

    df_neighbors = pd.read_csv(os.path.join(neighbors_location, 'neighbors.csv'))

    # Extract the closest k neighbors
    df_neighbors = df_neighbors.head(min(df_neighbors.shape[0],k))


    # Starts Predicition construction
    # ---------------------

    print(ident + '   Constructs Prediction Data')
    # Gets current (Keeps NANs)
    df_current_prediction = extract_prediction_data(agglomeration_method, [location], [polygon_id], days_back, days_ahead, max_day = None, smooth_days = smooth_days)


    # Constructs the data with mobility ratio

    locations = df_neighbors.location.values.tolist()
    polygons_ids = df_neighbors.polygon_id.values.tolist()


    max_day = df_current_prediction.elapsed_days.max()

    # Gets current for neighbors (Drops NANs)
    print(ident + '   Constructs Prediction Data for Neighbors')
    df_neighbor_prediction = extract_prediction_data(agglomeration_method, locations, polygons_ids, days_back, days_ahead, max_day = max_day, smooth_days = smooth_days).dropna()

    df_prediction = pd.concat((df_current_prediction,df_neighbor_prediction), ignore_index = True)


    # Saves
    print(ident + '   Saves')
    df_prediction.to_csv(os.path.join(folder_location ,'training_data.csv'), index = False)


    print(ident + '   Exports Statistics')

    with open(os.path.join(folder_location, 'dataset_construction_statistics.txt'), 'w') as file:
        
        file.write('Agglomeration Method Used: {}'.format(agglomeration_method) + '\n')
        file.write('Prediction Summary:' + '\n')
        file.write('   Parameters:' + '\n')
        file.write('      Days Back: {}'.format(days_back) + '\n')
        file.write('      Days Ahead: {}'.format(days_ahead) + '\n')
        file.write('      Smooth Days: {}'.format(smooth_days) + '\n')
        file.write('      Num Neighbors: {}'.format(k) + '\n')
            


    print(ident + 'Done!')



if __name__ == "__main__":
    # Reads the parameters from excecution
    location  = sys.argv[1] # location name
    agglomeration_method = sys.argv[2] # Aglomeration name
    polygon_name = sys.argv[3] # polygon name
    polygon_id  = sys.argv[4] # polygon id
    polygon_display_name = sys.argv[5] # polygon display name
    k = int(sys.argv[6]) #Number of neighbors

    main(location, agglomeration_method, polygon_name, polygon_id, polygon_display_name, k)