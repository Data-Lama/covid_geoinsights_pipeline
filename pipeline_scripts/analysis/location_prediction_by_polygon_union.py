# location prediction by polygon union
# Location Prediction Analysis

# Imports
from general_functions import *

# Other imports
import os, sys

from pathlib import Path

import pandas as pd
import numpy as np
import constants as con

import matplotlib.pyplot as plt
import seaborn as sns
sns.set_style("whitegrid")

#Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')


ident = '         '

# Reads the parameters from excecution
location_name  = sys.argv[1] # location name
location_folder = sys.argv[2] # location folder
agglomeration_method_parameter = sys.argv[3] # Aglomeration name


# Parameters
# Plotting
k_plot = 4
fig_size = (15,8)
suptitle_font_size = 14
individual_plot_size = 12
axis_font_size = 12

folder_name = 'entire_location'

# Constructs the export
data_folder = os.path.join(data_dir, 'data_stages')

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

    # Loads the polygons
    polygons = pd.read_csv(os.path.join(agglomerated_folder_location, 'polygons.csv'))

    predictions_folder_location =  os.path.join(analysis_dir, location_folder, agglomeration_method, 'prediction')
    folder_location = os.path.join(predictions_folder_location, folder_name)

    # Creates the folder if does not exists
    if not os.path.exists(folder_location):
        os.makedirs(folder_location)

    # Extract the prediction results for all polygon
    dfs = []

    for folder in os.listdir(predictions_folder_location):        
        if os.path.isdir(os.path.join(predictions_folder_location, folder)) and folder != folder_name:
            data_set_location = os.path.join(predictions_folder_location, folder, 'predicted_data.csv')
            # If exists it imports it
            if os.path.exists(data_set_location):
                data_set = pd.read_csv(data_set_location, parse_dates = ['target_date'])
                dfs.append(data_set)

    if len(dfs) == 0:
        raise ValueError(f'No predictions precomputed polygon predictions found for location: {location_name}, please excecute the script: polygon_prediction_analysis.py for selected polygons before this one.')

    # Concatenates all data frames
    df_results = pd.concat(dfs)

    # Extract the selected Polygons
    included_polygons = df_results.polygon_id.unique()

    # Extracts coverage
    coverage = polygons[polygons.poly_id.isin(included_polygons)].num_cases.sum()/polygons.num_cases.sum()

    # Removes polygon_id
    df_results.drop('polygon_id', axis = 1, inplace = True)
    

    # Plots the prediction
    print(ident + '   Plots Prediction')
    df1 = df_results[['target_date','target_num_cases','target_num_cases_accum']].copy()
    #Groups
    df1 = df1.dropna().groupby('target_date').sum().reset_index()
    df1.rename(columns = {'target_num_cases': 'cases', 'target_num_cases_accum': 'cases_accum'}, inplace = True)
    df1['Tipo'] = 'Real' 

    df2 = df_results[['target_date','predicted_num_cases','predicted_num_cases_accum']].copy()
    #Groups
    df2 = df2.groupby('target_date').sum().reset_index()
    df2.rename(columns = {'predicted_num_cases': 'cases', 'predicted_num_cases_accum': 'cases_accum'}, inplace = True)
    df2['Tipo'] = 'Predecido' 

    df_plot = pd.concat((df1,df2), ignore_index = True)

    df_plot['cases'] = df_plot.cases.astype(float)
    df_plot['cases_accum'] = df_plot.cases_accum.astype(float)

    # Plots
    fig, ax = plt.subplots(2,1, figsize=(15,8))


    fig.suptitle('Predicción para {} ({}% de los Cases)'.format(location_name, np.round(100*coverage,1)), fontsize=suptitle_font_size)

    # Plot individual Lines
    sns.lineplot(x = 'target_date', y = 'cases', hue = 'Tipo', data = df_plot, ax = ax[0])
    sns.lineplot(x = 'target_date', y = 'cases_accum', hue = 'Tipo', data = df_plot, ax = ax[1])

    # Plot titles
    ax[0].set_title('Flujo Casos', fontsize=individual_plot_size)
    ax[1].set_title('Flujo Casos' + ' (Acumulados)', fontsize=individual_plot_size)

    # Plots Axis
    ax[0].set_xlabel('Día de la Epidemia', fontsize=axis_font_size)
    ax[1].set_xlabel('Día de la Epidemia', fontsize=axis_font_size)
    ax[0].set_ylabel('Casos', fontsize=axis_font_size)
    ax[1].set_ylabel('Casos (Acumulados)', fontsize=axis_font_size)


    fig.tight_layout(pad=3.0)

    fig.savefig(os.path.join(folder_location, 'prediction_{}.png'.format(location_folder)))

    print(ident + '   Exports Statistics')

    with open(os.path.join(folder_location, 'statistics.txt'), 'w') as file:
        
        file.write('Agglomeration Method Used: {}'.format(agglomeration_method) + '\n')
        file.write('Coverage: {}%'.format(np.round(100*coverage,2)))
        file.write("   From: {}".format(' '.join([str(p) for p in included_polygons])))



    print(ident + 'Done!')

