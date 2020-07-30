# Neighbor extraction for polygon


# Imports
from timeseries_functions import *
from prediction_functions import *
from general_functions import *
import constants as con

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

ident = '         '

# Reads the parameters from excecution
location_display_name = sys.argv[1] # location
location = sys.argv[2] # location
agglomeration_method_parameter = sys.argv[3] # Aglomeration name



print(ident + 'Excecuting started for {} '.format(location_display_name))

# Global variables
# Series comparison
start_day = 0
lag = 0
strech = 30
k = None # Includes all neighbors
smooth_days = 2


# Plotting
k_plot = 4
fig_size = (15,8)
suptitle_font_size = 14
individual_plot_size = 12
axis_font_size = 12



# Checks which aglomeration is received
if agglomeration_method_parameter.upper() == 'ALL':
    agglomeration_methods = con.agglomeration_methods
else:
    agglomeration_methods = [agglomeration_method_parameter]


i = 0
for agglomeration_method in agglomeration_methods:

    i = i + 1
    # Agglomerated folder location
    agglomerated_folder_location = os.path.join(data_dir, 'data_stages', location, 'agglomerated', agglomeration_method)

    if not os.path.exists(agglomerated_folder_location):
        print(ident + 'No data found for {} Agglomeration ({} of {}). Skipping'.format(agglomeration_method,i , len(agglomeration_methods)))
        continue

    # Extracts Neighbors
    print(ident + '   Extracts All closest neighbors ({})'.format(agglomeration_method))

    # Constructs the export
    folder_location = os.path.join(analysis_dir, location, agglomeration_method, 'neighbors/entire_location')


    # Creates the folder if does not exists
    if not os.path.exists(folder_location):
        os.makedirs(folder_location)



    df_neighbors = get_closest_neighbors(location = location, agglomeration_method = agglomeration_method, polygon_id = None, k = k, start_day = start_day, lag = lag, strech = strech,  verbose = False, smooth_days = smooth_days)
    df_neighbors.to_csv(os.path.join(folder_location, 'neighbors.csv'), index = False)


    # Plots Timeseries
    # ---------------------

    print(ident + '   Plots Time Series (Closest: {} neighbors)'.format(k_plot))

    # First non accumulative
    df_neighbors_plot = df_neighbors.head(k_plot)

    all_series = []

    time_series = extract_timeseries_cases(location, agglomeration_method, polygon_id = None, lag = lag, accum = False, smooth_days = smooth_days)
    all_series.append(time_series)


    for ind, row in df_neighbors_plot.iterrows():

        time_series = extract_timeseries_cases(row.location, agglomeration_method, polygon_id = None, accum = False, smooth_days = smooth_days)
        all_series.append(time_series)
        

    df_plot = pd.concat(all_series, ignore_index = True)



    # Second:  accumulative

    all_series = []

    time_series = extract_timeseries_cases(location, agglomeration_method, polygon_id = None, lag = lag, accum = True, smooth_days = smooth_days)
    all_series.append(time_series)

    for ind, row in df_neighbors_plot.iterrows():
        
        time_series = extract_timeseries_cases(row.location, agglomeration_method, polygon_id = None, accum = True, smooth_days = smooth_days)
        all_series.append(time_series)
        

    df_plot_accum = pd.concat(all_series, ignore_index = True)


    # Cleans for publication
    df_plot.location_id = df_plot.location_id.apply(clean_for_publication)
    df_plot_accum.location_id = df_plot_accum.location_id.apply(clean_for_publication)

    # Plots Figures
    fig, ax = plt.subplots(2,1, figsize=fig_size)

    # Global title
    fig.suptitle('Flujos Similares a {}'.format(location_display_name), fontsize=suptitle_font_size)

    sns.lineplot(x = 'day', y = 'value', hue = 'location_id', data = df_plot, ax = ax[0])
    sns.lineplot(x = 'day', y = 'value', hue = 'location_id', data = df_plot_accum, ax = ax[1])


    # Plot titles
    ax[0].set_title('Flujo Casos', fontsize=individual_plot_size)
    ax[1].set_title('Flujo Casos' + ' (Acumulados)', fontsize=individual_plot_size)

    # Plots Axis
    ax[0].set_xlabel('Día de la Epidemia', fontsize=axis_font_size)
    ax[1].set_xlabel('Día de la Epidemia', fontsize=axis_font_size)
    ax[0].set_ylabel('Casos', fontsize=axis_font_size)
    ax[1].set_ylabel('Casos (Acumulados)', fontsize=axis_font_size)


    # Plot Legends
    ax[0].legend().texts[0].set_text("Lugar")
    ax[1].legend().texts[0].set_text("Lugar")


    fig.tight_layout(pad=3.0)

    fig.savefig(os.path.join(folder_location , 'neighbor_timeseries_{}.png'.format(location)))


print(ident + 'Done')

