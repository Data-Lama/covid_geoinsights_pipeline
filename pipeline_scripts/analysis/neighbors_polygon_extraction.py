# Neighbor extraction for polygon


# Imports
from timeseries_functions import *
from prediction_functions import *
from general_functions import *

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
location  = sys.argv[1] # location name
agglomeration_method = sys.argv[2] # Aglomeration name
polygon_name = sys.argv[3] # polygon name
polygon_id  = sys.argv[4] # polygon id
polygon_display_name = sys.argv[5] # polygon display name


try:
    polygon_id = int(polygon_id)
except:
    pass


# Constructs the export
folder_location = os.path.join(analysis_dir, location, agglomeration_method, 'neighbors', polygon_name)


# Creates the folder if does not exists
if not os.path.exists(folder_location):
    os.makedirs(folder_location)


print(ident + 'Excecuting started for {} (polygon {} of {})'.format(polygon_display_name, polygon_id, location))

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

# Extracts Neighbors
print(ident + '   Extracts All closest neighbors')

df_neighbors = get_closest_neighbors(location = location, agglomeration_method = agglomeration_method, polygon_id = polygon_id, k = k, start_day = start_day, lag = lag, strech = strech,  verbose = False, smooth_days = smooth_days)
df_neighbors.to_csv(os.path.join(folder_location, 'neighbors.csv'), index = False)


# Plots Timeseries
# ---------------------

print(ident + '   Plots Time Series (Closest: {} neighbors)'.format(k_plot))

# First non accumulative
df_neighbors_plot = df_neighbors.head(k_plot)

all_series = []

time_series = extract_timeseries(location, agglomeration_method, polygon_id, lag = lag, accum = False, smooth_days = smooth_days)
time_series.to_csv('temp1.csv', index = False)
time_series['location'] = polygon_display_name
all_series.append(time_series)

for ind, row in df_neighbors_plot.iterrows():
    
    if row.polygon_id != polygon_id:
        time_series = extract_timeseries(row.location, agglomeration_method, row.polygon_id, accum = False, smooth_days = smooth_days)
        time_series['location'] = str(row.polygon_name) + '-' + row.location
        all_series.append(time_series)
    

df_plot = pd.concat(all_series, ignore_index = True)



# Second:  accumulative

all_series = []

time_series = extract_timeseries(location, agglomeration_method, polygon_id, lag = lag, accum = True, smooth_days = smooth_days)
time_series['location'] = polygon_display_name
all_series.append(time_series)

for ind, row in df_neighbors_plot.iterrows():
    
    if row.polygon_id != polygon_id:
        time_series = extract_timeseries(row.location, agglomeration_method, row.polygon_id, accum = True, smooth_days = smooth_days)
        time_series['location'] = str(row.polygon_name) + '-' + row.location
        all_series.append(time_series)
    

df_plot_accum = pd.concat(all_series, ignore_index = True)


# Cleans for publication
df_plot.location = df_plot.location.apply(clean_for_publication)
df_plot_accum.location = df_plot_accum.location.apply(clean_for_publication)

# Plots Figures
fig, ax = plt.subplots(2,1, figsize=fig_size)

# Global title
fig.suptitle('Flujos Similares a {}'.format(polygon_display_name), fontsize=suptitle_font_size)

sns.lineplot(x = 'day', y = 'num_cases', hue = 'location', data = df_plot, ax = ax[0])
sns.lineplot(x = 'day', y = 'num_cases', hue = 'location', data = df_plot_accum, ax = ax[1])


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

fig.savefig(os.path.join(folder_location , 'neighbor_timeseries_{}.png'.format(polygon_name)))


