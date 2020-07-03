# Polygon Prediction Analysis

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
folder_location = os.path.join(analysis_dir, location, agglomeration_method, 'prediction', polygon_name)


# Creates the folder if does not exists
if not os.path.exists(folder_location):
	os.makedirs(folder_location)


print(ident + 'Excecuting analysis for {} (polygon {} of {})'.format(polygon_display_name, polygon_id, location))

# Global variables
# Series comparison
start_day = 0
lag = 0
base_locations = None
strech = 30
accum = False
k = 20
smooth_days = 2

# Prediction build
days_back = 5
days_ahead = 8
alpha_options = [1,100,500,1000]
iterations = 50

# Plotting
k_plot = 4
fig_size = (15,8)
suptitle_font_size = 14
individual_plot_size = 12
axis_font_size = 12

# Extracts Neighbors
print(ident + '   Extracts the {} closest neighbors'.format(k))

df_neighbors = get_closest_neighbors(location = location, agglomeration_method = agglomeration_method, polygon_id = polygon_id, k = k, start_day = start_day, lag = lag, strech = strech,  verbose = False, accum = accum, smooth_days = smooth_days)
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


# Starts Predicition construction
# ---------------------

print(ident + '   Constructs Prediction Data')
# Gets current (Keeps NANs)
df_current_prediction = extract_prediction_data(agglomeration_method, [location], [polygon_id], days_back, days_ahead, max_day = None, smooth_days = smooth_days)


locations = df_neighbors.location.values.tolist()
polygons_ids = df_neighbors.polygon_id.values.tolist()


max_day = df_current_prediction.elapsed_days.max()

# Gets current (Drops NANs)
df_neighbor_prediction = extract_prediction_data(agglomeration_method, locations, polygons_ids, days_back, days_ahead, max_day = max_day, smooth_days = smooth_days).dropna()

df_prediction = pd.concat((df_current_prediction,df_neighbor_prediction), ignore_index = True)

df_prediction.to_csv(os.path.join(folder_location ,'prediction_data.csv'), index = False)

# Trains the model
print(ident + '   Trains the model')
df_results, summary_dict, weights = predict_location(location, polygon_id, df_prediction, alpha_options = alpha_options, iterations = iterations, verbose = False)

# Plots the prediction
print(ident + '   Plots Prediction')
df1 = df_results[['target_date','target_num_cases','target_num_cases_accum']].copy()
df1.rename(columns = {'target_num_cases': 'cases', 'target_num_cases_accum': 'cases_accum'}, inplace = True)
df1['Tipo'] = 'Real' 

df2 = df_results[['target_date','predicted_num_cases','predicted_num_cases_accum']].copy()
df2.rename(columns = {'predicted_num_cases': 'cases', 'predicted_num_cases_accum': 'cases_accum'}, inplace = True)
df2['Tipo'] = 'Predecido' 

df_plot = pd.concat((df1,df2), ignore_index = True)

df_plot['cases'] = df_plot.cases.astype(float)
df_plot['cases_accum'] = df_plot.cases_accum.astype(float)

# Plots
fig, ax = plt.subplots(2,1, figsize=(15,8))

fig.suptitle('Predicción para {}'.format(polygon_display_name), fontsize=suptitle_font_size)

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

fig.savefig(os.path.join(folder_location, 'prediction_{}.png'.format(polygon_name)))

print(ident + '   Exports Statistics')

with open(os.path.join(folder_location, 'statistics.txt'), 'w') as file:
	
	file.write('Agglomeration Method Used: {}'.format(agglomeration_method) + '\n')
	file.write('Prediction Summary:' + '\n')
	file.write('   Parameters:' + '\n')
	file.write('      Days Back: {}'.format(days_back) + '\n')
	file.write('      Days Ahead: {}'.format(days_ahead) + '\n')
	file.write('      Num Neighbors: {}'.format(k) + '\n')
	file.write('\n')
	file.write('   Result Statistics:' + '\n')
	for concept in summary_dict:
		file.write('      {}: {}'.format(concept, summary_dict[concept]) + '\n')
		


print(ident + 'Done!')
