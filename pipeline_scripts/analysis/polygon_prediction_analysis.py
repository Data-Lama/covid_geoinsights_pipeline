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

# Checks if the polygon has prediction dataset dataset
if not os.path.exists(os.path.join(folder_location,'training_data.csv')):
	raise ValueError(f'No Training Dataset found for polygon {polygon_name} ({polygon_id}). Please excecute the script: polygon_prediction_dataset_builder.py, before this one.')



print(ident + 'Excecuting analysis for {} (polygon {} of {})'.format(polygon_display_name, polygon_id, location))

# Global variables
# Neighbor extraction


# Prediction build
alpha_options = [1,100,500,1000]
iterations = 50

# Plotting
k_plot = 4
fig_size = (15,8)
suptitle_font_size = 14
individual_plot_size = 12
axis_font_size = 12

# Extracts Neighbors
print(ident + '   Extracts the Trainig Dataset')

df_prediction = pd.read_csv(os.path.join(folder_location ,'training_data.csv'), parse_dates = ['current_date','target_date'])


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

with open(os.path.join(folder_location, 'prediction_statistics.txt'), 'w') as file:
	
	file.write('Agglomeration Method Used: {}'.format(agglomeration_method) + '\n')
	file.write('   Result Statistics:' + '\n')
	for concept in summary_dict:
		file.write('      {}: {}'.format(concept, summary_dict[concept]) + '\n')
		


print(ident + 'Done!')
