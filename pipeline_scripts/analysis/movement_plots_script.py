# Movement Plots

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

# Plotting parameters
fig_size = (15,8)
suptitle_font_size = 14
individual_plot_size = 12
axis_font_size = 12
max_selected = 8

# Reads the parameters from excecution
location_name  =  sys.argv[1] # location name
location_folder =  sys.argv[2] # locatio folder name
agglomeration_method_parameter = sys.argv[3] # Aglomeration name


# Checks which aglomeration is received
if agglomeration_method_parameter.upper() == 'ALL':
	agglomeration_methods = con.agglomeration_methods
else:
	agglomeration_methods = [agglomeration_method_parameter]


i = 0
for agglomeration_method in agglomeration_methods:

	i += 1

	# Agglomerated folder location
	agglomerated_folder_location = os.path.join(data_dir, 'data_stages', location_folder, 'agglomerated', agglomeration_method)

	if not os.path.exists(agglomerated_folder_location):
		print(ident + 'No data found for {} Agglomeration ({} of {}). Skipping'.format(agglomeration_method,i , len(agglomeration_methods)))
		continue

	# Export folder location
	export_folder_location = os.path.join(analysis_dir, location_folder, agglomeration_method, 'movement_plots', 'entire_location')

	# Creates the folder if does not exists
	if not os.path.exists(export_folder_location):
		os.makedirs(export_folder_location)

	# Unified folder location
	unified_folder_location = os.path.join(data_dir, 'data_stages', location_folder, 'unified')



	print(ident + 'Excecuting Movement Analysis for {} with {} Agglomeration ({} of {}))'.format(location_name, agglomeration_method,i , len(agglomeration_methods)))

	# Loads the data
	print(ident + '   Extracts the movement for {}'.format(location_name))
	df_movement_all = pd.read_csv(os.path.join(unified_folder_location, 'movement.csv'), parse_dates = ['date_time'])

	if location_folder == 'colombia':
		df_movement_all = df_movement_all[df_movement_all.date_time <= pd.to_datetime('2020-04-22')].copy()

	# Plots movement all
	print(ident + '   Plots movement for {} (All)'.format(location_name))

	fig = plt.figure(figsize=fig_size)

	ax = sns.lineplot(data = df_movement_all, x = 'date_time', y = 'percent_change')
	ax.set_title('Cambio Movimiento en {} según datos Geoinsights (Facebook)'.format(location_name), fontsize=suptitle_font_size)
	ax.set_xlabel('Fecha', fontsize=axis_font_size)
	ax.set_ylabel('Promedio Cambio Porcentual ', fontsize=axis_font_size)

	fig.savefig(os.path.join(export_folder_location,'mov_{}.png'.format(location_folder)))

	# Plots movement Selected
	print(ident + '   Plots movement for {} (Selected)'.format(location_name))

	selected_polygons = df_movement_all.start_polygon_id.value_counts().to_frame().sort_values('start_polygon_id', ascending = False).index[0:max_selected].values
	df_movement_plot = df_movement_all[df_movement_all.start_polygon_id.isin(set(selected_polygons))]

	fig = plt.figure(figsize=fig_size)
	ax = sns.lineplot(data = df_movement_plot, x = 'date_time', y = 'percent_change', hue = 'start_polygon_name')
	ax.set_title('Cambio Movimiento en {} según datos Geoinsights (Facebook) en Lugares Principales'.format(location_name), fontsize=suptitle_font_size)
	ax.set_xlabel('Fecha', fontsize=axis_font_size)
	ax.set_ylabel('Promedio Cambio Porcentual ', fontsize=axis_font_size)
	ax.legend().texts[0].set_text("Lugar")

	fig.savefig(os.path.join(export_folder_location,'mov_fb_polygon_{}.png'.format(location_folder)))

	print(ident + '   Done')
	print()

print(ident + 'Done')