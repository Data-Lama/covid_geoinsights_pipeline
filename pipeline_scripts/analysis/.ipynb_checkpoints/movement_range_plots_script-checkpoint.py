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


def clean_name(s):

	d = {}
	d['Santaf- de Bogot-'] = 'Bogotá'
	d['Medell-n'] = 'Medellín'
	d['Santiago de Cali'] = 'Cali'
	d['Cartagena de Indias'] = 'Cartagena'
	d['Santa Marta (Dist. Esp.)'] = 'Santa Marta'

	for k in d:
		s = s.replace(k, d[k])

	return(s)

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

# Constructs the export
data_dir = Path(os.path.realpath(__file__)).parent.parent.parent


# Checks which aglomeration is received
if agglomeration_method_parameter.upper() == 'ALL':
	agglomeration_methods = con.agglomeration_methods
else:
	agglomeration_methods = [agglomeration_method_parameter]


i = 0
for agglomeration_method in agglomeration_methods:

	i += 1

	# Agglomerated folder location
	agglomerated_folder_location = os.path.join(data_dir, 'data/data_stages', location_folder, 'agglomerated', agglomeration_method)

	if not os.path.exists(agglomerated_folder_location):
		print(ident + 'No data found for {} Agglomeration ({} of {}). Skipping'.format(agglomeration_method,i , len(agglomeration_methods)))
		continue

	# Export folder location
	export_folder_location = os.path.join(data_dir, 'analysis', location_folder, agglomeration_method, 'movement_plots')

	# Creates the folder if does not exists
	if not os.path.exists(export_folder_location):
		os.makedirs(export_folder_location)

	# Unified folder location
	unified_folder_location = os.path.join(data_dir, 'data/data_stages', location_folder, 'unified')



	print(ident + 'Excecuting Movement Analysis for {} with {} Agglomeration ({} of {}))'.format(location_name, agglomeration_method,i , len(agglomeration_methods)))

	# Loads the data
	print(ident + '   Extracts the movement for {}'.format(location_name))

	# Loads the movement range
	df = pd.read_csv(os.path.join(unified_folder_location, 'movement_range.csv'), parse_dates = ['ds'])
	
	# Plots movement all
	print(ident + '   Plots movement for {} (All)'.format(location_name))
	
	# Global Movmeent Plot
	df_plot = df
	fig = plt.figure(figsize=fig_size)
	ax = sns.lineplot(data = df_plot, x = 'ds', y = 'all_day_bing_tiles_visited_relative_change')
	ax.set_title('Cambio Porcentual Movimiento {} según datos Geoinsights (Facebook)'.format(location_name), fontsize=suptitle_font_size)
	ax.set_xlabel('Fecha', fontsize=axis_font_size)
	ax.set_ylabel('Porcentaje Cambio', fontsize=axis_font_size)
	fig.savefig(os.path.join(export_folder_location,'mov_range_{}.png'.format(location_folder)))
	
	
	if location_name == 'Colombia':
		
	
		# Plots movement Selected
		print(ident + '   Plots movement for {} (Selected)'.format(location_name))
		
		# Creates Polygons
		# Selected places
		selected = set(['Bogotá','Medellín', 'Cali','Villavicencio','Barranquilla', 'Cartagena', 'Santa Marta', 'Leticia'])

		poly = df.groupby(['polygon_id','polygon_name']).size().reset_index().rename(columns = {0:'total'})
		poly['Lugar'] = poly.polygon_name.apply(clean_name)
		poly = poly[poly.Lugar.isin(selected)]

		# Selected Movement Plot
		df_plot = df.merge(poly, on = 'polygon_id')

		fig = plt.figure(figsize=fig_size)
		ax = sns.lineplot(data = df_plot, x = 'ds', y = 'all_day_bing_tiles_visited_relative_change', hue = 'Lugar')
		ax.set_title('Cambio Porcentual Movimiento Lugares Seleccionados según datos Geoinsights (Facebook)'.format(location_name), fontsize=suptitle_font_size)
		ax.set_xlabel('Fecha', fontsize=axis_font_size)
		ax.set_ylabel('Porcentaje', fontsize=axis_font_size)
		
		fig.savefig(os.path.join(export_folder_location,'mov_range_fb_polygon_{}.png'.format(location_folder)))

print(ident + 'Done')