# Movement Plots for selected polygons
# Other imports
import os, sys

from pathlib import Path

import pandas as pd
import numpy as np
from datetime import timedelta
import constants as con

import matplotlib.pyplot as plt
import seaborn as sns
sns.set_style("whitegrid")

# Direcotries
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')


ident = '         '

# Plotting parameters
fig_size = (15,8)
suptitle_font_size = 16
individual_plot_size = 12
axis_font_size = 12
max_selected = 8

# Reads the parameters from excecution
location_name  =  sys.argv[1] # location name
location_folder =  sys.argv[2] # polygon name
agglomeration_method_parameter = sys.argv[3] # Aglomeration name



# Change Function
def get_percentage_difference(df, smooth_days = 1):


	df.sort_values('date_time', inplace = True)
	base_line = np.mean(df['movement'].values[0:(min(5,df.shape[0]))])
	df['movement_change'] = 100*(df['movement'] - base_line)/base_line    
	df['movement_change'] = df['movement_change'].rolling(smooth_days, min_periods=1).mean()

	return(df)

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
	export_folder_location = os.path.join(analysis_dir, location_folder, agglomeration_method, 'agglomerated_polygons_plots')

	# Creates the folder if does not exists
	if not os.path.exists(export_folder_location):
		os.makedirs(export_folder_location)

	print(ident + 'Excecuting Movement Analysis for {} with {} Agglomeration ({} of {})'.format(location_name, agglomeration_method,i , len(agglomeration_methods)))

	# Loads the data
	print(ident + '      Extracts the movement for {}'.format(location_name))
	df_movement = pd.read_csv(os.path.join(agglomerated_folder_location, 'movement.csv'), parse_dates = ['date_time'])
	polygons = pd.read_csv(os.path.join(agglomerated_folder_location, 'polygons.csv'))
	df_cases = pd.read_csv(os.path.join(agglomerated_folder_location, 'cases.csv'), parse_dates = ['date_time'])


	polygons = polygons.sort_values('num_cases', ascending = False)
	polygons.loc[polygons.index[max_selected:],'poly_name'] = 'Otros (Promedio)'

	# Adds the name to the movement
	df_movement = df_movement.merge(polygons[['poly_id','poly_name']], left_on = 'start_poly_id', right_on = 'poly_id').rename(columns = {'poly_name':'start_poly_name'})
	df_movement = df_movement.merge(polygons[['poly_id','poly_name']], left_on = 'end_poly_id', right_on = 'poly_id').rename(columns = {'poly_name':'end_poly_name'})

	df_cases = df_cases.merge(polygons[['poly_id','poly_name']], on = 'poly_id')


	# Plots Internal movement all
	print(ident + '      Plots internal Movement')

	fig = plt.figure(figsize=fig_size)
	df_plot = df_movement.loc[df_movement.start_poly_id == df_movement.end_poly_id, ['date_time','start_poly_name', 'movement']].groupby(['date_time','start_poly_name']).mean().reset_index()
	df_plot = df_plot[df_plot.date_time <= (df_plot.date_time.max() - timedelta(days = 1))]

	df_plot = df_plot.groupby('start_poly_name').apply(lambda df: get_percentage_difference(df, 5)).dropna()
	
	# Adjust from percentage to rate
	df_plot['movement_change'] = df_plot['movement_change'].divide(100)

	ax = sns.lineplot(data = df_plot, x = 'date_time', y = 'movement_change', hue = 'start_poly_name')
	ax.set_title('Cambio Porcentual Movimiento Interno en Unidades de Control para {}'.format(location_name), fontsize=suptitle_font_size)
	ax.set_xlabel('Fecha', fontsize=axis_font_size)
	ax.set_ylabel('Proporción (0-1)', fontsize=axis_font_size)
	ax.legend().texts[0].set_text("Unidad de Control")
	
	fig.savefig(os.path.join(export_folder_location,'internal_movement_selected_polygons_{}.png'.format(location_folder)))


	# Plots External movement all
	print(ident + '      Plots External Movement')

	fig = plt.figure(figsize=fig_size)
	df_plot = df_movement.loc[df_movement.start_poly_id != df_movement.end_poly_id, ['date_time','start_poly_name', 'end_poly_name', 'movement']].copy()
	# Assigns the start value to not other
	df_plot.loc[df_plot.start_poly_name == 'Otros (Promedio)', 'start_poly_name'] = df_plot.loc[df_plot.start_poly_name == 'Otros (Promedio)', 'end_poly_name'] 
	
	df_plot = df_plot[['date_time','start_poly_name', 'movement']].groupby(['date_time','start_poly_name']).mean().reset_index()
	
	df_plot = df_plot.groupby('start_poly_name').apply(lambda df: get_percentage_difference(df, 5)).dropna()
	
	ax = sns.lineplot(data = df_plot, x = 'date_time', y = 'movement_change', hue = 'start_poly_name')
	ax.set_title('Cambio Porcentual  Movimiento Externo en Unidades de Control para {}'.format(location_name), fontsize=suptitle_font_size)
	ax.set_xlabel('Fecha', fontsize=axis_font_size)
	ax.set_ylabel('Proporción (0-1)', fontsize=axis_font_size)
	ax.legend().texts[0].set_text("Unidad de Control")

	fig.savefig(os.path.join(export_folder_location,'external_movement_selected_polygons_{}.png'.format(location_folder)))


	# Plots Cases
	print(ident + '      Plots Cases')

	fig = plt.figure(figsize=fig_size)
	df_plot = df_cases[['date_time','poly_name', 'num_cases']].groupby(['date_time','poly_name']).sum().reset_index()
	df_plot['num_cases'] = df_plot['num_cases'].rolling(4, min_periods=1).mean()
	ax = sns.lineplot(data = df_plot, x = 'date_time', y = 'num_cases', hue = 'poly_name')
	
	ax.set_title('Número de Casos en Unidades de Control para {}'.format(location_name), fontsize=suptitle_font_size)
	ax.set_xlabel('Fecha', fontsize=axis_font_size)
	ax.set_ylabel('Número de Casos', fontsize=axis_font_size)
	ax.legend().texts[0].set_text("Unidad de Control")

	fig.savefig(os.path.join(export_folder_location,'cases_selected_polygons_{}.png'.format(location_folder)))

	# Plots Cases
	print(ident + '      Plots Accumulative Cases')
	def accumulative(df):
		df['num_cases'] = df['num_cases'].rolling(min_periods=1, window=df.shape[0]).sum()
		return(df)

	fig = plt.figure(figsize=fig_size)
	df_plot = df_cases[['date_time','poly_name', 'num_cases']].groupby(['date_time','poly_name']).sum().reset_index().sort_values('date_time')


	df_plot = df_plot.groupby('poly_name').apply(accumulative).reset_index()

	ax = sns.lineplot(data = df_plot, x = 'date_time', y = 'num_cases', hue = 'poly_name')
	ax.set_title('Número de Casos Acumulados en Unidades de Control para {}'.format(location_name), fontsize=suptitle_font_size)
	ax.set_xlabel('Fecha', fontsize=axis_font_size)
	ax.set_ylabel('Número de Casos', fontsize=axis_font_size)
	ax.legend().texts[0].set_text("Unidad de Control")

	fig.savefig(os.path.join(export_folder_location,'cases_accum_selected_polygons_{}.png'.format(location_folder)))



	print(ident + 'Done')