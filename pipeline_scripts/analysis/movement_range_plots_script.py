# Movement Plots

# Other imports
import os, sys

from pathlib import Path

import pandas as pd
import numpy as np
import constants as con
import general_functions as gf

from datetime import timedelta

import matplotlib.pyplot as plt
import seaborn as sns
sns.set_style("whitegrid")

#Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')


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
max_selected = 7
height = 2.5
aspect= 5
num_ticks = 6

# Reads the parameters from excecution
location_name  =  sys.argv[1] # location name
location_folder =  sys.argv[2] # locatio folder name
agglomeration_method_parameter = sys.argv[3] # Aglomeration name

miles_stones_color = 'lightblue'
miles_stones_width = 1

cut_line_color = 'red'
cut_stones_width = 1


# Checks which aglomeration is received
if agglomeration_method_parameter.upper() == 'ALL':
	agglomeration_methods = con.agglomeration_methods
else:
	agglomeration_methods = [agglomeration_method_parameter]


i = 0
for agglomeration_method in agglomeration_methods:

	i += 1


	unit_type, unit_type_prural = gf.get_agglomeration_names(agglomeration_method)

	# Agglomerated folder location
	agglomerated_folder_location = os.path.join(data_dir, 'data_stages', location_folder, 'agglomerated', agglomeration_method)

	# Unified folder location
	unified_folder_location = os.path.join(data_dir, 'data_stages', location_folder, 'unified')

	# Raw folder location
	raw_folder_location = os.path.join(data_dir, 'data_stages', location_folder, 'raw')	


	movement_range_file = os.path.join(agglomerated_folder_location, 'movement_range.csv')

	if not os.path.exists(movement_range_file):
		print(ident + 'No Movement range data found for {} Agglomeration ({} of {}). Skipping'.format(agglomeration_method,i , len(agglomeration_methods)))
		continue


	# Export folder location
	export_folder_location = os.path.join(analysis_dir, location_folder, agglomeration_method, 'movement_plots', 'entire_location')

	# Creates the folder if does not exists
	if not os.path.exists(export_folder_location):
		os.makedirs(export_folder_location)


	print(ident + 'Excecuting Movement Analysis for {} with {} Agglomeration ({} of {}))'.format(location_name, agglomeration_method,i , len(agglomeration_methods)))

	# Loads the data
	print(ident + '   Extracts the movement and cases for {}'.format(location_name))

	# Reads Polygons
	polygons = pd.read_csv(os.path.join(agglomerated_folder_location, 'polygons.csv'))

	df_mov_range = pd.read_csv(movement_range_file, parse_dates = ['date_time'])

	# Loads the movement range
	df_mov = df_mov_range.copy()





	# Merges with polygons
	pop_attr = "attr_population"

	if pop_attr in polygons.columns:
		print(ident + '   Population Found, averaging movement by population.')

		# Averages with population size as factor
		df_mov = df_mov.merge(polygons, on = 'poly_id')
		df_mov = df_mov[['date_time',pop_attr, 'movement_change']]

		df_mov['prod'] = df_mov['movement_change']*df_mov[pop_attr]

		# Grpoupby
		df_mov = df_mov[['date_time','prod',pop_attr]].groupby('date_time').sum().reset_index()

		# Final Variable
		df_mov['value'] = df_mov['prod']/df_mov[pop_attr]


	else:
		df_mov.rename(columns = {'movement_change':'value'}, inplace = True)


	df_mov = df_mov[['date_time','value']].copy()
	df_mov['type'] = 'movement'
	df_mov['Tipo'] = 'Movimiento'

	df_mov_plot = df_mov[['date_time','value','type','Tipo']].copy()

	# Loads cases
	df_cases_raw = pd.read_csv(os.path.join(unified_folder_location, 'cases.csv'), parse_dates = ['date_time'])
	df_cases_all = df_cases_raw.rename(columns = {'num_cases':'value', 'geo_id': 'polygon_id'})
	df_cases_all = df_cases_all[['date_time','value', 'polygon_id']].copy()
	df_cases = df_cases_all[['date_time','value']].groupby('date_time').sum().reset_index()


	# Somooths
	df_cases['value'] = gf.smooth_curve(df_cases['value'], con.smooth_days )
	df_cases['type'] = 'cases'
	df_cases['Tipo'] = 'Casos (Fecha Diagnóstico)' 

	df_cases_plot = df_cases[['date_time','value','type','Tipo']].copy()


	if location_name == 'Colombia':


		# First Date
		date = 'fecha reporte web'
		# Adds the date reported
		df_cases_other_date = pd.read_csv(os.path.join(raw_folder_location, 'cases/cases_raw.csv'), parse_dates = ['fecha reporte web', 'Fecha de notificación'], 
			date_parser = lambda x: pd.to_datetime(x, errors="coerce"), low_memory = False)

		df_cases_other_date = df_cases_other_date[[date]].rename(columns = {date:'date_time'})


		# If max date is established
		#df_cases_other_date = df_cases_other_date[df_cases_other_date.date_time < pd.to_datetime('2020-08-01')].copy()		


		df_cases_other_date['value'] = 1
		df_cases_other_date = df_cases_other_date[['date_time','value']].groupby('date_time').sum().reset_index()

		# Somooths
		#df_cases_other_date['value'] = gf.smooth_curve(df_cases_other_date['value'], 2 )

		# Type
		df_cases_other_date['Tipo'] = 'Casos (Fecha Reporte Web)' 
		df_cases_other_date['type'] = 'cases'

		df_cases_plot = pd.concat((df_cases_plot, df_cases_other_date), ignore_index = True)


		# Second Date
		date = 'FIS'

		# Adds the date reported
		df_cases_other_date = pd.read_csv(os.path.join(raw_folder_location, 'cases/cases_raw.csv'), parse_dates = ['FIS', 'Fecha de notificación'], 
			date_parser = lambda x: pd.to_datetime(x, errors="coerce"), low_memory = False)


		df_cases_other_date = df_cases_other_date[[date]].rename(columns = {date:'date_time'})

		# back days
		back_days = 20
		df_cases_other_date = df_cases_other_date[df_cases_other_date.date_time <= (df_cases_other_date.date_time.max() - timedelta(days = back_days))].copy()

		df_cases_other_date['value'] = 1
		df_cases_other_date = df_cases_other_date[['date_time','value']].groupby('date_time').sum().reset_index()

		# Somooths
		df_cases_other_date['value'] = gf.smooth_curve(df_cases_other_date['value'], 5 )

		# Type
		df_cases_other_date['Tipo'] = 'Casos (Inicio Síntomas)' 
		df_cases_other_date['type'] = 'cases'

		df_cases_plot = pd.concat((df_cases_plot, df_cases_other_date), ignore_index = True)		


	# Loads milestones
	df_miles = None
	milestones_locations = os.path.join(raw_folder_location, 'milestones/milestones.csv')
	if  os.path.exists(milestones_locations):

		df_miles = pd.read_csv(milestones_locations, parse_dates = ['date_time'], dayfirst = True)
		df_miles.sort_values('date_time', inplace = True)

		df_miles.index = [i for i in range(1,(df_miles.shape[0] + 1))]

		# Adjusts columns

		df_miles_exp = df_miles.rename(columns = {'measure':'Medidas adoptadas COVID-19','date_time':'Fecha','soport_document':'Documento Soporte'})
		df_miles_exp['Num.'] = df_miles.index.values
		df_miles_exp = df_miles_exp[['Num.','Medidas adoptadas COVID-19','Fecha','Documento Soporte']]
		df_miles_exp.to_csv(os.path.join(export_folder_location,'milestones.csv'), index = False)



	df = pd.concat((df_mov_plot, df_cases_plot), ignore_index = True)
	# Plots movement all
	print(ident + '   Plots movement for {} (All)'.format(location_name))

	# Global Movmeent Plot
	df_plot = df

	g = sns.relplot(x="date_time", y="value",row="type", hue = 'Tipo',
	            height=height, aspect=aspect, facet_kws=dict(sharey=False),
	            kind="line", data=df_plot)


	# Axis
	g.set_axis_labels("Fecha", "")
	g.axes[0,0].set_ylabel('Proporción (0-1)')
	g.axes[1,0].set_ylabel('Número Casos')

	# Titles
	g.axes[0,0].set_title('Cambio Porcentual en el Movimiento a Nivel Nacional')
	g.axes[1,0].set_title('Casos Diarios a Nivel Nacional')


	#Adds milestones
	if df_miles is not None:
		limits = g.axes[0,0].get_ylim()
		top_pos = limits[1] - (limits[1] - limits[0])*0.05


		for ind, row in df_miles.iterrows():
			g.axes[0,0].text(row.date_time - timedelta(hours = 12), top_pos, str(ind))
			g.axes[0,0].axvline( row.date_time, color=miles_stones_color, linestyle='--', lw=miles_stones_width, ymin = 0.0,  ymax = 0.9)
			g.axes[1,0].axvline( row.date_time, color=miles_stones_color, linestyle='--', lw=miles_stones_width, ymin = 0.0,  ymax = 1)


	# Adds the horizontal line
	g.axes[0,0].axhline( -0.5, color = cut_line_color, linestyle='--', lw = cut_stones_width, xmin = 0.0,  xmax = 1)

	min_dat, max_date = g.axes[0,0].get_xlim()

	tick = np.round(min_dat) + 4
	ticks = []
	jump = 15
	while tick  < max_date:
		ticks.append(tick)
		tick += jump



	g.axes[1,0].xaxis.set_ticks(ticks)
	g.savefig(os.path.join(export_folder_location,'mov_range_{}.png'.format(location_folder)))


	print(ident + '   Plots Movement Range for Polygons {}'.format(location_name))	

	# Reads Polygons
	polygons = pd.read_csv(os.path.join(agglomerated_folder_location, 'polygons.csv'))

	polygons = polygons.sort_values('num_cases', ascending = False)
	polygons.loc[polygons.index[max_selected:],'poly_name'] = 'Otros (Promedio)'


	df_plot = df_mov_range.merge(polygons, on = 'poly_id')

	fig = plt.figure(figsize=(19,8))

	ax = sns.lineplot(data = df_plot, x = 'date_time', y = 'movement_change', hue = 'poly_name')
	ax.set_title('Cambio Porcentual en Movilidad en Unidades {} para {}'.format(unit_type_prural, location_name), fontsize=suptitle_font_size)
	ax.set_xlabel('Fecha', fontsize=axis_font_size)
	ax.set_ylabel('Proporción (0-1)', fontsize=axis_font_size)
	ax.legend().texts[0].set_text(f"Unidad {unit_type}")


	# Adds the horizontal line
	ax.axhline( -0.5, color = cut_line_color, linestyle='--', lw = cut_stones_width, xmin = 0.0,  xmax = 1)		

	fig.savefig(os.path.join(export_folder_location,'movement_range_selected_polygons_{}.png'.format(location_folder)))


	



print(ident + 'Done')