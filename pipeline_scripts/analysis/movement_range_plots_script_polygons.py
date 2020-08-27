# Movement Plots For specific polygons

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
location_folder =  sys.argv[1] # locatio folder name
agglomeration_method = sys.argv[2] # Aglomeration name
selected_polygons_name = sys.argv[3] # Selected polygon names
selected_polygons_folder_name = sys.argv[4] # Selected polygon names

if len(sys.argv) <= 5:
	raise ValueError('No polygons ids provided!!!!')


selected_polygons = []
i = 5
while i < len(sys.argv):
	selected_polygons.append(sys.argv[i])
	i += 1


miles_stones_color = 'lightblue'
miles_stones_width = 1

cut_line_color = 'red'
cut_stones_width = 1



unit_type, unit_type_prural = gf.get_agglomeration_names(agglomeration_method)

# Agglomerated folder location
agglomerated_folder_location = os.path.join(data_dir, 'data_stages', location_folder, 'agglomerated', agglomeration_method)

# Unified folder location
unified_folder_location = os.path.join(data_dir, 'data_stages', location_folder, 'unified')

# Raw folder location
raw_folder_location = os.path.join(data_dir, 'data_stages', location_folder, 'raw')	


movement_range_file = os.path.join(agglomerated_folder_location, 'movement_range.csv')

if not os.path.exists(movement_range_file):
	raise ValueError(ident + 'No Movement range data found for {} Agglomeration'.format(agglomeration_method))


# Export folder location
export_folder_location = os.path.join(analysis_dir, location_folder, agglomeration_method, 'movement_plots', selected_polygons_folder_name)

# Creates the folder if does not exists
if not os.path.exists(export_folder_location):
	os.makedirs(export_folder_location)


print(ident + 'Excecuting Movement Analysis for {} with {} Agglomeration Over {} Selected Polygons)'.format(location_folder, agglomeration_method, len(selected_polygons)))

# Loads the data
print(ident + '   Extracts the movement and cases for {}'.format(selected_polygons_name))

# Reads Polygons
polygons = pd.read_csv(os.path.join(agglomerated_folder_location, 'polygons.csv'))
polygons.poly_id = polygons.poly_id.astype(str)

# For the national average
polygons_all = polygons.copy()

polygons = polygons[polygons.poly_id.isin(selected_polygons)].copy()

df_mov_range = pd.read_csv(movement_range_file, parse_dates = ['date_time'])
df_mov_range.poly_id = df_mov_range.poly_id.astype(str)

# For the national movement
df_mov_range_all = df_mov_range.copy()

df_mov_range = df_mov_range[df_mov_range.poly_id.isin(selected_polygons)]


# Loads the movement range
df_mov = df_mov_range.copy()


# Merges with polygons
pop_attr = "attr_population"

if pop_attr in polygons.columns:
	print(ident + '   Population Found, averaging movement by population.')

	# Averages with population size as factor
	df_mov = df_mov.merge(polygons[['poly_id',pop_attr]], on = 'poly_id')
	df_mov = df_mov[['date_time',pop_attr, 'movement_change']]

	df_mov['prod'] = df_mov['movement_change']*df_mov[pop_attr]

	# Grpoupby
	df_mov = df_mov[['date_time','prod',pop_attr]].groupby('date_time').sum().reset_index()

	# Final Variable
	df_mov['value'] = df_mov['prod']/df_mov[pop_attr]

	# National Level

	# Averages with population size as factor
	df_mov_range_all = df_mov_range_all.merge(polygons_all[['poly_id', pop_attr]], on = 'poly_id')
	df_mov_range_all = df_mov_range_all[['date_time',pop_attr, 'movement_change']]

	df_mov_range_all['prod'] = df_mov_range_all['movement_change']*df_mov_range_all[pop_attr]

	# Grpoupby
	df_mov_range_all = df_mov_range_all[['date_time','prod',pop_attr]].groupby('date_time').sum().reset_index()

	# Final Variable
	df_mov_range_all['value'] = df_mov_range_all['prod']/df_mov_range_all[pop_attr]

	


else:
	df_mov.rename(columns = {'movement_change':'value'}, inplace = True)
	df_mov_range_all.rename(columns = {'movement_change':'value'}, inplace = True)


df_mov = df_mov[['date_time','value']].copy()
df_mov['type'] = 'movement'
df_mov['Tipo'] = 'Movimiento Local'

df_mov_range_all = df_mov_range_all[['date_time','value']].copy()
df_mov_range_all['type'] = 'movement'
df_mov_range_all['Tipo'] = 'Movimiento Nacional'


df_mov_plot = pd.concat((df_mov[['date_time','value','type','Tipo']], df_mov_range_all[['date_time','value','type','Tipo']]), ignore_index = True)

# Loads cases
df_cases_raw = pd.read_csv(os.path.join(unified_folder_location, 'cases.csv'), parse_dates = ['date_time'])
df_cases_all = df_cases_raw.rename(columns = {'num_cases':'value', 'geo_id': 'polygon_id'})
df_cases_all = df_cases_all[['date_time','value', 'polygon_id']].copy()
df_cases_all = df_cases_all[df_cases_all.polygon_id.isin(selected_polygons)]

df_cases = df_cases_all[['date_time','value']].groupby('date_time').sum().reset_index()


# Somooths
df_cases['value'] = gf.smooth_curve(df_cases['value'], con.smooth_days )
df_cases['type'] = 'cases'
df_cases['Tipo'] = 'Casos (Fecha Diagnóstico)' 

df_cases_plot = df_cases[['date_time','value','type','Tipo']].copy()


df = pd.concat((df_mov_plot, df_cases_plot), ignore_index = True)


print(ident + '   Plots Movement Range for Polygons {}'.format(selected_polygons_name))	



# Reads Polygons
polygons = polygons.sort_values('num_cases', ascending = False)
if polygons.shape[0] > max_selected:
	polygons.loc[polygons.index[max_selected:],'poly_name'] = 'Otros (Promedio)'


df_plot = df_mov_range.merge(polygons[['poly_id','poly_name']], on = 'poly_id')

fig = plt.figure(figsize=(19,8))

ax = sns.lineplot(data = df_plot, x = 'date_time', y = 'movement_change', hue = 'poly_name')
ax.set_title('Cambio Porcentual en Movilidad en Unidades {} para {}'.format(unit_type_prural, selected_polygons_name), fontsize=suptitle_font_size)
ax.set_xlabel('Fecha', fontsize=axis_font_size)
ax.set_ylabel('Proporción (0-1)', fontsize=axis_font_size)
ax.legend().texts[0].set_text(f"Unidad {unit_type}")


# Adds the horizontal line
ax.axhline( -0.5, color = cut_line_color, linestyle='--', lw = cut_stones_width, xmin = 0.0,  xmax = 1)		

fig.savefig(os.path.join(export_folder_location, f'movement_range_selected_polygons_{selected_polygons_folder_name}.png'))



print(ident + 'Done')