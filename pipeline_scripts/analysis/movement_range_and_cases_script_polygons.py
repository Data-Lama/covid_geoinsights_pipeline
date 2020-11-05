# Movement Plots For specific polygons

# Other imports
import os, sys
#sys.path
#sys.path.append('pipeline_scripts/functions/')

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
jump = 25

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

# Agglomerated folder location n
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
polygons 	     = pd.read_csv(os.path.join(agglomerated_folder_location, 'polygons.csv'), converters={'poly_id': str})

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
df_mov_range_all['Tipo'] = 'Movimiento Global'


df_mov_plot = pd.concat((df_mov[['date_time','value','type','Tipo']], df_mov_range_all[['date_time','value','type','Tipo']]), ignore_index = True)

# Converts to percentage
df_mov_plot['vaue'] = 100*df_mov_plot['vaue']

# Loads cases
df_cases_raw = pd.read_csv(os.path.join(agglomerated_folder_location, 'cases.csv'), parse_dates = ['date_time'])
df_cases_all = df_cases_raw.rename(columns = {'num_cases':'value'})
df_cases_all = df_cases_all[['date_time','value', 'poly_id']].copy()
df_cases_all = df_cases_all[df_cases_all.poly_id.isin(selected_polygons)]
df_cases = df_cases_all[['date_time','value']].groupby('date_time').sum().reset_index()

# Smooths 
df_cases['value'] = gf.smooth_curve( df_cases['value'], con.smooth_days )
df_cases['type'] = 'cases'
df_cases['Tipo'] = 'Casos' 

df_cases_plot = df_cases[['date_time','value','type','Tipo']].copy()


df = pd.concat((df_mov_plot, df_cases_plot), ignore_index = True)
# Plots movement all
print(ident + '   Plots movement for {} (All)'.format(selected_polygons_name))

# Global Movmeent Plot
df_plot = df


g = sns.relplot(x="date_time", y="value",row="type", hue = 'Tipo',
            height=height, aspect=aspect, facet_kws=dict(sharey=False),
            kind="line", data=df_plot)


# Axis
g.set_axis_labels("Fecha", "")
g.axes[0,0].set_ylabel('Porcentaje (%)')
g.axes[1,0].set_ylabel('Número Casos')

# Titles
g.axes[0,0].set_title(f'Cambio Porcentual en el Movimiento en {selected_polygons_name}')
g.axes[1,0].set_title(f'Casos Diarios en {selected_polygons_name}')


# Adds the horizontal line
g.axes[0,0].axhline( -0.5, color = cut_line_color, linestyle='--', lw = cut_stones_width, xmin = 0.0,  xmax = 1)

min_dat, max_date = g.axes[0,0].get_xlim()

tick = np.round(min_dat) + 4
ticks = []
while tick  < max_date:
	ticks.append(tick)
	tick += jump



g.axes[1,0].xaxis.set_ticks(ticks)
g.savefig(os.path.join(export_folder_location, f'mov_range_{selected_polygons_folder_name}.png'))



print(ident + 'Done')