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
polygons 	     = pd.read_csv(os.path.join(agglomerated_folder_location, 'polygons.csv'), converters={'poly_id': str})

# For the national average
polygons_all = polygons.copy()

# Polgons to be considered.
polygons = polygons[polygons.poly_id.isin(selected_polygons)].copy()

# Read movement data-range file
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

# Movimiento local is "inter_polygon" movement Movimiento globla is "outer_polygon" movement (importation/exportation process)
df_mov_plot = pd.concat((df_mov[['date_time','value','type','Tipo']], df_mov_range_all[['date_time','value','type','Tipo']]), ignore_index = True)

# Loads cases
df_cases_raw = pd.read_csv(os.path.join(agglomerated_folder_location, 'cases.csv'), parse_dates = ['date_time'])
df_cases_all = df_cases_raw.rename(columns = {'num_cases':'value'})
df_cases_all = df_cases_all[['date_time','value', 'poly_id']].copy()
df_cases_all = df_cases_all[df_cases_all.poly_id.isin(selected_polygons)]

# here date_time is given by FECHA DE DIAGNÓSTICO
df_cases = df_cases_all[['date_time','value']].groupby('date_time').sum().reset_index()



#idx_start = np.searchsorted(daily_cases['Smoothed_'+col], cutoff)
#daily_cases['Smoothed_'+col] = daily_cases['Smoothed_'+col].iloc[idx_start:]



# Smooths 
df_cases['smoothed_value'] = df_cases['value'].rolling(7,
	win_type='gaussian',
	min_periods=1,
	center=True).mean(std=2).round()

df_cases['type'] = 'cases'
df_cases['Tipo'] = 'Casos' 

df_cases_plot = df_cases[['date_time', 'smoothed_value', 'value','type','Tipo']].copy()


df = pd.concat((df_mov_plot, df_cases_plot), ignore_index = True)
# Plots movement all
print(ident + '   Plots movement for {} (All)'.format(selected_polygons_name))

# Global Movmeent Plot
df_plot = df.copy().set_index('date_time')

from matplotlib.dates import date2num, num2date
from matplotlib import dates as mdates
from matplotlib import ticker
from matplotlib.colors import ListedColormap
from matplotlib.patches import Patch

####### change plots #######
fig, ax = plt.subplots(2, 1, figsize=fig_size)
ax[0].plot( df_plot.query("Tipo == 'Movimiento Local'").value.index.values, df_plot.query("Tipo == 'Movimiento Local'").value, label='Movimiento Local', color='r', linewidth=2)
ax[0].plot( df_plot.query("Tipo == 'Movimiento Local'").value.index.values, df_plot.query("Tipo == 'Movimiento Global'").value, label='Movimiento Global', color='k', linewidth=2)

ax[1].bar(  df_plot.query("Tipo == 'Casos'").value.index.values, df_plot.query("Tipo == 'Casos'").value.values, label='Casos', color='k', alpha=0.3, zorder=1)
ax[1].plot( df_plot.query("Tipo == 'Casos'").smoothed_value.index.values, df_plot.query("Tipo == 'Casos'").smoothed_value.values, label='Promedio movil semanal', color='k', linewidth=2)

# Adds the horizontal line
#g.axes[0,0].axhline( -0.5, color = cut_line_color, linestyle='--', lw = cut_stones_width, xmin = 0.0,  xmax = 1)
min_date = df_plot.query("Tipo == 'Movimiento Global'").index.values[0]
max_date = df_plot.query("Tipo == 'Movimiento Global'").index.values[-1]

#ax1xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
ax[0].set_xlim( min_date-pd.Timedelta( days=2 ), max_date+pd.Timedelta(days=2) )
ax[1].set_xlim( min_date-pd.Timedelta( days=2 ), max_date+pd.Timedelta(days=2) )

# Axis
ax[1].axes.set_xlabel( "Fecha" )
ax[0].axes.set_ylabel('Cambio Porcentual')
ax[1].axes.set_ylabel('Numero de Casos')

# Titles
ax[0].set_title(f'Cambio Porcentual en el Movimiento en {selected_polygons_name}')
#ax[1].set_title(f'Casos Diarios en {selected_polygons_name}')
ax[0].legend(frameon=False)
ax[1].legend(frameon=False)
ax[0].set_xticks([])        
#ax[0].spines['left'].set_visible(False)
#ax[0].spines['bottom'].set_visible(False)
#ax[0].spines['right'].set_visible(False)
ax[1].grid(which='major', axis='y', c='k', alpha=.1, zorder=-2)
ax[0].grid(which='major', axis='y', c='k', alpha=.1, zorder=-2)

#ax[1].spines['left'].set_visible(False)
ax[1].spines['bottom'].set_visible(False)
#ax[1].spines['right'].set_visible(False)


ax[1].xaxis.set_major_locator(mdates.MonthLocator())
#ax[1].xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))


ax[1].xaxis.set_minor_locator(mdates.DayLocator())
ax[1].xaxis.set_major_locator(mdates.WeekdayLocator())
ax[1].xaxis.set_major_locator(mdates.MonthLocator())
ax[1].xaxis.set_major_formatter(mdates.DateFormatter('%b'))

ax[0].xaxis.set_minor_locator(mdates.DayLocator())
ax[0].xaxis.set_major_locator(mdates.WeekdayLocator())
ax[0].xaxis.set_major_locator(mdates.MonthLocator())
ax[0].xaxis.set_major_formatter(mdates.DateFormatter('%b'))

#ax[1].yaxis.set_major_locator(ticker.MultipleLocator(1))
#ax[1].yaxis.set_major_formatter(ticker.StrMethodFormatter("{x:.0f}"))

#ax[1].spines['right'].set_visible(False)
plt.tight_layout()
#ax[1].grid(None)
#ax[0] = ax[1].twinx()

plt.show()




fig.savefig(os.path.join(export_folder_location, f'mov_range_{selected_polygons_folder_name}.png'))

print(ident + 'Done')