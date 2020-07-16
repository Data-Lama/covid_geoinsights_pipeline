import os
import sys
import datetime
import numpy as np
import pandas as pd
import geopandas as gpd
import contextily as ctx
from datetime import date
import matplotlib.pyplot as plt

from pipeline_scripts.functions.general_functions import load_README
# import polygon_info_timewindow 

# Constants
WINDOW_SIZE = 7 # in days

# Direcotries
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')
report_dir = config.get_property('report_dir')

# Reads the parameters from excecution
location_name  =  sys.argv[1] # location name

# Get name of files
deltas_file_path = os.path.join(analysis_dir, location_name, 'geometry', 'polygon_info_window', 'polygon_info_total_window_5days.csv')
march_file_path = os.path.join(analysis_dir, location_name, 'geometry', 'polygon_info_window', 'polygon_info_total_window_5days-2020-04-07.csv')
shape_file_path = os.path.join(data_dir, 'data_stages', location_name, 'raw', 'geo', 'Municpios_Dane_2017.shp')
output_file_path = os.path.join(report_dir)

# Import shapefile
geo_df = gpd.read_file(shape_file_path)

def calculate_delta(t_0, t_1):
    t_1.set_index('node_id', inplace=True)
    t_0.set_index('node_id', inplace=True)
    df_delta = (t_1.sub(t_0, axis='columns')).divide(t_0, axis='columns').fillna(0)
    df_delta = df_delta.rename(columns = {'external_movement':'delta_external_movement',
                                                            'external_num_cases':'delta_external_num_cases',
                                                            'inner_movement': 'delta_inner_movement',
                                                            'num_cases': 'delta_num_cases'})

    return df_delta

# Get deltas
df_municipalites = pd.read_csv(deltas_file_path).replace([np.inf, -np.inf], np.nan)
df_municipalites_t0 = pd.read_csv(march_file_path).replace([np.inf, -np.inf], np.nan)
df_municipalites_t0.drop(columns=['delta_external_movement', 
                                                            'delta_external_num_cases',
                                                            'delta_inner_movement',
                                                            'delta_num_cases'], axis=1, inplace=True)
deltas = calculate_delta(df_municipalites_t0, df_municipalites.drop(columns=['delta_external_movement', 
                                                            'delta_external_num_cases',
                                                            'delta_inner_movement',
                                                            'delta_num_cases'], axis=1))
                    
df_municipalites_t0 = df_municipalites_t0.merge(deltas.reset_index(), right_on='node_id', left_on='node_id')                                                            

# Get choropleth map 15-day window 
choropleth_map_recent = geo_df.merge(df_municipalites, left_on='Codigo_Dan', right_on='node_id')
choropleth_map_recent.to_crs(epsg=3857, inplace=True)
ax = choropleth_map_recent.fillna(0).plot(column='delta_external_movement', cmap='Reds', figsize=(15,9),
                                    scheme='EqualInterval', k=6, legend=True, linewidth=0.5)
ax.set_axis_off()
ctx.add_basemap(ax, source=ctx.providers.CartoDB.VoyagerNoLabels)
plt.suptitle('Incremento Porcentual de Movimiento Incidente por Municipio')
plt.title('Ventana 15 dias atrás')
plt.savefig(os.path.join(output_file_path, 'choropleth_map_{}_15-day-window.png'.format(location_name)), bbox_inches="tight")

# Get choropleth map historic
choropleth_map_historic = geo_df.merge(df_municipalites_t0, left_on='Codigo_Dan', right_on='node_id')
choropleth_map_historic.replace([np.inf, -np.inf], np.nan, inplace=True)
choropleth_map_historic.to_crs(epsg=3857, inplace=True)
ax = choropleth_map_historic.fillna(0).plot(column='delta_external_movement', cmap='Reds', figsize=(15,9),
                                    scheme='natural_breaks', k=6, legend=True, linewidth=0.5)
ax.set_axis_off()

ctx.add_basemap(ax, source=ctx.providers.CartoDB.VoyagerNoLabels)
plt.suptitle('Incremento Porcentual de Movimiento Incidente por Municipio')
plt.title('Comparativo a Marzo 2020')
plt.savefig(os.path.join(output_file_path, 'choropleth_map_{}_historic.png'.format(location_name)), bbox_inches="tight")