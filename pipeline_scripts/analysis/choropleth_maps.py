import os
import sys
import datetime
import mapclassify
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

# Reads the parameters from excecution
location_name  =  sys.argv[1] # location name
location_folder = sys.argv[2] # polygon name

# Constants
ident = '         '
RIVERS_DICT = {'sinu':'RÍO SINÚ',
       'magdalena':'RÍO MAGDALENA',
       'cauca':'RÍO CAUCA',
       'meta':'RÍO META',
       'amazonas':'RÌO AMAZONAS'}

RIVERS = ['RÍO SINÚ','RÍO MAGDALENA','RÍO CAUCA','RÍO META','RÌO AMAZONAS']

# Get name of files
deltas_file_path = os.path.join(analysis_dir, location_name, location_folder, 'polygon_info_window', 'polygon_info_total_window_5days.csv')
march_file_path = os.path.join(analysis_dir, location_name, location_folder, 'polygon_info_window', 'polygon_info_total_window_5days-2020-04-07.csv')
community_file = os.path.join(data_dir, 'data_stages', location_name, 'agglomerated', 'community', 'polygon_community_map.csv')
shape_file_path = os.path.join(data_dir, 'data_stages', location_name, 'raw', 'geo', 'Municpios_Dane_2017.shp')
river_file_path = os.path.join(data_dir, 'data_stages', location_name, 'raw', 'geo', 'river_lines', 'River_lines.shp')
output_file_path = os.path.join(analysis_dir, location_name, location_folder, 'polygon_info_window')

# Import shapefile
geo_df = gpd.read_file(shape_file_path)
geo_df['Codigo_Dan'] = geo_df['Codigo_Dan'].astype(int) # To allow for indexing
rivers_df = gpd.read_file(river_file_path)
rivers_df = rivers_df.to_crs('epsg:3116')

# Get functional units
df_functional_units = pd.read_csv(community_file)

# returns name of river node intersects or nan
def is_polygon_on_river(node_id, buffer=False):
       geometry = geo_df.set_index('Codigo_Dan').at[node_id, 'geometry']
       for river in RIVERS:
              river_geometry = rivers_df.loc[rivers_df['NOMBRE_GEO'] == river]['geometry']
              for line in river_geometry:
                     if buffer:
                            line = line.buffer(buffer)
                     if geometry.intersects(line):
                            return river
       return np.nan

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
print(ident+'   Building recent map (15 day window)')

# Get choropleth map 15-day window 
choropleth_map_recent = geo_df.merge(df_municipalites, left_on='Codigo_Dan', right_on='node_id')
choropleth_map_recent.to_crs(epsg=3857, inplace=True)
scheme = [0, 0.49, 1, 2, 5, 10]
ax = choropleth_map_recent.fillna(0).plot(column='delta_external_movement', cmap='Reds', figsize=(15,9),
                                    scheme='user_defined', classification_kwds={'bins':scheme}, legend=True, linewidth=0.5)

ax.set_axis_off()
rivers_df.to_crs(epsg=3857, inplace=True)
rivers_df.plot(ax=ax, alpha=0.1)
ctx.add_basemap(ax, source=ctx.providers.CartoDB.VoyagerNoLabels)
plt.suptitle('Incremento Porcentual de Movimiento Incidente por Municipio')
plt.title('Comparativo entre el día del reporte y 15 días atrás')
plt.savefig(os.path.join(output_file_path, 'choropleth_map_{}_15-day-window.png'.format(location_name)), bbox_inches="tight")

print(ident+'   Building historic map (15 day window)')

# Get choropleth map historic
choropleth_map_historic = geo_df.merge(df_municipalites_t0, left_on='Codigo_Dan', right_on='node_id')
choropleth_map_historic.replace([np.inf, -np.inf], np.nan, inplace=True)
choropleth_map_historic.to_crs(epsg=3857, inplace=True)
ax = choropleth_map_historic.fillna(0).plot(column='delta_external_movement', cmap='Reds', figsize=(15,9),
                                    scheme='user_defined', classification_kwds={'bins':scheme}, k=6, legend=True, linewidth=0.5)
ax.set_axis_off()

ctx.add_basemap(ax, source=ctx.providers.CartoDB.VoyagerNoLabels)
rivers_df.to_crs(epsg=3857, inplace=True)
rivers_df.plot(ax=ax, alpha=0.1)
plt.suptitle('Incremento Porcentual de Movimiento Incidente por Municipio')
plt.title('Comparativo entre el día del reporte y los primeros 15 días de Abril')
plt.savefig(os.path.join(output_file_path, 'choropleth_map_{}_historic.png'.format(location_name)), bbox_inches="tight")

translate = {'delta_num_cases':'Incremento numero de casos',
            'delta_inner_movement':'Incremento flujo dentro del municipio',
            'delta_external_num_cases':'Incremento numero de casos en municipios vecinos',
            'delta_external_movement':'Incremento flujo hacia el municipio',
            'community_name':'Unidad funcional'}

# Get darker names in table
df_highlights_recent = choropleth_map_recent[choropleth_map_recent['delta_external_movement'] > 1].fillna(0)
df_highlights_historic = choropleth_map_historic[choropleth_map_historic['delta_external_movement'] > 5].fillna(0)

# Get polygons on river
choropleth_map_on_river = choropleth_map_recent[choropleth_map_recent['delta_external_movement'] > 0].fillna(0)
choropleth_map_on_river['river'] = choropleth_map_on_river.apply(lambda x: is_polygon_on_river(x.node_id), axis=1)
choropleth_map_on_river.to_csv(os.path.join(output_file_path, 'detail_choropleth_recent_rivers.csv'), encoding = 'latin-1', 
       index=False, float_format="%.3f", columns=['Departamen', 'Municipio', 'river', 'delta_external_movement'])


# Merge with functional_units
df_highlights_recent = df_highlights_recent.merge(df_functional_units, left_on='node_id', right_on='poly_id', how='left')
df_highlights_historic = df_highlights_historic.merge(df_functional_units, left_on='node_id', right_on='poly_id', how='left')

df_highlights_recent.drop(columns=['OBJECTID', 'POBT_2018', 'POBH_2018', 'POBM_2018', 'VIVT_2018',
       'Departamen', 'Pob_Urbana', 'Pob_Rural', 'Total_2018',
       'Codigo_Dan', 'No_Bog', 'SabanaBOG', 'Km2', 'Ha', 'Bog', 'Shape_Leng',
       'Shape_Area', 'geometry', 'node_id', 'external_movement',
       'external_num_cases', 'num_cases', 'inner_movement', 'community_id', 'poly_name', 'poly_id'], inplace=True)
df_highlights_recent.rename(columns=translate, inplace=True)

df_highlights_historic.drop(columns=['OBJECTID', 'POBT_2018', 'POBH_2018', 'POBM_2018', 'VIVT_2018',
       'Departamen', 'Pob_Urbana', 'Pob_Rural', 'Total_2018',
       'Codigo_Dan', 'No_Bog', 'SabanaBOG', 'Km2', 'Ha', 'Bog', 'Shape_Leng',
       'Shape_Area', 'geometry', 'node_id', 'external_movement',
       'external_num_cases', 'num_cases', 'inner_movement', 'community_id', 'poly_name', 'poly_id'], inplace=True)

df_highlights_historic.rename(columns=translate, inplace=True)

# Arrange columns
df_highlights_historic = df_highlights_historic[['Unidad funcional', 'Municipio', 
                                        'Incremento numero de casos', 'Incremento flujo dentro del municipio',
                                        'Incremento numero de casos en municipios vecinos','Incremento flujo hacia el municipio']]
df_highlights_recent = df_highlights_recent[['Unidad funcional', 'Municipio', 
                                        'Incremento numero de casos', 'Incremento flujo dentro del municipio',
                                        'Incremento numero de casos en municipios vecinos', 'Incremento flujo hacia el municipio']]

df_highlights_recent.to_csv(os.path.join(output_file_path, 'detail_choropleth_recent.csv'), float_format="%.3f", index=False)
df_highlights_historic.to_csv(os.path.join(output_file_path, 'detail_choropleth_historic.csv'), float_format="%.3f", index=False)