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

if len(sys.argv) <= 3:
	selected_polygons_boolean = False
else :
    selected_polygons_boolean = True
    selected_polygons = []
    i = 3
    while i < len(sys.argv):
        selected_polygons.append(sys.argv[i])
        i += 1
    selected_polygon_name = selected_polygons.pop(0)


# Constants
ident = '         '
RIVERS_DICT = {'sinu':'RÍO SINÚ',
       'magdalena':'RÍO MAGDALENA',
       'cauca':'RÍO CAUCA',
       'meta':'RÍO META',
       'amazonas':'RÌO AMAZONAS'}

RIVERS = ['RÍO SINÚ','RÍO MAGDALENA','RÍO CAUCA','RÍO META','RÌO AMAZONAS']

# Get name of files
movement = os.path.join(data_dir, 'data_stages', location_name, 'agglomerated', location_folder, 'movement.csv')
community_file = os.path.join(data_dir, 'data_stages', location_name, 'agglomerated', 'community', 'polygon_community_map.csv')
shape_file_path = os.path.join(data_dir, 'data_stages', location_name, 'raw', 'geo', 'Municpios_Dane_2017.shp')
river_file_path = os.path.join(data_dir, 'data_stages', location_name, 'raw', 'geo', 'river_lines', 'River_lines.shp')
output_file_path = os.path.join(analysis_dir, location_name, location_folder, 'polygon_info_window')

# Import shapefile
geo_df = gpd.read_file(shape_file_path)
geo_df['Codigo_Dan'] = geo_df['Codigo_Dan'].astype(int) # To allow for indexing
rivers_df = gpd.read_file(river_file_path)
rivers_df = rivers_df.to_crs('epsg:3116')

#Load movement 
try:  
    df_movement = pd.read_csv(movement, low_memory=False, parse_dates=['date_time'])
except:
    df_movement = pd.read_csv(movement, low_memory=False, encoding = 'latin-1', parse_dates=['date_time'])

# Get functional units
df_functional_units = pd.read_csv(community_file)

# Get windows
day_t3 = pd.Timestamp('today') - datetime.timedelta(days = WINDOW_SIZE)
day_t2 = day_t3 - datetime.timedelta(days = WINDOW_SIZE)
day_t0 = pd.Timestamp(datetime.datetime.strptime("2020-04-02", '%Y-%m-%d'))
day_t1 = day_t0 + datetime.timedelta(days = WINDOW_SIZE)

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
       df_delta = (t_1.sub(t_0, axis='columns')).divide(t_0, axis='columns').fillna(0)
       df_delta = df_delta.rename(columns = {'external_movement':'delta_external_movement',
                                                               'internal_movement': 'delta_inner_movement'})

       return df_delta

# Get deltas
df_municipalites_t2 = df_movement[df_movement["date_time"] >= day_t3].copy()
df_municipalites_t1 = df_movement.loc[(df_movement["date_time"] >= day_t2) & (df_movement["date_time"] < day_t3)].copy()
df_municipalites_t0 = df_movement.loc[(df_movement["date_time"] >= day_t0) & (df_movement["date_time"] <= day_t1)].copy()

# Calculate average internal and external movement
# t_0
df_municipalites_t0_int = df_municipalites_t0[df_municipalites_t0["start_poly_id"] == df_municipalites_t0["end_poly_id"]].copy()
df_municipalites_t0_int = df_municipalites_t0_int.groupby("start_poly_id").mean().reset_index()
df_municipalites_t0_int.drop(columns=["end_poly_id"], inplace=True)
df_municipalites_t0_ext = df_municipalites_t0[df_municipalites_t0["start_poly_id"] != df_municipalites_t0["end_poly_id"]].copy()
df_municipalites_t0_ext = df_municipalites_t0_ext.groupby(["start_poly_id", "date_time"]).sum().reset_index()
df_municipalites_t0_ext.drop(columns=["end_poly_id"], inplace=True)
df_municipalites_t0_ext = df_municipalites_t0_ext.groupby("start_poly_id").mean().reset_index()

df_municipalites_t0_ext.rename(columns={"start_poly_id":"poly_id", "movement":"external_movement"}, inplace=True)
df_municipalites_t0_int.rename(columns={"start_poly_id":"poly_id", "movement":"internal_movement"}, inplace=True)

df_municipalites_t0 = df_municipalites_t0_ext.merge(df_municipalites_t0_int, on="poly_id", how="outer").fillna(0)
df_municipalites_t0.set_index("poly_id", inplace=True)

# t_1
df_municipalites_t1_int = df_municipalites_t1[df_municipalites_t1["start_poly_id"] == df_municipalites_t1["end_poly_id"]].copy()
df_municipalites_t1_int = df_municipalites_t1_int.groupby("start_poly_id").mean().reset_index()
df_municipalites_t1_int.drop(columns=["end_poly_id"], inplace=True)
df_municipalites_t1_ext = df_municipalites_t1[df_municipalites_t1["start_poly_id"] != df_municipalites_t1["end_poly_id"]].copy()
df_municipalites_t1_ext = df_municipalites_t1_ext.groupby(["start_poly_id", "date_time"]).sum().reset_index()
df_municipalites_t1_ext.drop(columns=["end_poly_id"], inplace=True)
df_municipalites_t1_ext = df_municipalites_t1_ext.groupby("start_poly_id").mean().reset_index()

df_municipalites_t1_ext.rename(columns={"start_poly_id":"poly_id", "movement":"external_movement"}, inplace=True)
df_municipalites_t1_int.rename(columns={"start_poly_id":"poly_id", "movement":"internal_movement"}, inplace=True)

df_municipalites_t1 = df_municipalites_t1_ext.merge(df_municipalites_t1_int, on="poly_id", how="outer").fillna(0)
df_municipalites_t1.set_index("poly_id", inplace=True)

# t_2
df_municipalites_t2_int = df_municipalites_t2[df_municipalites_t2["start_poly_id"] == df_municipalites_t2["end_poly_id"]].copy()
df_municipalites_t2_int = df_municipalites_t2_int.groupby("start_poly_id").mean().reset_index()
df_municipalites_t2_int.drop(columns=["end_poly_id"], inplace=True)
df_municipalites_t2_ext = df_municipalites_t2[df_municipalites_t2["start_poly_id"] != df_municipalites_t2["end_poly_id"]].copy()
df_municipalites_t2_ext = df_municipalites_t2_ext.groupby(["start_poly_id", "date_time"]).sum().reset_index()
df_municipalites_t2_ext.drop(columns=["end_poly_id"], inplace=True)
df_municipalites_t2_ext = df_municipalites_t2_ext.groupby("start_poly_id").mean().reset_index()

df_municipalites_t2_ext.rename(columns={"start_poly_id":"poly_id", "movement":"external_movement"}, inplace=True)
df_municipalites_t2_int.rename(columns={"start_poly_id":"poly_id", "movement":"internal_movement"}, inplace=True)

df_municipalites_t2 = df_municipalites_t2_ext.merge(df_municipalites_t2_int, on="poly_id", how="outer").fillna(0)
df_municipalites_t2.set_index("poly_id", inplace=True)

# Get deltas
df_deltas_historic = calculate_delta(df_municipalites_t0, df_municipalites_t2)
df_deltas_historic = df_deltas_historic.replace([np.inf, -np.inf], np.nan).dropna(axis=0)

df_deltas_recent = calculate_delta(df_municipalites_t1, df_municipalites_t2)
df_deltas_recent = df_deltas_recent.replace([np.inf, -np.inf], np.nan).dropna(axis=0)                                                                    

# If asked for specific polygons, get subset
if selected_polygons_boolean:
       df_deltas_historic.reset_index(inplace=True)
       df_deltas_recent.reset_index(inplace=True)
       df_deltas_recent = df_deltas_recent[df_deltas_recent["poly_id"].isin(selected_polygons)].set_index("poly_id")
       df_deltas_historic = df_deltas_historic[df_deltas_historic["poly_id"].isin(selected_polygons)].set_index("poly_id")
       output_file_path = os.path.join(output_file_path, selected_polygon_name)

       # Check if folder exists
       if not os.path.isdir(output_file_path):
              os.makedirs(output_file_path)

print(ident+'   Building recent map (15 day window)')

# Get choropleth map 15-day window 
choropleth_map_recent = geo_df.merge(df_deltas_recent, left_on='Codigo_Dan', right_on='poly_id')
choropleth_map_recent.rename(columns={"Codigo_Dan":"poly_id"}, inplace=True)
choropleth_map_recent.replace([np.inf, -np.inf], np.nan, inplace=True)
choropleth_map_recent.to_crs(epsg=3857, inplace=True)
scheme = [0, 0.49, 1, 2, 5, 10]
ax = choropleth_map_recent.fillna(0).plot(column='delta_external_movement', cmap='Reds', figsize=(30,18),
                                    scheme='user_defined', classification_kwds={'bins':scheme}, legend=True, linewidth=0.5)

ax.set_axis_off()

ctx.add_basemap(ax, source=ctx.providers.CartoDB.VoyagerNoLabels)
rivers_df.to_crs(epsg=3857, inplace=True)
rivers_df.plot(ax=ax, alpha=0.1)
plt.title('Comparativo entre el último Viernes y 15 días atrás')
plt.savefig(os.path.join(output_file_path, 'choropleth_map_{}_15-day-window.png'.format(location_name)), bbox_inches="tight")


print(ident+'   Building historic map (15 day window)')

# Get choropleth map historic
choropleth_map_historic = geo_df.merge(df_deltas_historic, left_on='Codigo_Dan', right_on='poly_id')
choropleth_map_historic.rename(columns={"Codigo_Dan":"poly_id"}, inplace=True)
choropleth_map_historic.replace([np.inf, -np.inf], np.nan, inplace=True)
choropleth_map_historic.to_crs(epsg=3857, inplace=True)
ax = choropleth_map_historic.fillna(0).plot(column='delta_external_movement', cmap='Reds', figsize=(30,18),
                                    scheme='user_defined', classification_kwds={'bins':scheme}, k=6, legend=True, linewidth=0.5)
ax.set_axis_off()

ctx.add_basemap(ax, source=ctx.providers.CartoDB.VoyagerNoLabels)
rivers_df.to_crs(epsg=3857, inplace=True)
rivers_df.plot(ax=ax, alpha=0.1)
plt.title('Comparativo entre el último Viernes y los primeros 15 días de Abril')
plt.savefig(os.path.join(output_file_path, 'choropleth_map_{}_historic.png'.format(location_name)), bbox_inches="tight")


translate = {'delta_inner_movement':'Incremento flujo dentro del municipio',
            'delta_external_movement':'Incremento flujo hacia el municipio',
            'community_name':'Unidad funcional'}


# Get darker names in table
df_highlights_recent = choropleth_map_recent[choropleth_map_recent['delta_external_movement'] > 0.5].fillna(0)
df_highlights_historic = choropleth_map_historic[choropleth_map_historic['delta_external_movement'] > 1].fillna(0)

# Get polygons on river
choropleth_map_on_river = choropleth_map_recent[choropleth_map_recent['delta_external_movement'] > 0].fillna(0)
choropleth_map_on_river['river'] = choropleth_map_on_river.apply(lambda x: is_polygon_on_river(x.poly_id), axis=1)
choropleth_map_on_river.to_csv(os.path.join(output_file_path, 'detail_choropleth_recent_rivers.csv'), encoding = 'latin-1', 
       index=False, float_format="%.3f", columns=['Departamen', 'Municipio', 'river', 'delta_external_movement'])


# Merge with functional_units
df_highlights_recent = df_highlights_recent.merge(df_functional_units, left_on='poly_id', right_on='poly_id', how='left')
df_highlights_historic = df_highlights_historic.merge(df_functional_units, left_on='poly_id', right_on='poly_id', how='left')

df_highlights_recent.drop(columns=['OBJECTID', 'POBT_2018', 'POBH_2018', 'POBM_2018', 'VIVT_2018',
       'Departamen', 'Pob_Urbana', 'Pob_Rural', 'Total_2018',
       'No_Bog', 'SabanaBOG', 'Km2', 'Ha', 'Bog', 'Shape_Leng',
       'Shape_Area', 'geometry', 'community_id', 'poly_name', 'poly_id'], inplace=True)

df_highlights_recent.rename(columns=translate, inplace=True)

df_highlights_historic.drop(columns=['OBJECTID', 'POBT_2018', 'POBH_2018', 'POBM_2018', 'VIVT_2018',
       'Departamen', 'Pob_Urbana', 'Pob_Rural', 'Total_2018',
       'No_Bog', 'SabanaBOG', 'Km2', 'Ha', 'Bog', 'Shape_Leng',
       'Shape_Area', 'geometry', 'community_id', 'poly_name', 'poly_id'], inplace=True)

df_highlights_historic.rename(columns=translate, inplace=True)

# Arrange columns
df_highlights_historic = df_highlights_historic[['Unidad funcional', 'Municipio', 
                                        'Incremento flujo dentro del municipio',
                                        'Incremento flujo hacia el municipio']]
df_highlights_recent = df_highlights_recent[['Unidad funcional', 'Municipio', 
                                        'Incremento flujo dentro del municipio',
                                         'Incremento flujo hacia el municipio']]

df_highlights_recent.to_csv(os.path.join(output_file_path, 'detail_choropleth_recent.csv'), float_format="%.3f", index=False)
df_highlights_historic.to_csv(os.path.join(output_file_path, 'detail_choropleth_historic.csv'), float_format="%.3f", index=False)