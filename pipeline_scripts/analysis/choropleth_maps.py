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
from matplotlib.colors import LinearSegmentedColormap

from pipeline_scripts.functions.general_functions import load_README
# import polygon_info_timewindow 

# Constants
WINDOW_SIZE = 7 # in days
edgecolor = None
RT_MISSING = 1000000
LEGEND_SIZE = "large"


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

scheme = "user_defined"
bins = {'bins':[0, 0.49, 1, 2, 5, 10, 50], 'bins_rt':[0, 0.5, 0.8, 1, 1.5, 2, 5]} 
colors = [(1, 0.96, 0.94), (0.99, 0.83, 0.76), (0.98, 0.62, 0.51), (0.98, 0.41, 0.29), (0.89, 0.18, 0.15), (0.69, 0.07, 0.09), (0.37, 0.04, 0.1)] 
colors_rt = [(0, 0.28, 0.16),(0.66, 0.85, 0.55), (0.86, 0.94, 0.65),(0.98, 0.99, 0.78), (0.89, 0.18, 0.15), (0.69, 0.07, 0.09), (0.37, 0.04, 0.1)]
grey = (0.83,0.83,0.83)

# colors_rt = [(0.98, 0.99, 0.78), (0.86, 0.94, 0.65), (0.66, 0.85, 0.55), (0.42, 0.75, 0.45), (0.11, 0.49, 0.25), (0, 0.41, 0.21), (0, 0.28, 0.16)] 

# Get name of files
movement = os.path.join(data_dir, 'data_stages', location_name, 'agglomerated', location_folder, 'movement.csv')
community_file = os.path.join(data_dir, 'data_stages', location_name, 'agglomerated', 'community', 'polygon_community_map.csv')
shape_file_path = os.path.join(data_dir, 'data_stages', location_name, 'raw', 'geo', 'Municpios_Dane_2017.shp')
river_file_path = os.path.join(data_dir, 'data_stages', location_name, 'raw', 'geo', 'river_lines', 'River_lines.shp')
output_file_path = os.path.join(analysis_dir, location_name, location_folder, 'polygon_info_window')
readme = os.path.join(agglomerated_file_path, "README.txt")

if selected_polygons_boolean:
    rt = os.path.join(analysis_dir, location_name, location_folder, "r_t", selected_polygon_name)
else:
    rt = os.path.join(analysis_dir, location_name, "community", "r_t", "entire_location")

# Load r_t
files = os.listdir(rt)
r_t_files = []

for i in files:
    if i[-4:] == ".csv":
       r_t_files.append(i) 

df_rt = pd.DataFrame(columns=["date_time", "poly_id", "ML", "Low_50", "High_50"])

for i in r_t_files:
    poly_id = i[:-7]
    try:
        poly_id = int(poly_id)
    except ValueError:
        continue
    df_tmp = pd.read_csv(os.path.join(rt, i), parse_dates=['date'])
    df_tmp.rename(columns={"date": "date_time"}, inplace=True)
    df_tmp["poly_id"] = poly_id
    df_rt = pd.concat([df_rt, df_tmp])

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
# Set window size
readme_dict = gf.load_README(readme)
max_date = readme_dict["Movement"].split(",")[1].strip()
max_date = max_date.split(" ")[1].strip()
day_t3 = pd.Timestamp(datetime.datetime.strptime(max_date, '%Y-%m-%d')) - datetime.timedelta(days = WINDOW_SIZE)
day_t2 = day_t3 - datetime.timedelta(days = WINDOW_SIZE)
day_t0 = pd.Timestamp(datetime.datetime.strptime("2020-04-02", '%Y-%m-%d'))
day_t1 = day_t0 + datetime.timedelta(days = WINDOW_SIZE)

# RT alerts
df_rt = df_rt[df_rt["date_time"] >= day_t3]
df_rt = df_rt.groupby("poly_id").mean()

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

def set_color(x, bins, colors):
       if np.isnan(x):
              return grey, "Sin RT"
       index = 0
       for i in range(len(bins)):
              if x <= bins[i]:
                     index = i
                     break
       if index == 0:
              label = "(-10, {}]".format(bins[index])
       else:
              label = "({}, {}]".format(bins[index-1], bins[index])

       return colors[index], label

def construct_legend(bins):
       l = []
       for i in range(len(bins)):              
              if i == 0:
                     label = "(-10, {}]".format(bins[i])
              else:
                     label = "({}, {}]".format(bins[i-1], bins[i])
              l.append(label)
       return l

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
       edgecolor = "grey"
       df_deltas_historic.reset_index(inplace=True)
       df_deltas_recent.reset_index(inplace=True)
       df_deltas_recent = df_deltas_recent[df_deltas_recent["poly_id"].isin(selected_polygons)].set_index("poly_id")
       df_deltas_historic = df_deltas_historic[df_deltas_historic["poly_id"].isin(selected_polygons)].set_index("poly_id")
       output_file_path = os.path.join(output_file_path, selected_polygon_name)
       # scheme = "fisher_jenks"
       # bins = None
       # get labels

       # Check if folder exists
       if not os.path.isdir(output_file_path):
              os.makedirs(output_file_path)
else :
     output_file_path = os.path.join(output_file_path, "entire_location") 

print(ident+"   Building choropleth map for last week's RT")
# Plot rt_choroplet

gdf_rt = geo_df.merge(df_rt.reset_index(), left_on='Codigo_Dan', right_on="poly_id", how="outer")
if selected_polygons_boolean:
       gdf_rt = gdf_rt[gdf_rt["Codigo_Dan"].isin(selected_polygons)]
gdf_rt.to_crs(epsg=3857, inplace=True)
gdf_rt["color"], gdf_rt["label"] = zip(*gdf_rt.apply(lambda x: set_color(x.ML, bins["bins_rt"], colors_rt), axis=1))
# print(gdf_rt.head(20))
# print(gdf_rt.columns)
# raise Exception("DONE")
ax = gdf_rt.plot(figsize=(30,18), color=gdf_rt['color'], label=gdf_rt['label'], linewidth=0.5, edgecolor=edgecolor)
ax.set_axis_off()

if selected_polygons_boolean:
       gdf_rt["label"] = gdf_rt.apply(lambda x: x.geometry.representative_point(), axis=1)
       gdf_rt["label_x"] = gdf_rt.apply(lambda p: p.label.x, axis=1)
       gdf_rt["label_y"] = gdf_rt.apply(lambda p: p.label.y, axis=1)
       for x, y, label in zip(gdf_rt.label_x, gdf_rt.label_y, gdf_rt.Municipio):
              ax.annotate(label, xy=(x, y), xytext=(3, 3), textcoords="offset points", fontsize=10)

ctx.add_basemap(ax, source=ctx.providers.CartoDB.VoyagerNoLabels)
rivers_df.to_crs(epsg=3857, inplace=True)
rivers_df.plot(ax=ax, alpha=0.1)
# Here we create a legend: The convoluted way
legend = construct_legend(bins["bins_rt"])
for i in range(len(legend)):
    plt.scatter([], [], color=colors_rt[i], label=str(legend[i]))
plt.scatter([], [], color=grey, label=str("Sin RT"))
plt.legend(scatterpoints=1, frameon=False, labelspacing=1, title='RT', fontsize=LEGEND_SIZE)
plt.title('Promedio de RT para la Última Semana')
plt.savefig(os.path.join(output_file_path, 'choropleth_map_{}_rt.png'.format(location_name)), bbox_inches="tight")



print(ident+'   Building recent map (15 day window)')
# Get choropleth map 15-day window
choropleth_map_recent = geo_df.merge(df_deltas_recent, left_on='Codigo_Dan', right_on='poly_id')
choropleth_map_recent.rename(columns={"Codigo_Dan":"poly_id"}, inplace=True)
choropleth_map_recent.replace([np.inf, -np.inf], np.nan, inplace=True)
choropleth_map_recent.to_crs(epsg=3857, inplace=True)

choropleth_map_recent["color"], choropleth_map_recent["label"] = zip(*choropleth_map_recent.apply(lambda x: set_color(x.delta_external_movement, bins["bins"], colors), axis=1))
ax = choropleth_map_recent.fillna(0).plot(figsize=(30,18), color=choropleth_map_recent['color'], label=choropleth_map_recent['label'] \
       , linewidth=0.5, edgecolor=edgecolor)

ax.set_axis_off()

if selected_polygons_boolean:
       choropleth_map_recent["label"] = choropleth_map_recent.apply(lambda x: x.geometry.representative_point(), axis=1)
       choropleth_map_recent["label_x"] = choropleth_map_recent.apply(lambda p: p.label.x, axis=1)
       choropleth_map_recent["label_y"] = choropleth_map_recent.apply(lambda p: p.label.y, axis=1)
       for x, y, label in zip(choropleth_map_recent.label_x, choropleth_map_recent.label_y, choropleth_map_recent.Municipio):
              ax.annotate(label, xy=(x, y), xytext=(3, 3), textcoords="offset points", fontsize=10)

ctx.add_basemap(ax, source=ctx.providers.CartoDB.VoyagerNoLabels)
rivers_df.to_crs(epsg=3857, inplace=True)
rivers_df.plot(ax=ax, alpha=0.1)

# Here we create a legend: The convoluted way
legend = construct_legend(bins["bins"])
for i in range(len(legend)):
    plt.scatter([], [], color=colors[i], label=str(legend[i]))
plt.legend(scatterpoints=1, frameon=False, labelspacing=1, title='Movimiento', fontsize=LEGEND_SIZE)

plt.title('Comparativo entre el último Viernes y 15 días atrás')
plt.savefig(os.path.join(output_file_path, 'choropleth_map_{}_15-day-window.png'.format(location_name)), bbox_inches="tight")

# Saves data
choropleth_map_recent.to_crs(epsg=4326, inplace=True)
choropleth_map_recent.to_csv(os.path.join(output_file_path, 'choropleth_map_{}_15-day-window_data.csv'.format(location_name)), index = False)

print(ident+'   Building historic map (15 day window)')

# Get choropleth map historic
choropleth_map_historic = geo_df.merge(df_deltas_historic, left_on='Codigo_Dan', right_on='poly_id')
choropleth_map_historic.rename(columns={"Codigo_Dan":"poly_id"}, inplace=True)
choropleth_map_historic.replace([np.inf, -np.inf], np.nan, inplace=True)
choropleth_map_historic.to_crs(epsg=3857, inplace=True)
# ax = choropleth_map_historic.fillna(0).plot(column='delta_external_movement', cmap='Reds', figsize=(30,18),
#                                     scheme=scheme, classification_kwds=bins, legend=True, linewidth=0.5, edgecolor=edgecolor)
choropleth_map_historic["color"], choropleth_map_historic["label"] = zip(*choropleth_map_historic.apply(lambda x: set_color(x.delta_external_movement, bins["bins"], colors), axis=1))
ax = choropleth_map_historic.fillna(0).plot(figsize=(30,18), color=choropleth_map_historic['color'], label=choropleth_map_historic['label'] \
       , linewidth=0.5, edgecolor=edgecolor)

ax.set_axis_off()

if selected_polygons_boolean:
       choropleth_map_historic["label"] = choropleth_map_historic.apply(lambda x: x.geometry.representative_point(), axis=1)
       choropleth_map_historic["label_x"] = choropleth_map_historic.apply(lambda p: p.label.x, axis=1)
       choropleth_map_historic["label_y"] = choropleth_map_historic.apply(lambda p: p.label.y, axis=1)
       for x, y, label in zip(choropleth_map_historic.label_x, choropleth_map_historic.label_y, choropleth_map_historic.Municipio):
              ax.annotate(label, xy=(x, y), xytext=(3, 3), textcoords="offset points", fontsize=10)

ctx.add_basemap(ax, source=ctx.providers.CartoDB.VoyagerNoLabels)
rivers_df.to_crs(epsg=3857, inplace=True)
rivers_df.plot(ax=ax, alpha=0.1)

# Here we create a legend: The convoluted way
for i in range(len(legend)):
    plt.scatter([], [], color=colors[i], label=str(legend[i]))
plt.legend(scatterpoints=1, frameon=False, labelspacing=1, title='Movimiento', fontsize=LEGEND_SIZE)

plt.title('Comparativo entre el último Viernes y los primeros 15 días de Abril')
plt.savefig(os.path.join(output_file_path, 'choropleth_map_{}_historic.png'.format(location_name)), bbox_inches="tight")

# Saves data
choropleth_map_historic.to_crs(epsg=4326, inplace=True)
choropleth_map_historic.to_csv(os.path.join(output_file_path, 'choropleth_map_{}_historic_data.csv'.format(location_name)), index = False)


translate = {'delta_inner_movement':'Incremento flujo dentro del municipio',
            'delta_external_movement':'Incremento flujo hacia el municipio',
            'community_name':'Unidad funcional'}

translate_rt = {'ML':"RT", 'community_name':'Unidad funcional'}


# Get darker names in table
df_highlights_recent = choropleth_map_recent[choropleth_map_recent['delta_external_movement'] > 0.48].fillna(0)
df_highlights_historic = choropleth_map_historic[choropleth_map_historic['delta_external_movement'] > 1].fillna(0)
df_rt_detail = gdf_rt[gdf_rt["ML"] > 1].copy()

# Get polygons on river
# choropleth_map_on_river = choropleth_map_recent[choropleth_map_recent['delta_external_movement'] > 0].fillna(0)
# choropleth_map_on_river['river'] = choropleth_map_on_river.apply(lambda x: is_polygon_on_river(x.poly_id), axis=1)
# choropleth_map_on_river.to_csv(os.path.join(output_file_path, 'detail_choropleth_recent_rivers.csv'), encoding = 'latin-1', 
#        index=False, float_format="%.3f", columns=['Departamen', 'Municipio', 'river', 'delta_external_movement'])

# Merge with functional_units
df_highlights_recent = df_highlights_recent.merge(df_functional_units, left_on='poly_id', right_on='poly_id', how='left')
df_highlights_historic = df_highlights_historic.merge(df_functional_units, left_on='poly_id', right_on='poly_id', how='left')
df_rt_detail = df_rt_detail.merge(df_functional_units, left_on='poly_id', right_on='poly_id', how='left')

df_rt_detail.drop(columns=['OBJECTID', 'POBT_2018', 'POBH_2018', 'POBM_2018', 'VIVT_2018',
       'Departamen', 'Pob_Urbana', 'Pob_Rural', 'Total_2018',
       'No_Bog', 'SabanaBOG', 'Km2', 'Ha', 'Bog', 'Shape_Leng',
       'Shape_Area', 'geometry', 'community_id', 'poly_name', 'poly_id'], inplace=True)

df_rt_detail.rename(columns=translate_rt, inplace=True)

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

df_rt_detail = df_rt_detail[['Unidad funcional', 'Municipio', 'RT']]

df_highlights_recent.to_csv(os.path.join(output_file_path, 'detail_choropleth_recent.csv'), float_format="%.3f", index=False)
df_highlights_historic.to_csv(os.path.join(output_file_path, 'detail_choropleth_historic.csv'), float_format="%.3f", index=False)
df_rt_detail.to_csv(os.path.join(output_file_path, "detail_choropleth_rt.csv"), float_format="%.3f", index=False)