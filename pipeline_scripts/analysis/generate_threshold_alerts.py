import os
import sys
import random
import datetime
import numpy as np
import pandas as pd
import geopandas as gpd
import contextily as ctx
from datetime import date
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap

import numpy as np

from pipeline_scripts.functions.general_functions import get_neighbors_cases_average, \
    get_internal_movement_stats_overtime, get_external_movement_stats_overtime, get_mean_external_movement

from pipeline_scripts.analysis.general_statistics import new_cases

# Direcotries
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')

# Reads the parameters from excecution
location_name  =  sys.argv[1] # location name
location_folder =  sys.argv[2] # polygon name
criteria_parameter = sys.argv[3] # min_record or min_date

# Constants
ident = '         '
WINDOW_SIZE = 7
MAX_POINTS = WINDOW_SIZE
NUM_CASES_THRESHOLD_RED = 0.5 # This should be set to the desired slope of the epidemiological curve
NUM_CASES_THRESHOLD_YELLOW = 0.2  # This should be set to the desired slope of the epidemiological curve

TRANSLATE = {'internal_alert':'Alerta interna',
            'joint_internal_alert': 'Alerta interna',
            'alert_num_cases':'Alerta de primer caso detectado',
            'external_alert': 'Alerta externa',
            'node_name':'Municipio',
            'community_name': 'Unidad funcional'}


# Colors for map
COLORS = {'ROJO':'#b30000',
'AMARILLO':'#ffcc00',
'VERDE':'#006600'}

COLORS_ = {'#b30000':'ROJO',
'#ffcc00':'AMARILLO',
'#006600':'VERDE'}

# Get name of files
constructed_file_path = os.path.join(data_dir, 'data_stages', location_name, 'constructed', location_folder, 'daily_graphs')
output_file_path = os.path.join(analysis_dir, location_name, location_folder, 'alerts')
time_window_file_path = os.path.join(analysis_dir, location_name, location_folder, 'polygon_info_window')
community_file = os.path.join(data_dir, 'data_stages', location_name, 'agglomerated', 'community', 'polygon_community_map.csv')
shape_file_path = os.path.join(data_dir, 'data_stages', location_name, 'raw', 'geo', 'Municpios_Dane_2017.shp')
edges = os.path.join(constructed_file_path, 'edges.csv')
nodes = os.path.join(constructed_file_path, 'nodes.csv')

# Check if folder exists
if not os.path.isdir(output_file_path):
    os.makedirs(output_file_path)

# Get file names

# polygons_file = os.path.join(data_dir, 'data_stages', location_name, 'constructed', location_folder, 'daily_graphs', 'node_locations.csv')
# 
# output_file_path = os.path.join(analysis_dir, location_name, location_folder, 'polygon_info_window')

#Load community map database
try:  
    df_community = pd.read_csv(community_file, low_memory=False)
except:
    df_community = pd.read_csv(community_file, low_memory=False, encoding = 'latin-1')

# Import shapefile
geo_df = gpd.read_file(shape_file_path)
total_window = os.path.join(time_window_file_path, 'polygon_info_total_window_5days.csv')

# Load time-windows
df_total_window = pd.read_csv(total_window)

# Get dfs
df_nodes = pd.read_csv(nodes, parse_dates=['date_time'])
df_edges = pd.read_csv(edges, parse_dates=['date_time'])

# Set window size
first_day = pd.Timestamp('today') - datetime.timedelta(days = WINDOW_SIZE)

# Get nodes
# df_nodes_recent = df_nodes[df_nodes['date_time'] >= first_day]
df_nodes_recent = df_nodes.loc[df_nodes['date_time'] >= first_day]
df_nodes_recent['external_movement'] = df_nodes_recent.apply(lambda x: get_mean_external_movement(x.node_id, x.date_time, df_edges), axis=1)


def set_mov_alert(points):
    if points > 0.5: return 'ROJO'
    elif points > 0: return 'AMARILLO'
    else: return 'VERDE'

def set_cases_alert_delta(delta):
    if delta > NUM_CASES_THRESHOLD_RED: return 'ROJO'
    elif delta > NUM_CASES_THRESHOLD_YELLOW: return 'AMARILLO'
    else: return 'VERDE'

def set_cases_alert_firstcase(new_cases, node_id):
    if node_id in new_cases: return 'ROJO'
    else: return 'VERDE'

def join_alert(alter_num_cases, alert_internal_mov):
    if alter_num_cases == 'ROJO': return 'ROJO'
    else: return alert_internal_mov

def set_color(color):
    return COLORS[color]

def get_points(node_id, movement_stats, movement, variable):
    if movement > movement_stats.at[node_id, variable]:
        return 1
    else: return 0

def  calculate_alerts_record():
    movement_stats = pd.DataFrame({'node_id':df_nodes['node_id'].unique()})
    movement_stats = movement_stats.merge(get_external_movement_stats_overtime(df_nodes, df_edges), how='outer', on='node_id')
    movement_stats = movement_stats.merge(get_internal_movement_stats_overtime(df_nodes), how='outer', on='node_id')
    movement_stats.set_index('node_id', inplace=True)

    # Calculate alerts
    # df_alerts = pd.DataFrame({'node_id':df_nodes['node_id'].unique()})
    df_nodes_recent['points_internal_movement'] = df_nodes_recent.apply(lambda x: get_points(x.node_id, movement_stats, x.inner_movement, 'internal_movement_one_std'), axis=1)
    df_nodes_recent['points_external_movement'] = df_nodes_recent.apply(lambda x: get_points(x.node_id, movement_stats, x.external_movement, 'external_movement_one_std'), axis=1)
    df_alerts = df_nodes_recent.groupby('node_id')[['points_internal_movement', 'points_external_movement']].apply(sum)
    df_alerts = df_alerts.divide(MAX_POINTS)
    df_alerts['internal_alert'] = df_alerts.apply(lambda x: set_mov_alert(x.points_internal_movement), axis=1)
    df_alerts['external_alert'] = df_alerts.apply(lambda x: set_mov_alert(x.points_external_movement), axis=1)
    return df_alerts

def calculate_threshold_min_date():
    return NotImplemented

# ---------------------------------------- #
# ---------- calculate alerts ------------ #
# ---------------------------------------- #

df_alerts = calculate_alerts_record()
alert_num_cases = pd.DataFrame({'node_id':df_nodes['node_id'].unique()})

# Internal alerts for num_cases based on delta
# alert_num_cases['alert_num_cases'] = df_total_window.apply(lambda x: set_cases_alert_delta(x.delta_num_cases), axis=1)

# Internal alerts for num_cases based on first reported case
new_cases = new_cases()
alert_num_cases['alert_num_cases'] = df_total_window.apply(lambda x: set_cases_alert_firstcase(new_cases, x.node_id), axis=1)


df_alerts = df_alerts.merge(alert_num_cases, on='node_id', how='outer')
df_alerts['joint_internal_alert'] = df_alerts.apply(lambda x: join_alert(x.alert_num_cases, x.internal_alert), axis=1)

# set_colors
df_alerts['joint_internal_alert_color'] = df_alerts.apply(lambda x: set_color(x.joint_internal_alert), axis=1)
df_alerts['external_alert_color'] = df_alerts.apply(lambda x: set_color(x.external_alert), axis=1)

# Map alerts
df_alerts = geo_df.merge(df_alerts, left_on='Codigo_Dan', right_on='node_id')
cmap = ListedColormap([(1,0.8,0), (0.8, 0, 0), (0,0.4,0)], name='alerts')
df_alerts.to_crs(epsg=3857, inplace=True)

print(ident+ "  Drawing alert maps.")

# Draw maps
for i in ['joint_internal_alert', 'external_alert']:

    print(ident + '     Drawing {}_map'.format(i))
    color_key = i+"_color"
    ax = df_alerts.plot(figsize=(15,9), linewidth=0.5, color=df_alerts[color_key], missing_kwds={'color': 'lightgrey'})
    colors = [COLORS_[x] for x in df_alerts[color_key].unique()]

    ax.legend(labels=colors, loc='upper right')
    ax.set_axis_off()
    ctx.add_basemap(ax, source=ctx.providers.CartoDB.VoyagerNoLabels)
    plt.title(TRANSLATE[i])
    # plt.show()
    plt.savefig(os.path.join(output_file_path, 'map_{}.png'.format(i)), bbox_inches="tight")

# Write alerts table
red_alerts = df_alerts.loc[(df_alerts['joint_internal_alert'] == 'ROJO') | (df_alerts['external_alert'] == 'ROJO')]
red_alerts = pd.merge(red_alerts, df_community.drop(columns=['community_id', 'poly_name'], axis=1), left_on='node_id', right_on='poly_id')
red_alerts.rename(columns={'internal_alert':'Alerta interna',
            'alert_num_cases':'Alerta de primer caso detectado',
            'external_alert': 'Alerta externa',
            'node_name':'Municipio',
            'community_name': 'Unidad funcional'}, inplace=True)

red_alerts.sort_values(by=['Unidad funcional'], inplace=True)

red_alerts.to_csv(os.path.join(output_file_path, 'alerts.csv'), columns=['Unidad funcional', 'Municipio',                                          
                                        'Alerta interna',
                                        'Alerta de primer caso detectado',
                                        'Alerta externa'], index=False)
