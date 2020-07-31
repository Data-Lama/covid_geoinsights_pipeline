import os
import sys
import numpy as np
import pandas as pd
import geopandas as gpd
import contextily as ctx
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap
# Direcotries
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')


# Reads the parameters from excecution
location_name  =  sys.argv[1] # location name
location_folder =  sys.argv[2] # polygon name
window_size_parameter = sys.argv[3] # window size
time_unit = sys.argv[4] # time unit for window [days, hours]

# Constants
ident = '         '
ident_1 = '             '

NUM_CASES_THRESHOLD = 0.5
INNER_MOV_THRESHOLD = 0.5

NUM_EXT_CASES_THRESHOLD = 0.5
EXTERNAL_MOV_THRESHOLD = 0.5

# Colors for map
COLORS = {'RED':'#b30000',
'YELLOW':'#ffcc00',
'GREEN':'#006600'}

COLORS_ = {'#b30000':'RED',
'#ffcc00':'YELLOW',
'#006600':'GREEN'}

# Colors for map esp
COLORS_SP = {'ROJO':'#b30000',
'AMARILLO':'#ffcc00',
'VERDE':'#006600'}

COLORS_SP_ = {'#b30000':'ROJO',
'#ffcc00':'AMARILLO',
'#006600':'VERDE'}

print(ident+'Generating alert files for {} {}'.format(location_name, location_folder))

print(ident + ' Using the following thresholds:\n{}internal_num_cases: \
{}\n{}internal movement: {}\n{}external_num_cases: {}\n{}external_movement: {}'.format(ident_1, 
NUM_CASES_THRESHOLD, ident_1, INNER_MOV_THRESHOLD, ident_1, NUM_EXT_CASES_THRESHOLD, ident_1, EXTERNAL_MOV_THRESHOLD))

# Get file names
time_window_file_path = os.path.join(analysis_dir, location_name, location_folder, 'polygon_info_window')
polygons_file = os.path.join(data_dir, 'data_stages', location_name, 'constructed', location_folder, 'daily_graphs', 'node_locations.csv')
community_file = os.path.join(data_dir, 'data_stages', location_name, 'agglomerated', 'community', 'polygon_community_map.csv')
output_file_path = os.path.join(analysis_dir, location_name, location_folder, 'polygon_info_window')
shape_file_path = os.path.join(data_dir, 'data_stages', location_name, 'raw', 'geo', 'Municpios_Dane_2017.shp')

# Import shapefile
geo_df = gpd.read_file(shape_file_path)
backward_window = os.path.join(time_window_file_path, 'polygon_info_backward_window_5days.csv')
forward_window = os.path.join(time_window_file_path, 'polygon_info_forward_window_5days.csv')
total_window = os.path.join(time_window_file_path, 'polygon_info_total_window_5days.csv')

# Load time-windows
df_backward_window = pd.read_csv(backward_window)
df_forward_window = pd.read_csv(forward_window)
df_total_window = pd.read_csv(total_window)

#Load polygon id reference database
try:  
    df_polygons = pd.read_csv(polygons_file, low_memory=False)
except:
    df_polygons = pd.read_csv(polygons_file, low_memory=False, encoding = 'latin-1')

#Load community map database
try:  
    df_community = pd.read_csv(community_file, low_memory=False)
except:
    df_community = pd.read_csv(community_file, low_memory=False, encoding = 'latin-1')

# Internal alerts
alert_backward_num_cases = df_backward_window[(df_backward_window['delta_num_cases'] >= NUM_CASES_THRESHOLD)].replace([np.inf, -np.inf], np.nan).dropna(axis='rows')
alert_forward_num_cases = df_forward_window[(df_forward_window['delta_num_cases'] >= NUM_CASES_THRESHOLD)].replace([np.inf, -np.inf], np.nan).dropna(axis='rows')
alert_total_num_cases = df_total_window[(df_total_window['delta_num_cases'] >= NUM_CASES_THRESHOLD)].replace([np.inf, -np.inf], np.nan).dropna(axis='rows')

alert_backward_inner_mov = df_backward_window[(df_backward_window['delta_inner_movement'] >= INNER_MOV_THRESHOLD)].replace([np.inf, -np.inf], np.nan).dropna(axis='rows')
alert_forward_inner_mov = df_forward_window[(df_forward_window['delta_inner_movement'] >= INNER_MOV_THRESHOLD)].replace([np.inf, -np.inf], np.nan).dropna(axis='rows')
alert_total_inner_mov = df_total_window[(df_total_window['delta_inner_movement'] >= INNER_MOV_THRESHOLD)].replace([np.inf, -np.inf], np.nan).dropna(axis='rows')

# Red alert will be considered that which find increase in both the total window and the forward window
# Yellow alert will be considered that which finds increase only in the total window
red_alert_num_cases = set(alert_total_num_cases['node_id']).intersection(alert_forward_num_cases['node_id'])
yellow_alert_num_cases = set(alert_forward_num_cases['node_id'])

red_alert_inner_mov = set(alert_total_inner_mov['node_id']).intersection(alert_forward_inner_mov['node_id'])
yellow_alert_inner_mov = set(alert_forward_inner_mov['node_id'])

# External alerts
alert_backward_external_cases = df_backward_window[(df_backward_window['delta_external_num_cases'] >= NUM_EXT_CASES_THRESHOLD)].replace([np.inf, -np.inf], np.nan).dropna(axis='rows')
alert_forward_external_cases = df_forward_window[(df_forward_window['delta_external_num_cases'] >= NUM_EXT_CASES_THRESHOLD)].replace([np.inf, -np.inf], np.nan).dropna(axis='rows')
alert_total_external_cases = df_total_window[(df_total_window['delta_external_num_cases'] >= NUM_EXT_CASES_THRESHOLD)].replace([np.inf, -np.inf], np.nan).dropna(axis='rows')

alert_backward_external_mov = df_backward_window[(df_backward_window['delta_external_movement'] >= EXTERNAL_MOV_THRESHOLD)].replace([np.inf, -np.inf], np.nan).dropna(axis='rows')
alert_forward_external_mov = df_forward_window[(df_forward_window['delta_external_movement'] >= EXTERNAL_MOV_THRESHOLD)].replace([np.inf, -np.inf], np.nan).dropna(axis='rows')
alert_total_external_mov = df_total_window[(df_total_window['delta_external_movement'] >= EXTERNAL_MOV_THRESHOLD)].replace([np.inf, -np.inf], np.nan).dropna(axis='rows')

# Red alert will be considered that which find increase in both the total window and the forward window
# Yellow alert will be considered that which finds increase only in the total window
red_alert_external_num_cases = set(alert_total_external_cases['node_id']).intersection(alert_forward_external_cases['node_id'])
yellow_alert_external_num_cases = set(alert_forward_external_cases['node_id'])

red_alert_external_mov = set(alert_total_external_mov['node_id']).intersection(alert_forward_external_mov['node_id'])
yellow_alert_external_mov = set(alert_forward_external_mov['node_id'])

def set_alert(node_id, red_alert, yellow_alert):
    if node_id in red_alert: return 'ROJO'
    elif node_id in yellow_alert: return 'AMARILLO'
    else: return 'VERDE'

def set_color(node_id, red_alert, yellow_alert):
    if node_id in red_alert: return COLORS['RED']
    elif node_id in yellow_alert: return COLORS['YELLOW']
    else: return COLORS['GREEN']

def join_alert(mov_alert, num_cases_alert):
    if mov_alert == 'ROJO':
        return 'ROJO'
    if mov_alert == 'AMARILLO' and num_cases_alert == 'ROJO':
        return 'ROJO'
    if num_cases_alert == 'ROJO' or num_cases_alert == 'AMARILLO':
        return 'AMARILLO'
    return 'VERDE'

df_polygons['internal_num_cases_alert'] = df_polygons.apply(lambda x: set_alert(x.node_id, red_alert_num_cases, yellow_alert_num_cases), axis=1)
df_polygons['internal_movement_alert'] = df_polygons.apply(lambda x: set_alert(x.node_id, red_alert_inner_mov, yellow_alert_inner_mov), axis=1)
df_polygons['external_movement_alert'] = df_polygons.apply(lambda x: set_alert(x.node_id, red_alert_external_mov, yellow_alert_external_mov), axis=1)
df_polygons['external_num_cases_alert'] = df_polygons.apply(lambda x: set_alert(x.node_id, red_alert_external_num_cases, yellow_alert_external_num_cases), axis=1)
df_polygons['external_joint_alert'] = df_polygons.apply(lambda x: join_alert(x.external_movement_alert, x.external_num_cases_alert), axis=1)
df_polygons['internal_joint_alert'] = df_polygons.apply(lambda x: join_alert(x.internal_movement_alert, x.internal_num_cases_alert), axis=1)

df_polygons['internal_num_cases_color'] = df_polygons.apply(lambda x: set_color(x.node_id, red_alert_num_cases, yellow_alert_num_cases), axis=1)
df_polygons['internal_movement_color'] = df_polygons.apply(lambda x: set_color(x.node_id, red_alert_inner_mov, yellow_alert_inner_mov), axis=1)
df_polygons['external_movement_color'] = df_polygons.apply(lambda x: set_color(x.node_id, red_alert_external_mov, yellow_alert_external_mov), axis=1)
df_polygons['external_num_cases_color'] = df_polygons.apply(lambda x: set_color(x.node_id, red_alert_external_num_cases, yellow_alert_external_num_cases), axis=1)
df_polygons['external_joint_color'] = df_polygons.apply(lambda x: COLORS_SP[x.external_joint_alert], axis=1)
df_polygons['internal_joint_color'] = df_polygons.apply(lambda x: COLORS_SP[x.internal_joint_alert], axis=1)

alerts = df_polygons.loc[(df_polygons['internal_num_cases_alert'] == 'ROJO') | (df_polygons['internal_movement_alert'] == 'ROJO') \
     | (df_polygons['external_movement_alert'] == 'ROJO')]

alerts = pd.merge(alerts, df_community.drop(columns=['community_id', 'poly_name'], axis=1), left_on='node_id', right_on='poly_id')
translate = {'internal_num_cases_alert':'Alerta numero de casos',
                        'internal_movement_alert':'Alerta flujo dentro del municipio',
                        'external_num_cases_alert':'Alerta numero de casos en municipios vecinos',
                        'external_movement_alert':'Alerta flujo hacia el municipio',
                        'internal_joint_alert': 'Alerta interna compuesta',
                        'external_joint_alert': 'Alerta externa compuesta',
                        'node_name':'Municipio',
                        'community_name': 'Unidad funcional'}
alerts.rename(columns=translate, inplace=True)
alerts.drop(columns=['lat', 'lon', 'poly_id'], inplace=True)
alerts.sort_values(by=['Unidad funcional'], inplace=True)

alerts.to_csv(os.path.join(output_file_path, 'alerts.csv'), columns=['Unidad funcional',
                                        'Municipio',                                          
                                        'Alerta numero de casos',
                                        'Alerta flujo dentro del municipio',
                                        'Alerta numero de casos en municipios vecinos',
                                        'Alerta flujo hacia el municipio'], index=False)

alerts.to_csv(os.path.join(output_file_path, 'alerts_num_cases_delta.csv'), columns=['node_id','Unidad funcional',
                                        'Municipio',                                          
                                        'Alerta numero de casos',
                                        'Alerta numero de casos en municipios vecinos'], index=False)


# Map alerts
df_polygons = geo_df.merge(df_polygons, left_on='Codigo_Dan', right_on='node_id')
cmap = ListedColormap([(1,0.8,0), (0.8, 0, 0), (0,0.4,0)], name='alerts')
df_polygons.to_crs(epsg=3857, inplace=True)

print(ident+ "  Drawing alert maps.")

# Draw maps
for i in ['internal_num_cases_alert', 'internal_movement_alert', 
            'external_num_cases_alert', 'external_movement_alert', 
            'internal_joint_alert', 'external_joint_alert']:
    print(ident + '     Drawing {}_map'.format(i))
    color_key = i.replace('alert', 'color')
    ax = df_polygons.plot(figsize=(15,9), linewidth=0.5, color=df_polygons[color_key], missing_kwds={'color': 'lightgrey'})
    colors = [COLORS_[x] for x in df_polygons[color_key].unique()]

    ax.legend(labels=colors, loc='upper right')
    ax.set_axis_off()
    ctx.add_basemap(ax, source=ctx.providers.CartoDB.VoyagerNoLabels)
    plt.title(translate[i])
    # plt.show()
    plt.savefig(os.path.join(output_file_path, 'map_{}.png'.format(i)), bbox_inches="tight")


