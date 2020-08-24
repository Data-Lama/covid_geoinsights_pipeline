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

from pipeline_scripts.functions.general_functions import get_mean_movement_stats_overtime, get_std_movement_stats_overtime
from pipeline_scripts.functions import general_functions as gf

# Direcotries
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')

# Reads the parameters from excecution
location_name  =  sys.argv[1] # location name
location_folder =  sys.argv[2] # polygon name
criteria_parameter = sys.argv[3] # min_record or min_date

if len(sys.argv) <= 4:
	selected_polygons_boolean = False
else :
    selected_polygons_boolean = True
    selected_polygons = []
    i = 4
    while i < len(sys.argv):
        selected_polygons.append(sys.argv[i])
        i += 1
    selected_polygon_name = selected_polygons.pop(0)

# Constants
ident = '         '
WINDOW_SIZE = 7
MAX_POINTS = WINDOW_SIZE
NUM_CASES_THRESHOLD_RED = 1 #5 # This should be set to the desired slope of the epidemiological curve
NUM_CASES_THRESHOLD_YELLOW = 0.5 #2  # This should be set to the desired slope of the epidemiological curve
DONE = False

TRANSLATE = {'internal_alert':'Alerta interna',
            'joint_internal_alert': 'Alerta interna',
            'alert_num_cases':'Alerta de primer caso detectado',
            'external_alert': 'Alerta externa',
            'node_name':'Municipio',
            'community_name': 'Unidad funcional',
            'max_alert': 'Alertas generales'}


# Colors for map
COLORS = {'ROJO':'#b30000',
'AMARILLO':'#ffcc00',
'VERDE':'#006600'}

COLORS_ = {'#b30000':'ROJO',
'#ffcc00':'AMARILLO',
'#006600':'VERDE'}

# Get name of files
agglomerated_file_path = os.path.join(data_dir, 'data_stages', location_name, 'agglomerated', location_folder)
output_file_path = os.path.join(analysis_dir, location_name, location_folder, 'alerts')
cases = os.path.join(agglomerated_file_path, 'cases.csv')
movement = os.path.join(agglomerated_file_path, 'movement.csv')
socioecon = os.path.join(data_dir, 'data_stages', location_name, 'raw', 'socio_economic', 'estadisticas_por_municipio.csv')

# If polygon_union_wrapper
if selected_polygons_boolean:
    time_window_file_path = os.path.join(analysis_dir, location_name, location_folder, 'polygon_info_window', selected_polygon_name)
else:
    time_window_file_path = os.path.join(analysis_dir, location_name, location_folder, 'polygon_info_window', "entire_location")

total_window = os.path.join(time_window_file_path, 'deltas_forward_window_5days.csv')


# Geofiles
community_file = os.path.join(data_dir, 'data_stages', location_name, 'agglomerated', 'community', 'polygon_community_map.csv')
poly_locations = os.path.join(agglomerated_file_path, 'polygons.csv')
shape_file_path = os.path.join(data_dir, 'data_stages', location_name, 'raw', 'geo', 'Municpios_Dane_2017.shp')

# Check if folder exists
if not os.path.isdir(output_file_path):
    os.makedirs(output_file_path)

#Load community map database
try:  
    df_community = pd.read_csv(community_file, low_memory=False)
except:
    df_community = pd.read_csv(community_file, low_memory=False, encoding = 'latin-1')

#Load num_cases 
try:  
    df_cases = pd.read_csv(cases, low_memory=False, parse_dates=['date_time'])
except:
    df_cases = pd.read_csv(cases, low_memory=False, encoding = 'latin-1', parse_dates=['date_time'])

# Load movement
df_movement = pd.read_csv(movement, parse_dates=['date_time'])

# Import shapefile
geo_df = gpd.read_file(shape_file_path)

# Load time-windows
df_total_window = pd.read_csv(total_window)


# Set window size
first_day = pd.Timestamp('today') - datetime.timedelta(days = WINDOW_SIZE)

# Get socio-economic variables
df_socioecon = pd.read_csv(socioecon)
df_ipm = df_socioecon[["node_id", "ipm"]].copy()
df_age = df_socioecon[["node_id","porcentaje_sobre_60"]].copy()
df_age["porcentaje_sobre_60"] = df_age["porcentaje_sobre_60"].multiply(100)

# Get polygons
df_movement_recent = df_movement.loc[df_movement['date_time'] >= first_day].copy()
df_inner_movement = df_movement_recent[df_movement_recent['start_poly_id'] == df_movement_recent['end_poly_id']].copy()
df_external_movement = df_movement_recent[df_movement_recent['start_poly_id'] != df_movement_recent['end_poly_id']].copy()
df_external_movement = df_external_movement.groupby(["start_poly_id", "date_time"]).sum()
df_external_movement.reset_index(inplace=True)

df_inner_movement.rename(columns={"movement":"inner_movement", "start_poly_id":"poly_id"}, inplace=True)
df_inner_movement.drop(columns=["end_poly_id"], inplace=True)
df_external_movement.rename(columns={"movement":"external_movement", "start_poly_id":"poly_id"}, inplace=True)
df_external_movement.drop(columns=["end_poly_id"], inplace=True)

df_movement_recent = df_inner_movement.merge(df_external_movement, on=["poly_id", "date_time"], how="outer").fillna(0)

# print(df_movement_recent.sort_values("poly_id").head(10))

# Get overtime stats
df_mean_movement_stats = get_mean_movement_stats_overtime(df_movement).reset_index()
df_mean_movement_stats.rename(columns={"inner_movement":"mean_inner_movement", "external_movement":"mean_external_movement"}, inplace=True)
df_std_movement_stats = get_std_movement_stats_overtime(df_movement).reset_index()
df_std_movement_stats.rename(columns={"inner_movement":"std_inner_movement", "external_movement":"std_external_movement"}, inplace=True)

df_movement_stats = df_mean_movement_stats.merge(df_std_movement_stats, on="poly_id", how="outer")
df_movement_stats["one_std_over_mean_inner_movement"] = df_movement_stats["mean_inner_movement"].add(df_movement_stats["std_inner_movement"])
df_movement_stats["one-half_std_over_mean_inner_movement"] = df_movement_stats["one_std_over_mean_inner_movement"].add(df_movement_stats["std_inner_movement"].divide(2))
df_movement_stats["one_std_over_mean_external_movement"] = df_movement_stats["mean_external_movement"].add(df_movement_stats["std_external_movement"])
df_movement_stats["one-half_std_over_mean_external_movement"] = df_movement_stats["one_std_over_mean_external_movement"].add(df_movement_stats["std_external_movement"].divide(2))
df_movement_stats.set_index("poly_id", inplace=True)


def set_mov_alert(points):
    if points > 0.5: return 'ROJO'
    elif points > 0: return 'AMARILLO'
    else: return 'VERDE'

def set_cases_alert_delta(delta):
    if delta > NUM_CASES_THRESHOLD_RED: return 'ROJO'
    elif delta > NUM_CASES_THRESHOLD_YELLOW: return 'AMARILLO'
    else: return 'VERDE'

def set_cases_alert_firstcase(new_cases, poly_id):
    if poly_id in new_cases: return 'ROJO'
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

def  calculate_alerts_record(df_stats, df_movement_recent):
    
    movement_alerts = df_movement_recent.copy()
    movement_alerts['points_inner_movement'] = movement_alerts.apply(lambda x: get_points(x.poly_id, df_stats, x.inner_movement, 'one-half_std_over_mean_inner_movement'), axis=1)
    movement_alerts['points_external_movement'] = movement_alerts.apply(lambda x: get_points(x.poly_id, df_stats, x.external_movement, 'one-half_std_over_mean_external_movement'), axis=1)
    movement_alerts = movement_alerts.groupby('poly_id')[['points_inner_movement', 'points_external_movement']].apply(sum)
    movement_alerts = movement_alerts.divide(MAX_POINTS)
    movement_alerts['internal_alert'] =  movement_alerts.apply(lambda x: set_mov_alert(x.points_inner_movement), axis=1)
    movement_alerts['external_alert'] = movement_alerts.apply(lambda x: set_mov_alert(x.points_external_movement), axis=1)
    
    return movement_alerts

def calculate_threshold_min_date():
    return NotImplemented

def get_max_alert(alert_1, alert_2, alert_3, alert_4, alert_5):
    if (alert_1 == 'ROJO') or (alert_2 == 'ROJO') or (alert_3 == 'ROJO') or (alert_4 == 'ROJO') or (alert_5 == 'ROJO'):
        return 'ROJO'
    if (alert_1 == 'AMARILLO') or (alert_2 == 'AMARILLO') or (alert_3 == 'AMARILLO') or (alert_4 == 'AMARILLO') or (alert_5 == 'AMARILLO'):
        return 'AMARILLO'
    else: return 'VERDE'

# ---------------------------------------- #
# ---------- calculate alerts ------------ #
# ---------------------------------------- #

# Movement alerts 
df_alerts = calculate_alerts_record(df_movement_stats, df_movement_recent).reset_index()
df_alerts.drop(columns=["points_inner_movement", "points_external_movement"])


# Num cases alerts
df_alerts_cases = df_total_window.copy()
df_alerts_cases["alert_internal_num_cases"] = df_alerts_cases.apply(lambda x: set_cases_alert_delta(x.delta_num_cases), axis=1)
df_alerts_cases["alert_external_num_cases"] = df_alerts_cases.apply(lambda x: set_cases_alert_delta(x.delta_external_num_cases), axis=1)
df_alerts_cases.drop(columns=["delta_num_cases", "delta_external_num_cases"], inplace=True)

# Internal alerts for num_cases based on first reported case
new_cases = gf.new_cases(df_cases, WINDOW_SIZE)
df_alerts['alert_first_case'] = df_alerts.apply(lambda x: set_cases_alert_firstcase(new_cases, x.poly_id), axis=1)
df_alerts.drop(columns=["points_inner_movement", "points_external_movement"], inplace=True)

# Merge 
df_alerts = df_alerts.merge(df_alerts_cases, on="poly_id", how="outer").fillna("VERDE")
df_alerts['max_alert'] = df_alerts.apply(lambda x: get_max_alert(x.internal_alert, x.external_alert, x.alert_first_case, 
                                                                    x.alert_internal_num_cases, x.alert_external_num_cases), axis=1)

df_alerts = df_alerts.merge(df_community.drop(columns=["community_id"], axis=1), on='poly_id', how='outer')
df_alerts[['Municipio', 'Departamento']] = df_alerts.poly_name.str.split('-',expand=True) 
df_alerts = df_alerts.fillna("VERDE")

# set_colors
df_alerts['max_alert_color'] = df_alerts.apply(lambda x: set_color(x.max_alert), axis=1)
df_alerts['external_alert_color'] = df_alerts.apply(lambda x: set_color(x.external_alert), axis=1)
df_alerts['internal_alert_color'] = df_alerts.apply(lambda x: set_color(x.internal_alert), axis=1)
df_alerts['external_num_cases_alert_color'] = df_alerts.apply(lambda x: set_color(x.alert_external_num_cases), axis=1)
df_alerts['internal_num_cases_alert_color'] = df_alerts.apply(lambda x: set_color(x.alert_internal_num_cases), axis=1)
df_alerts['first_case_color_alert'] = df_alerts.apply(lambda x: set_color(x.alert_first_case), axis=1)

# If asked for specific polygons, get subset
if selected_polygons_boolean:
    df_alerts = df_alerts[df_alerts["poly_id"].isin(selected_polygons)]
    if df_alerts.empty:
        print("{}{}No alerts found for the given polygon. Skipping".format(ident, ident))
        DONE = True
    output_file_path = os.path.join(output_file_path, selected_polygon_name)
else:
    output_file_path = os.path.join(output_file_path, "entire_location")

if not DONE:

    # Check if folder exists
    if not os.path.isdir(output_file_path):
        os.makedirs(output_file_path)

    # Write alerts table
    red_alerts = df_alerts.loc[(df_alerts['max_alert'] == 'ROJO')].copy()
    red_alerts = red_alerts.merge(df_age, how="outer", left_on="poly_id", right_on="node_id").dropna()
    red_alerts = red_alerts.merge(df_ipm, how="outer", left_on="poly_id", right_on="node_id").dropna()
    red_alerts.sort_values(by=['Departamento','Municipio'], inplace=True)
    red_alerts.rename(columns={'internal_alert': 'Alerta interna (movimiento)', 'community_name':'Unidad funcional',
    'external_alert':'Alerta externa (movimiento)', 'alert_first_case':'Alerta de primer caso detectado', 
    "alert_external_num_cases":"Alerta numero de casos en municipios vecinos", "alert_internal_num_cases":"Alerta numero de casos",
    "porcentaje_sobre_60":"Personas mayores a 60", "ipm":"IPM"}, inplace=True)
    red_alerts.to_csv(os.path.join(output_file_path, 'alerts.csv'), columns=['Departamento', 'Municipio', 'Unidad funcional',                                           
                                            'Alerta interna (movimiento)',
                                            'Alerta numero de casos',
                                            'Alerta de primer caso detectado',
                                            'Alerta externa (movimiento)', 
                                            'Alerta numero de casos en municipios vecinos',
                                            'Personas mayores a 60',
                                            'IPM'], index=False, float_format='%.3f', sep=",")

    # Map alerts
    df_alerts = geo_df.merge(df_alerts, left_on='Codigo_Dan', right_on='poly_id')
    cmap = ListedColormap([(1,0.8,0), (0.8, 0, 0), (0,0.4,0)], name='alerts')
    df_alerts.to_crs(epsg=3857, inplace=True)

    print(ident+ "  Drawing alert maps.")


    # Draw maps
    for i in ['internal_alert', 'external_alert', 'max_alert']:

        print(ident + '     Drawing {}_map'.format(i))
        color_key = i+"_color"
        ax = df_alerts.plot(figsize=(15,9), linewidth=0.5, color=df_alerts[color_key], missing_kwds={'color': 'lightgrey'}, alpha=0.8)
        colors = [COLORS_[x] for x in df_alerts[color_key].unique()]

        if selected_polygons_boolean:
            df_alerts["label"] = df_alerts.apply(lambda x: x.geometry.representative_point(), axis=1)
            df_alerts["label_x"] = df_alerts.apply(lambda p: p.label.x, axis=1)
            df_alerts["label_y"] = df_alerts.apply(lambda p: p.label.y, axis=1)
            for x, y, label in zip(df_alerts.label_x, df_alerts.label_y, df_alerts.Municipio_x):
                    ax.annotate(label, xy=(x, y), xytext=(3, 3), textcoords="offset points", fontsize=5)

        ax.legend(labels=colors, loc='upper right')
        ax.set_axis_off()
        ctx.add_basemap(ax, source=ctx.providers.CartoDB.VoyagerNoLabels)
        plt.title(TRANSLATE[i])
        # plt.show()
        plt.savefig(os.path.join(output_file_path, 'map_{}.png'.format(i)), bbox_inches="tight")



