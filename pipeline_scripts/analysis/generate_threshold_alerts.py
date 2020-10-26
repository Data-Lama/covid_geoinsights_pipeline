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

from pipeline_scripts.functions.general_functions import get_mean_movement_stats_overtime, get_std_movement_stats_overtime
from pipeline_scripts.functions import general_functions as gf

# Direcotries
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')

# Reads the parameters from excecution
location_name  =  sys.argv[1] # location name
location_folder =  sys.argv[2] # polygon name

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
WINDOW_SIZE = 7
MAX_POINTS = WINDOW_SIZE
NUM_CASES_THRESHOLD_RED = 1 #5 # This should be set to the desired slope of the epidemiological curve
NUM_CASES_THRESHOLD_YELLOW = 0.5 #2  # This should be set to the desired slope of the epidemiological curve
NATIONAL_IPM = 19.6
NATIONAL_OLDAGE = 12
NATIONAL_SUBSIDIZED = 53
RED_RT = 1
YELLOW_RT = 0.8

DONE = False

TRANSLATE = {'internal_alert': 'Alerta interna (movimiento)', 
                'community_name':'Unidad funcional',
                'external_alert':'Alerta externa (movimiento)', 
                'alert_first_case':'Alerta de primer caso detectado', 
                "alert_external_num_cases":"Alerta numero de casos en municipios vecinos", 
                "alert_internal_num_cases":"Alerta numero de casos",
                "vulnerability_alert":"Alerta de vulnerabilidad", 
                "movement_range_alert": "Alerta interna (movimiento)",
                "max_alert": "Alerta agregada",
                "rt_alert":"Alerta por RT"}

# Colors for map
COLORS = {'ROJO':'#b30000',
'AMARILLO':'#ffcc00',
'VERDE':'#006600',
'BLANCO': '#d4d4d4'}

COLORS_ = {'#b30000':'ROJO',
'#ffcc00':'AMARILLO',
'#006600':'VERDE',
'#d4d4d4':'BLANCO'}

# Get name of files
agglomerated_file_path = os.path.join(data_dir, 'data_stages', location_name, 'agglomerated', location_folder)
output_file_path = os.path.join(analysis_dir, location_name, location_folder, 'alerts')
cases = os.path.join(agglomerated_file_path, 'cases.csv')
movement_range = os.path.join(agglomerated_file_path, 'movement_range.csv')
movement = os.path.join(agglomerated_file_path, 'movement.csv')
socioecon = os.path.join(data_dir, 'data_stages', location_name, 'raw', 'socio_economic', 'estadisticas_por_municipio.csv')
readme = os.path.join(data_dir, 'data_stages', location_name, 'agglomerated', "geometry", "README.txt")


# Geofiles
community_file = os.path.join(data_dir, 'data_stages', location_name, 'agglomerated', "community", "polygon_community_map.csv")
poly_locations = os.path.join(agglomerated_file_path, 'polygons.csv')
shape_file_path = os.path.join(data_dir, 'data_stages', location_name, 'raw', 'geo', 'Municpios_Dane_2017.shp')

# If polygon_union_wrapper
if selected_polygons_boolean:
    rt = os.path.join(analysis_dir, location_name, location_folder, "r_t", selected_polygon_name)
    time_window_file_path = os.path.join(analysis_dir, location_name, location_folder, 'polygon_info_window', selected_polygon_name)
    threshold = os.path.join(analysis_dir, location_name, "community", "r_t", selected_polygon_name, "mobility_thresholds.csv")
else:
    time_window_file_path = os.path.join(analysis_dir, location_name, location_folder, 'polygon_info_window', "entire_location")
    rt = os.path.join(analysis_dir, location_name, "community", "r_t", "entire_location")
    threshold = os.path.join(analysis_dir, location_name, "community", "r_t", "entire_location", "mobility_thresholds.csv")

total_window = os.path.join(time_window_file_path, 'deltas_forward_window_5days.csv')

# Load df_movement_threshold
df_movement_threshold = pd.read_csv(threshold)
aggregated_threshold = df_movement_threshold.set_index("poly_id").loc["aggregated"]
df_movement_threshold = df_movement_threshold.set_index("poly_id").drop("aggregated").reset_index()
df_movement_threshold["poly_id"] = df_movement_threshold["poly_id"].astype(int)

# Loads RT
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
df_movement_range = pd.read_csv(movement_range, parse_dates=['date_time'])

# Import shapefile
geo_df = gpd.read_file(shape_file_path)

# Load time-windows
df_total_window = pd.read_csv(total_window)

# Set window size
readme_dict = gf.load_README(readme)
max_date = readme_dict["Movement"].split(",")[1].strip()
max_date = max_date.split(" ")[1].strip()
first_day = pd.Timestamp(datetime.datetime.strptime(max_date, '%Y-%m-%d')) - datetime.timedelta(days = WINDOW_SIZE)

# Get socio-economic variables
df_socioecon = pd.read_csv(socioecon)
df_ipm = df_socioecon[["node_id", "ipm"]].copy()
df_age = df_socioecon[["node_id","porcentaje_sobre_60"]].copy()
df_age["porcentaje_sobre_60"] = df_age["porcentaje_sobre_60"].multiply(100)
df_eps = df_socioecon[["node_id","porcentaje_subsidiado"]].copy()
df_eps["porcentaje_subsidiado"] = df_eps["porcentaje_subsidiado"].multiply(100)

df_ipm.set_index("node_id", inplace=True)
df_age.set_index("node_id", inplace=True)
df_eps.set_index("node_id", inplace=True)

# Implementation for movement range change
df_movement_range_recent = df_movement_range.loc[df_movement_range['date_time'] >= first_day].copy()
df_movement_range_recent = df_movement_range.groupby("poly_id").mean()

# RT alerts
df_rt = df_rt[df_rt["date_time"] >= first_day]
df_rt_alert = df_rt.groupby("poly_id").mean()

# Get internal and external movement 
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

def expand_to_geometry(poly_id, df, key):
    if poly_id in df.index:
        return df.at[poly_id, key]
    else: return np.nan

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

def calculate_alerts_record(df_stats, df_movement_recent):
    
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

def get_max_alert(alerts):
    if "ROJO" in alerts:
        return 'ROJO'
    if "AMARILLO" in alerts:
        return 'AMARILLO'
    if 'VERDE' in alerts:
        return "VERDE"
    else: return "BLANCO"

def get_vulnerability_alert(poly_id):
    alerts = []
    ipm = df_ipm.at[poly_id, "ipm"]
    age = df_age.at[poly_id, "porcentaje_sobre_60"]
    eps = df_eps.at[poly_id, "porcentaje_subsidiado"]
    if (ipm > NATIONAL_IPM):
        alerts.append("IPM")
    if (age > NATIONAL_OLDAGE):
        alerts.append(">60 ANOS")
    if (eps > NATIONAL_SUBSIDIZED):
        alerts.append("EPS SUBSIDIADO")
    else: return "--"
    
    return ";".join(alerts)

def set_vulnerability_color(vul_alert):
    if vul_alert != "--":
        return "ROJO"
    else: return "VERDE"

def set_rt_alert_color(rt):
    if np.isnan(rt):
        return "BLANCO"
    if rt >= RED_RT:
        return "ROJO"
    if rt >= YELLOW_RT:
        return "AMARILLO"
    else: return "VERDE"

def set_movement_range_alert(mov_range, threshold):
    if np.isnan(threshold): return "BLANCO"
    if mov_range >= threshold:
        return "ROJO"
    else: return "VERDE"

# ---------------------------------------- #
# ---------- calculate alerts ------------ #
# ---------------------------------------- #

# Expand rt and thresholds to geometries
if location_folder == "geometry" and not selected_polygons_boolean:
    df_community.set_index("poly_id", inplace=True)
    #RT
    df_rt_geometry = pd.DataFrame({"poly_id":df_community.index})
    df_rt_geometry["ML"] = df_rt_geometry.apply(lambda x: expand_to_geometry(df_community.at[x.poly_id, "community_id"], df_rt_alert, "ML"), axis=1)
    df_rt_alert = df_rt_geometry

    # Thresholds
    df_thresholds_geometry = pd.DataFrame({"poly_id":df_community.index})
    df_thresholds_geometry["threshold"] = df_thresholds_geometry.apply(lambda x: expand_to_geometry(df_community.at[x.poly_id, "community_id"], df_movement_threshold.set_index("poly_id"), "mob_th"), axis=1)
    df_movement_threshold = df_thresholds_geometry

df_movement_range_recent = df_movement_range_recent.merge(df_movement_threshold, on="poly_id", how="outer")
df_movement_range_recent["movement_range_alert"] = df_movement_range_recent.apply(lambda x: set_movement_range_alert(x.movement_change, x.threshold), axis=1)

df_alerts = calculate_alerts_record(df_movement_stats, df_movement_recent).reset_index()
df_alerts.drop(columns=["points_inner_movement", "points_external_movement"], inplace=True)
df_alerts = df_alerts.merge(df_movement_range_recent[["poly_id", "movement_range_alert"]], on="poly_id", how="outer")

# Internal alerts for num_cases based on first reported case
new_cases = gf.new_cases(df_cases, WINDOW_SIZE)
df_alerts['alert_first_case'] = df_alerts.apply(lambda x: set_cases_alert_firstcase(new_cases, x.poly_id), axis=1)

# RT alerts
df_rt_alert["rt_alert"] = df_rt_alert.apply(lambda x: set_rt_alert_color(x.ML), axis=1)
df_alerts = df_alerts.merge(df_rt_alert, how="outer", on="poly_id").fillna("BLANCO")
df_alerts.drop(columns=["ML"], inplace=True)

# Num cases alerts
df_alerts_cases = df_total_window.copy()
df_alerts_cases["alert_internal_num_cases"] = df_alerts_cases.apply(lambda x: set_cases_alert_delta(x.delta_num_cases), axis=1)
df_alerts_cases["alert_external_num_cases"] = df_alerts_cases.apply(lambda x: set_cases_alert_delta(x.delta_external_num_cases), axis=1)
df_alerts_cases.drop(columns=["delta_num_cases", "delta_external_num_cases"], inplace=True)
df_alerts = df_alerts.merge(df_alerts_cases, on="poly_id", how="outer").fillna("VERDE")

# Set max alert
df_alerts['max_alert'] = df_alerts.apply(lambda x: get_max_alert([x.external_alert, x.movement_range_alert, x.alert_first_case, 
                                                                    x.alert_internal_num_cases, x.alert_external_num_cases, x.rt_alert]), axis=1)

# Merge with community file to get community names
df_alerts = df_alerts.merge(df_community.drop(columns=["community_id"], axis=1), on='poly_id', how='outer')
df_alerts[['Municipio', 'Departamento']] = df_alerts.poly_name.str.split('-',expand=True) 

df_alerts['vulnerability_alert'] = df_alerts.apply(lambda x: get_vulnerability_alert(x.poly_id), axis=1)

# set_colors
alert_list = ['max_alert', 'external_alert', 'movement_range_alert', 'alert_external_num_cases', \
'alert_internal_num_cases', 'alert_first_case', 'rt_alert', "internal_alert"]
for i in alert_list:
    df_alerts[f"{i}_color"] = df_alerts.apply(lambda x: set_color(x[i]), axis=1)

df_alerts['vulnerability_alert_color'] = df_alerts.apply(lambda x: set_vulnerability_color(x.vulnerability_alert), axis=1)

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

    if selected_polygons_boolean:
        red_alerts = df_alerts
    else: 
        red_alerts = df_alerts.loc[(df_alerts['max_alert'] == 'ROJO')].copy()

    # Write alerts table
    red_alerts = red_alerts.merge(df_age, how="outer", left_on="poly_id", right_on="node_id").dropna()
    red_alerts = red_alerts.merge(df_ipm, how="outer", left_on="poly_id", right_on="node_id").dropna()
    red_alerts.sort_values(by=['Departamento','Municipio'], inplace=True)
    red_alerts.rename(columns=TRANSLATE, inplace=True)
    red_alerts.to_csv(os.path.join(output_file_path, 'alerts.csv'), columns=['Departamento', 'Municipio', 'Unidad funcional',                                           
                                                                            'Alerta interna (movimiento)',
                                                                            'Alerta numero de casos',
                                                                            'Alerta de primer caso detectado',
                                                                            'Alerta externa (movimiento)', 
                                                                            'Alerta numero de casos en municipios vecinos',
                                                                            'Alerta por RT',
                                                                            'vulnerability_alert_color',
                                                                            'Alerta de vulnerabilidad'], index=False, float_format='%.3f', sep=",")

    # Map alerts
    df_alerts = geo_df.merge(df_alerts, left_on='Codigo_Dan', right_on='poly_id')
    cmap = ListedColormap([(1,0.8,0), (0.8, 0, 0), (0,0.4,0)], name='alerts')
    df_alerts.to_crs(epsg=3857, inplace=True)

    # Draw maps
    print(ident+ "  Drawing alert maps.")
    for i in ['internal_alert', 'movement_range_alert', 'external_alert', 'max_alert', 'rt_alert']:

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


    # Saves data
    print(ident + '   Saving Data')
    df_alerts.to_crs(epsg=4326, inplace=True)
    df_alerts.to_csv(os.path.join(output_file_path, 'map_data.csv'), index = False)






