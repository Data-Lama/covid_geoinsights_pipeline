import os
import sys
import numpy as np
import pandas as pd

# Direcotries
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')

# Reads the parameters from excecution
location_name  =  sys.argv[1] # location name
location_folder =  sys.argv[2] # polygon name
window_size_parameter = sys.argv[3] # window size
time_unit = sys.argv[4] # time unit for window [days, hours]

ident = '         '

NUM_CASES_THRESHOLD = 0.5
INNER_MOV_THRESHOLD = 0.5

NUM_EXT_CASES_THRESHOLD = 0.5
EXTERNAL_MOV_THRESHOLD = 0.5


# Get file names
time_window_file_path = os.path.join(analysis_dir, location_name, location_folder, 'polygon_info_window')
polygons_file = os.path.join(data_dir, 'data_stages', location_name, 'constructed', location_folder, 'daily_graphs', 'node_locations.csv')
alert_report_path = os.path.join(analysis_dir, location_name, location_folder, 'polygon_info_window','polygon_info_window_{}{}_alert_report.csv'.format(window_size_parameter, time_unit))


files = [i for i in os.listdir(time_window_file_path) if os.path.isfile(os.path.join(time_window_file_path,i)) and 'polygon_info' in i]
if len(files) == 0:
    raise Exception("No polygon time-window files found. Please run polygon_info_timewindow.py first")
files = [i for i in files if "{}{}".format(window_size_parameter, time_unit) in i]

backward_window = [i for i in files if i.startswith("polygon_info_backward_window")][0]
forward_window = [i for i in files if i.startswith("polygon_info_forward_window")][0]
total_window = [i for i in files if i.startswith("polygon_info_total_window")][0]

# Load time-windows
df_backward_window = pd.read_csv(os.path.join(time_window_file_path, backward_window))
df_forward_window = pd.read_csv(os.path.join(time_window_file_path, forward_window))
df_total_window = pd.read_csv(os.path.join(time_window_file_path, total_window))

#Load polygon id reference database
try:  
    df_polygons = pd.read_csv(polygons_file, low_memory=False, )
except:
    df_polygons = pd.read_csv(polygons_file, low_memory=False, encoding = 'latin-1')

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
yellow_alert_num_cases = set(alert_total_num_cases['node_id'])

red_alert_inner_mov = set(alert_total_inner_mov['node_id']).intersection(alert_forward_inner_mov['node_id'])
yellow_alert_inner_mov = set(alert_total_inner_mov['node_id'])

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
yellow_alert_external_num_cases = set(alert_total_external_cases['node_id'])

red_alert_external_mov = set(alert_total_external_mov['node_id']).intersection(alert_forward_external_mov['node_id'])
yellow_alert_external_mov = set(alert_total_external_mov['node_id'])

print(red_alert_external_mov)
print(red_alert_external_num_cases)
# Alert for external threat:
# RED: if both external movement and external cases are red
# YELLOW: if either external movement or external cases are red, or if both are yellow
# GREEN: otherwise

red_alert_external_threat = red_alert_external_mov.intersection(red_alert_external_num_cases)
yellow_alert_external_threat = red_alert_external_mov.difference(red_alert_external_threat)\
    .union(red_alert_external_num_cases.difference(red_alert_external_threat))\
    .union(yellow_alert_external_num_cases.intersection(yellow_alert_external_mov))


def set_alert(node_id, red_alert, yellow_alert):
    if node_id in red_alert: return 'RED'
    elif node_id in yellow_alert: return 'YELLOW'
    else: return 'GREEN'

df_polygons['internal_alert_num_cases'] = df_polygons.apply(lambda x: set_alert(x.node_id, red_alert_num_cases, yellow_alert_num_cases), axis=1)
df_polygons['internal_alert_movement'] = df_polygons.apply(lambda x: set_alert(x.node_id, red_alert_inner_mov, yellow_alert_inner_mov), axis=1)
df_polygons['external_threat_alert'] = df_polygons.apply(lambda x: set_alert(x.node_id, red_alert_external_threat, yellow_alert_external_threat), axis=1)

df_polygons.to_csv(alert_report_path, index=False)