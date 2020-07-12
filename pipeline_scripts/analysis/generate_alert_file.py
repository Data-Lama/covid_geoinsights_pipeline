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
EXTERNAL_THREAT_THRESHOLD = 0.5

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


alert_backward = df_backward_window.loc[(df_backward_window['delta_num_cases'] >= NUM_CASES_THRESHOLD)].replace([np.inf, -np.inf], np.nan).dropna(axis='rows')
alert_forward = df_forward_window.loc[(df_forward_window['delta_num_cases'] >= NUM_CASES_THRESHOLD)].replace([np.inf, -np.inf], np.nan).dropna(axis='rows')
alert_total = df_total_window.loc[(df_total_window['delta_num_cases'] >= NUM_CASES_THRESHOLD)].replace([np.inf, -np.inf], np.nan).dropna(axis='rows')

red_alert = set(alert_total['node_id']).intersection(alert_forward['node_id'])
yellow_alert = set(alert_total['node_id'])

print(red_alert)
print(yellow_alert)

def set_alert(node_id):
    if node_id in red_alert: return 'RED'
    elif node_id in yellow_alert: return 'YELLOW'
    else: return 'GREEN'

df_polygons['alert'] = df_polygons.apply(lambda x: set_alert(x.node_id), axis=1)

df_polygons.to_csv(alert_report_path, index=False)