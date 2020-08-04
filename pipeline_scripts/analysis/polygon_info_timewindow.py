'''
This script receives (1) location_name (2) polygon_name (3) time-window size (4) time unit and (5) optionally a date to center window around (t_0).
If no date is specified the date of the time the script is run will be used as t_0. 

Two files are produced (in .csv format): one describes the time-window going back from t_0 
and the other the time-window going forward and including from t_0. They both have the following information:

The script calculates for each node of the given agglomeration.
    1. num_cases: number of cases in the node averaged over the time window
    2. external_num_cases: number of cases in the node's imediate neighbors over the time window
    3. delta_num_cases: the percentage increase of cases in the node 
    4. delta_external_cases: the percentage increase of cases in the neighboring nodes

    ** Deltas are calculated as follows:
    backward-window: ((t_0 - t_-1) / t_-1)
    forward_window: ((t_1 - t_0) / t_0)
'''

import os
import sys
import random
import datetime
import numpy as np
import pandas as pd
from datetime import date
import matplotlib.pyplot as plt

import numpy as np

from pipeline_scripts.functions.general_functions import load_README, get_neighbor_cases_total

# Direcotries
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')

# Reads the parameters from excecution
location_name  =  sys.argv[1] # location name
location_folder =  sys.argv[2] # polygon name
window_size_parameter = sys.argv[3] # window size
time_unit = sys.argv[4] # time unit for window [days, hours]

# Get name of files
agglomerated_file_path = os.path.join(data_dir, 'data_stages', location_name, 'agglomerated', location_folder)
output_file_path = os.path.join(analysis_dir, location_name, location_folder, 'polygon_info_window')
cases = os.path.join(agglomerated_file_path, 'cases.csv')
movement = os.path.join(agglomerated_file_path, 'movement.csv')

date_zero = None
output_backward_delta_file = os.path.join(output_file_path, 'deltas_backward_window_{}{}.csv'.format(window_size_parameter,time_unit))
output_forward_delta_file = os.path.join(output_file_path,'deltas_forward_window_{}{}.csv'.format(window_size_parameter,time_unit))
output_total_delta_file = os.path.join(output_file_path,'deltas_total_window_{}{}.csv'.format(window_size_parameter,time_unit))
output_backward_file = os.path.join(output_file_path, 'backward_window_{}{}.csv'.format(window_size_parameter,time_unit))
output_forward_file = os.path.join(output_file_path,'forward_window_{}{}.csv'.format(window_size_parameter,time_unit))
output_total_file = os.path.join(output_file_path,'total_window_{}{}.csv'.format(window_size_parameter,time_unit))
output_readme_file = os.path.join(output_file_path,'polygon_info_readme_{}{}.txt'.format(window_size_parameter,time_unit))

ident = '         '

# Import README
readme_file_path = os.path.join(data_dir, 'data_stages', location_name, 'constructed', location_folder, 'daily_graphs', 'README.txt')
if os.path.exists(readme_file_path):
    readme = load_README(readme_file_path)
else:
    raise Exception('No README.txt found for {}'.format(readme_file_path))

# Set window size to int
window_size = int(window_size_parameter)

# Units for margin
margins = { 'hours': datetime.timedelta(hours = window_size),
            'days': datetime.timedelta(days = window_size),
            'weeks': datetime.timedelta(weeks = window_size)
            }

# Check if folder exists
if not os.path.isdir(output_file_path):
    os.makedirs(output_file_path)

#Load movement and cases
try:  
    df_cases = pd.read_csv(cases, low_memory=False, parse_dates=['date_time'])
except:
    df_cases = pd.read_csv(cases, low_memory=False, encoding = 'latin-1', parse_dates=['date_time'])

try:  
    df_movement = pd.read_csv(movement, low_memory=False, parse_dates=['date_time'])
except:
    df_movement = pd.read_csv(movement, low_memory=False, encoding = 'latin-1', parse_dates=['date_time']) 


# Get nodes within time window. Date zero will be counted with backward window.
margin = margins[time_unit] 

# Date zero
if date_zero == None:
    date_zero = pd.Timestamp('today') - margin
else:
    date_zero = datetime.datetime.strptime(date_zero, '%Y-%m-%d')

# Get minimun and maximun datetimes
min_time = date_zero - margin
max_time = date_zero + margin + margin

# Check that there is data for the stablished window
min_data_avail = datetime.datetime.strptime(readme['Min_Date'], '%Y-%m-%d %H:%M:%S')
max_data_avail = datetime.datetime.strptime(readme['Max_Date'], '%Y-%m-%d %H:%M:%S')

# #Check date_zero has data
if max_time > max_data_avail:
    max_time = max_data_avail
    date_zero = max_time - margin - margin
    min_time = date_zero - margin

print(ident + "Getting information for {} with {} agglomeration between {} and {}".format(location_name, location_folder, min_time, max_time))

with open(output_readme_file, 'w') as f:
    f.write("min_date: {}\n".format(min_time))
    f.write("max_date: {}\n".format(max_time))
    f.write("date_zero: {}\n".format(date_zero))




def calculate_delta(t_0, t_1):
    df_delta = (t_1.sub(t_0, axis='columns')).divide(t_0, axis='columns').fillna(0)
    df_delta = df_delta.rename(columns = {'external_movement':'delta_external_movement',
                                                            'external_num_cases':'delta_external_num_cases',
                                                            'inner_movement': 'delta_inner_movement',
                                                            'num_cases': 'delta_num_cases'})

    return df_delta

def get_window_information(date_zero, min_time, max_time):

    # Get windows
    df_backward_window = df_cases.loc[(df_cases['date_time'] < date_zero) & (df_cases['date_time'] >= min_time)].copy()
    df_middle_window = df_cases.loc[(df_cases['date_time'] >= date_zero) & (df_cases['date_time'] < max_time - margin)].copy()
    df_forward_window = df_cases.loc[(df_cases['date_time'] >= date_zero + margin) & (df_cases['date_time'] <= max_time)].copy()
    df_total_window = df_cases.loc[(df_cases['date_time'] > min_time) & (df_cases['date_time'] <= max_time)].copy()

    # Add neighbor_cases_average backward window
    df_backward_window["external_num_cases"] = df_backward_window.apply(lambda x: get_neighbor_cases_total(x.poly_id, x.date_time, df_movement, df_cases), axis=1)
    df_backward_window =  df_backward_window.groupby("poly_id")[["num_cases", "external_num_cases"]].mean()

    # Add neighbor_cases_average middle window
    df_middle_window["external_num_cases"] = df_middle_window.apply(lambda x: get_neighbor_cases_total(x.poly_id, x.date_time, df_movement, df_cases), axis=1)
    df_middle_window =  df_middle_window.groupby("poly_id")[["num_cases", "external_num_cases"]].mean()
    
    # Add neighbor_cases_average forward window
    df_forward_window["external_num_cases"] = df_forward_window.apply(lambda x: get_neighbor_cases_total(x.poly_id, x.date_time, df_movement, df_cases), axis=1)
    df_forward_window =  df_forward_window.groupby("poly_id")[["num_cases", "external_num_cases"]].mean()
    
    # Add neighbor_cases_average total window
    df_total_window["external_num_cases"] = df_total_window.apply(lambda x: get_neighbor_cases_total(x.poly_id, x.date_time, df_movement, df_cases), axis=1)
    df_total_window =  df_total_window.groupby("poly_id")[["num_cases", "external_num_cases"]].mean()
    
    return [df_backward_window, df_middle_window, df_forward_window, df_total_window]

def get_window_deltas(df_backward_window, df_middle_window, df_forward_window, df_total_window):
    ############# Calculate deltas ###############
    ##############################################

    # ((t_0 - t_-1) / t_-1)
    df_delta_backward = (df_middle_window.sub(df_backward_window, axis='columns')).divide(df_backward_window, axis='columns').fillna(0)
    df_delta_backward = df_delta_backward.rename(columns = {'external_num_cases':'delta_external_num_cases',
                                                            'num_cases': 'delta_num_cases'})

    # ((t_1 - t_0) / t_0)
    df_delta_forward = (df_forward_window.sub(df_middle_window, axis='columns')).divide(df_middle_window, axis='columns').fillna(0)
    df_delta_forward = df_delta_forward.rename(columns = {'external_num_cases':'delta_external_num_cases',
                                                            'num_cases': 'delta_num_cases'})
    
    # ((t_1 - t_-1) / t_-1)
    df_delta_total = (df_forward_window.sub(df_backward_window, axis='columns')).divide(df_backward_window, axis='columns').fillna(0)
    df_delta_total = df_delta_total.rename(columns = {'external_num_cases':'delta_external_num_cases',
                                                            'num_cases': 'delta_num_cases'})

    return [df_delta_backward, df_delta_forward, df_delta_total]

windows = get_window_information(date_zero, min_time, max_time)
deltas = get_window_deltas(windows[0], windows[1], windows[2], windows[3])

# # Write to file
deltas[0].to_csv(output_backward_delta_file)
deltas[1].to_csv(output_forward_delta_file)
deltas[2].to_csv(output_total_delta_file)

windows[0].to_csv(output_backward_file)
windows[2].to_csv(output_forward_file)
windows[3].to_csv(output_total_file)
