'''
This script receives (1) location_name (2) polygon_name (3) time-window size (4) time unit and (5) optionally a date to center window around (t_0).
If no date is specified the date of the time the script is run will be used as t_0. 

Two files are produced (in .csv format): one describes the time-window going back from t_0 
and the other the time-window going forward and including from t_0. They both have the following information:

The script calculates for each node of the given agglomeration.
    1. num_cases: number of cases in the node averaged over the time window
    2. inner_movement: internal movement in the node averaged over the time window
    3. external_movement: external movement between the node and its imediate neighbors over the time window
    4. external_num_cases: number of cases in the node's imediate neighbors over the time window
    5. delta_num_cases: the percentage increase of cases in the node 
    6. delta_inner_movement: the percentage increase of movement in the node 
    7. delta_external_cases: the percentage increase of cases in the neighboring nodes
    8. delta_external_movement: the percentage increase of movement between the node and its imediate neighbors

    ** Deltas are calculated as follows:
    backward-window: ((t_0 - t_-1) / t_-1)
    forward_window: ((t_1 - t_0) / t_0)
'''

import os
import sys
import datetime
import numpy as np
import pandas as pd
from datetime import date

from pipeline_scripts.functions.general_functions import load_README

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
constructed_file_path = os.path.join(data_dir, 'data_stages', location_name, 'constructed', location_folder, 'daily_graphs')
output_file_path = os.path.join(analysis_dir, location_name, location_folder, 'polygon_info_window')
edges = os.path.join(constructed_file_path, 'edges.csv')

if len(sys.argv) < 6:
    date_zero = None
    output_backward_file = os.path.join(output_file_path, 'polygon_info_backward_window_{}{}.csv'.format(window_size_parameter,time_unit))
    output_forward_file = os.path.join(output_file_path,'polygon_info_forward_window_{}{}.csv'.format(window_size_parameter,time_unit))
    output_forward_file = os.path.join(output_file_path,'polygon_info_forward_window_{}{}.csv'.format(window_size_parameter,time_unit))
    output_total_file = os.path.join(output_file_path,'polygon_info_total_window_{}{}.csv'.format(window_size_parameter,time_unit))
    output_readme_file = os.path.join(output_file_path,'polygon_info_readme_{}{}.txt'.format(window_size_parameter,time_unit))
    nodes = os.path.join(constructed_file_path, 'nodes.csv')
else:
    date_zero = sys.argv[5] # time around which to calculate window
    output_backward_file = os.path.join(output_file_path,'polygon_info_backward_window_{}{}-{}.csv'.format(window_size_parameter,time_unit,date_zero))
    output_forward_file = os.path.join(output_file_path,'polygon_info_forward_window_{}{}-{}.csv'.format(window_size_parameter,time_unit,date_zero))
    output_forward_file = os.path.join(output_file_path,'polygon_info_forward_window_{}{}-{}.csv'.format(window_size_parameter,time_unit,date_zero))
    output_total_file = os.path.join(output_file_path,'polygon_info_total_window_{}{}-{}.csv'.format(window_size_parameter,time_unit,date_zero))
    output_readme_file = os.path.join(output_file_path,'polygon_info_readme_{}{}-{}.txt'.format(window_size_parameter,time_unit,date_zero))
    nodes = os.path.join(constructed_file_path, 'nodes.csv')

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

# Get nodes and edges
df_nodes = pd.read_csv(nodes, parse_dates=['date_time'])
df_edges = pd.read_csv(edges, parse_dates=['date_time'])

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

def get_mean_external_movement(node_id, time):
    df_neighbors = df_edges.loc[(df_edges['start_id'] == node_id) | (df_edges['end_id'] == node_id)]
    if df_neighbors.dropna().empty:
        return 0
    else:
        df_neighbors = df_neighbors.loc[df_neighbors['date_time'] == time]
        if df_neighbors.dropna().empty:
            return 0
        else:
            return df_neighbors.mean()['movement']

# Returns a list of the neighbors of a given node at a given time
def get_neighbors(node_id, date_time):
    df_neighbors = df_edges.loc[(df_edges['start_id'] == node_id) | (df_edges['end_id'] == node_id)]
    if df_neighbors.dropna().empty:
        return None
    else:
        df_neighbors = df_neighbors.loc[df_neighbors['date_time'] == date_time]
        if df_neighbors.dropna().empty:
            return None
        else:
            neighbors = pd.unique(df_neighbors[['start_id', 'end_id']].values.ravel('K'))
            return neighbors[neighbors != node_id]

def get_neighbor_cases_average(neighbors, date_time):
    total = 0
    for node in neighbors:
        df_num_cases = df_nodes.loc[(df_nodes['node_id']==node) & (df_nodes['date_time']==date_time)].reset_index()
        total += df_num_cases.iloc[0]['num_cases']
    return total / len(neighbors)

def get_neighbors_cases_average(node_id, date_time):
    neighbors = get_neighbors(node_id, date_time)
    if neighbors is not None:
        return get_neighbor_cases_average(neighbors, date_time)

def get_sinlge_window(min_time, max_time):
    df_window = df_nodes.loc[(df_nodes['date_time'] >= min_time) & (df_nodes['date_time'] < max_time)]

    # Calculate average number of cases and inner movement
    df_window_avg = df_window.groupby(['node_id']).mean().drop(['day', 'population'], axis=1).reset_index()

    # Add neighbor_cases_average backward window
    df_window_avg_info = pd.DataFrame()
    df_window_avg_info['node_id'] = df_window['node_id']
    df_window_avg_info['external_movement'] = df_window.apply(lambda x: get_mean_external_movement(x.node_id, x.date_time), axis=1).fillna(0)
    df_window_avg_info['external_num_cases'] = df_window.apply(lambda x: get_neighbors_cases_average(x.node_id, x.date_time), axis=1).fillna(0)
    df_window_avg_info = df_window_avg_info.groupby(['node_id']).mean().reset_index()
    df_final = df_window_avg_info.merge(df_window_avg, on=['node_id'], how='outer')
    df_final = df_final.set_index(df_final['node_id']).drop(['node_id'], axis=1)

    return df_final

def calculate_delta(t_0, t_1):
    df_delta = (t_1.sub(t_0, axis='columns')).divide(t_0, axis='columns').fillna(0)
    df_delta = df_delta.rename(columns = {'external_movement':'delta_external_movement',
                                                            'external_num_cases':'delta_external_num_cases',
                                                            'inner_movement': 'delta_inner_movement',
                                                            'num_cases': 'delta_num_cases'})

    return df_delta

def get_window_information(date_zero, min_time, max_time):

    # Get windows
    df_backward_window = df_nodes.loc[(df_nodes['date_time'] < date_zero) & (df_nodes['date_time'] >= min_time)]
    df_middle_window = df_nodes.loc[(df_nodes['date_time'] >= date_zero) & (df_nodes['date_time'] < max_time - margin)]
    df_forward_window = df_nodes.loc[(df_nodes['date_time'] >= date_zero + margin) & (df_nodes['date_time'] <= max_time)]
    df_total_window = df_nodes.loc[(df_nodes['date_time'] > min_time) & (df_nodes['date_time'] <= max_time)]

    # Calculate average number of cases and inner movement 
    df_cases_avg_backward = df_backward_window.groupby(['node_id']).mean().drop(['day', 'population'], axis=1).reset_index()
    df_cases_avg_middle = df_middle_window.groupby(['node_id']).mean().drop(['day', 'population'], axis=1).reset_index()
    df_cases_avg_forward = df_forward_window.groupby(['node_id']).mean().drop(['day', 'population'], axis=1).reset_index()
    df_cases_avg_total = df_total_window.groupby(['node_id']).mean().drop(['day', 'population'], axis=1).reset_index()

    # Add neighbor_cases_average backward window
    df_backward_window_info = pd.DataFrame()
    df_backward_window_info['node_id'] = df_backward_window['node_id']
    df_backward_window_info['external_movement'] = df_backward_window.apply(lambda x: get_mean_external_movement(x.node_id, x.date_time), axis=1).fillna(0)
    df_backward_window_info['external_num_cases'] = df_backward_window.apply(lambda x: get_neighbors_cases_average(x.node_id, x.date_time), axis=1).fillna(0)
    df_backward_window_info = df_backward_window_info.groupby(['node_id']).mean().reset_index()
    df_final_backward = df_backward_window_info.merge(df_cases_avg_backward, on=['node_id'], how='outer')
    df_final_backward = df_final_backward.set_index(df_final_backward['node_id']).drop(['node_id'], axis=1)

    # Add neighbor_cases_average forward window
    df_forward_window_info = pd.DataFrame()
    df_forward_window_info['node_id'] = df_forward_window['node_id']
    df_forward_window_info['external_movement'] = df_forward_window.apply(lambda x: get_mean_external_movement(x.node_id, x.date_time), axis=1).fillna(0)
    df_forward_window_info['external_num_cases'] = df_forward_window.apply(lambda x: get_neighbors_cases_average(x.node_id, x.date_time), axis=1).fillna(0)
    df_forward_window_info = df_forward_window_info.groupby(['node_id']).mean().reset_index()
    df_final_forward = df_forward_window_info.merge(df_cases_avg_forward, on=['node_id'], how='outer')
    df_final_forward = df_final_forward.set_index(df_final_forward['node_id']).drop(['node_id'], axis=1)

    # Add neighbor_cases_average to middle window
    df_middle_window_info = pd.DataFrame()
    df_middle_window_info['node_id'] = df_middle_window['node_id']
    df_middle_window_info['external_movement'] = df_middle_window.apply(lambda x: get_mean_external_movement(x.node_id, x.date_time), axis=1).fillna(0)
    df_middle_window_info['external_num_cases'] = df_middle_window.apply(lambda x: get_neighbors_cases_average(x.node_id, x.date_time), axis=1).fillna(0)
    df_middle_window_info = df_middle_window_info.groupby(['node_id']).mean().reset_index()
    df_final_middle = df_middle_window_info.merge(df_cases_avg_middle, on=['node_id'], how='outer')
    df_final_middle = df_final_middle.set_index(df_final_middle['node_id']).drop(['node_id'], axis=1)

    # Add neighbor_cases_average to total window
    df_total_window_info = pd.DataFrame()
    df_total_window_info['node_id'] = df_total_window['node_id']
    df_total_window_info['external_movement'] = df_total_window.apply(lambda x: get_mean_external_movement(x.node_id, x.date_time), axis=1).fillna(0)
    df_total_window_info['external_num_cases'] = df_total_window.apply(lambda x: get_neighbors_cases_average(x.node_id, x.date_time), axis=1).fillna(0)
    df_total_window_info = df_total_window_info.groupby(['node_id']).mean().reset_index()
    df_final_total = df_total_window_info.merge(df_cases_avg_total, on=['node_id'], how='outer')
    df_final_total = df_final_total.set_index(df_final_total['node_id']).drop(['node_id'], axis=1)

    ############# Calculate deltas ###############
    ##############################################

    # ((t_0 - t_-1) / t_-1)
    df_delta_backward = (df_final_middle.sub(df_final_backward, axis='columns')).divide(df_final_backward, axis='columns').fillna(0)
    df_delta_backward = df_delta_backward.rename(columns = {'external_movement':'delta_external_movement',
                                                            'external_num_cases':'delta_external_num_cases',
                                                            'inner_movement': 'delta_inner_movement',
                                                            'num_cases': 'delta_num_cases'})

    # ((t_1 - t_0) / t_0)
    df_delta_forward = (df_final_forward.sub(df_final_middle, axis='columns')).divide(df_final_middle, axis='columns').fillna(0)
    df_delta_forward = df_delta_forward.rename(columns = {'external_movement':'delta_external_movement',
                                                            'external_num_cases':'delta_external_num_cases',
                                                            'inner_movement': 'delta_inner_movement',
                                                            'num_cases': 'delta_num_cases'})

    # ((t_1 - t_-1) / t_-1)
    df_delta_total = (df_final_forward.sub(df_final_backward, axis='columns')).divide(df_final_backward, axis='columns').fillna(0)
    df_delta_total = df_delta_total.rename(columns = {'external_movement':'delta_external_movement',
                                                            'external_num_cases':'delta_external_num_cases',
                                                            'inner_movement': 'delta_inner_movement',
                                                            'num_cases': 'delta_num_cases'})

    # Add deltas
    df_final_forward = df_final_forward.merge(df_delta_forward, left_index=True, right_index=True, how='outer').replace([np.inf, -np.inf], np.nan)
    df_final_backward = df_final_backward.merge(df_delta_backward, left_index=True, right_index=True, how='outer').replace([np.inf, -np.inf], np.nan)
    df_final_total = df_final_total.merge(df_delta_total, left_index=True, right_index=True, how='outer').replace([np.inf, -np.inf], np.nan)

    return [df_final_backward, df_final_backward, df_final_total]

windows = get_window_information(date_zero, min_time, max_time)

# # Write to file
windows[0].to_csv(output_backward_file)
windows[1].to_csv(output_forward_file)
windows[2].to_csv(output_total_file)