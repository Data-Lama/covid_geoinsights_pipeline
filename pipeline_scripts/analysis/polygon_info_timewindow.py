import os
import sys
import datetime
import pandas as pd

# Direcotries
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')

# Reads the parameters from excecution
location_name  =  sys.argv[1] # location name
location_folder =  sys.argv[2] # polygon name
date_zero = sys.argv[3] # time around which to calculate window
window_size_parameter = sys.argv[4] # window size
time_unit = sys.argv[5] # time unit for window [days, hours]

# Set window size to int
window_size = int(window_size_parameter)

# Units for margin
margins = { 'hours': datetime.timedelta(hours = window_size),
            'days': datetime.timedelta(days = window_size),
            'weeks': datetime.timedelta(weeks = window_size)
            }

# Date zero
date_zero = datetime.datetime.strptime(date_zero, '%Y-%m-%d')

# Get name of files
constructed_file_path = os.path.join(data_dir, 'data_stages', location_name, 'constructed', location_folder, 'daily_graphs')
output_file_path = os.path.join(analysis_dir, location_name, location_folder, 'polygon_info_window')
output_backward_file = os.path.join(output_file_path,'polygon_info_backward_window_{}{}.csv'.format(window_size_parameter,time_unit))
output_forward_file = os.path.join(output_file_path,'polygon_info_forward_window_{}{}.csv'.format(window_size_parameter,time_unit))
nodes = os.path.join(constructed_file_path, 'nodes.csv')
edges = os.path.join(constructed_file_path, 'edges.csv')

# Check if folder exists
if not os.path.isdir(output_file_path):
    os.makedirs(output_file_path)

# Get nodes and edges
df_nodes = pd.read_csv(nodes, parse_dates=['date_time'])
df_edges = pd.read_csv(edges, parse_dates=['date_time'])

# Get nodes within time window. Date zero will be counted with backward window.
margin = margins[time_unit]
df_backward_window = df_nodes.loc[(df_nodes['date_time'] <= date_zero) & (df_nodes['date_time'] >= date_zero - margin)]
df_forward_window = df_nodes.loc[(df_nodes['date_time'] > date_zero) & (df_nodes['date_time'] <= date_zero + margin)]

# Get minimun and maximun datetimes
min_time = df_backward_window['date_time'].min()
max_time = df_forward_window['date_time'].max()

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
        

# Calculate average number of cases and inner movement 
df_cases_avg_backward = df_backward_window.groupby(['node_id']).mean().drop(['day', 'population'], axis=1).reset_index()
df_cases_avg_forward = df_forward_window.groupby(['node_id']).mean().drop(['day', 'population'], axis=1).reset_index()

# Calculate num cases per node for min_time and9 max_time
min_time_cases = df_backward_window.loc[df_backward_window['date_time'] == min_time].drop(['day', 'population', 'inner_movement'], axis=1).set_index('node_id')
zero_time_cases = df_backward_window.loc[df_backward_window['date_time'] == date_zero].drop(['day', 'population', 'inner_movement'], axis=1).set_index('node_id')
max_time_cases = df_forward_window.loc[df_forward_window['date_time'] == max_time].drop(['day', 'population', 'inner_movement'], axis=1).set_index('node_id')

# Calculate deltas
# delta backward window ((t_0 - t_-1) / t_-1)
delta_backward = pd.DataFrame()
delta_backward = (zero_time_cases - min_time_cases).drop(['date_time'], axis=1).divide(min_time_cases.drop(['date_time'], axis=1), axis=1).fillna(0)
delta_backward = delta_backward.rename(columns={'num_cases':'delta_backward'})

# delta forward window ((t_1 - t_0) / t_0)
delta_forward = pd.DataFrame()
delta_forward = (max_time_cases - zero_time_cases).drop(['date_time'], axis=1).divide(zero_time_cases.drop(['date_time'], axis=1), axis=1).fillna(0)
delta_forward = delta_forward.rename(columns={'num_cases':'delta_forward'})

# delta total ((t_-1 - t_1) / t_-1)
delta_total = pd.DataFrame()
delta_total = (min_time_cases - max_time_cases).drop(['date_time'], axis=1).divide(min_time_cases.drop(['date_time'], axis=1), axis=1).fillna(0)
delta_total = delta_total.rename(columns={'num_cases':'delta_total'})

# delta = delta_total.merge(delta_forward, on=['node_id'], how='outer').merge(delta_backward, on=['node_id'], how='outer')

# Add neighbor_cases_average backward window
df_backward_window_info = pd.DataFrame()
df_backward_window_info['node_id'] = df_backward_window['node_id']
df_backward_window_info['external_movement'] = df_backward_window.apply(lambda x: get_mean_external_movement(x.node_id, x.date_time), axis=1).fillna(0)
df_backward_window_info['external_num_cases'] = df_backward_window.apply(lambda x: get_neighbors_cases_average(x.node_id, x.date_time), axis=1).fillna(0)
df_backward_window_info = df_backward_window_info.groupby(['node_id']).mean().reset_index()

# This is not working
df_final_backward = df_backward_window_info.merge(df_cases_avg_backward, on=['node_id'], how='outer')

# Add neighbor_cases_average forward window
df_forward_window_info = pd.DataFrame()
df_forward_window_info['node_id'] = df_forward_window['node_id']
df_forward_window_info['external_movement'] = df_forward_window.apply(lambda x: get_mean_external_movement(x.node_id, x.date_time), axis=1).fillna(0)
df_forward_window_info['external_num_cases'] = df_forward_window.apply(lambda x: get_neighbors_cases_average(x.node_id, x.date_time), axis=1).fillna(0)
df_forward_window_info = df_forward_window_info.groupby(['node_id']).mean().reset_index()

# This is not working
df_final_forward = df_forward_window_info.merge(df_cases_avg_forward, on=['node_id'], how='outer')

# Add deltas
df_final_forward = df_final_forward.merge(delta_forward, on=['node_id'], how='outer')
df_final_backward = df_final_backward.merge(delta_backward, on=['node_id'], how='outer')

# Write to file
df_final_backward.to_csv(output_backward_file, index=False)
df_final_forward.to_csv(output_forward_file, index=False)
