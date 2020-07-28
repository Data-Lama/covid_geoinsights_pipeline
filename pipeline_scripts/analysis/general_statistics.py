import os
import sys
import csv
import pandas as pd
import datetime

# Direcotries
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')

# Constants
WINDOW = 15

# Reads the parameters from excecution
location_name  =  sys.argv[1] # location name
location_folder =  sys.argv[2] # polygon name


# Format
date_format = "%d/%m/%Y"

# Get files
constructed_file_path = os.path.join(data_dir, 'data_stages', location_name, 'constructed', location_folder, 'daily_graphs')
output_file_path = os.path.join(analysis_dir, location_name, location_folder, 'stats')
edges = os.path.join(constructed_file_path, 'edges.csv')
nodes = os.path.join(constructed_file_path, 'nodes.csv')

# Check if folder exists
if not os.path.isdir(output_file_path):
    os.makedirs(output_file_path)

# Load dfs
df_edges = pd.read_csv(edges, parse_dates=['date_time'])
df_nodes = pd.read_csv(nodes, parse_dates=['date_time'])


def get_max_min(variable, df):
    max_index = df[variable].idxmax()
    min_index = df[variable].idxmin()
    max_info = {'node_id':df.iloc[max_index]['node_id'],
                'date':df.iloc[max_index]['date_time'],
                variable:df.iloc[max_index][variable],
                'day':df.iloc[max_index]['day']}
    min_info = {'node_id':df.iloc[min_index]['node_id'],
            'date':df.iloc[min_index]['date_time'],
            variable:df.iloc[min_index][variable],
            'day':df.iloc[min_index]['day']}

    return (max_info, min_info)

def get_day_max_min(variable, df):
    df_byday = df.groupby('date_time').sum()
    df_byday.reset_index(inplace=True)
    max_index = df_byday[variable].idxmax()
    min_index = df_byday[variable].idxmin()
    max_info = {'date':df_byday.iloc[max_index]['date_time'],
                variable:df_byday.iloc[max_index][variable],
                'day':df_byday.iloc[max_index]['day']}
    min_info = {'date':df_byday.iloc[min_index]['date_time'],
            variable:df_byday.iloc[min_index][variable],
            'day':df_byday.iloc[min_index]['day']}
    return (max_info, min_info)

def get_nodes_with_cases(df):
    total = df.groupby('node_id').sum()
    nodes_with_cases = total[total['num_cases'] > 0]
    nodes_with_cases.reset_index(inplace=True)
    nodes_with_cases = set(nodes_with_cases['node_id'])

    return nodes_with_cases

# Returns dataframe with max and min num_cases and movement per node
def max_min_day_by_node(df):
    df_max = df.groupby('node_id').max()
    df_max.drop(columns=['date_time', 'population', 'day'], inplace=True)
    df_max.rename(columns={'num_cases':'max_num_cases',
                            'inner_movement':'max_inner_movement'}, inplace=True)
    df_max.reset_index(inplace=True)
    
    df_min = df.groupby('node_id').min()
    df_min.drop(columns=['date_time', 'population', 'day'], inplace=True)
    df_min.rename(columns={'num_cases':'min_num_cases',
                            'inner_movement':'min_inner_movement'}, inplace=True)
    df_min.reset_index(inplace=True)

    return df_max.merge(df_min, on='node_id', how='outer')

def get_polygons_no_new_cases(df, window_size):
    today = datetime.datetime.today()
    x_days_ago = today - datetime.timedelta(days = window_size)
    df_window_for = df[df['date_time'] > x_days_ago]
    df_window_for_sum = df_window_for.groupby('node_id').sum()
    df_no_cases = df_window_for_sum[df_window_for_sum['num_cases'] == 0]
    df_no_cases.reset_index(inplace=True)
    set_no_new_cases = set(df_no_cases['node_id'])
    
    df_window_back = df[df['date_time'] < x_days_ago]
    df_window_back_sum = df_window_back.groupby('node_id').sum()
    df_prev_cases = df_window_back_sum[df_window_back_sum['num_cases'] > 0]
    df_prev_cases.reset_index(inplace=True)
    set_previous_cases = set(df_prev_cases['node_id'])
    
    return set_previous_cases.intersection(set_no_new_cases)

# Get the information of the max and min historical points 
max_inner_mov = get_max_min('inner_movement', df_nodes)[0]
min_inner_mov = get_max_min('inner_movement', df_nodes)[1]
max_num_cases = get_max_min('num_cases', df_nodes)[0]
min_num_cases = get_max_min('num_cases', df_nodes)[1]

# Get the information of the day with max + min
max_inner_mov_day = get_day_max_min('inner_movement', df_nodes)[0]
min_inner_mov_day = get_day_max_min('inner_movement', df_nodes)[1]
max_num_cases_day = get_day_max_min('num_cases', df_nodes)[0]
min_num_cases_day = get_day_max_min('num_cases', df_nodes)[1]

<<<<<<< HEAD
# print(max_inner_mov)
# print(min_inner_mov_day)
# print(max_num_cases_day)
# print(min_num_cases_day)

# Get the number of polygons that reported having their first case in the last n days
num_days_first_case = 7
today = datetime.datetime.today()
num_days_ago = today - datetime.timedelta(days = num_days_first_case)
historic = df_nodes[df_nodes['date_time'] < num_days_ago]
=======
# Get the number of polygons that reported having their first case in the last 5 days
today = datetime.datetime.today()
x_days_ago = today - datetime.timedelta(days = WINDOW)
historic = df_nodes[df_nodes['date_time'] < x_days_ago]
>>>>>>> 2b2adfddf45bd1c66830e9c7d81cc14b31788a88
historic_set = get_nodes_with_cases(historic)
current_set = get_nodes_with_cases(df_nodes)
intersection = current_set.intersection(historic_set)

new_case_polygon = current_set - intersection
no_new_case_polygon = get_polygons_no_new_cases(df_nodes, WINDOW)
stats_by_node = max_min_day_by_node(df_nodes)


# Extraxts number of oplygons without new cases
days_back = 15
last_days = df_nodes[df_nodes.date_time >= df_nodes.date_time.max() - datetime.timedelta(days = days_back)]
total_last_days = last_days[['node_id','num_cases']].groupby('node_id').sum().reset_index()

no_case_polygons_last_days = int((total_last_days.num_cases == 0).sum())


stats = {
    'num_first_case':len(new_case_polygon),
<<<<<<< HEAD
    'no_case_polygons_last_days' : no_case_polygons_last_days,
    'day_max_mov': max_inner_mov_day['date'].strftime(date_format),
    'day_min_mov': min_inner_mov_day['date'].strftime(date_format),
    'day_max_cases': max_num_cases_day['date'].strftime(date_format),
    'day_min_cases': min_num_cases_day['date'].strftime(date_format),
=======
    'no_case_polygons_last_days':len(no_new_case_polygon),
    'day_max_mov': max_inner_mov_day['date'],
    'day_min_mov': min_inner_mov_day['date'],
    'day_max_cases': max_num_cases_day['date'],
    'day_min_cases': min_num_cases_day['date'],
>>>>>>> 2b2adfddf45bd1c66830e9c7d81cc14b31788a88
    'max_move_in_day': max_inner_mov_day['inner_movement'],
    'min_move_in_day': min_inner_mov_day['inner_movement'],
    'max_cases_in_day': max_num_cases_day['num_cases'],
    'min_cases_in_day': min_num_cases_day['num_cases'],
    
}

# General stats path
general_stats_path = os.path.join(output_file_path, 'general_stats.csv')
with open(general_stats_path, 'w') as out:
    out.write('parameter_name,parameter_value\n')
    for key in stats.keys():
        out.write('{},{}\n'.format(key, stats[key]))

# Stats by node
stats_by_node_path = os.path.join(output_file_path, 'stats_by_node.csv')
stats_by_node.to_csv(stats_by_node_path)