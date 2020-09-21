import os
import sys
import csv
import pandas as pd
import datetime

import general_functions as gf

# Direcotries
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')

# Constants
WINDOW = 15

# Reads the parameters from excecution
location_name  =  sys.argv[1] # location name
location_folder =  sys.argv[2] # polygon name

if len(sys.argv) <= 3:
        selected_polygons_boolean = False
else:
    selected_polygons_boolean = True
    selected_polygons = []
    i = 3
    while i < len(sys.argv):
        selected_polygons.append(sys.argv[i])
        i += 1
    polygon_name = selected_polygons.pop(0)

# Format
date_format = "%d/%m/%Y"

# Get files
agglomerated_file_path = os.path.join(data_dir, 'data_stages', location_name, 'agglomerated', location_folder)
movement = os.path.join(agglomerated_file_path, 'movement.csv')
cases = os.path.join(agglomerated_file_path, 'cases.csv')


df_movement = pd.read_csv(movement, low_memory=False, parse_dates=['date_time'])

#Load num_cases 
try:  
    df_cases = pd.read_csv(cases, low_memory=False, parse_dates=['date_time'])
except:
    df_cases = pd.read_csv(cases, low_memory=False, encoding = 'latin-1', parse_dates=['date_time'])


def get_nodes_with_cases(df):
    total = df.groupby('poly_id').sum()
    nodes_with_cases = total[total['num_cases'] > 0]
    nodes_with_cases.reset_index(inplace=True)
    nodes_with_cases = set(nodes_with_cases['poly_id'])

    return nodes_with_cases

# Returns dataframe with max and min num_cases and movement per node
def max_min_day_by_node(df):
    df_max = df.groupby('poly_id').max()
    df_max.drop(columns=['date_time', 'population', 'day'], inplace=True)
    df_max.rename(columns={'num_cases':'max_num_cases',
                            'inner_movement':'max_inner_movement'}, inplace=True)
    df_max.reset_index(inplace=True)
    
    df_min = df.groupby('poly_id').min()
    df_min.drop(columns=['date_time', 'population', 'day'], inplace=True)
    df_min.rename(columns={'num_cases':'min_num_cases',
                            'inner_movement':'min_inner_movement'}, inplace=True)
    df_min.reset_index(inplace=True)

    return df_max.merge(df_min, on='poly_id', how='outer')

def get_polygons_no_new_cases(df, window_size):
    today = datetime.datetime.today()
    x_days_ago = today - datetime.timedelta(days = window_size)
    df_window_for = df[df['date_time'] > x_days_ago].copy()
    df_window_for_sum = df_window_for.groupby('poly_id').sum()
    df_no_cases = df_window_for_sum[df_window_for_sum['num_cases'] == 0].copy()
    df_no_cases.reset_index(inplace=True)
    set_no_new_cases = set(df_no_cases['poly_id'])
    
    df_window_back = df[df['date_time'] < x_days_ago].copy()
    df_window_back_sum = df_window_back.groupby('poly_id').sum()
    df_prev_cases = df_window_back_sum[df_window_back_sum['num_cases'] > 0].copy()
    df_prev_cases.reset_index(inplace=True)
    set_previous_cases = set(df_prev_cases['poly_id'])
    
    return set_previous_cases.intersection(set_no_new_cases)

if selected_polygons_boolean:
    output_file_path = os.path.join(analysis_dir, location_name, location_folder, 'stats', polygon_name)
    df_cases = df_cases[df_cases["poly_id"].isin(selected_polygons)].reset_index()
    df_movement = df_movement[(df_movement["start_poly_id"].isin(selected_polygons) | df_movement["end_poly_id"].isin(selected_polygons))].reset_index()
else:
    output_file_path = os.path.join(analysis_dir, location_name, location_folder, 'stats', "entire_location")

# Check if folder exists
if not os.path.isdir(output_file_path):
    os.makedirs(output_file_path)

# Get inner and external movement 
df_inner_movement = df_movement.loc[df_movement["start_poly_id"] == df_movement["end_poly_id"]].copy()
df_inner_movement.reset_index(inplace=True)
df_inner_movement.drop(columns=["end_poly_id", "index"], inplace=True)
df_inner_movement.rename(columns={"start_poly_id":"poly_id"}, inplace=True)

df_external_movement = df_movement.loc[df_movement["start_poly_id"] != df_movement["end_poly_id"]].copy()
df_external_movement_tot = df_external_movement.groupby(["date_time", "start_poly_id"]).sum()
df_external_movement_tot.reset_index(inplace=True)
df_external_movement_tot.drop(columns="end_poly_id", inplace=True)
df_external_movement_tot.rename(columns={"start_poly_id":"poly_id"}, inplace=True)
df_external_movement_tot.reset_index(inplace=True)

# Get the information of the max and min historical points 
max_inner_mov = gf.get_max_min('movement', df_inner_movement)[0]
min_inner_mov = gf.get_max_min('movement', df_inner_movement)[1]
max_external_mov = gf.get_max_min('movement', df_external_movement_tot)[0]
min_external_mov = gf.get_max_min('movement', df_external_movement_tot)[1]
max_num_cases = gf.get_max_min('num_cases', df_cases)[0]
min_num_cases = gf.get_max_min('num_cases', df_cases)[1]

# Get the information of the day with max + min
max_inner_mov_day = gf.get_day_max_min('movement', df_inner_movement)[0]
min_inner_mov_day = gf.get_day_max_min('movement', df_inner_movement)[1]
max_external_mov_day = gf.get_day_max_min('movement', df_external_movement_tot)[0]
min_external_mov_day = gf.get_day_max_min('movement', df_external_movement_tot)[1]
max_num_cases_day = gf.get_day_max_min('num_cases', df_cases)[0]
min_num_cases_day = gf.get_day_max_min('num_cases', df_cases)[1]

no_new_case_polygon = get_polygons_no_new_cases(df_cases, WINDOW)

# Extracts number of oplygons without new cases
days_back = 15
last_days = df_cases[df_cases.date_time >= df_cases.date_time.max() - datetime.timedelta(days = days_back)]
total_last_days = last_days[['poly_id','num_cases']].groupby('poly_id').sum().reset_index()

no_case_polygons_last_days = int((total_last_days.num_cases == 0).sum())


stats = {
    'num_first_case':len(gf.new_cases(df_cases, WINDOW)),
    'no_case_polygons_last_days':len(no_new_case_polygon),
    'day_max_mov': max_inner_mov_day['date'].strftime(date_format),
    'day_min_mov': min_inner_mov_day['date'].strftime(date_format),
    'day_max_out_mov': max_external_mov_day['date'].strftime(date_format),
    'day_min_out_mov': min_external_mov_day['date'].strftime(date_format),
    'day_max_cases': max_num_cases_day['date'].strftime(date_format),
    'day_min_cases': min_num_cases_day['date'].strftime(date_format),
    'max_move_in_day': max_inner_mov_day['movement'],
    'min_move_in_day': min_inner_mov_day['movement'],
    'max_move_in_day': max_external_mov_day['movement'],
    'min_move_in_day': min_external_mov_day['movement'],
    'max_cases_in_day': max_num_cases_day['num_cases'],
    'min_cases_in_day': min_num_cases_day['num_cases'],
    
}

# General stats path
general_stats_path = os.path.join(output_file_path, 'general_stats.csv')
with open(general_stats_path, 'w') as out:
    out.write('parameter_name,parameter_value\n')
    for key in stats.keys():
        out.write('{},{}\n'.format(key, stats[key]))

# # Stats by node
# stats_by_node_path = os.path.join(output_file_path, 'stats_by_node.csv')
# stats_by_node.to_csv(stats_by_node_path)