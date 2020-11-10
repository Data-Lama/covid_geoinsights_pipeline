import pandas as pd
import numpy as np
import os
import pdb
import json
import matplotlib.pyplot as plt
from pipeline_scripts.functions.adjust_cases_observations_function import prepare_cases, adjust_onset_for_right_censorship, confirmed_to_onset
from pipeline_scripts.functions.Rt_plot     import plot_cases_rt

import warnings
warnings.filterwarnings("ignore")

import sys

# Constants
indent = "\t"

# Directories
from global_config import config

data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')

# Reads the parameters from excecution
location_folder   =  sys.argv[1]    # location name  ### Colombia 
agglomeration_method =  sys.argv[2] # agglomeration method ### geometry for example

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

# Agglomerated folder location
agglomerated_folder = os.path.join(data_dir, 'data_stages', location_folder, 'agglomerated', agglomeration_method )

# Get cases
df_cases = pd.read_csv( os.path.join( agglomerated_folder, 'cases.csv' ) )

## add time delta
df_polygons   = pd.read_csv( os.path.join( agglomerated_folder ,  "polygons.csv") )

df_time_delay = pd.read_csv( os.path.join( data_dir, 'data_stages', location_folder, "unified", "cases_diag.csv") )

df_time_delay["attr_time-delay_union"] = df_time_delay.apply(lambda x: np.fromstring(x["attr_time-delay_union"], sep="|"), axis=1)
df_time_delay.set_index("geo_id", inplace=True)
df_polygons["attr_time_delay"] = df_polygons.apply(lambda x: list(df_time_delay.loc[x.poly_id]["attr_time-delay_union"])[0], axis=1)

if selected_polygons_boolean:
    print(indent + f"Calculating rt for {len(selected_polygons)} polygons in {selected_polygon_name}")
    # Set polygons to int
    selected_polygons = [int(x) for x in selected_polygons]
    selected_polygons_folder_name = selected_polygon_name
    df_cases = df_cases[df_cases["poly_id"].isin(selected_polygons)].copy()
    df_time_delay = df_time_delay[df_time_delay.index.isin(selected_polygons)].copy()
else:
    print(indent + f"Calculating rt for {location_folder} entire location.")
    selected_polygons_folder_name = "entire_location"

# Export folder location
export_folder_location = os.path.join(analysis_dir, location_folder, agglomeration_method, 'r_t', selected_polygons_folder_name)

# Check if folder exists
if not os.path.isdir(export_folder_location):
        os.makedirs(export_folder_location)

skipped_polygons = []
computed_polygons = []
from tqdm import tqdm
if selected_polygons_boolean:
        
    df_all = df_time_delay[['date_time', 'location', 'num_cases']].copy().reset_index().rename(columns={'geo_id': 'poly_id'})
    df_polygons = df_polygons[['poly_id', 'attr_time_delay']].set_index('poly_id')
    df_polygons = df_polygons.dropna()
    #df_polygons[df_polygons==-1]=df_polygons.loc[11001].to_numpy()[0]

    print(indent + indent + f"Calculating individual polygon rt.")
    polys_not = []
    for idx, poly_id in tqdm( enumerate(list( df_all['poly_id'].unique()) )):
        ###### running just for capitals ######
        #if str(poly_id)[-3:]!='001':
        #    continue

        print(indent + indent + indent + f" {poly_id}.", end="\r")
        computed_polygons.append(poly_id)
        df_poly_id = df_all[df_all['poly_id'] == poly_id ].copy()

        df_poly_id['date_time'] = pd.to_datetime( df_poly_id['date_time'] )
        df_poly_id = df_poly_id.groupby('date_time').sum()[['num_cases']]
        all_cases = df_poly_id['num_cases'].sum()
        p_delay = df_polygons.loc[11001].to_numpy()[0]
        
        if p_delay.shape[0]<30:
            # if delay is not enough assume is like bogta delay
            p_delay = df_polygons.loc[11001].to_numpy()[0]

        if all_cases > 100:
            df_poly_id = df_poly_id.reset_index().set_index('date_time').resample('D').sum().fillna(0)
            df_poly_id = confirmed_to_onset(df_poly_id, p_delay, min_onset_date=None)

            df_poly_id = prepare_cases(df_poly_id, col='num_cases', cutoff=0)
            min_time = df_poly_id.index[0]
            FIS_KEY = 'date'
            path_to_save = os.path.join(export_folder_location, str(poly_id)+'_Rt.png')
            (_, _, result) = plot_cases_rt(df_poly_id, 'num_cases', 'smoothed_num_cases' , key_df=FIS_KEY, pop=None, CI=50, min_time=min_time, state=None, path_to_save=path_to_save)
            
            result.to_csv(os.path.join(export_folder_location, str(poly_id)+'_Rt.csv'))
            plt.close()
        else:
            skipped_polygons.append(poly_id)
    print('\nWARNING: Rt was not computed for polygons: {}'.format(''.join([str(p)+', ' for p in skipped_polygons]) ))


df_all = df_cases.copy()
df_all = df_time_delay[['date_time', 'location', 'num_cases']].copy().reset_index().rename(columns={'geo_id': 'poly_id'})
df_all['date_time'] = pd.to_datetime( df_all['date_time'] )
df_all    = df_all.groupby('date_time').sum()[['num_cases']]
all_cases = df_all['num_cases'].sum()

print(indent + indent + f"Calculating aggregated rt.")
if all_cases > 100:

    df_all = df_all.reset_index().set_index('date_time').resample('D').sum().fillna(0)
    df_polygons_agg = df_polygons.copy()
    p_delay = df_polygons_agg.reset_index().set_index('poly_id').loc[11001]['attr_time_delay']
    
    df_all = confirmed_to_onset(df_all, p_delay, min_onset_date=None)

    df_all, _ = adjust_onset_for_right_censorship(df_all, p_delay, col_name='num_cases')
    df_all['num_cases_adjusted'] = np.round(df_all['num_cases_adjusted'])

    df_all = df_all.iloc[:-10]

    df_all = prepare_cases(df_all, col='num_cases_adjusted', cutoff=0)
    min_time = df_all.index[0]
    FIS_KEY = 'date'

    path_to_save = os.path.join(export_folder_location, 'aggregated_Rt.png')
    df_all.iloc[-10:]['num_cases_adjusted'] = df_all.iloc[-10:]['smoothed_num_cases_adjusted']
    (_, _, result) = plot_cases_rt(df_all, 'num_cases_adjusted', 'num_cases_adjusted', key_df=FIS_KEY, pop=None, CI=50, min_time=min_time, state=None, path_to_save=path_to_save)
    result.to_csv(os.path.join(export_folder_location,'aggregated_Rt.csv'))
else:
    print('WARNING: for poly_id {} Rt was not computed...'.format(poly_id))

print(indent + indent + f"Writting computation stats for rt.")
with open(os.path.join(export_folder_location,'rt_computation_stats.txt'), "w") as fp:
    fp.write(f"computed polygons: {computed_polygons}\n")
    fp.write(f"skipped polygons: {skipped_polygons}\n")