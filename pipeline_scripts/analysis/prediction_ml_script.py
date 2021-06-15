import pandas as pd
import numpy as np
import os
import shutil
import pdb
import json
import matplotlib.pyplot as plt
from pipeline_scripts.functions.adjust_cases_observations_function import prepare_cases, adjust_onset_for_right_censorship, confirmed_to_onset
from pipeline_scripts.functions.Rt_plot import plot_cases_rt
from tqdm import tqdm

import warnings
warnings.filterwarnings("ignore")

import sys

# Constants
indent = "\t"

T_future = 27 # 27 forecast days.

# Directories
from global_config import config

data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')

# Reads the parameters from excecution
location_folder      =  sys.argv[1]    # location name  ### Colombia
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
df_polygons = pd.read_csv( os.path.join( agglomerated_folder ,  "polygons.csv") )
df_polygons = df_polygons.dropna(subset=['attr_time-delay_dist_mix'], axis=0) 
df_polygons["attr_time_delay"] = df_polygons.apply(lambda x: np.fromstring(x["attr_time-delay_dist_mix"], sep="|"), axis=1)
df_polygons = df_polygons[df_polygons['attr_time_delay'].map(lambda x: len(x)) > 0]
df_polygons = df_polygons[df_polygons['attr_time_delay'].map(lambda x: ~np.isnan(list(x)[0] ))]
df_polygons = df_polygons[df_polygons['attr_time_delay'].map(lambda x: ~np.isinf(  x.sum() ))]


if selected_polygons_boolean:
    print(indent + f"Calculating rt for {len(selected_polygons)} polygons in {selected_polygon_name}")
    # Set polygons to int
    selected_polygons = [int(x) for x in selected_polygons]
    df_polygons = df_polygons[df_polygons["poly_id"].isin(selected_polygons)]
    selected_polygons_folder_name = selected_polygon_name
    df_cases = df_cases[df_cases["poly_id"].isin(selected_polygons)].copy()
else:

    print(indent + f"Calculating rt for {location_folder} entire location.")
    selected_polygons_folder_name = "entire_location"

# Export folder location
export_folder_location = os.path.join(analysis_dir, location_folder, agglomeration_method, 'r_t', selected_polygons_folder_name)

# Remove directory to recalculate everything in it
if os.path.isdir(export_folder_location):
    shutil.rmtree(export_folder_location)

# Check if folder exists
if not os.path.isdir(export_folder_location):
        os.makedirs(export_folder_location)

skipped_polygons = []
computed_polygons = []

# Get agg_time_delay
df_polygons_agg = df_polygons.copy()
df_polygons_agg = df_polygons_agg[df_polygons_agg['attr_time_delay'].map(lambda x: ~np.isinf(  x.sum() ))]
agg_p_delay = pd.DataFrame(list(df_polygons_agg['attr_time_delay'])).mean().to_numpy()

poly_id = 11001
for poly_id in df_cases.poly_id.unique():
    df_cases_poly_id = df_cases.copy().set_index('poly_id'); df_cases_poly_id = df_cases_poly_id.loc[poly_id]


    #### función que retorne forecast ####
    forecast_date_poly_id =
    df_ml_forecast = create_df_response(samples, time, date_init ='2020-03-06',  quantiles = [50, 80, 95], forecast_horizon=27, use_future=False)
    df_ml_forecast = df_ml_forecast[df_ml_forecast.type=='forecast']

    df_forecast = pd.DataFrame(columns=[ 'location', 'poly_id', 'horizon', 'temporal_res', 'target_variable', 'forecast_date', 'type', 'quantile', 'value'])
    df_forecast[]


def create_df_response(samples, time, date_init ='2020-03-06',  quantiles = [50, 80, 95], forecast_horizon=27, use_future=False):
    """[summary]

    Args:
        samples ([type]): [description]
        time ([type]): [description]
        date_init (str, optional): [description]. Defaults to '2020-03-06'.
        forecast_horizon (int, optional): [description]. Defaults to 27.
        use_future (bool, optional): [description]. Defaults to False.

    Returns:
        [type]: [description]
    """
    dates_fitted   = pd.date_range(start=pd.to_datetime(date_init), periods=time)
    dates_forecast = pd.date_range(start=dates_fitted[-1]+datetime.timedelta(1), periods=forecast_horizon)
    dates = list(dates_fitted)
    types = ['estimate']*len(dates_fitted)
    if use_future:
        dates += list(dates_forecast)
        types  += ['forecast']*len(dates_forecast)

    results_df = pd.DataFrame(samples.T)
    df_response = pd.DataFrame(index=dates)
    # Calculate key statistics
    df_response['mean']        = results_df.mean(axis=1).values
    df_response['median']      = results_df.median(axis=1).values
    df_response['std']         = results_df.std(axis=1).values

    for quant in quantiles:
        low_q  = ((100-quant)/2)/100
        high_q = 1-low_q

        df_response[f'low_{quant}']  = results_df.quantile(q=low_q, axis=1).values
        df_response[f'high_{quant}'] = results_df.quantile(q=high_q, axis=1).values

    df_response['type']        =  types
    df_response.index.name = 'date'
    return df_response
s