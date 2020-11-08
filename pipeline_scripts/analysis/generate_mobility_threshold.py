import os
import sys
import asyncio
import numpy as np
import pymc3 as pm
import pandas as pd
import scipy.stats as stats    
from scipy.stats import gamma  
import pickle
import warnings

import logging
logger = logging.getLogger("pymc3")
logger.propagate = False
warnings.filterwarnings("ignore")

# Local functions 
from pipeline_scripts.functions.mobility_th_functions import calculate_threshold as mov_th
from pipeline_scripts.functions.mobility_th_functions import statistics_from_trace_model as df_from_model
from pipeline_scripts.functions.mobility_th_functions import mov_th_mcmcm_model as estimate_mov_th

from pipeline_scripts.functions.adjust_cases_observations_function import confirmed_to_onset, adjust_onset_for_right_censorship, prepare_cases 

# Directories
from global_config import config

data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')

# Contants
DEFAULT_DELAY_DIST = 11001

# Reads the parameters from excecution
location_name        =  'colombia'        # sys.argv[1] # location name
agglomeration_folder =  'community'  # sys.argv[2] # agglomeration folder


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

# Data paths
agglomerated_path  = os.path.join(data_dir, "data_stages", location_name, "agglomerated", agglomeration_folder)
cases_path = os.path.join(agglomerated_path, 'cases.csv')
mov_range_path = os.path.join(agglomerated_path, 'movement_range.csv')
polygons_path = os.path.join(agglomerated_path, 'polygons.csv')

time_delay_path = os.path.join(data_dir, "data_stages", location_name, "unified", "cases.csv")

df_time_delay = pd.read_csv(time_delay_path, parse_dates=["date_time"])
df_time_delay['attr_time-delay_union'] = df_time_delay.apply(lambda x:[float(v) for v in x['attr_time-delay_union'].split('|') ], axis=1)
df_time_delay.rename(columns={"geo_id":"poly_id", "num_cases":"num_cases_diag"}, inplace=True)
df_cases_diag = df_time_delay[["date_time", "poly_id", "num_cases_diag"]]

# Load dataframes
df_mov_ranges = pd.read_csv(mov_range_path, parse_dates=["date_time"])
df_cases = pd.read_csv(cases_path, parse_dates=["date_time"])
df_polygons = pd.read_csv(polygons_path)

# Add polygon names
df_mov_ranges = df_mov_ranges.merge(df_polygons[["poly_name", "poly_id"]], on="poly_id", how="outer").dropna()

if selected_polygons_boolean:
    df_mov_ranges = df_mov_ranges[df_mov_ranges["poly_id"].isin(selected_polygons)]
    df_cases = df_cases[ df_cases["poly_id"].isin(selected_polygons)]
    df_cases_diag = df_cases_diag[df_cases_diag["poly_id"].isin(selected_polygons)]
    df_polygons = df_polygons[df_polygons["poly_id"].isin(selected_polygons)]
    output_folder = os.path.join(analysis_dir, location_name, agglomeration_folder, "r_t", selected_polygon_name)
else:
    output_folder = os.path.join(analysis_dir, location_name, agglomeration_folder, "r_t", "entire_location")

# Check if folder exists
if not os.path.isdir(output_folder):
        os.makedirs(output_folder)

# Get time delay
print(f"    Extracts time delay per polygon")
time_delays = {}
df_time_delay["attr_time_delay"] = df_time_delay.apply(lambda x: np.fromstring(x["attr_time-delay_union"], sep="|"), axis=1)
df_time_delay.set_index("poly_id", inplace=True)

# Loop over polygons to run model and calculate thresholds
print(f"    Runs model and calculates mobility thresholds")

df_mov_thresholds            = pd.DataFrame(columns =['poly_id', 'R0', 'Beta', 'mob_th'])
df_mov_thresholds['poly_id'] = list(df_mov_ranges.poly_id.unique())+['aggregated']
df_mov_thresholds            = df_mov_thresholds.set_index('poly_id')

# Palmira: 76520
for poly_id in df_mov_ranges.poly_id.unique():

    df_mov_poly_id = df_mov_ranges[df_mov_ranges['poly_id'] == poly_id][["date_time", "poly_id", "movement_change"]].sort_values("date_time").copy()
    df_cases_diag_id = df_cases_diag[df_cases_diag["poly_id"] == poly_id][["date_time", "num_cases_diag"]]

    all_cases_id = df_cases_diag_id.num_cases_diag.sum()
    p_delay = time_delays[poly_id]

    path_to_save_tr = os.path.join(output_folder, 'MCMC', str(poly_id) )

    if all_cases_id > 100:

        # Check if folder exists
        if not os.path.isdir(path_to_save_tr):
                os.makedirs(path_to_save_tr)

        print(f"        Running model for {poly_id}")
        df_mov_poly_id.set_index("date_time", inplace=True)
        df_cases_diag_id.set_index("date_time", inplace=True)
        
        df_cases_diag_id = df_cases_diag_id.resample('1D').sum().fillna(0)
        df_cases_diag_id = confirmed_to_onset(df_cases_diag_id, p_delay, "num_cases_diag", min_onset_date=None)
        df_cases_diag_id = df_cases_diag_id.resample('1D').sum().fillna(0)

        min_date = max(min(df_mov_poly_id.index.values), min(df_cases_diag_id.index.values))
        max_date = min(max(df_mov_poly_id.index.values), max(df_cases_diag_id.index.values))

        df_onset_mcmc  = df_cases_diag_id.loc[min_date:max_date]['num_cases_diag']
        df_mov_df_mcmc = df_mov_poly_id.loc[min_date:max_date]['movement_change']
        df_mcmc = pd.Series(df_cases_diag_id['num_cases_diag'], name='num_cases_diag')

        # Smooths cases rolling window
        df_cases_diag_id = prepare_cases(df_cases_diag_id, col='num_cases_diag', cutoff=0)

        onset = df_onset_mcmc
        mt_resampled = df_mov_df_mcmc.resample('1D').sum()
        mt = mt_resampled.rolling(7).mean(std=2).fillna(0)
        mt[mt==0] = mt_resampled[mt==0] 
        mt = mt.rolling(7).mean(std=2).fillna(0)
        mt[mt==0] = mt_resampled[mt==0] 
        mt = (mt-mt.values.min())/(mt.values.max()-mt.values.min())

        min_date = max(min(mt.index.values), min(onset.index.values))
        max_date = min(max(mt.index.values), max(onset.index.values))
        
        onset = onset.loc[min_date:max_date]
        mt = mt.loc[min_date:max_date]

        dict_result = estimate_mov_th(mt, onset+1, poly_id, None)
            
        df_mov_thresholds.loc[dict_result['poly_id']]['R0']     = dict_result['R0']
        df_mov_thresholds.loc[dict_result['poly_id']]['Beta']   = dict_result['beta']
        df_mov_thresholds.loc[dict_result['poly_id']]['mob_th'] = -dict_result['mob_th']

    else:
        dict_result = {'poly_id': poly_id}
        df_mov_thresholds.loc[dict_result['poly_id']]['R0']     = np.nan
        df_mov_thresholds.loc[dict_result['poly_id']]['Beta']   = np.nan
        df_mov_thresholds.loc[dict_result['poly_id']]['mob_th'] = np.nan



df_mov_poly_id = df_mov_ranges[["date_time", "poly_id", "movement_change"]].sort_values("date_time").copy()
df_cases_diag_id = df_cases_diag[["date_time", "num_cases_diag"]].copy()
all_cases_id = df_cases_diag_id.num_cases_diag.sum()
p_delay = time_delays[poly_id]

if all_cases_id > 100:
    df_mov_poly_id.set_index("date_time", inplace=True)
    df_cases_diag_id.set_index("date_time", inplace=True)
    
    df_cases_diag_id = df_cases_diag_id.resample('D').sum().fillna(0)
    df_cases_diag_id = confirmed_to_onset(df_cases_diag_id, p_delay, "num_cases_diag", min_onset_date=None)

    min_date = max(min(df_mov_poly_id.index.values), min(df_cases_diag_id.index.values))
    max_date = min(max(df_mov_poly_id.index.values), max(df_cases_diag_id.index.values))

    df_onset_mcmc = df_cases_diag_id.loc[min_date:max_date]['num_cases_diag']
    df_mov_df_mcmc = df_mov_poly_id.loc[min_date:max_date]['movement_change']
    df_mcmc = pd.Series(df_cases_diag_id['num_cases_diag'], name='num_cases_diag')

    # Smooths cases rolilng window
    df_cases_diag_id = prepare_cases(df_cases_diag_id, col='num_cases_diag', cutoff=0)

    onset = df_onset_mcmc
    mt_resampled = df_mov_df_mcmc.resample('1D').sum()
    mt = mt_resampled.rolling(7).mean(std=2).fillna(0)
    mt[mt==0] = mt_resampled[mt==0] 
    mt = mt.rolling(7).mean(std=2).fillna(0)
    mt[mt==0] = mt_resampled[mt==0] 
    mt = (mt-mt.values.min())/(mt.values.max()-mt.values.min())

    min_date = max(min(mt.index.values), min(onset.index.values))
    max_date = min(max(mt.index.values), max(onset.index.values))
    
    onset = onset.loc[min_date:max_date]
    onset = onset.resample('1D').sum().fillna(0)
    mt = mt.loc[min_date:max_date]

    if not os.path.isdir(path_to_save_tr):
        os.makedirs(path_to_save_tr)

    dict_result = estimate_mov_th(mt, onset+1, 'aggregated', os.path.join(path_to_save_tr, 'mob_th_trace.pymc3.pkl'))

    df_mov_thresholds.loc[dict_result['poly_id']]['R0']     = dict_result['R0']
    df_mov_thresholds.loc[dict_result['poly_id']]['Beta']   = dict_result['beta']
    df_mov_thresholds.loc[dict_result['poly_id']]['mob_th'] = -dict_result['mob_th']
else:
    dict_result = {'poly_id': poly_id}

    df_mov_thresholds.loc[dict_result['poly_id']]['R0']     = np.nan
    df_mov_thresholds.loc[dict_result['poly_id']]['Beta']   = np.nan
    df_mov_thresholds.loc[dict_result['poly_id']]['mob_th'] = np.nan

df_mov_thresholds.to_csv( os.path.join( output_folder ,'mobility_thresholds.csv'))
