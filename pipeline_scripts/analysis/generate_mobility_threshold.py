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
logger = logging.getLogger('pymc3')
logger.setLevel(logging.ERROR)
logger.propagate = False
warnings.filterwarnings("ignore")
warnings.simplefilter("ignore")

# Local functions 
from pipeline_scripts.functions.mobility_th_functions import calculate_threshold as mob_th
from pipeline_scripts.functions.mobility_th_functions import statistics_from_trace_model as df_from_model
from pipeline_scripts.functions.mobility_th_functions import mob_th_mcmcm_model as estimate_mob_th
from pipeline_scripts.functions.adjust_cases_observations_function import confirmed_to_onset, adjust_onset_for_right_censorship, prepare_cases 

# Directories
from global_config import config

data_dir     = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')

# Contants
DEFAULT_DELAY_DIST = 11001

# Reads the parameters from excecution
location_name        =  sys.argv[1] # location name
agglomeration_folder =  sys.argv[2] # agglomeration folder

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
agglomerated_path = os.path.join(data_dir, "data_stages", location_name, "agglomerated", agglomeration_folder)
cases_path        = os.path.join(agglomerated_path, 'cases.csv')
mov_range_path    = os.path.join(agglomerated_path, 'movement_range.csv')
polygons_path     = os.path.join(agglomerated_path, 'polygons.csv')

# Load dataframes
df_mov_ranges = pd.read_csv(mov_range_path, parse_dates=["date_time"])
df_cases      = pd.read_csv(cases_path, parse_dates=["date_time"])
df_polygons   = pd.read_csv( polygons_path )
df_cases_diag = df_cases[["date_time", "poly_id", "num_cases"]]

# Loads time-delay
df_polygons = pd.read_csv( os.path.join( agglomerated_path ,  "polygons.csv") )
df_polygons = df_polygons.dropna(subset=['attr_time-delay_dist_mix'], axis=0) 
df_polygons["attr_time_delay"] = df_polygons.apply(lambda x: np.fromstring(x["attr_time-delay_dist_mix"], sep="|"), axis=1)
df_polygons = df_polygons[df_polygons['attr_time_delay'].map(lambda x: len(x)) > 0]
df_polygons = df_polygons[df_polygons['attr_time_delay'].map(lambda x: ~np.isnan(list(x)[0] ))]

df_time_delay = df_polygons[["poly_id", "attr_time_delay"]]

# Get agg_time_delay
df_polygons_agg = df_time_delay.copy()
df_polygons_agg = df_polygons_agg[df_polygons_agg['attr_time_delay'].map(lambda x: ~np.isinf(  x.sum() ))]
agg_p_delay = pd.DataFrame(list(df_polygons_agg['attr_time_delay'])).mean().to_numpy()


# Add polygon names
df_mov_ranges = df_mov_ranges.merge(df_polygons[["poly_name", "poly_id"]], on="poly_id", how="outer").dropna()

if selected_polygons_boolean:
    df_mov_ranges = df_mov_ranges[df_mov_ranges["poly_id"].isin(selected_polygons)]
    df_cases      = df_cases[ df_cases["poly_id"].isin(selected_polygons)]
    df_cases_diag = df_cases_diag[df_cases_diag["poly_id"].isin(selected_polygons)]
    df_polygons   = df_polygons[df_polygons["poly_id"].isin(selected_polygons)]
    output_folder = os.path.join(analysis_dir, location_name, agglomeration_folder, "mobility_threshold", selected_polygon_name)
else:
    output_folder = os.path.join(analysis_dir, location_name, agglomeration_folder, "mobility_threshold", "entire_location")

# Check if folder exists
if not os.path.isdir(output_folder):
        os.makedirs(output_folder)

# Loop over polygons to run model and calculate thresholds
print(f"    Runs model and calculates mobility thresholds")
df_mob_thresholds            = pd.DataFrame(columns =['poly_id', 'R0', 'Beta', 'mob_th'])
df_mob_thresholds['poly_id'] = list(df_mov_ranges.poly_id.unique())+['aggregated']
df_mob_thresholds            = df_mob_thresholds.set_index('poly_id')


# Get time delay
print(f"    Extracts time delay per polygon")
time_delays = {}
total = len(df_mov_ranges.poly_id.unique())
ite = 0
for poly_id in df_mov_ranges.poly_id.unique():
    
    ite += 1
    
    df_mov_poly_id   = df_mov_ranges[df_mov_ranges['poly_id'] == poly_id][["date_time", "poly_id", "movement_change"]].sort_values("date_time").copy()
    df_cases_diag_id = df_cases_diag[df_cases_diag["poly_id"] == poly_id][["date_time", "num_cases"]]

    all_cases_id = df_cases_diag_id['num_cases'].sum()

    try:
        p_delay      = df_time_delay.set_index("poly_id").at[poly_id, 'attr_time-delay_dist_mix']
        if p_delay.size == 0:
            p_delay      = agg_p_delay
        p_delay[0] = 0            
    except:
        p_delay      = agg_p_delay


    path_to_save_tr = os.path.join(output_folder, 'MCMC', str(poly_id) )

    if all_cases_id > 100:

        # Check if folder exists
        if not os.path.isdir(path_to_save_tr):
                os.makedirs(path_to_save_tr)

        print(f"        Running model for {poly_id} ({ite} of {total})")
        df_mov_poly_id.set_index("date_time", inplace=True)
        df_cases_diag_id.set_index("date_time", inplace=True)
        
        df_cases_diag_id = df_cases_diag_id.resample('1D').sum().fillna(0)
        df_cases_diag_id = confirmed_to_onset(df_cases_diag_id, p_delay, "num_cases", min_onset_date=None)
        df_cases_diag_id = df_cases_diag_id.resample('1D').sum().fillna(0)

        min_date = max(min(df_mov_poly_id.index.values), min(df_cases_diag_id.index.values))
        max_date = min(max(df_mov_poly_id.index.values), max(df_cases_diag_id.index.values))

        df_onset_mcmc  = df_cases_diag_id.loc[min_date:max_date]['num_cases']
        df_mov_df_mcmc = df_mov_poly_id.loc[min_date:max_date]['movement_change']
        df_mcmc        = pd.Series(df_cases_diag_id['num_cases'], name='num_cases')

        # Smooths cases rolling window
        df_cases_diag_id = prepare_cases(df_cases_diag_id, col='num_cases', cutoff=0)

        onset        = df_onset_mcmc
        mt_resampled = df_mov_df_mcmc.resample('1D').sum()
        mt           = mt_resampled.rolling(7).mean(std=2).fillna(0)
        mt[mt==0]    = mt_resampled[mt==0] 
        mt           = mt.rolling(7).mean(std=2).fillna(0)
        mt[mt==0]    = mt_resampled[mt==0]
        if mt.empty:
            dict_result = {'poly_id': poly_id}
            df_mob_thresholds.loc[dict_result['poly_id']]['R0']     = np.nan
            df_mob_thresholds.loc[dict_result['poly_id']]['Beta']   = np.nan
            df_mob_thresholds.loc[dict_result['poly_id']]['mob_th'] = np.nan
            continue
        mt           = (mt-mt.values.min())/(mt.values.max()-mt.values.min())

        min_date = max(min(mt.index.values), min(onset.index.values))
        max_date = min(max(mt.index.values), max(onset.index.values))
        
        onset = onset.loc[min_date:max_date]
        mt    = mt.loc[min_date:max_date]

        dict_result = estimate_mob_th(mt, onset+1, poly_id, None)
            
        df_mob_thresholds.loc[dict_result['poly_id']]['R0']     = dict_result['R0']
        df_mob_thresholds.loc[dict_result['poly_id']]['Beta']   = dict_result['beta']
        df_mob_thresholds.loc[dict_result['poly_id']]['mob_th'] = -dict_result['mob_th']

    else:
        dict_result = {'poly_id': poly_id}
        df_mob_thresholds.loc[dict_result['poly_id']]['R0']     = np.nan
        df_mob_thresholds.loc[dict_result['poly_id']]['Beta']   = np.nan
        df_mob_thresholds.loc[dict_result['poly_id']]['mob_th'] = np.nan

df_mov_poly_id   = df_mov_ranges[["date_time", "poly_id", "movement_change"]].sort_values("date_time").copy()
df_cases_diag_id = df_cases_diag[["date_time", "num_cases"]].copy()
all_cases_id     = df_cases_diag_id.num_cases.sum()

if all_cases_id > 100:
    df_mov_poly_id.set_index("date_time", inplace=True)
    df_cases_diag_id.set_index("date_time", inplace=True)
    
    df_cases_diag_id = df_cases_diag_id.resample('D').sum().fillna(0)
    df_cases_diag_id = confirmed_to_onset(df_cases_diag_id, agg_p_delay, "num_cases", min_onset_date=None)

    min_date = max(min(df_mov_poly_id.index.values), min(df_cases_diag_id.index.values))
    max_date = min(max(df_mov_poly_id.index.values), max(df_cases_diag_id.index.values))

    df_onset_mcmc  = df_cases_diag_id.loc[min_date:max_date]['num_cases']
    df_mov_df_mcmc = df_mov_poly_id.loc[min_date:max_date]['movement_change']
    df_mcmc        = pd.Series(df_cases_diag_id['num_cases'], name='num_cases')

    # Smooths cases rolilng window
    df_cases_diag_id = prepare_cases(df_cases_diag_id, col='num_cases', cutoff=0)
    onset        = df_onset_mcmc
    mt_resampled = df_mov_df_mcmc.resample('1D').sum()
    mt           = mt_resampled.rolling(7).mean(std=2).fillna(0)
    mt[mt==0]    = mt_resampled[mt==0] 
    mt           = mt.rolling(7).mean(std=2).fillna(0)
    mt[mt==0]    = mt_resampled[mt==0] 
    
    if mt.empty:
        dict_result = {'poly_id': "aggregated"}
        df_mob_thresholds.loc[dict_result['poly_id']]['R0']     = np.nan
        df_mob_thresholds.loc[dict_result['poly_id']]['Beta']   = np.nan
        df_mob_thresholds.loc[dict_result['poly_id']]['mob_th'] = np.nan
    else:
        mt           = (mt-mt.values.min())/(mt.values.max()-mt.values.min())

        min_date = max(min(mt.index.values), min(onset.index.values))
        max_date = min(max(mt.index.values), max(onset.index.values))

        onset = onset.loc[min_date:max_date]
        onset = onset.resample('1D').sum().fillna(0)
        mt    = mt.loc[min_date:max_date]

        if not os.path.isdir(path_to_save_tr):
            os.makedirs(path_to_save_tr)

        dict_result = estimate_mob_th(mt, onset+1, 'aggregated', os.path.join(path_to_save_tr, 'mob_th_trace.pymc3.pkl'))
        df_mob_thresholds.loc[dict_result['poly_id']]['R0']     = dict_result['R0']
        df_mob_thresholds.loc[dict_result['poly_id']]['Beta']   = dict_result['beta']
        df_mob_thresholds.loc[dict_result['poly_id']]['mob_th'] = -dict_result['mob_th']
else:
    dict_result = {'poly_id': "aggregated"}
    df_mob_thresholds.loc[dict_result['poly_id']]['R0']     = np.nan
    df_mob_thresholds.loc[dict_result['poly_id']]['Beta']   = np.nan
    df_mob_thresholds.loc[dict_result['poly_id']]['mob_th'] = np.nan
    
print(f"    Writes thresholds to file.")
df_mob_thresholds.to_csv( os.path.join( output_folder ,'mobility_thresholds.csv'))