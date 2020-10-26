import os
import sys
import asyncio
import numpy as np
import pymc3 as pm
import pandas as pd
import scipy.stats as stats    
from scipy.stats import gamma  
import pickle

# Local functions 
import pipeline_scripts.functions.Rt_estimate
from  pipeline_scripts.functions.Rt_estimate import calculate_threshold as mov_th

# Direcotries
from global_config import config

data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')

# Contants
DEFAULT_DELAY_DIST = 11001
MAX_DATE = pd.to_datetime("2020-10-20")

# Reads the parameters from excecution
location_name  =  sys.argv[1] # location name
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
agglomerated_path  = os.path.join(data_dir, "data_stages", location_name, "agglomerated", agglomeration_folder)
cases_path = os.path.join(agglomerated_path, 'cases.csv')
mov_range_path = os.path.join(agglomerated_path, 'movement_range.csv')
polygons_path = os.path.join(agglomerated_path, 'polygons.csv')

# TODO must generate cases_diag automatically !!! AND Agglomerate properly 
time_delay_path = os.path.join(data_dir, "data_stages", location_name, "unified", "cases_diag.csv")
df_time_delay = pd.read_csv(time_delay_path, parse_dates=["date_time"])
df_time_delay.rename(columns={"geo_id":"poly_id", "num_cases":"num_cases_diag"}, inplace=True)
df_cases_diag = df_time_delay[["date_time", "poly_id", "num_cases_diag"]]

# Load dataframes
df_mov_ranges = pd.read_csv(mov_range_path, parse_dates=["date_time"])
df_cases = pd.read_csv(cases_path, parse_dates=["date_time"])
df_polygons = pd.read_csv(polygons_path)

# Add polygon names
df_mov_ranges = df_mov_ranges.merge(df_polygons[["poly_name", "poly_id"]], on="poly_id", how="outer").dropna()

# Crop to max_date
df_mov_ranges = df_mov_ranges[df_mov_ranges["date_time"] <= MAX_DATE]
df_cases = df_cases[df_cases["date_time"] <= MAX_DATE]
df_cases_diag = df_cases_diag[df_cases_diag["date_time"] <= MAX_DATE]

if selected_polygons_boolean:
    df_mov_ranges = df_mov_ranges[df_mov_ranges["poly_id"].isin(selected_polygons)]
    df_cases = df_cases[df_cases["poly_id"].isin(selected_polygons)]
    df_polygons = df_polygons[df_polygons["poly_id"].isin(selected_polygons)]
    output_folder = os.path.join(analysis_dir, location_name, agglomeration_folder, "r_t", selected_polygon_name)
else:
    output_folder = os.path.join(analysis_dir, location_name, agglomeration_folder, "r_t", "entire_location")

# Check if folder exists
if not os.path.isdir(output_folder):
        os.makedirs(output_folder)

# Define functions for model
def confirmed_to_onset(confirmed, p_delay, col_name, min_onset_date=None):
    min_onset_date = pd.to_datetime(min_onset_date)
    # Reverse cases so that we convolve into the past
    convolved = np.convolve(np.squeeze(confirmed.iloc[::-1].values), p_delay)

    # Calculate the new date range
    dr = pd.date_range(end=confirmed.index[-1],
                        periods=len(convolved))
    # Flip the values and assign the date range
    onset = pd.Series(np.flip(convolved), index=dr, name=col_name)
    if min_onset_date:
        onset = np.round(onset.loc[min_onset_date:])
    else: 
        onset = np.round(onset.iloc[onset.values>=1])

    onset.index.name = 'date'
    return pd.DataFrame(onset)


######## this might work but CAREFULL
def adjust_onset_for_right_censorship(onset, p_delay, col_name='num_cases'):
    onset_df =  onset[col_name]
    cumulative_p_delay = p_delay.cumsum()
    
    # Calculate the additional ones needed so shapes match
    ones_needed = len(onset) - len(cumulative_p_delay)
    padding_shape = (0, ones_needed)
    
    # Add ones and flip back
    cumulative_p_delay = np.pad(
        cumulative_p_delay,
        padding_shape,
        constant_values=1)
    cumulative_p_delay = np.flip(cumulative_p_delay)
    
    # Adjusts observed onset values to expected terminal onset values
    onset[col_name+'_adjusted'] = onset_df / cumulative_p_delay
    
    return onset, cumulative_p_delay

# Smooths cases using a rolling window and gaussian sampling
def prepare_cases(daily_cases, col='num_cases', cutoff=0):
    daily_cases['smoothed_'+col] = daily_cases[col].rolling(7,
        win_type='gaussian',
        min_periods=1,
        center=True).mean(std=2).round()

    idx_start = np.searchsorted(daily_cases['smoothed_'+col], cutoff)
    daily_cases['smoothed_'+col] = daily_cases['smoothed_'+col].iloc[idx_start:]

    return daily_cases

def df_from_model(rt_trace):
    
    r_t = rt_trace
    mean = np.mean(r_t, axis=0)
    median = np.median(r_t, axis=0)
    hpd_90 = pm.stats.hpd(r_t, hdi_prob=.9)
    hpd_50 = pm.stats.hpd(r_t, hdi_prob=.5)
    

    df = pd.DataFrame(data=np.c_[mean, median, hpd_90, hpd_50],
                 columns=['mean', 'median', 'lower_90', 'upper_90', 'lower_50','upper_50'])
    return df


def estimate_mov_th(mobility_data, cases_onset_data, poly_id, path_to_save_trace):
    onset = cases_onset_data 
    mt = mobility_data

    with pm.Model() as Rt_mobility_model:            
        # Create the alpha and beta parameters
        # Assume a normal distribution
        beta  = pm.Uniform('beta', lower=-100, upper=100)
        Ro = pm.Uniform('R0', lower=2, upper=5)

        # The effective reproductive number is given by:
        Rt = pm.Deterministic('Rt', Ro*pm.math.exp(-beta*(1+mt[:-1].values)))
        serial_interval = pm.Gamma('serial_interval', alpha=6, beta=1.5)
        GAMMA = 1/serial_interval
        lam = onset[:-1].values * pm.math.exp( GAMMA * (Rt- 1))
        observed = onset.round().values[1:]

        # Likelihood
        cases = pm.Poisson('cases', mu=lam, observed=observed)

        with Rt_mobility_model:
            # Draw the specified number of samples
            N_SAMPLES = 10000

            # Using Metropolis Hastings Sampling
            step     = pm.Metropolis(vars=[ Rt_mobility_model.beta, Rt_mobility_model.R0 ], S = np.array([ (100+100)**2 , (5-2)**2 ]) )
            Rt_trace = pm.sample( N_SAMPLES, tune=1000, chains=20, step=step )

        BURN_IN = 2000
        rt_info = df_from_model(Rt_trace.get_values(burn=BURN_IN,varname='Rt'))

        R0_dist   = Rt_trace.get_values(burn=BURN_IN, varname='R0')
        beta_dist = Rt_trace.get_values(burn=BURN_IN,varname='beta')
        mb_th = mov_th(beta_dist.mean(), R0_dist.mean())

        if path_to_save_trace:
            with open(model_fpath, 'wb') as buff:
                pickle.dump({'model': model, 'trace': trace, 'X_shared': X_shared}, buff)
    return {'poly_id': poly_id, 'R0':R0_dist.mean(), 'beta':beta_dist.mean(), 'mob_th':mb_th }

    
# Get time delay
print(f"    Extracts time delay per polygon")
time_delays = {}
df_time_delay["attr_time_delay"] = df_time_delay.apply(lambda x: np.fromstring(x["attr_time-delay_union"], sep="|"), axis=1)
df_time_delay.set_index("poly_id", inplace=True)
for poly_id in df_time_delay.index.unique():
    # If the time delay distribution is not long enough, use bogota
    if len(df_time_delay.at[poly_id, "attr_time_delay"]) < 30:
        time_delays[poly_id] = df_time_delay.loc[[DEFAULT_DELAY_DIST], "attr_time_delay"].values[0]
    else:
        time_delays[poly_id] = df_time_delay.loc[[poly_id], "attr_time_delay"].values[0]

# Loop over polygons to run model and calculate thresholds
print(f"    Runs model and calculates mobility thresholds")

df_mov_thresholds = pd.DataFrame(columns =['poly_id', 'R0', 'Beta', 'mob_th'])
df_mov_thresholds['poly_id'] = list(df_mov_ranges.poly_id.unique())+['aggregated']
df_mov_thresholds = df_mov_thresholds.set_index('poly_id')

for poly_id in df_mov_ranges.poly_id.unique():

    df_mov_poly_id = df_mov_ranges[df_mov_ranges['poly_id'] == poly_id][["date_time", "poly_id", "movement_change"]].sort_values("date_time").copy()
    df_cases_diag_id = df_cases_diag[df_cases_diag["poly_id"] == poly_id][["date_time", "num_cases_diag"]]

    all_cases_id = df_cases_diag_id.num_cases_diag.sum()
    p_delay = time_delays[poly_id]

    path_to_save_tr = os.path.join(output_folder, str(poly_id) )

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
        mt = mt.loc[min_date:max_date]

        dict_result = estimate_mov_th(mt, onset+1, poly_id, os.path.join(path_to_save_tr, 'mob_th_trace.pymc3.pkl'))
            
        df_mov_thresholds.loc[dict_result['poly_id']]['R0']     = dict_result['R0']
        df_mov_thresholds.loc[dict_result['poly_id']]['Beta']   = dict_result['beta']
        df_mov_thresholds.loc[dict_result['poly_id']]['mob_th'] = -dict_result['mob_th']
    else:
        dict_result = {'poly_id': poly_id}
        df_mov_thresholds.loc[dict_result['poly_id']]['R0']     = np.nan
        df_mov_thresholds.loc[dict_result['poly_id']]['Beta']   = np.nan
        df_mov_thresholds.loc[dict_result['poly_id']]['mob_th'] = np.nan



df_mov_poly_id = df_mov_ranges[["date_time", "poly_id", "movement_change"]].sort_values("date_time").copy()
df_cases_diag_id = df_cases_diag[["date_time", "num_cases_diag"]]

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

    dict_result = estimate_mov_th(mt, onset, 'aggregated', os.path.join(path_to_save_tr, 'mob_th_trace.pymc3.pkl'))

    df_mov_thresholds.loc[dict_result['poly_id']]['R0']     = dict_result['R0']
    df_mov_thresholds.loc[dict_result['poly_id']]['Beta']   = dict_result['beta']
    df_mov_thresholds.loc[dict_result['poly_id']]['mob_th'] = -dict_result['mob_th']
else:
    dict_result = {'poly_id': poly_id}

    df_mov_thresholds.loc[dict_result['poly_id']]['R0']     = np.nan
    df_mov_thresholds.loc[dict_result['poly_id']]['Beta']   = np.nan
    df_mov_thresholds.loc[dict_result['poly_id']]['mob_th'] = np.nan

df_mov_thresholds.to_csv( os.path.join( output_folder ,'mobility_thresholds.csv'))
