from matplotlib.dates import date2num, num2date
from matplotlib import dates as mdates
from matplotlib import ticker
from matplotlib.colors import ListedColormap
from matplotlib.patches import Patch
from matplotlib import pyplot as plt
import pandas as pd
import numpy as np
import os
import pdb
import json
from scipy import stats as sps
from scipy.interpolate import interp1d
from pipeline_scripts.functions.Rt_estimate import get_posteriors, highest_density_interval
import pymc3 as pm
import sys

# Constants
indent = "\t"

# Direcotries
from global_config import config

data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')

# Reads the parameters from excecution
location_folder   =  sys.argv[1]    # location name  ### Colombia 
agglomeration_method =  sys.argv[2] # agglomeration method ### Colombia for example

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
df_cases = pd.read_csv( os.path.join( agglomerated_folder, 'cases.csv' ), parse_dates=['date_time'])
df_movement = pd.read_csv( os.path.join( agglomerated_folder, 'movement_range.csv' ), parse_dates=['date_time'])
df_movement['date_time'] = pd.to_datetime(df_movement['date_time'])

mov_df_plot   = df_movement[['poly_id', 'date_time', 'movement_change']]

## add time delta
df_polygons   = pd.read_csv(os.path.join(agglomerated_folder,  "polygons.csv"))
df_time_delay = pd.read_csv(os.path.join(data_dir, 'data_stages', location_folder, "unified", "cases_diag.csv"))
df_time_delay["attr_time-delay_union"] = df_time_delay.apply(lambda x: np.fromstring(x["attr_time-delay_union"], sep="|"), axis=1)
df_time_delay.set_index("geo_id", inplace=True)
df_polygons["attr_time_delay"] = df_polygons.apply(lambda x: list(df_time_delay.loc[x.poly_id]["attr_time-delay_union"])[0], axis=1)


if selected_polygons_boolean:
    print(indent + f"Calculating rt for {len(selected_polygons)} polygons in {selected_polygon_name}")
    # Set polygons to int
    selected_polygons = [int(x) for x in selected_polygons]
    selected_polygons_folder_name = selected_polygon_name
    df_cases = df_cases[df_cases["poly_id"].isin(selected_polygons)].copy()

else:
    print(indent + f"Calculating rt for {location_folder} entire location.")
    selected_polygons_folder_name = "entire_location"
    selected_polygons_boolean = True


def prepare_cases(daily_cases, col='Cases', cutoff=0):
    daily_cases['Smoothed_'+col] = daily_cases[col].rolling(7,
        win_type='gaussian',
        min_periods=1,
        center=True).mean(std=2).round()

    idx_start = np.searchsorted(daily_cases['Smoothed_'+col], cutoff)
    daily_cases['Smoothed_'+col] = daily_cases['Smoothed_'+col].iloc[idx_start:]

    return daily_cases

def confirmed_to_onset(confirmed, p_delay, min_onset_date=None):
    min_onset_date = pd.to_datetime(min_onset_date)
    # Reverse cases so that we convolve into the past
    convolved = np.convolve(np.squeeze(confirmed.iloc[::-1].values), p_delay)

    # Calculate the new date range
    dr = pd.date_range(end=confirmed.index[-1],
                        periods=len(convolved))
    # Flip the values and assign the date range
    onset = pd.Series(np.flip(convolved), index=dr, name='num_cases')
    if min_onset_date:
        onset = np.round(onset.loc[min_onset_date:])
    else: 
        onset = np.round(onset.iloc[onset.values>=1])

    onset.index.name = 'date_time'
    return pd.DataFrame(onset)

######## this might work but CAREFULL
def adjust_onset_for_right_censorship(onset, p_delay, col_name='Cases'):
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

# Export folder location
export_folder_location = os.path.join(analysis_dir, location_folder, agglomeration_method, 'r_t', selected_polygons_folder_name)

# Check if folder exists
if not os.path.isdir(export_folder_location):
        os.makedirs(export_folder_location)

skipped_polygons = []
computed_polygons = []
from tqdm import tqdm


if selected_polygons_boolean:
    #pdb.set_trace()
    df_all = df_cases.copy()
    df_all = df_time_delay[['date_time', 'location', 'num_cases']].copy().reset_index().rename(columns={'geo_id': 'poly_id'})
    df_polygons = df_polygons[['poly_id', 'attr_time_delay']].set_index('poly_id')
    df_polygons = df_polygons.dropna()
    #df_polygons[df_polygons==-1]=df_polygons.loc[11001].to_numpy()[0]

    print(indent + indent + f"Calculating individual polygon rt.")
    polys_not = []
    for idx, poly_id in tqdm( enumerate(list( df_all['poly_id'].unique()) )):

        print(indent + indent + indent + f" {poly_id}.", end="\r")
        computed_polygons.append(poly_id)
        df_poly_id_cases = df_all[df_all['poly_id'] == poly_id ].copy()
        df_mov_poly_id = mov_df_plot[mov_df_plot['poly_id'] == poly_id ].copy()
        df_mov_poly_id = df_mov_poly_id.groupby('date_time').mean()

        df_poly_id_cases['date_time'] = pd.to_datetime( df_poly_id_cases['date_time'] )
        df_poly_id_cases = df_poly_id_cases.groupby('date_time').sum()[['num_cases']]
        all_cases = df_poly_id_cases['num_cases'].sum()
        p_delay = df_polygons.loc[poly_id].to_numpy()[0]
        




        if p_delay.shape[0]<30:
            # if delay is not enough assume is like bogta delay
            p_delay = df_polygons.loc[11001].to_numpy()[0]
        if all_cases > 100:

            df_poly_id_cases = df_poly_id_cases.reset_index().set_index('date_time').resample('D').sum().fillna(0)
            df_poly_id_cases = confirmed_to_onset(df_poly_id_cases, p_delay, min_onset_date=None)

            min_date = np.maximum( df_mov_poly_id.index.values[0], df_poly_id_cases.index.values[0])
            max_date = np.minimum( df_mov_poly_id.index.values[-1], df_poly_id_cases.index.values[-1])

            df_onset_mcmc  = df_poly_id_cases.loc[min_date:max_date]['num_cases']
            df_mov_df_mcmc = df_mov_poly_id.loc[min_date:max_date]['movement_change']
            df_mcmc = pd.Series(df_poly_id_cases['num_cases'], name='num_cases')

            df_poly_id_cases = prepare_cases(df_poly_id_cases, col='num_cases', cutoff=0)

            onset = df_onset_mcmc
            mt_resampled = df_mov_df_mcmc.resample('1D').sum()
            mt           = mt_resampled.rolling(7).mean(std=2).fillna(0)
            mt[mt==0] = mt_resampled[mt==0] 
            mt  = mt.rolling(7).mean(std=2).fillna(0)
            mt[mt==0]     = mt_resampled[mt==0] 
            mt = (mt-mt.values.min())/( mt.values.max()-mt.values.min()  )

            with pm.Model() as Rt_mobility_model:
                
                # Create the alpha and beta parameters
                # Assume a uniform distribution
                Ro    = pm.Uniform('R0', lower=3, upper=6)
                beta  = pm.Uniform('beta', lower=-10, upper=10)

                serial_interval = pm.Gamma('serial_interval', alpha=6, beta=1.5)
                # rt = ro*exp(-beta*(1-mt))

                # The effective reproductive number is given by:
                Rt = pm.Deterministic('Rt', Ro*pm.math.exp(-beta*(1-mt[1:].values)))
                
                GAMMA = 1/serial_interval

                observed = onset.round().values[:-1] 
                expected_today = observed * pm.math.exp( GAMMA * (Rt-1) )
                expected_today = pm.math.maximum(1, expected_today)
                # Likelihood
                cases    = pm.Poisson('cases', mu=expected_today, observed=observed)

                with Rt_mobility_model:
                    # Draw the specified number of samples
                    N_SAMPLES = 10000
                    # Using Metropolis-Hastings Sampling 
                    step     = pm.Metropolis(vars=[Rt_mobility_model.beta, Rt_mobility_model.R0] , S = np.array([ (100+100)**2 , 9 ]) )
                    Rt_trace = pm.sample( N_SAMPLES, tune=1000, chains=40, step=step )

            min_time = df_poly_id_cases.index[0]
            FIS_KEY = 'date_time'

            #pdb.set_trace()
            plt.close()
        else:
            skipped_polygons.append(poly_id)
    print('\nWARNING: Rt was not computed for polygons: {}'.format(''.join([str(p)+', ' for p in skipped_polygons]) ))

df_all = df_cases.copy()
df_all['date_time'] = pd.to_datetime( df_all['date_time'] )
df_all    = df_all.groupby('date_time').sum()[['num_cases']]
all_cases = df_all['num_cases'].sum()

