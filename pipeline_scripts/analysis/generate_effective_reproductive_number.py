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

import sys

# Constants
indent = "\t"

# Direcotries
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')

# Reads the parameters from excecution
location_folder   =  sys.argv[1]    # location name
agglomeration_method =  sys.argv[2] # agglomeration method

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
agglomerated_folder_location = os.path.join(data_dir, 'data_stages', location_folder, 'agglomerated', agglomeration_method, "cases.csv")

# Get cases
df_cases = pd.read_csv(agglomerated_folder_location)

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

def plot_cases_rt(cases_df, col_cases, col_cases_smoothed , pop=None, CI=50, min_time=pd.to_datetime('2020-02-26'), state=None, path_to_save=None):
    fig, ax = plt.subplots(2,1, figsize=(12.5, 10) )


    index = cases_df[col_cases].index.get_level_values(FIS_KEY)
    if pop:
        values_cases    = cases_df[col_cases].values*100000/pop
        values_cases_sm = cases_df[col_cases_smoothed].values*100000/pop
    else: 
        values_cases    = cases_df[col_cases].values
        values_cases_sm = cases_df[col_cases_smoothed].values+1

    # Plot smoothed cases
    ax[0].bar(index, values_cases, color='k', alpha=0.3, zorder=1,  label= 'Cases')
    ax[0].plot(index, values_cases_sm, color='k', zorder=1,  label= '7 day rolling avg.')

    ax[0].tick_params(axis='both', labelsize=15)
    ax[1].tick_params(axis='both', labelsize=15)

    # Formatting
    ax[0].xaxis.set_major_locator(mdates.MonthLocator())
    ax[0].xaxis.set_major_formatter(mdates.DateFormatter('%b'))
    ax[0].xaxis.set_minor_locator(mdates.DayLocator())
    ax[0].xaxis.set_major_locator(mdates.WeekdayLocator())
    ax[0].xaxis.set_major_locator(mdates.MonthLocator())
    #ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    ax[0].set_xlim( min_time-pd.Timedelta( days=1 ), index[-1]+pd.Timedelta(days=1) )
    #ax.tick_params( axis='x',  rotation=90 )
    if pop:
        ax[0].set_ylabel('Incidence per. 100.000', fontsize=15 )
    ax[0].set_ylabel('Incidence', fontsize=15 )

    #ax.set_title( 'Amazonas', fontsize=15 )
    
    ax[0].legend(fontsize=15, frameon=False)
    ax[0].spines['top'].set_visible(False)
    ax[0].spines['right'].set_visible(False)

    ax[0].yaxis.set_major_locator(ticker.MultipleLocator( np.round(values_cases.max()/100+0.1*100//5 ) )  )
    ax[0].yaxis.set_major_formatter(ticker.StrMethodFormatter("{x:.0f}"))
    #ax.yaxis.tick_right()
    ax[0].spines['left'].set_visible(False)
    ax[0].spines['bottom'].set_visible(False)
    ax[0].spines['right'].set_visible(False)
    ax[0].margins(0)
    ax[0].grid(which='major', axis='y', c='k', alpha=.1, zorder=-2)


    cases_df = cases_df.iloc[list(cases_df[col_cases_smoothed].cumsum()>1)]
    posteriors, log_likelihood = get_posteriors(cases_df[col_cases_smoothed]+1, sigma=.25)
    posteriors = posteriors[posteriors.keys()[1:]]
    posteriors_cm  = posteriors.dropna(axis=1)


    # Note that this takes a while to execute - it's not the most efficient algorithm
    hdis = highest_density_interval(posteriors, p=CI/100)
    CI = str(CI) 
    most_likely = posteriors.idxmax().rename('ML')
    result = pd.concat([most_likely, hdis], axis=1)
    result = result.reset_index().rename( columns={'date_time': 'date', 'FIS': 'date'}).set_index('date')

    #min_time = cases_dpto_FIS.index[0] #pd.to_datetime('2020-03-01')
    ABOVE  = [1, 0, 0]
    MIDDLE = [1, 1, 1]
    BELOW  = [0, 0, 0]

    cmap = ListedColormap(np.r_[
        np.linspace(BELOW,MIDDLE,25),
        np.linspace(MIDDLE,ABOVE,25)
    ])
    color_mapped = lambda y: np.clip(y, .5, 1.5)-.5

    index = result['ML'].index.get_level_values('date')
    values = result['ML'].values

    # Plot dots and line
    ax[1].plot(index, values, c='k', zorder=1, alpha=.25)

    ax[1].scatter(index,
               values,
               s=40,
               lw=.5,
               c=cmap(color_mapped(values)),
               edgecolors='k', zorder=2)

    # Smooth CI by 1 day either side
    lowfn = interp1d(date2num(index),
                     result[f'Low_{CI}'].values,
                     bounds_error=False,
                     fill_value='extrapolate')

    highfn = interp1d(date2num(index),
                      result[f'High_{CI}'].values,
                      bounds_error=False,
                      fill_value='extrapolate')

    extended = pd.date_range(start=min_time,
                             end=index[-1]+pd.Timedelta(days=1))

    ax[1].fill_between(extended,
                    lowfn(date2num(extended)),
                    highfn(date2num(extended)),
                    color='k',
                    alpha=.3,
                    lw=0,
                    zorder=3)
    # Formatting
    ax[1].xaxis.set_major_locator(mdates.MonthLocator())
    ax[1].xaxis.set_major_formatter(mdates.DateFormatter('%b'))
    ax[1].xaxis.set_minor_locator(mdates.DayLocator())
    ax[1].xaxis.set_major_locator(mdates.WeekdayLocator())
    ax[1].xaxis.set_major_locator(mdates.MonthLocator())
    #ax1xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    ax[1].set_xlim( min_time-pd.Timedelta( days=1 ), index[-1]+pd.Timedelta(days=1) )
    ax[0].set_xlim( min_time-pd.Timedelta( days=1 ), index[-1]+pd.Timedelta(days=1) )
    #ax1tick_params( axis='x',  rotation=90 )
    #ax[1].set_ylabel('Incidence per. 100.000', fontsize=15 )
    #ax1set_title( 'Amazonas', fontsize=15 )
    #ax[1].legend(fontsize=15, frameon=False, title='Region', title_fontsize=15)
    ax[1].spines['top'].set_visible(False)
    ax[1].spines['right'].set_visible(False)

    ax[1].yaxis.set_major_locator(ticker.MultipleLocator(1))
    ax[1].set_ylim([-0.05,3.7])
    ax[1].yaxis.set_major_formatter(ticker.StrMethodFormatter("{x:.0f}"))
    #ax1yaxis.tick_right()
    ax[1].spines['left'].set_visible(False)
    ax[1].spines['bottom'].set_visible(False)
    ax[1].spines['right'].set_visible(False)
    ax[1].margins(0)
    ax[1].grid(which='major', axis='y', c='k', alpha=.1, zorder=-2)
    ax[1].set_ylabel(r'Rt', size=15)
    ax[0].set_title(state, size=15)
    # plt.show()

    if path_to_save:
        plt.tight_layout()
        fig.savefig(path_to_save, dpi=400)
        
    return (lowfn, highfn, result)

# Export folder location
export_folder_location = os.path.join(analysis_dir, location_folder, agglomeration_method, 'r_t', selected_polygons_folder_name)

# Check if folder exists
if not os.path.isdir(export_folder_location):
        os.makedirs(export_folder_location)

skipped_polygons = []
computed_polygons = []
if selected_polygons_boolean:
    #pdb.set_trace()
    df_all = df_cases.copy()
    print(indent + indent + f"Calculating individual polygon rt.")
    for idx, poly_id in enumerate( list( df_all['poly_id'].unique()) ):
        print(indent + indent + indent + f" {poly_id}.")
        computed_polygons.append(poly_id)
        df_poly_id = df_all[df_all['poly_id'] == poly_id ].copy()

        df_poly_id['date_time'] = pd.to_datetime( df_poly_id['date_time'] )
        df_poly_id = df_poly_id.groupby('date_time').sum()[['num_cases']]
        all_cases = df_poly_id['num_cases'].sum()
        if all_cases > 100:
            df_poly_id = df_poly_id.reset_index().set_index('date_time').resample('D').sum().fillna(0)

            df_poly_id = prepare_cases(df_poly_id, col='num_cases', cutoff=0)
            min_time = df_poly_id.index[0]
            FIS_KEY = 'date_time'
            path_to_save = os.path.join(export_folder_location, str(poly_id)+'_Rt.png')
            #pdb.set_trace()
            (_, _, result) = plot_cases_rt(df_poly_id, 'num_cases', 'Smoothed_num_cases' , pop=None, CI=50, min_time=min_time, state=None, path_to_save=path_to_save)
            result.to_csv(os.path.join(export_folder_location, str(poly_id)+'_Rt.csv'))
        else:
            skipped_polygons.append(poly_id)
            print('\nWARNING: for poly_id {} Rt was not computed...'.format(poly_id))
            print('\tpoly_id: {} has only {} cases must be greater than 100\n'.format(poly_id, all_cases))

df_all = df_cases.copy()
df_all['date_time'] = pd.to_datetime( df_all['date_time'] )
df_all = df_all.groupby('date_time').sum()[['num_cases']]
all_cases = df_all['num_cases'].sum()

print(indent + indent + f"Calculating aggregated rt.")
if all_cases > 100:
    df_all = df_all.reset_index().set_index('date_time').resample('D').sum().fillna(0)

    df_all = prepare_cases(df_all, col='num_cases', cutoff=0)
    min_time = df_all.index[0]
    FIS_KEY = 'date_time'

    path_to_save = os.path.join(export_folder_location, 'aggregated_Rt.png')
    (_, _, result) = plot_cases_rt(df_all, 'num_cases', 'Smoothed_num_cases' , pop=None, CI=50, min_time=min_time, state=None, path_to_save=path_to_save)
    result.to_csv(os.path.join(export_folder_location,'aggregated_Rt.csv'))

else:
    print('WARNING: for poly_id {} Rt was not computed...'.format(poly_id))

print(indent + indent + f"Writting computation stats for rt.")
with open(os.path.join(export_folder_location,'rt_computation_stats.txt'), "w") as fp:
    fp.write(f"computed polygons: {computed_polygons}\n")
    fp.write(f"skipped polygons: {skipped_polygons}\n")