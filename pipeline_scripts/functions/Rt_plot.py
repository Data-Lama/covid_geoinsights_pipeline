from matplotlib.dates import date2num, num2date
from matplotlib import dates as mdates
from matplotlib import ticker
from matplotlib.colors import ListedColormap
from matplotlib.patches import Patch
from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
from scipy import stats as sps
from scipy.interpolate import interp1d
from pipeline_scripts.functions.Rt_estimate import estimate_rt

def plot_cases_rt(cases_df, col_cases, col_cases_smoothed, pop=None, CI=50, key_df='date', min_time=pd.to_datetime('2020-02-26'), state=None, path_to_save=None):
    fig, ax = plt.subplots(2,1, figsize=(12.5, 10) )
    index = cases_df[col_cases].index.get_level_values( key_df )
    if pop: 
        values_cases    = cases_df[col_cases].values*100000/pop
        values_cases_sm = cases_df[col_cases_smoothed].values*100000/pop
    else: 
        values_cases    = cases_df[col_cases].values
        values_cases_sm = cases_df[col_cases_smoothed].values+1

    # Plot smoothed cases
    ax[0].bar(index, values_cases, color='k', alpha=0.3, zorder=1,  label= 'Casos')
    ax[0].plot(index, values_cases_sm, color='k', zorder=1,  label= 'Promedio movil semanal.')

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
        ax[0].set_ylabel('Incidencia por 100.000 hab.', fontsize=15 )
    ax[0].set_ylabel('Incidencia', fontsize=15 )
    
    ax[0].legend(fontsize=15, frameon=False)
    ax[0].spines['top'].set_visible(False)
    ax[0].spines['right'].set_visible(False)    

    max_cases_tick = values_cases.max()
    if 0<max_cases_tick<=10:
        tick_loc = 2
    elif 10<max_cases_tick<=50:
        tick_loc = 10
    elif 50<max_cases_tick<=100:
        tick_loc = 20       
    elif 100<max_cases_tick<=200:
        tick_loc = 40       
    elif 200<max_cases_tick<=1000:
        tick_loc = 150 
    elif 1000<max_cases_tick<=5000:
        tick_loc = 1000 
    elif 5000<max_cases_tick<=15000:
        tick_loc = 3000 
    elif 15000<max_cases_tick<=20000:
        tick_loc = 5000 
    elif 20000<max_cases_tick<=25000:
        tick_loc = 7000 
    elif 25000<max_cases_tick<=30000:
        tick_loc = 8000 
    elif 35000<max_cases_tick<=40000:
        tick_loc = 10000 
    else:    
        tick_loc = np.round( max_cases_tick/100+0.1*100//5 )  

    ax[0].yaxis.set_major_locator(ticker.MultipleLocator(tick_loc) )
    ax[0].yaxis.set_major_formatter(ticker.StrMethodFormatter("{x:.0f}"))
    #ax.yaxis.tick_right()
    ax[0].spines['left'].set_visible(False)
    ax[0].spines['bottom'].set_visible(False)
    ax[0].spines['right'].set_visible(False)
    ax[0].margins(0)
    ax[0].grid(which='major', axis='y', c='k', alpha=.1, zorder=-2)

    cases_df = cases_df.iloc[list(cases_df[col_cases_smoothed].cumsum()>1)]
    
    result = estimate_rt(cases_df, col_cases_smoothed, CI=CI)

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
    ax[1].set_ylabel(r'$R_t$', size=15)
    ax[0].set_title(state, size=15)
    # plt.show()

    if path_to_save:
        plt.tight_layout()
        fig.savefig(path_to_save, dpi=400)
        
    return (lowfn, highfn, result)
