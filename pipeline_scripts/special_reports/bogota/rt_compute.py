import os
import sys
import math
import numpy as np
import pandas as pd
import networkx as nx
from shapely import wkt

import geopandas as gpd
import matplotlib.pyplot as plt

# Local imports
import pipeline_scripts.special_reports.bogota.bogota_constants as cons

sys.path.insert(0,'../../..')

from global_config import config

data_dir    = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')

# Reads the parameters from excecution
location_name        =  'bogota'   # sys.argv[1] # location name # bogota
agglomeration_folder =  'geometry' # sys.argv[2] # agglomeration name #

movement_path = os.path.join(data_dir, "data_stages", location_name, "agglomerated", agglomeration_folder, "movement.csv")
polygons_path = os.path.join(data_dir, "data_stages", location_name, "agglomerated", agglomeration_folder, "polygons.csv")
cases_path    = os.path.join(data_dir, "data_stages", location_name, "agglomerated", agglomeration_folder, "cases.csv")

out_folder = os.path.join(analysis_dir, location_name, agglomeration_folder, "rt")

# Check if folder exists
if not os.path.isdir(out_folder):
        os.makedirs(out_folder)

# Load movement
df_cases = pd.read_csv(cases_path, parse_dates=["date_time"])
df_bog   = df_cases.copy(); df_bog = df_bog.groupby('date_time').sum().resample('D').sum().fillna(0)

# Load geometry
df_polygons = pd.read_csv(polygons_path)
df_polygons["geometry"] = df_polygons["geometry"].apply(wkt.loads)
gdf_polygons = gpd.GeoDataFrame(df_polygons)

from pipeline_scripts.special_reports.bogota.special_functions.rt_estim import Rt_model

estimate = Rt_model()
def estimate_rt(incidence):
    rt_epiestim = estimate.calc_parametric_rt(incidence,
                                        mean_si=4.7,
                                        std_si=2.3,
                                        win_start=0,
                                        win_end=13, mean_prior=2.3, std_prior=2)

    return rt_epiestim

result  = estimate_rt(df_bog[['num_cases']]).dataframe.set_index('dates').dropna()

from matplotlib.dates import date2num, num2date
from matplotlib.colors import ListedColormap
from matplotlib import dates as mdates
from matplotlib.patches import Patch
from matplotlib import pyplot as plt
from matplotlib import ticker

from scipy.interpolate import interp1d

min_time = rt.index.values[0]
#min_time = cases_dpto_FIS.index[0] #pd.to_datetime('2020-03-01')
ABOVE  = [1, 0, 0]
MIDDLE = [1, 1, 1]
BELOW  = [0, 0, 0]

cmap = ListedColormap(np.r_[
    np.linspace(BELOW,MIDDLE,25),
    np.linspace(MIDDLE,ABOVE,25)
])
color_mapped = lambda y: np.clip(y, .5, 1.5)-.5

index = rt.index.get_level_values('dates')
values = rt['Rt_median'].values


fig, ax = plt.subplots(1, 1, figsize=(15.5, 7.2))
# Plot dots and line
ax.plot(index, values, c='k', zorder=1, alpha=.25)

ax.scatter(index,
            values,
            s=40,
            lw=.5,
            c=cmap(color_mapped(values)),
            edgecolors='k', zorder=2)

# Smooth CI by 1 day either side
lowfn = interp1d(date2num(index),
                    result['low_ci_025'].values,
                    bounds_error=False,
                    fill_value='extrapolate')

highfn = interp1d(date2num(index),
                    result['high_ci_975'].values,
                    bounds_error=False,
                    fill_value='extrapolate')

extended = pd.date_range(start=min_time,
                            end=index[-1]+pd.Timedelta(days=1))

ax.fill_between(extended,
                lowfn(date2num(extended)),
                highfn(date2num(extended)),
                color='k',
                alpha=.3,
                lw=0,
                zorder=3)
# Formatting
ax.xaxis.set_major_locator(mdates.MonthLocator())
ax.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
ax.xaxis.set_minor_locator(mdates.DayLocator())
ax.xaxis.set_major_locator(mdates.WeekdayLocator())
ax.xaxis.set_major_locator(mdates.MonthLocator())
#ax1xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
#ax.set_xlim( min_time-pd.Timedelta( days=1 ), index[-1]+pd.Timedelta(days=1) )
#ax1tick_params( axis='x',  rotation=90 )
#ax.legend(fontsize=15, frameon=False, title='Region', title_fontsize=15)
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)

ax.yaxis.set_major_locator(ticker.MultipleLocator(1))
ax.set_ylim([-0.05,3.7])
ax.yaxis.set_major_formatter(ticker.StrMethodFormatter("{x:.0f}"))
#ax1yaxis.tick_right()
ax.spines['left'].set_visible(False)
ax.spines['bottom'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.margins(0)
ax.grid(which='major', axis='y', c='k', alpha=.1, zorder=-2)
ax.set_ylabel(r'$R_t$', size=15)
fig.savefig(os.path.join(out_folder, 'rt.png'), dpi=400)
plt.close()