import os
import sys
import unidecode
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Direcotries
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')

# Constants
indent = "\t"

# import scripts
from pipeline_scripts.functions.adjust_cases_observations_function import prepare_cases, adjust_onset_for_right_censorship, confirmed_to_onset
from pipeline_scripts.functions.Rt_plot     import plot_cases_rt

# Import selected polygons
selected_polygons = pd.read_csv('pipeline_scripts/configuration/selected_polygons.csv')

# Get export_files.csv
export_file = os.path.join("pipeline_scripts", "configuration", "export_files.csv")
df_export_files = pd.read_csv(export_file)

# Get countries
countries = list(selected_polygons["location_name"].unique())

skipped_polygons = []
computed_polygons = []

# Get polygons per country to avoid loading data over and over
for idx, r in selected_polygons.iterrows():
    print(indent + indent + f"{r['location_name']}." + f" {r['poly_id']}.", end="\r")

    export_folder_location = os.path.join(analysis_dir, r['location_name'],  r['agglomeration'], 'r_t', 'entire_location')

    # Get cases
    df_cases = pd.read_csv( os.path.join(data_dir, 'data_stages', r['location_name'], 'agglomerated', r['agglomeration'], 'cases.csv'), index_col='poly_id', parse_dates=['date_time'])
    df_poly_id = df_cases.loc[r['poly_id']].reset_index()

    all_cases = df_poly_id['num_cases'].sum()

    if all_cases > 100:
        computed_polygons.append(r['poly_id'])
        df_polygons   = pd.read_csv( os.path.join(data_dir, 'data_stages', r['location_name'], 'agglomerated',r['agglomeration'] ,  "polygons.csv"), index_col='poly_id')
        try: 
            df_polygons = df_polygons.loc[r['poly_id']]
            p_delay = np.fromstring(df_polygons["attr_time-delay_dist_mix"], sep="|")
        except:
            df_polygons["attr_time-delay_dist_mix"] = df_polygons["attr_time-delay_dist_mix"].fillna("")
            df_polygons["attr_time_delay"] = df_polygons.apply(lambda x: np.fromstring(x["attr_time-delay_dist_mix"], sep="|"), axis=1)
            p_delay = pd.DataFrame(list(df_polygons['attr_time_delay'])).mean().to_numpy()

        df_poly_id['date_time'] =   pd.to_datetime(df_poly_id['date_time'])
        df_poly_id = df_poly_id.reset_index().set_index('date_time').resample('D').sum().fillna(0)
        df_poly_id = confirmed_to_onset(df_poly_id['num_cases'], p_delay, min_onset_date=None)

        df_poly_id = prepare_cases(df_poly_id, col='num_cases', cutoff=0)
        min_time = df_poly_id.index[0]

        path_to_save = os.path.join(export_folder_location, str(r['poly_id'])+'_Rt.png')
        (_, _, result) = plot_cases_rt(df_poly_id, 'num_cases', 'smoothed_num_cases' , key_df='date', pop=None, CI=50, min_time=min_time, state=None, path_to_save=path_to_save)

        result.to_csv(os.path.join(export_folder_location, str(r['poly_id'])+'_Rt.csv'))
        plt.close()
    else:
        skipped_polygons.append(r['poly_id'])
        print('\nWARNING: Rt was not computed for polygon: {}'.format(r['poly_id']))

print(indent + indent + f"Writting computation stats for rt.")
with open(os.path.join(export_folder_location,'rt_computation_stats.txt'), "w") as fp:
    fp.write(f"computed polygons: {computed_polygons}\n")
    fp.write(f"skipped polygons: {skipped_polygons}\n")