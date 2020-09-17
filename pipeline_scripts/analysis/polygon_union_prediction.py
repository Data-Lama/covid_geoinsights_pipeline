# location prediction by polygon union
# Location Prediction Analysis

# Imports
from general_functions import *

from sklearn.metrics import mean_squared_error


# Other imports
import os, sys
from datetime import timedelta, datetime
from pathlib import Path

import pandas as pd
import numpy as np
import constants as con
import general_functions as gf

import matplotlib.pyplot as plt
import seaborn as sns
sns.set_style("whitegrid")

#Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')



ident = '         '



def main(location_folder, agglomeration_method, selected_polygons_name, selected_polygons_folder_name, selected_polygons):

    gap_hours = 20

    # Parameters
    # Plotting
    k_plot = 4
    fig_size = (15,8)
    suptitle_font_size = 14
    individual_plot_size = 12
    axis_font_size = 12
    alpha = 0.2


    # Constructs the export
    data_folder = os.path.join(data_dir, 'data_stages')


    # Agglomerated folder location
    agglomerated_folder_location = os.path.join(data_dir, 'data_stages', location_folder, 'agglomerated', agglomeration_method)

    if not os.path.exists(agglomerated_folder_location):
        raise ValueError('No data found for {} Agglomeration'.format(agglomeration_method ))
        

    # Loads the polygons
    polygons = pd.read_csv(os.path.join(agglomerated_folder_location, 'polygons.csv'))
    polygons.poly_id = polygons.poly_id.astype(str)

    # Computes folder name
    polygons['folder_name'] = polygons.poly_name.apply(lambda s : gf.create_folder_name(s))

    # Loads cases
    cases = pd.read_csv(os.path.join(agglomerated_folder_location, 'cases.csv'), parse_dates = ['date_time'])
    cases.poly_id = cases.poly_id.astype(str)

    # If no selected polygons includes all
    if len(selected_polygons) > 0:
        polygons = polygons[polygons.poly_id.isin(selected_polygons)].copy()
        cases = cases[cases.poly_id.isin(selected_polygons)].copy()


    polygons_predictions_folder_location =  os.path.join(analysis_dir, location_folder, agglomeration_method, 'prediction/polygons')
    predictions_folder_location =  os.path.join(analysis_dir, location_folder, agglomeration_method, 'prediction/polygon_unions')

    # Folder location
    folder_location = os.path.join(predictions_folder_location, selected_polygons_folder_name)

    # Creates the folder if does not exists
    if not os.path.exists(folder_location):
        os.makedirs(folder_location)




    # Extract the prediction results for all polygon
    dfs = []
    dfs_simulations = []

    for folder in os.listdir(polygons_predictions_folder_location):        
        if os.path.isdir(os.path.join(polygons_predictions_folder_location, folder)) and folder in polygons['folder_name'].values:
            data_set_location = os.path.join(polygons_predictions_folder_location, folder, 'predicted_data.csv')
            data_set_location_simulation = os.path.join(polygons_predictions_folder_location, folder, 'simulation_data.csv')
            statistics_location = os.path.join(polygons_predictions_folder_location, folder, 'prediction_statistics.csv')

            if os.path.exists(data_set_location):

                df_stat = pd.read_csv(statistics_location)
                df_stat.index = df_stat['name']

                excecution_date = pd.to_datetime(df_stat.loc['date_time','value'])

                hours_ago = (pd.to_datetime(datetime.now()) - excecution_date).total_seconds()/3600

                if  hours_ago < gap_hours:

                    data_set = pd.read_csv(data_set_location, parse_dates = ['target_date'])
                    dfs.append(data_set)

                    data_set_simulations = pd.read_csv(data_set_location_simulation, parse_dates = ['target_date'])
                    dfs_simulations.append(data_set_simulations)

                else:
                    print(ident + f'   Location: {folder} was found but excecuted to long ago: {np.round(hours_ago,1)} hours. Skipping')
                

    if len(dfs) == 0:
        raise ValueError(f'No predictions precomputed polygon predictions found for location: {location_folder}, please excecute the script: polygon_prediction_analysis.py for selected polygons before this one.')


    # Concatenates all data frames
    df_results = pd.concat(dfs)
    df_simulations = pd.concat(dfs_simulations)

    # Converts to string
    df_results.polygon_id = df_results.polygon_id.astype(str)


    # Extract the selected Polygons
    included_polygons = df_results.polygon_id.unique()

    # Extracts coverage
    coverage = polygons[polygons.poly_id.isin(included_polygons)].num_cases.sum()/polygons.num_cases.sum()


    # Extracts the real cases
    df_real_cases = cases[['date_time','num_cases']].rename(columns = {'num_cases':'cases','date_time':'target_date'})
    df_real_cases = df_real_cases.merge(df_results.dropna()[['target_date']].drop_duplicates(), on = 'target_date', how = 'right').fillna(0)

    df_real_cases = df_real_cases.groupby('target_date').sum().reset_index()

    # Smoothes
    df_real_cases['cases'] = gf.smooth_curve(df_real_cases['cases'], con.smooth_days )


    df_real_cases['cases_accum'] = df_real_cases['cases'].rolling(min_periods=1, window=df_real_cases.shape[0]).sum()


    # Removes polygon_id
    df_results.drop('polygon_id', axis = 1, inplace = True)
    df_results.sort_values('target_date', inplace = True)

    # Plots the prediction
    print(ident + '   Plots Prediction')
    df1 = df_real_cases.copy()
    df1['Tipo'] = 'Real' 

    # Histroic Predict
    df2 = df_results.loc[ ~df_results.target_num_cases.isna(), ['target_date','predicted_num_cases','predicted_num_cases_accum']].copy()
    #Groups
    df2 = df2.groupby('target_date').sum().reset_index()
    df2.rename(columns = {'predicted_num_cases': 'cases', 'predicted_num_cases_accum': 'cases_accum'}, inplace = True)
    df2['Tipo'] = 'Predecido Histórico'

    # Adjusts
    df2['cases'] = df2['cases']/coverage
    df2['cases_accum'] = df2['cases_accum']/coverage

    # Future Prediction
    # Future Prediction
    days_back = 1
    start_date = df_results[df_results.target_num_cases.isna()].target_date.min()
    start_date_delayed =  start_date - timedelta(days = days_back)
    df3 = df_results.loc[ df_results.target_date >= start_date_delayed, ['target_date','predicted_num_cases','predicted_num_cases_accum']].copy()
    #Groups
    df3 = df3.groupby('target_date').sum().reset_index()
    df3.rename(columns = {'predicted_num_cases': 'cases', 'predicted_num_cases_accum': 'cases_accum'}, inplace = True)
    df3['Tipo'] = 'Proyección' 

    # Adjusts
    df3['cases'] = df3['cases']/coverage
    df3['cases_accum'] = df3['cases_accum']/coverage


    # Errors
    # Computes RMSE
    y1 = df1.sort_values('target_date').cases.values
    y2 = df2.sort_values('target_date').cases.values

    rmse = mean_squared_error(y1, y2, squared = False)
    mpe = 100*np.mean(np.abs(y1 - y2)/(y1 +1))

    df_grouped = df_results.groupby('target_date').sum().reset_index()

    x_values = df3.target_date.values

    # Non cumulative
    y_values = df3.cases.values
    lower_bound = [ y_values[i] - rmse for i in range(len(y_values))]
    upper_bound = [ y_values[i] + rmse for i in range(len(y_values))]

    # Cumulative
    y_values = df3.cases_accum.values
    lower_bound_accum = [ y_values[i] - rmse*(i+1) for i in range(len(y_values))]
    upper_bound_accum = [ y_values[i] + rmse*(i+1) for i in range(len(y_values))]

    # Unifies
    df_plot = pd.concat((df1,df2, df3), ignore_index = True)


    df_plot['cases'] = df_plot.cases.astype(float)
    df_plot['cases_accum'] = df_plot.cases_accum.astype(float)

    # Plots
    fig, ax = plt.subplots(2,1, figsize=(15,8))


    fig.suptitle('Predicción para {}'.format(selected_polygons_name), fontsize=suptitle_font_size)

    # Plot individual Lines
    sns.lineplot(x = 'target_date', y = 'cases', hue = 'Tipo', data = df_plot, ax = ax[0])
    sns.lineplot(x = 'target_date', y = 'cases_accum', hue = 'Tipo', data = df_plot, ax = ax[1])

    # Adds confidence intervals
    ax[0].fill_between(x_values, lower_bound, upper_bound, alpha=alpha)
    ax[1].fill_between(x_values, lower_bound_accum, upper_bound_accum, alpha=alpha)

    # Plot titles
    ax[0].set_title('Flujo Casos', fontsize=individual_plot_size)
    ax[1].set_title('Flujo Casos' + ' (Acumulados)', fontsize=individual_plot_size)

    # Plots Axis
    ax[0].set_xlabel('Día de la Epidemia', fontsize=axis_font_size)
    ax[1].set_xlabel('Día de la Epidemia', fontsize=axis_font_size)
    ax[0].set_ylabel('Casos', fontsize=axis_font_size)
    ax[1].set_ylabel('Casos (Acumulados)', fontsize=axis_font_size)


    fig.tight_layout(pad=3.0)

    fig.savefig(os.path.join(folder_location, 'prediction_{}.png'.format(selected_polygons_folder_name)))

    plt.close()

    # Saves Data
    df_plot.to_csv(os.path.join(folder_location, 'prediction_{}_data.csv'.format(selected_polygons_folder_name)), index = False)


    # -----------------------------
    # -----------------------------
    # ------- Simulation
    print(ident + '   Plots Simulations')


    # Adjusts simulations
    df_simulations = df_simulations.groupby(['target_date','ratio']).sum().reset_index()
    df_simulations['predicted_num_cases_accum'] = df_simulations['predicted_num_cases_accum']/coverage
    df_simulations['predicted_num_cases'] = df_simulations['predicted_num_cases']/coverage


    df_plot = df_simulations.copy()


    def get_legend(ratio):


        if ratio == 1:
            return('Sin Cambio')

        elif ratio < 1:
            return(f'Disminución {int(np.round(100*(1-ratio)))}%')

        else:
            return(f'Aumento {int(np.round(100*(ratio-1)))}%')



    df_plot['Movilidad'] = df_plot.ratio.apply(get_legend)


    # Plots
    fig, ax = plt.subplots(2,1, figsize=(15,8))

    fig.suptitle('Simulación de Cambio en la Movilidad para {}'.format(selected_polygons_name), fontsize=suptitle_font_size)

    # Plot individual Lines
    sns.lineplot(x = 'target_date', y = 'predicted_num_cases', hue = 'Movilidad', data = df_plot, ax = ax[0])
    sns.lineplot(x = 'target_date', y = 'predicted_num_cases_accum', hue = 'Movilidad', data = df_plot, ax = ax[1])


    # Plot titles
    ax[0].set_title('Flujo Casos', fontsize=individual_plot_size)
    ax[1].set_title('Flujo Casos' + ' (Acumulados)', fontsize=individual_plot_size)

    # Plots Axis
    ax[0].set_xlabel('Fecha', fontsize=axis_font_size)
    ax[1].set_xlabel('Fecha', fontsize=axis_font_size)
    ax[0].set_ylabel('Casos', fontsize=axis_font_size)
    ax[1].set_ylabel('Casos (Acumulados)', fontsize=axis_font_size)


    fig.tight_layout(pad=3.0)
    fig.savefig(os.path.join(folder_location, 'simulations_{}.png'.format(selected_polygons_folder_name)))    

    plt.close()

    # Saves Data
    df_plot.to_csv(os.path.join(folder_location, 'simulations_{}_data.csv'.format(selected_polygons_folder_name)), index = False)


    print(ident + '   Exports Statistics')


    with open(os.path.join(folder_location, 'statistics.csv'), 'w') as file:
        
        file.write('attribute_name,attribute_value\n')
        file.write('agglomeration_method,{}\n'.format(agglomeration_method))
        file.write('coverage,{}\n'.format(np.round(100*coverage,2)))
        file.write("polygons_included,{}\n".format(' '.join([str(p) for p in included_polygons])))
        file.write('rmse,{}\n'.format(int(np.round(rmse))))
        file.write('mpe,{}\n'.format(np.round(mpe,2)))



    print(ident + 'Done!')


if __name__ == "__main__":

    # Reads the parameters from excecution
    location_folder = sys.argv[1] # location folder
    agglomeration_method = sys.argv[2] # Aglomeration name
    selected_polygons_name = sys.argv[3] 

    selected_polygons_folder_name = 'entire_location'
    selected_polygons = []

    if len(sys.argv) > 4:

        selected_polygons_name =  sys.argv[4]

        i = 5
        while i < len(sys.argv):
            selected_polygons.append(sys.argv[i])
            i += 1

        if len(selected_polygons) == 0:
            raise ValueError('If selected polygos name is given, at least one polygon id must be provided')


    main(location_folder, agglomeration_method, selected_polygons_name, selected_polygons_folder_name, selected_polygons)