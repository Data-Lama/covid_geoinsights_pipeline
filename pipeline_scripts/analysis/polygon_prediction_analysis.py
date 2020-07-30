# Polygon Prediction Analysis

# Imports
from timeseries_functions import *
from prediction_functions import *
from general_functions import *

# Other imports
import os, sys
from datetime import timedelta
from sklearn.metrics import mean_squared_error

from pathlib import Path

import pandas as pd
import numpy as np


import matplotlib.pyplot as plt
import seaborn as sns
sns.set_style("whitegrid")


#Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')

ident = '         '


# Is defined inside function for using it directly from python
def main(location, agglomeration_method, polygon_name, polygon_id, polygon_display_name, ident = '         '):


	try:
		polygon_id = int(polygon_id)
	except:
		pass


	# Constructs the export
	folder_location = os.path.join(analysis_dir, location, agglomeration_method, 'prediction', polygon_name)


	# Creates the folder if does not exists
	if not os.path.exists(folder_location):
		os.makedirs(folder_location)

	# Checks if the polygon has prediction dataset dataset
	if not os.path.exists(os.path.join(folder_location,'training_data.csv')):
		raise ValueError(f'No Training Dataset found for polygon {polygon_name} ({polygon_id}). Please excecute the script: polygon_prediction_dataset_builder.py, before this one.')



	print(ident + 'Excecuting analysis for {} (polygon {} of {})'.format(polygon_display_name, polygon_id, location))

	# Global variables
	# Neighbor extraction


	# Prediction build
	alpha_options = [1,100,500,1000]
	iterations = 50

	# Plotting
	k_plot = 4
	fig_size = (15,8)
	suptitle_font_size = 14
	individual_plot_size = 12
	axis_font_size = 12
	alpha = 0.2

	# Extracts Neighbors
	print(ident + '   Extracts the Trainig Dataset')

	df_prediction = pd.read_csv(os.path.join(folder_location ,'training_data.csv'), parse_dates = ['current_date','target_date'])

	df_training_mobility = pd.read_csv(os.path.join(folder_location ,'mobility_ratio_data.csv'), parse_dates = ['current_date','target_date'])


	# Trains the model
	print(ident + '   Trains the model')
	df_results, summary_dict, final_clf, scaler, weights = predict_location(location, polygon_id, df_prediction, alpha_options = alpha_options, iterations = iterations, verbose = False)

	df_results['polygon_id'] = polygon_id

	df_results.to_csv(os.path.join(folder_location, 'predicted_data.csv'), index = False)


	# Reduces Movement and predicts
	# Extracts the 
	num_cols = weights.shape[0]
	mobility_dfs = []

	for ratio in df_training_mobility.mobility_ratio.unique():

		df_temp = df_training_mobility[df_training_mobility.mobility_ratio == ratio]
		X_temp = df_temp[df_temp.columns.values[0:num_cols]]

		pred = final_clf.predict( scaler.transform(X_temp))
		df_mob_temp = pd.DataFrame({'target_date':df_temp['target_date'], 'predicted_num_cases': pred, 'ratio': ratio})

		# Cases Accum
		df_mob_temp['predicted_num_cases_accum'] = df_mob_temp['predicted_num_cases'].rolling(min_periods=1, window=df_mob_temp.shape[0]).sum()

		# Adds it
		mobility_dfs.append(df_mob_temp)


	df_prediction_mobility = pd.concat(mobility_dfs, ignore_index = True) 

	df_prediction_mobility.to_csv(os.path.join(folder_location, 'predicted_data_mobility_ratio.csv'), index = False)


	# Plots the prediction
	print(ident + '   Plots Prediction')

	# Actual
	df1 = df_results[['target_date','target_num_cases','target_num_cases_accum']].dropna()
	df1.rename(columns = {'target_num_cases': 'cases', 'target_num_cases_accum': 'cases_accum'}, inplace = True)
	df1['Tipo'] = 'Real' 

	# Histroic Prediction
	df2 = df_results.loc[ ~df_results.target_num_cases.isna(), ['target_date','predicted_num_cases','predicted_num_cases_accum']].copy()
	df2.rename(columns = {'predicted_num_cases': 'cases', 'predicted_num_cases_accum': 'cases_accum'}, inplace = True)
	df2['Tipo'] = 'Predecido Histórico' 

	# Future Prediction
	start_date = df_results[df_results.target_num_cases.isna()].target_date.min()
	start_date_delayed =  start_date - timedelta(days = 1)
	df3 = df_results.loc[ df_results.target_date >= start_date_delayed, ['target_date','predicted_num_cases','predicted_num_cases_accum']].copy()
	df3.rename(columns = {'predicted_num_cases': 'cases', 'predicted_num_cases_accum': 'cases_accum'}, inplace = True)
	df3['Tipo'] = 'Poyección' 


	# Future Prediction reduction 25%
	df4 = df_prediction_mobility.loc[ (df_prediction_mobility.ratio == 0.75) & (df_prediction_mobility.target_date >= start_date), ['target_date','predicted_num_cases','predicted_num_cases_accum']].copy()
	#df4 = df_prediction_mobility.loc[ (df_prediction_mobility.ratio == 0.75), ['target_date','predicted_num_cases','predicted_num_cases_accum']].copy()
	df4.rename(columns = {'predicted_num_cases': 'cases', 'predicted_num_cases_accum': 'cases_accum'}, inplace = True)
	df4['Tipo'] = 'Poyección Reduccion 25% Movilidad ' 

	df5 = df_prediction_mobility.loc[ (df_prediction_mobility.ratio == 1.25) & (df_prediction_mobility.target_date >= start_date), ['target_date','predicted_num_cases','predicted_num_cases_accum']].copy()
	#df5 = df_prediction_mobility.loc[ (df_prediction_mobility.ratio == 1.25), ['target_date','predicted_num_cases','predicted_num_cases_accum']].copy()
	df5.rename(columns = {'predicted_num_cases': 'cases', 'predicted_num_cases_accum': 'cases_accum'}, inplace = True)
	df5['Tipo'] = 'Poyección Aumento 25% Movilidad ' 


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


	df_plot = pd.concat((df1,df2,df3,df4,df5), ignore_index = True)

	df_plot['cases'] = df_plot.cases.astype(float)
	df_plot['cases_accum'] = df_plot.cases_accum.astype(float)

	# Plots
	fig, ax = plt.subplots(2,1, figsize=(15,8))

	fig.suptitle('Predicción para {}'.format(polygon_display_name), fontsize=suptitle_font_size)

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

	fig.savefig(os.path.join(folder_location, 'prediction_{}.png'.format(polygon_name)))

	print(ident + '   Exports Statistics')

	with open(os.path.join(folder_location, 'prediction_statistics.txt'), 'w') as file:
		
		file.write('Agglomeration Method Used: {}'.format(agglomeration_method) + '\n')
		file.write('   Result Statistics:' + '\n')
		for concept in summary_dict:
			file.write('      {}: {}'.format(concept, summary_dict[concept]) + '\n')
			

	weights.to_csv(os.path.join(folder_location, 'weights.csv'), index = False)

	print(ident + 'Done!')


if __name__ == "__main__":

	# Reads the parameters from excecution
	location  = sys.argv[1] # location name
	agglomeration_method = sys.argv[2] # Aglomeration name
	polygon_name = sys.argv[3] # polygon name
	polygon_id  = sys.argv[4] # polygon id
	polygon_display_name = sys.argv[5] # polygon display name

	# Excecution
	main(location, agglomeration_method, polygon_name, polygon_id, polygon_display_name)