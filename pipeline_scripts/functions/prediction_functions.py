# Prediction functions

# This modules builds the graphs, nodes and machine learning datasets
import pandas as pd

from sklearn.svm import SVR
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import Ridge
from sklearn.model_selection import train_test_split
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import mean_squared_error

import numpy as np

import os

# Imports all the necesary functions
import constructor_functions as constr


#Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')


data_folder = os.path.join(data_dir, 'data_stages')


def get_graphs(agglomeration_method, location):
	'''
	Gets the graph for a given location
	'''
	
	graphs_location = os.path.join(data_folder, location, 'constructed', agglomeration_method, 'daily_graphs/')
	
	if not os.path.exists(graphs_location):
		raise ValueError('No graphs found for location: {}'.format(location))
		
	nodes = pd.read_csv(os.path.join(graphs_location, 'nodes.csv'), parse_dates = ['date_time'])
	edges = pd.read_csv(os.path.join(graphs_location, 'edges.csv'), parse_dates = ['date_time'])
	node_locations = pd.read_csv(os.path.join(graphs_location, 'node_locations.csv'))
	
	return(nodes,edges, node_locations)




def extract_prediction_data(agglomeration_method, locations, polygons_ids, days_back, days_ahead, max_day = None, smooth_days = 1):
	
	dfs = []
	
	for i in range(len(locations)):
		
		loc = locations[i]
		poly_id = polygons_ids[i]
		
		nodes, edges, node_locations = get_graphs(agglomeration_method,loc)
				
		df_temp = constr.build_prediction_dataset_for_nodes(node_ids = [poly_id], nodes = nodes, edges = edges, days_back = days_back, days_ahead = days_ahead, max_day = max_day, smooth_days = smooth_days)
		df_temp['location'] = loc
		df_temp['poly_id'] = poly_id

		dfs.append(df_temp)

	# Merges
	df_polys_predict = pd.concat(dfs, ignore_index = True)
		
	return(df_polys_predict)
	

def extract_prediction_data_for_location(agglomeration_method, locations, days_back, days_ahead, max_day = None):
	
	dfs = []
	
	for i in range(len(locations)):
		
		loc = locations[i]
		
		nodes, edges, node_locations = get_graphs(agglomeration_method, loc)
				
		df_temp = constr.build_prediction_dataset_for_graphs(nodes = nodes, edges = edges, days_back = days_back, days_ahead = days_ahead, max_day = max_day)
		df_temp['location'] = loc

		dfs.append(df_temp)

	# Merges
	df_polys_predict = pd.concat(dfs, ignore_index = True)
		
	return(df_polys_predict)



def get_best_ridge_alpha(X, y, alpha_options, iterations, verbose = True, local_ident = '      '):
	'''
	Gets best ridge alpha
	'''
	


	param_grid = [{'alpha': alpha_options}]
	clf = Ridge()

	scores = []
	rmses = []
	alphas = {}
	
	if verbose:
		print(local_ident + 'Starts Model Tunning')
			
	for i in range(iterations):
		
		# Starts Scaler
		scaler = StandardScaler()
		# Splits
		X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

		# Fits Scaler
		scaler.fit(X_train)
		X_train = scaler.transform(X_train)
		X_test = scaler.transform(X_test)
		
		# Excecutes Grid Search
		tunned_clf = GridSearchCV(clf, param_grid, cv=5, return_train_score = True)
		tunned_clf.fit(X_train, y_train)
		
		# Extracts Parameters
		r2 = tunned_clf.score(X_test, y_test)
		rmse = mean_squared_error(y_test, tunned_clf.predict(X_test), squared=False)
		
		if verbose:
			print(local_ident + '   Iteration: {}'.format(i))
			print(local_ident + '   Best Alpha: {}'.format(tunned_clf.best_params_['alpha']))
			print(local_ident + '   R2: {}'.format(r2))
			print(local_ident + '   RMSE: {}'.format(rmse))
			print(local_ident + '   -------------')
		
		scores.append(r2)
		rmses.append(rmse)
		alpha = tunned_clf.best_params_['alpha']

		if alpha not in alphas:
			alphas[alpha] = 0
		alphas[alpha] += 1
		
	final_alpha = max(alphas, key=alphas.get)
	median_r2 = np.median(scores)
	median_rmse = np.median(rmses)
	
	if verbose:
		print(local_ident + '')
		print(local_ident + 'Done. Final Alpha: {}, Median R2: {}, Median RMSE: {}'.format(final_alpha, median_r2, median_rmse))
	
	return(final_alpha, median_r2, median_rmse)




def predict_location(location, poly_id, df_prediction, alpha_options = [80,100,120,160,180], iterations = 50, verbose = False, col_shift = 5):
	'''
	Predicts for a specific location
	'''

	response = {}
	
	# Extracts the polygon set (or location)
	if poly_id is not None:
		df_poly_all = df_prediction[(df_prediction.location == location) & (df_prediction.poly_id == poly_id)].copy()
	else:
		df_poly_all = df_prediction[(df_prediction.location == location)].copy()


	# Extracts the training set 
	current = df_prediction.dropna()
	
	# Training data
	X = current[current.columns[0:(-1*col_shift)]].values
	y = current.target_num_cases
	
	# Extracts the location set
	current_poly = df_poly_all.dropna()

	# Training data
	X_poly = current_poly[current_poly.columns[0:(-1*col_shift)]].values
	y_poly = current_poly.target_num_cases
	
	# Gets the alpha
	final_alpha, median_r2, median_rmse = get_best_ridge_alpha(X, y, alpha_options, iterations, verbose)
	
	# Constructs the final classifier
	# Scaler
	scaler = StandardScaler()
	scaler.fit(X)
	# Trasnforms
	X_transformed = scaler.transform(X)
	X_poly_transformed = scaler.transform(X_poly)
	final_clf = Ridge(alpha = final_alpha).fit(X_transformed,y)
	
	# Extracts Final statistics
	global_r2 = final_clf.score(X_transformed,y)
	global_rmse = mean_squared_error(y, final_clf.predict(X_transformed), squared=False)
	
	# Local Final statistics
	local_r2 = final_clf.score(X_poly_transformed,y_poly)
	local_rmse = mean_squared_error(y_poly, final_clf.predict(X_poly_transformed), squared=False)
	
	# Weights
	coef_w = pd.DataFrame({'coef': df_poly_all.columns[0:-col_shift].values, 'weight': final_clf.coef_, 'abs_weight': np.abs(final_clf.coef_)}).sort_values('abs_weight', ascending = False)
	
	
	# Constructs plotting data
	# Predicted Values
	df_poly_all['predicted_num_cases'] = final_clf.predict(scaler.transform(df_poly_all[df_poly_all.columns[0:(-1*col_shift)]].values))
	
	# Constructs the plotting data
	
	# Actual
	df_plot = df_poly_all[['target_date','target_num_cases','predicted_num_cases']].copy()

	# Accumulated
	df_plot['target_num_cases_accum'] = df_plot['target_num_cases'].rolling(min_periods=1, window=df_plot.shape[0]).sum()
	df_plot.loc[df_plot.target_num_cases.isna(),'target_num_cases_accum'] = np.nan
	df_plot['predicted_num_cases_accum'] = df_plot['predicted_num_cases'].rolling(min_periods=1, window=df_plot.shape[0]).sum()

	# Builds summary dict
	summary_dict = {'final_alpha': final_alpha, 'median_r2': median_r2, 'median_rmse':median_rmse, 
					'global_r2': global_r2, 'global_rmse': global_rmse,
					'local_r2': local_r2, 'local_rmse': local_rmse}
	
	
	return(df_plot, summary_dict, coef_w)
	
	