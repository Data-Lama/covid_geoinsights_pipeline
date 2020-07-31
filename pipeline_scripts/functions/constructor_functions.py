# Functions for building datasets over the clean sets

# This modules builds the graphs, nodes and machine learning datasets

import pandas as pd
import numpy as np
from pathlib import Path
import os
from geopy.geocoders import Nominatim
import time

from sklearn.metrics import pairwise_distances

from datetime import timedelta

import ot

import geo_functions as geo
km_constant = geo.km_constant





def build_graphs(polygons, cases, movement, population):
	'''
	Builds the graphs for the given input data. The data received must have the agglomerated format
	'''

	# defines the nodes
	all_nodes = polygons[['poly_id']].copy()

	# Extracts min date and max_date
	# TODO: Include the population
	min_date = max(cases.date_time.min(), movement.date_time.min())
	max_date = min(cases.date_time.max(), movement.date_time.max())

	# Iterates over the dates
	current_date = min_date
	nodes = []
	edges = []

	day = (min_date - cases.date_time.min()).days

	while current_date <= max_date:

		# Next Date
		next_date = current_date + timedelta(days = 1)

		# Population
		current_population = population.loc[(population.date_time >= current_date) & (population.date_time < next_date), ['poly_id','population']]
		current_population = current_population.groupby('poly_id').sum().reset_index()

		# Cases
		current_cases = cases.loc[(cases.date_time >= current_date) & (cases.date_time < next_date), ['poly_id','num_cases']]
		current_cases = current_cases.groupby('poly_id').sum().reset_index()

		# Movement
		current_movement = movement.loc[(movement.date_time >= current_date) & (movement.date_time < next_date), ['start_poly_id','end_poly_id','movement']]
		# Inner Movement
		inner_movement = current_movement.loc[current_movement.start_poly_id == current_movement.end_poly_id, ['end_poly_id','movement']].rename(columns = {'end_poly_id':'poly_id','movement':'inner_movement'})
		inner_movement = inner_movement.groupby('poly_id').sum().reset_index()
		# Edges
		current_edges = current_movement.loc[current_movement.start_poly_id != current_movement.end_poly_id].rename(columns = {'start_poly_id':'start_id', 'end_poly_id':'end_id'})
		current_edges = current_edges.groupby(['start_id','end_id']).sum().reset_index()



		# Merges all
		current_nodes = all_nodes.merge(current_cases, on = 'poly_id', how = 'left').fillna(0)
		current_nodes = current_nodes.merge(inner_movement, on = 'poly_id', how = 'left')
		current_nodes = current_nodes.merge(current_population, on = 'poly_id', how = 'left')

		# Renames
		current_nodes.rename(columns = {'poly_id':'node_id'}, inplace = True)

		# Adds dates
		current_nodes['date_time'] = current_date
		current_edges['date_time'] = current_date

		# Adds day
		current_nodes['day'] = day
		current_edges['day'] = day

		# Sorts them
		current_nodes = current_nodes[['date_time','day','node_id','num_cases','population','inner_movement']].sort_values('node_id')
		current_edges = current_edges[['date_time','day','start_id','end_id','movement']].sort_values(['start_id','end_id'])

		# Adds them
		nodes.append(current_nodes)
		edges.append(current_edges)

		# Advances
		day += 1
		current_date = next_date


	nodes = pd.concat(nodes, ignore_index = True)
	edges = pd.concat(edges, ignore_index = True)
	node_locations = polygons[['poly_id','poly_name','poly_lon','poly_lat']].rename(columns = {'poly_id':'node_id', 'poly_name':'node_name', 'poly_lon':'lon', 'poly_lat':'lat'}).sort_values('node_id')
	
	# Sorts the datasets

	return(nodes, edges, node_locations)




def build_distance(nodes, edges, node_locations):
	'''
	Method that builds the distanbce between graphs
	'''
	
	node_locations.sort_values('node_id', inplace = True)
	
	# Node ids
	node_ids = node_locations.node_id.values
	
	# Extracts the distances
	node_distance = pairwise_distances( node_locations[['lon','lat']], node_locations[['lon','lat']])*km_constant

	# The node ids will be the days
	graph_ids = np.sort(nodes.day.unique())
	n = len(graph_ids)
	
	# Days
	min_day = nodes.day.min()
	max_day = nodes.day.max() + 1
	
	graph_distances = np.zeros((n,n))

	for i in range(min_day, max_day):
		for j in range(i + 1, max_day):
			

			# First Graph
			graph_i = nodes.loc[nodes.day == i].copy()
			graph_i.index = graph_i.node_id
			graph_i = graph_i.loc[node_ids]
			
			distribution_i = graph_i.num_cases.values
			if np.sum(distribution_i) == 0:
				distribution_i = np.repeat(1,len(distribution_i))


			distribution_i = distribution_i / np.sum(distribution_i) 
			
			# Second Graph
			graph_j = nodes.loc[nodes.day == j].copy()
			graph_j.index = graph_j.node_id
			graph_j = graph_j.loc[node_ids]
			
			distribution_j = graph_j.num_cases.values
			if np.sum(distribution_j) == 0:
				distribution_j = np.repeat(1,len(distribution_j))
			
			distribution_j = distribution_j / np.sum(distribution_j) 
			

			# Distance
			graph_distances[i - min_day,j - min_day] = ot.emd2( distribution_i, distribution_j, node_distance)
			graph_distances[j - min_day,i - min_day] = graph_distances[i - min_day,j - min_day]


	df_final = pd.DataFrame(graph_distances)
	df_final.index = range(min_day, max_day)
	df_final.columns = range(min_day, max_day)

	return(df_final)




def build_graph_values(nodes, edges):
	'''
	Builds the summary values for each graph
	'''

	graph_ids = np.sort(nodes.day.unique())

	# Filter Values
	internal_movement = nodes[['day','inner_movement']].groupby('day').sum().inner_movement.values
	num_cases = nodes[['day','num_cases']].groupby('day').sum().num_cases.values

	df = pd.DataFrame({'day': graph_ids, 'inner_movement' : internal_movement, 'cases': num_cases})

	# Adds the external_movement
	ext_mov = external_movement = edges[['day','movement']].groupby('day').sum().reset_index().rename(columns = {'movement':'external_movement'})

	df = df.merge(ext_mov, on = ['day'], how = 'left').fillna(0)

	return(df)








def build_prediction_dataset_for_nodes(node_ids, nodes, edges, days_back, days_ahead, smooth_days, max_day = None):
	'''
	Builds Prediction data set for indivdual nodes

	'''


	date_min = nodes.date_time.min() + timedelta(days = days_back)
	date_max = nodes.date_time.max()

	nodes.sort_values(['day','node_id'], inplace = True)
	for n_id in nodes.node_id.unique():
		nodes.loc[nodes.node_id == n_id,'num_cases'] = nodes[nodes.node_id == n_id].num_cases.rolling(smooth_days, min_periods=1).mean()


	date_min = nodes.date_time.min() + timedelta(days = days_back)
	date_max = nodes.date_time.max()

	current_date = date_min

	dfs = []


	while current_date <= date_max:


		# Nodes		
		passed_nodes = nodes.loc[(nodes.date_time >= (current_date - timedelta(days = days_back))) & (nodes.date_time < current_date)]


		# Current number of cases
		if (current_date + timedelta(days_ahead)) <= date_max:
			target_nodes = nodes.loc[nodes.date_time == (current_date + timedelta(days_ahead))]
			
			target_num_cases = target_nodes.loc[target_nodes.node_id.isin(node_ids),'num_cases'].sum()
		else:
			# Projection
			target_num_cases = None

			
		# Edges
		passed_edges_all = edges.loc[(edges.date_time >= (current_date - timedelta(days = days_back))) & (edges.date_time < current_date)]

		final_dict = build_prediction_input_for_nodes(node_ids, passed_nodes, passed_edges_all, days_back)

		frame = pd.DataFrame(final_dict, index = [0])
		frame['current_date'] = current_date
		frame['target_date'] = current_date + timedelta(days_ahead)
		frame['target_num_cases'] = target_num_cases

		dfs.append(frame)

		# Advances
		current_date += timedelta(days = 1)

	df_ml = pd.concat(dfs, ignore_index = True)

	if max_day is not None:
		df_ml = df_ml[df_ml.elapsed_days <= max_day]
	
	return(df_ml)




def build_prediction_input_for_nodes(node_ids, passed_nodes, passed_edges_all, days_back):
	'''
	Contruct the input for a node.

	Returns the strucutre in a dictionary
	'''

	back_dates = passed_nodes[['date_time']].drop_duplicates().sort_values('date_time')
		
	# End
	passed_edges_end = passed_edges_all.loc[(passed_edges_all.end_id.isin(node_ids))]
	passed_edges_end = passed_edges_end[['date_time','start_id','movement']].rename(columns = {'start_id':'node_id'})

	
	passed_edges = pd.concat([passed_edges_end], ignore_index = True)
	
	# Attaches number of cases and calculates the in_degree
	passed_edges = passed_edges.merge(passed_nodes[['date_time','node_id', 'num_cases']], on = ['date_time','node_id'])
	passed_edges['degree'] = passed_edges.movement*passed_edges.num_cases
	
	# Sets to zero the missing values
	passed_edges = back_dates.merge(passed_edges, on = ['date_time'], how = 'left').fillna(0)

	#print()

	# Calculates in degree
	in_degree = passed_edges[['date_time', 'degree']].groupby('date_time').sum().reset_index().sort_values('date_time').degree.values
	in_movement = passed_edges[['date_time', 'movement']].groupby('date_time').sum().reset_index().sort_values('date_time').movement.values
	in_cases = passed_edges[['date_time', 'num_cases']].groupby('date_time').sum().reset_index().sort_values('date_time').num_cases.values
	
	# Calculates the passed internal movement and internal degree
	history = passed_nodes.loc[passed_nodes.node_id.isin(node_ids),['date_time','inner_movement','num_cases']]


	history['degree'] = history.inner_movement*history.num_cases
	
	internal_degree = history[['date_time', 'degree']].groupby('date_time').sum().reset_index().sort_values('date_time').degree.values
	internal_movement = history[['date_time', 'inner_movement']].groupby('date_time').sum().reset_index().sort_values('date_time').inner_movement.values
	internal_cases = history[['date_time', 'num_cases']].groupby('date_time').sum().reset_index().sort_values('date_time').num_cases.values
	

	final_dict = {}
	for i in range(0, days_back):
		day = days_back - i
		# values
		final_dict['interal_movement_{}'.format(day)] = internal_movement[i]
		final_dict['internal_degree_{}'.format(day)] = internal_degree[i]
		final_dict['internal_cases_{}'.format(day)] = internal_cases[i]
		final_dict['in_degree_{}'.format(day)] = in_degree[i]
		final_dict['in_movement_{}'.format(day)] = in_movement[i]
		final_dict['in_cases_{}'.format(day)] = in_cases[i]

	final_dict['elapsed_days'] = passed_nodes.day.max() + 1



	return(final_dict)




def build_prediction_dataset_for_graphs(nodes, edges, days_back, days_ahead, max_day = None):
	'''
	Builds Prediction data for the whole graph
	'''

	date_min = nodes.date_time.min() + timedelta(days = days_back)
	date_max = nodes.date_time.max()

	current_date = date_min

	dfs = []


	while current_date <= date_max:


		# Nodes
		passed_nodes = nodes.loc[(nodes.date_time >= (current_date - timedelta(days = days_back))) & (nodes.date_time < current_date)]

		back_dates = passed_nodes[['date_time']].drop_duplicates().sort_values('date_time')

		# Current number of cases
		if (current_date + timedelta(days_ahead)) <= date_max:
			target_nodes = nodes.loc[nodes.date_time == (current_date + timedelta(days_ahead))]
			target_num_cases = target_nodes['num_cases'].sum()
		else:
			# Projection
			target_num_cases = None

			
		# Edges
		passed_edges_all = edges.loc[(edges.date_time >= (current_date - timedelta(days = days_back))) & (edges.date_time < current_date)]
		
		# End
		passed_edges_end = passed_edges_all[['date_time','start_id','movement']].rename(columns = {'start_id':'node_id'})
		
		# Start
		#passed_edges_start = passed_edges_all.loc[(passed_edges_all.start_id.isin(node_ids))]
		#passed_edges_start = passed_edges_start[['date_time','end_id','movement']].rename(columns = {'end_id':'node_id'})
		
		passed_edges = pd.concat([passed_edges_end], ignore_index = True)
		
		# Attaches number of cases and calculates the in_degree
		passed_edges = passed_edges.merge(passed_nodes[['date_time','node_id', 'num_cases']], on = ['date_time','node_id'])
		passed_edges['degree'] = passed_edges.movement*passed_edges.num_cases

		# Sets to zero the missing values
		passed_edges = back_dates.merge(passed_edges, on = ['date_time'], how = 'left').fillna(0)
		
		# Calculates in degree
		external_degree = passed_edges[['date_time', 'degree']].groupby('date_time').sum().reset_index().sort_values('date_time').degree.values
		external_movement = passed_edges[['date_time', 'movement']].groupby('date_time').sum().reset_index().sort_values('date_time').movement.values
		
		# Calculates the passed internal movement and internal degree
		history = passed_nodes[['date_time','inner_movement','num_cases']].copy()
		history['degree'] = history.inner_movement*history.num_cases
		
		internal_degree = history[['date_time', 'degree']].groupby('date_time').sum().reset_index().sort_values('date_time').degree.values
		internal_movement = history[['date_time', 'inner_movement']].groupby('date_time').sum().reset_index().sort_values('date_time').inner_movement.values
		
		num_cases = history[['date_time', 'num_cases']].groupby('date_time').sum().reset_index().sort_values('date_time').num_cases.values
		

		final_dict = {}
		for i in range(0, days_back):
			day = days_back - i
			# values
			final_dict['interal_movement_{}'.format(day)] = internal_movement[i]
			final_dict['internal_degree_{}'.format(day)] = internal_degree[i]
			final_dict['external_degree_{}'.format(day)] = external_degree[i]
			final_dict['external_movement{}'.format(day)] = external_movement[i]
			final_dict['num_cases_{}'.format(day)] = num_cases[i]
			
		final_dict['elapsed_days'] = passed_nodes.day.max() + 1

		frame = pd.DataFrame(final_dict, index = [0])
		frame['current_date'] = current_date
		frame['target_date'] = current_date + timedelta(days_ahead)
		frame['target_num_cases'] = target_num_cases

		dfs.append(frame)

		# Advances
		current_date += timedelta(days = 1)

	df_ml = pd.concat(dfs, ignore_index = True)

	if max_day is not None:
		df_ml = df_ml[df_ml.elapsed_days <= max_day]
	
	return(df_ml)

