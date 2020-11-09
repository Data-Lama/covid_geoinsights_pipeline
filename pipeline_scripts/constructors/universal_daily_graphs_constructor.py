# Script that constructs the graphs and predictions datasets

# Imports all the necesary functions
import constructor_functions as constr


# Other imports
import os, sys

from pathlib import Path

import pandas as pd
import numpy as np

import constants as con

ident = '         '

#Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')

# Reads the parameters from excecution
location_name  = sys.argv[1] # location namme
location_folder_name  = sys.argv[2] # location folder namme
agglomeration_method_parameter = sys.argv[3] # Aglomeration name



# Sets the location
location_folder = os.path.join(data_dir, 'data_stages', location_folder_name)



if agglomeration_method_parameter.upper() == 'ALL':
	agglomeration_methods = con.agglomeration_methods
else:
	agglomeration_methods = [agglomeration_method_parameter]


i = 0
for agglomeration_method in agglomeration_methods:

	i += 1
	# Creates the folders if the don't exist
	# constructed

	# Agglomeration folder
	agglomeration_folder = os.path.join(location_folder, 'agglomerated', agglomeration_method)
	if not os.path.exists(agglomeration_folder):
		print(ident + 'No data found for {} Agglomeration ({} of {}). Skipping'.format(agglomeration_method,i , len(agglomeration_methods)))
		continue

	graphs_folder = os.path.join(location_folder, 'constructed', agglomeration_method, 'daily_graphs/')
	if not os.path.exists(graphs_folder):
		os.makedirs(graphs_folder)


	ident = '         '

	print(ident + 'Constructs for {} with {} Agglomeration ({} of {})'.format(location_name, agglomeration_method, i , len(agglomeration_methods)))
	print()
	print(ident + '   Builds Daily Graphs')

	# Loads Data
	print()
	print(ident + '      Loads Data:')

	print(ident + '         Polygons')
	polygons = pd.read_csv(os.path.join(agglomeration_folder, 'polygons.csv'), dtype={'poly_id': str})

	print(ident + '         Cases')
	cases = pd.read_csv(os.path.join(agglomeration_folder, 'cases.csv'), parse_dates = ['date_time'], dtype={'poly_id': str})

	print(ident + '         Movement')
	movement = pd.read_csv(os.path.join(agglomeration_folder,  'movement.csv'), parse_dates = ['date_time'], dtype={'start_poly_id': str, 'end_poly_id': str})

	print(ident + '         Population')
	population = pd.read_csv(os.path.join(agglomeration_folder,  'population.csv'), parse_dates = ['date_time'], dtype={'poly_id': str})

	print()
	print(ident + '      Constructs:')

	print(ident + '         Nodes and Edges')
	nodes, edges, node_locations = constr.build_graphs(polygons, cases, movement, population)

	# Saves the frames
	nodes.to_csv(os.path.join(graphs_folder, 'nodes.csv'), index = False)
	edges.to_csv(os.path.join(graphs_folder, 'edges.csv'), index = False)
	node_locations.to_csv(os.path.join(graphs_folder, 'node_locations.csv'), index = False)

	min_graph_date = nodes.date_time.min()
	max_graph_date = nodes.date_time.max()
	total_days = len(nodes.day.unique())
	elapsed_days = nodes.day.min()


	#print(ident + '         Distance Matrix')
	#distance_matrix = constr.build_distance(nodes, edges, node_locations)
	#distance_matrix.to_csv(os.path.join(graphs_folder, 'distance_matrix.csv'))



	print(ident + '         Graph Values')
	graph_values = constr.build_graph_values(nodes, edges)
	graph_values.to_csv(os.path.join(graphs_folder, 'graph_values.csv'), index = False)


	print(ident + '      Done!')
	print()
	print(ident + '      Saves Statistics:')

	#Saves the Statistics
	with open(os.path.join(graphs_folder, 'README.txt'), 'w') as file:

		file.write('Agglomeration Method Used: {}'.format(agglomeration_method) + '\n')
		file.write('Graphs Summary:' + '\n')
		file.write('   Min Date: {}'.format(min_graph_date) + '\n')
		file.write('   Max Date: {}'.format(max_graph_date) + '\n')
		file.write('   Start Day (Since First Case): {}'.format(elapsed_days) + '\n')
		file.write('   Total Days: {}'.format(total_days) + '\n')
		file.write('   Average Number of Cases: {}'.format(np.round(graph_values.cases.mean(), 2)) + '\n')
		file.write('   Average Internal Movement: {}'.format(np.round(graph_values.inner_movement.mean(), 2)) + '\n')
		file.write('   Average External Movement: {}'.format(np.round(graph_values.external_movement.mean(), 2)) + '\n')


	print(ident + '   Done! Data copied to: {}/constructed/{}/daily_graphs'.format(location_folder_name, agglomeration_method))
	print('')

print(ident + 'All Done!')