# Prediction dataset constructor

# Imports all the necesary functions
import constructor_functions as constr


# Other imports
import os, sys

from pathlib import Path

import pandas as pd



# Reads the parameters from excecution
location_name  = sys.argv[1] # location namme
location_folder_name  = sys.argv[2] # location folder name
agglomeration_method = sys.argv[3] # Aglomeration name
days_back  = int(sys.argv[4]) # Days to include as history
days_ahead  = int(sys.argv[5]) # Days ahead to predict


 

# Sets the location
data_dir = Path(os.path.realpath(__file__)).parent.parent.parent
location_folder = os.path.join(data_dir, 'data/data_stages', location_folder_name)

# Constructed folder
constructed_folder = os.path.join(location_folder, 'constructed/', agglomeration_method)
if not os.path.exists(constructed_folder):
	os.makedirs(constructed_folder)

# Creates the folders if the don't exist
prediction_folder = os.path.join(location_folder, 'constructed', agglomeration_method, 'prediction/')
if not os.path.exists(prediction_folder):
	os.makedirs(prediction_folder)



ident = '         '


print(ident + 'Constructs for {}'.format(location_name))
print()
print(ident + 'Builds Prediction Data Bases')

# Loads Data
print()
print(ident + '   Loads Data:')

print(ident + '      Nodes')
nodes = pd.read_csv(os.path.join(constructed_folder, 'daily_graphs/nodes.csv'), parse_dates=['date_time'])

print(ident + '      Edges')
edges = pd.read_csv(os.path.join(location_folder, 'daily_graphs/edges.csv'), parse_dates=['date_time'])


print()
print(ident + '   Constructs:')


# Complete Graph
print(ident + '      Complete Graph')

df_graph_predict = constr.build_prediction_dataset_for_graphs(nodes = nodes, edges = edges, days_back = days_back, days_ahead = days_ahead)
df_graph_predict['location'] = location_name

df_graph_predict.to_csv(os.path.join(prediction_folder, 'graph_prediction_data.csv'), index = False)


# Slected Polygons
print(ident + '      Selected Polygons')

selected_polys_location = os.path.join(location_folder, 'agglomerated', agglomeration_method, 'selected_polygons.csv')

if not os.path.exists(selected_polys_location):
	selected_polys = pd.DataFrame(columns = ['poly_id', 'selected_poly_name'])
	selected_polys.to_csv(selected_polys_location, index = False)

else:
	selected_polys = pd.read_csv(selected_polys_location)


if selected_polys.shape[0] == 0:
	print(ident + '         No selected polygons. Please include some (if you wish) in: {}'.format('agglomerated/[METHOD]/selected_polygons.csv'))

else:
	print(ident + '         Found {} Selected Polygons'.format(selected_polys.shape[0]))

	dfs = []
	for inid, row in selected_polys.iterrows():
		
		print(ident + '            Building for: {}'.format(row.selected_poly_name))
		df_temp = constr.build_prediction_dataset_for_nodes(node_ids = [row.poly_id], nodes = nodes, edges = edges, days_back = days_back, days_ahead = days_ahead)
		df_temp['selected_poly_name'] = row.selected_poly_name

		dfs.append(df_temp)

	# Merges
	df_polys_predict = pd.concat(dfs, ignore_index = True)
	df_polys_predict.to_csv(os.path.join(prediction_folder, 'selected_polygons_prediction_data.csv'), index = False)

	print(ident + '         Done')



#Saves the Statistics
with open(os.path.join(prediction_folder, 'README.txt'), 'w') as file:

	file.write('Prediction Summary:' + '\n')
	file.write('   Parameters:' + '\n')
	file.write('      Days Back: {}'.format(days_back) + '\n')
	file.write('      Days Ahead: {}'.format(days_ahead) + '\n')
	file.write('   For Graph:' + '\n')
	file.write('      Graph Train Size: {}'.format(df_graph_predict.target_num_cases.dropna().size) + '\n')
	file.write('      Graph Train Features: {}'.format(df_graph_predict.shape[1] - 4) + '\n')
	file.write('      Graph Potential Prediction Days: {}'.format(int(df_graph_predict.target_num_cases.isna().sum())) + '\n')
	file.write('   For Selected Polygons:' + '\n')
	file.write('      Selected Polygons: {}'.format(selected_polys.shape[0]) + '\n')

	if selected_polys.shape[0] > 0:

		file.write('      Individual Train Size: {}'.format(df_temp.target_num_cases.dropna().size) + '\n')
		file.write('      Total Train Size: {}'.format(df_temp.target_num_cases.dropna().size*selected_polys.shape[0]) + '\n')
		file.write('      Train Features: {}'.format(df_temp.shape[1] - 4) + '\n')
		file.write('      Potential Prediction Days: {}'.format(int(df_temp.target_num_cases.isna().sum())) + '\n')


print(ident + 'Done! Data copied to: {}/constructed/{}/prediction'.format(location_folder_name))
print('')
