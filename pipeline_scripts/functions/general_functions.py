import datetime
import cryptography
from cryptography.fernet import Fernet
import uuid
import pandas as pd
import os
import unidecode


# Module with general functions
from global_config import config
data_dir = config.get_property('data_dir')


def clean_for_publication(s):
	'''
	Function that cleans a string for publications
	Mostly converts to spansh
	'''

	s = ' '.join([sub.title() for sub in s.split('_')])


	subs = {}
	subs['11001'] = 'Bogotá'
	subs['colombia'] = 'Colombia'
	subs['italy'] = 'Italia'
	subs['brazil'] = 'Brasil'
	subs['peru'] = 'Perú'
	subs[' Usa'] = ' EEUU'
	subs[' Us'] = ' EEUU'


	subs['Bogotá D.C.-Bogotá d C.'] = 'Bogotá'
	subs['-'] = ' - '

	for k in subs:
		s = s.replace(k, subs[k])


	return(s)

def create_folder_name(s):
	'''
	given a string, return a valid file name
	'''
	s = unidecode.unidecode(s)

	s = s.lower()
	s = s.replace('.','')

	splitting_strings = ['-',':','_']
	for sp in splitting_strings:
		s = s.split(sp)[0]

	# Strips
	s = s.strip()
	s = s.replace('   ',' ')
	s = s.replace('  ',' ')

	s = s.replace(' ','_')

	return(s)


def load_README(path):
	readme = {}
	with open(path, 'r') as f:
		for line in f:
			line = line.strip()
			line = line.split(':')
			key = "_".join(line[0].split(' '))
			value = ":".join(line[1:]).strip()
			readme[key] = value
	return readme
			

def encrypt_df(df, filename, key_string):
    
	# coverts to bytes
	key = bytes(key_string, 'utf-8')

	temp_file = '.'  + str(uuid.uuid4())

	df.to_csv(temp_file, index = False)

	#  Open the file to encrypt
	with open(temp_file, 'rb') as f:
		data = f.read()

	fernet = Fernet(key)
	encrypted = fernet.encrypt(data)

	# Write the encrypted file
	with open(filename, 'wb') as f:
		f.write(encrypted)

	os.remove(temp_file)

    
def decrypt_df(filename, key_string):
    
	# coverts to bytes
	key = bytes(key_string, 'utf-8')

	temp_file = '.'  + str(uuid.uuid4())

	#  Open the file to decrypt
	with open(filename, 'rb') as f:
	    data = f.read()

	fernet = Fernet(key)
	decrypted = fernet.decrypt(data)

	# Open the decrypted file
	with open(temp_file, 'wb') as f:
	    f.write(decrypted)

	df = pd.read_csv(temp_file, low_memory=False)

	os.remove(temp_file)

	return(df)


def get_description(location_folder_name):
	'''
	Gets the description of a place
	'''

	df_description = pd.read_csv(os.path.join(data_dir, 'data_stages', location_folder_name, 'description.csv'), index_col = 0)
	return(df_description)

def is_encrypted(location_folder_name):
	'''
	Checks if the location folder description has the encrypted tag
	'''
	tag = 'encrypted'

	df = get_description(location_folder_name)

	if tag in df.index and df.loc[tag,'value'].upper() == 'TRUE':
		return(True)

	return(False)

# Returns a list of the neighbors of a given node at a given time
def get_neighbors(node_id, date_time, df_edges):
    df_neighbors = df_edges.loc[(df_edges['start_id'] == node_id) | (df_edges['end_id'] == node_id)]
    if df_neighbors.dropna().empty:
        return None
    else:
        df_neighbors = df_neighbors.loc[df_neighbors['date_time'] == date_time]
        if df_neighbors.dropna().empty:
            return None
        else:
            neighbors = pd.unique(df_neighbors[['start_id', 'end_id']].values.ravel('K'))
            return neighbors[neighbors != node_id]

def get_neighbor_cases_average(neighbors, date_time, df_nodes):
    total = 0
    for node in neighbors:
        df_num_cases = df_nodes.loc[(df_nodes['node_id']==node) & (df_nodes['date_time']==date_time)].reset_index()
        total += df_num_cases.iloc[0]['num_cases']
    return total / len(neighbors)

def get_neighbors_cases_average(node_id, date_time, df_edges, df_nodes):
    neighbors = get_neighbors(node_id, date_time, df_edges)
    if neighbors is not None:
        return get_neighbor_cases_average(neighbors, date_time, df_nodes)

def get_mean_external_movement(node_id, time, df_edges):
    df_neighbors = df_edges.loc[(df_edges['start_id'] == node_id) | (df_edges['end_id'] == node_id)]
    if df_neighbors.dropna().empty:
        return 0
    else:
        df_neighbors = df_neighbors.loc[df_neighbors['date_time'] == time]
        if df_neighbors.dropna().empty:
            return 0
        else:
            return df_neighbors.sum()['movement']
		
def get_min_average_mov(df_node):
    smallest = df_node.nsmallest(10, 'inner_movement').mean()
    return smallest['inner_movement']

# returns dataframe with the average movement of the lowest 10 datapoints
def get_min_internal_movement(df_nodes):
	df_min_internal_movement = pd.DataFrame({'node_id': df_nodes['node_id'].unique()})
	df_min_internal_movement['min_avg_movement'] = df_min_internal_movement.apply(lambda x: get_min_average_mov(df_nodes[df_nodes['node_id'] == int(x.node_id)]), axis=1)
	return df_min_internal_movement

# returns dataframe with the average movement
def get_mean_internal_movement(df_nodes):
	return df_nodes.groupby('node_id')['inner_movement'].mean()

# returns dataframe with the standard deviation movement
def get_std_internal_movement(df_nodes):
	# print(node_id)
	# df = df_nodes.groupby('node_id')['inner_movement'].std()
	# print(df.at[node_id])
	return df_nodes.groupby('node_id')['inner_movement'].std()

# returns dataframe with the average movement of the lowest 10 datapoints
def get_min_external_movement(df_edges):
    return NotImplemented

def get_mean_neighbor_movement(node_id, df_edges):
    df_neighbors = df_edges.loc[(df_edges['start_id'] == node_id) | (df_edges['end_id'] == node_id)]
    if df_neighbors.dropna().empty:
        return 0
    else:
        df_neighbors = df_neighbors.groupby('date_time')['movement'].sum()
        if df_neighbors.dropna().empty:
            return 0
        else:
            return df_neighbors.mean()
        
def get_std_neighbor_movement(node_id, df_edges):
    df_neighbors = df_edges.loc[(df_edges['start_id'] == node_id) | (df_edges['end_id'] == node_id)]
    if df_neighbors.dropna().empty:
        return 0
    else:
        df_neighbors = df_neighbors.groupby('date_time')['movement'].sum()
        if df_neighbors.dropna().empty:
            return 0
        else:
            return df_neighbors.std()

def get_external_movement_stats_overtime(df_nodes, df_edges):
	df_external_movement = pd.DataFrame({'node_id':df_nodes['node_id'].unique()})
	df_external_movement['mean_external_movement'] = df_external_movement.apply(lambda x: get_mean_neighbor_movement(x.node_id, df_edges), axis=1)
	df_external_movement['std_external_movement'] = df_external_movement.apply(lambda x: get_std_neighbor_movement(x.node_id, df_edges), axis=1)
	df_external_movement['external_movement_one_std'] = df_external_movement['mean_external_movement'].add(df_external_movement['std_external_movement'])
	df_external_movement['external_movement_one-half_std'] = df_external_movement['external_movement_one_std'].add(df_external_movement['std_external_movement'].divide(2))
	df_external_movement['external_movement_two_std'] = df_external_movement['external_movement_one_std'].add(df_external_movement['std_external_movement'])
	return df_external_movement

def get_internal_movement_stats_overtime(df_nodes):
	df_internal_movement = pd.DataFrame({'node_id':df_nodes['node_id'].unique()})
	df_internal_movement['mean_internal_movement'] = df_internal_movement.apply(lambda x: get_mean_internal_movement(df_nodes).at[int(x.node_id)], axis=1)
	df_internal_movement['std_internal_movement'] = df_internal_movement.apply(lambda x: get_std_internal_movement(df_nodes).at[int(x.node_id)], axis=1)
	df_internal_movement['internal_movement_one_std'] = df_internal_movement['mean_internal_movement'].add(df_internal_movement['std_internal_movement'])
	df_internal_movement['internal_movement_one-half_std'] = df_internal_movement['internal_movement_one_std'].add(df_internal_movement['std_internal_movement'].divide(2))
	df_internal_movement['internal_movement_two_std'] = df_internal_movement['internal_movement_one_std'].add(df_internal_movement['std_internal_movement'])
	return df_internal_movement

# returns dataframe with the standard deviation movement
def get_std_external_movement(df_nodes):
	return df_nodes.group_by('node_id').std()


