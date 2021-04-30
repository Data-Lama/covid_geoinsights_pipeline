import os
import uuid
import datetime
import unidecode
import numpy as np
import pandas as pd
import cryptography
from shapely import wkt
import geopandas as gpd
import matplotlib.pyplot as plt
from cryptography.fernet import Fernet

# Module with general functions
from global_config import config
data_dir = config.get_property('data_dir')
data_folder = os.path.join(data_dir, 'data_stages')



def get_agglomeration_names(agglomeration_method):

    if agglomeration_method == 'radial':
        unit_type = 'Radial'
        unit_type_prural = 'Radiales'

    elif agglomeration_method == 'geometry':
        unit_type = 'Adminsitrativa'
        unit_type_prural = 'Administrativas'

    elif agglomeration_method == 'community':
        unit_type = 'Funcional' 
        unit_type_prural = 'Funcionales'

    else:
        raise ValueError(f'Unsupported agglomeration method: {agglomeration_method}')

    return(unit_type, unit_type_prural) 

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
    '''
    Encrypts a given dataframe
    '''
    
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

def get_neighbors_poly(poly_id, date_time, df_movement):
    df_neighbors = df_movement.loc[(df_movement['start_poly_id'] == poly_id) | (df_movement['end_poly_id'] == poly_id)]
    if df_neighbors.dropna().empty:
        return []
    else:
        df_neighbors = df_neighbors.loc[df_neighbors['date_time'] == date_time]
        if df_neighbors.dropna().empty:
            return []
        else:
            neighbors = pd.unique(df_neighbors[['start_poly_id', 'end_poly_id']].values.ravel('K'))
            return neighbors[neighbors != poly_id]


def smooth_curve(ser, days):
    '''
    Method that smoothes a curve given the days and preserves the integral
    '''
    
    total = ser.sum()
    
    # Smoothes
    resp =  ser.rolling(days,  min_periods=1).mean()
    resp = resp + (total - resp.sum())/resp.size
    
    return(resp)




def has_aglomeration(location, agglomeration_method):

    cases_location = os.path.join(data_folder, location, 'agglomerated', agglomeration_method, 'cases.csv')

    return(os.path.exists(cases_location))

def get_agglomeration_equivalence(location, agglomeration_method):
    '''
    Gets the equivalence agglomeration

    '''

    if has_aglomeration(location, agglomeration_method):
        return(agglomeration_method)

    if agglomeration_method == 'geometry' and has_aglomeration(location, 'radial'):
        return('radial')

    if agglomeration_method == 'radial' and has_aglomeration(location, 'geometry'):
        return('geometry')

    return(None)



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

    
    return(nodes, edges, node_locations)



def extract_connected_neighbors(location, poly_id, agglomeration_method, num_days = 30):
    '''
    Extracts the connected neighbors of the last 30 days

    '''
    nodes, edges, node_locations = get_graphs(agglomeration_method, location)

    if edges.shape[0] == 0:
    	# Returns the geographic neighbors
    	return(get_geographic_neighbors(poly_id, location, agglomeration_method))

    nodes.node_id = nodes.node_id.astype(str)
    edges.start_id = edges.start_id.astype(str)
    edges.end_id = edges.end_id.astype(str)

    # filters
    nodes = nodes[nodes.date_time >= (nodes.date_time.max() - datetime.timedelta(days = num_days))].copy()
    edges = edges[edges.date_time >= (edges.date_time.max() - datetime.timedelta(days = num_days))].copy()
    edges = edges[(edges.start_id == poly_id) | (edges.end_id == poly_id)].copy()

    final_edges = edges.groupby(['date_time','day','start_id','end_id']).mean().reset_index()

    # Only the ones with positive movement
    final_edges = final_edges[final_edges.movement > 0]

    # extracts neighbors
    neighbors = np.unique(np.concatenate((final_edges.start_id, final_edges.end_id)))

    # removes itself
    neighbors = neighbors[neighbors!= poly_id]

    return(neighbors.tolist())

'''
returns a geoDataFrame of geograohic neighbors for a given poly_id
'''
def get_geographic_neighbors(poly_id, location, agglomeration_method):


    polygons = os.path.join(data_folder, location, 'agglomerated', agglomeration_method, "polygons.csv")
    df_polygons = pd.read_csv(polygons)

    # Sets the poly id to string
    df_polygons.poly_id = df_polygons.poly_id.astype(str)

    df_polygons['geometry'] = df_polygons['geometry'].apply(wkt.loads)
    gdf_polygons = gpd.GeoDataFrame(df_polygons, geometry='geometry')
    gdf_polygons.set_index("poly_id", inplace=True)
    polygon = gdf_polygons.at[poly_id, "geometry"]
    intersections = gdf_polygons[gdf_polygons.geometry.touches(polygon)]
    intersections.reset_index(inplace=True)
    neighbors = intersections["poly_id"].astype("str")

    return neighbors.tolist()

#############################################################################################
############################## General Statistics functions #################################
#############################################################################################

def new_cases(df_cases, window):
    # Get the number of polygons that reported having their first case in the last 5 days
    today = datetime.datetime.today()
    x_days_ago = today - datetime.timedelta(days = window)
    historic = df_cases[df_cases['date_time'] < x_days_ago]
    historic_set = set(historic[historic["num_cases"] > 0]["poly_id"].unique())
    current_set = set(df_cases[df_cases["num_cases"] > 0]["poly_id"].unique())
    intersection = current_set.intersection(historic_set)

    new_case_polygon = current_set - intersection
    return new_case_polygon

def get_max_min(variable, df):
    max_index = df[variable].idxmax()
    min_index = df[variable].idxmin()
    max_info = {'poly_id':df.iloc[max_index]['poly_id'],
                'date':df.iloc[max_index]['date_time'],
                variable:df.iloc[max_index][variable]}
    min_info = {'poly_id':df.iloc[min_index]['poly_id'],
            'date':df.iloc[min_index]['date_time'],
            variable:df.iloc[min_index][variable]}

    return (max_info, min_info)

def get_day_max_min(variable, df):
    df_byday = df.groupby('date_time').sum()
    df_byday.reset_index(inplace=True)
    max_index = df_byday[variable].idxmax()
    min_index = df_byday[variable].idxmin()
    max_info = {'date':df_byday.iloc[max_index]['date_time'],
                variable:df_byday.iloc[max_index][variable]}
    min_info = {'date':df_byday.iloc[min_index]['date_time'],
            variable:df_byday.iloc[min_index][variable]}
    return (max_info, min_info)

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

def get_neighbor_cases_total(poly_id, date_time, df_movement, df_cases):
    neighbors = get_neighbors_poly(poly_id, date_time, df_movement)
    cases_neighbors = df_cases[df_cases["poly_id"].isin(neighbors)] 
    cases_neighbors = cases_neighbors[cases_neighbors["date_time"] == date_time]
    return cases_neighbors["num_cases"].sum()

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
    return df_nodes.groupby('node_id')['inner_movement'].std()

# returns dataframe with the average movement of the lowest 10 datapoints
def get_min_external_movement(df_edges):
    return NotImplemented

def get_mean_neighbor_movement(node_id, df_edges, level):
    keys = {"constructed": {"start":"start_id", "end":"end_id"},
            "agglomerated": {"start":"start_poly_id", "end":"end_poly_id"}}

    df_neighbors = df_edges.loc[(df_edges[keys[level["start"]]] == node_id) | (df_edges[keys[level["end"]]] == node_id)]
    if df_neighbors.dropna().empty:
        return 0
    else:
        df_neighbors = df_neighbors.groupby('date_time')['movement'].sum()
        if df_neighbors.dropna().empty:
            return 0
        else:
            return df_neighbors.mean()


def get_std_neighbor_movement(node_id, df_edges, level):
    keys = {"constructed": {"start":"start_id", "end":"end_id"},
            "agglomerated": {"start":"start_poly_id", "end":"end_poly_id"}}
    df_neighbors = df_edges.loc[(df_edges[keys[level["start"]]] == node_id) | (df_edges[keys[level["end"]]] == node_id)]
    if df_neighbors.dropna().empty:
        return 0
    else:
        df_neighbors = df_neighbors.groupby('date_time')['movement'].sum()
        if df_neighbors.dropna().empty:
            return 0
        else:
            return df_neighbors.std()

def get_external_movement_stats_overtime(df_nodes, df_edges, level):
    keys = {"constructed": {"id":"node_id"},
            "agglomerated": {"id":"poly_id"}}

    df_external_movement = pd.DataFrame({keys[level["id"]]:df_nodes[keys[level["id"]]].unique()})
    df_external_movement['mean_external_movement'] = df_external_movement.apply(lambda x: get_mean_neighbor_movement(x.node_id, df_edges, level), axis=1)
    df_external_movement['std_external_movement'] = df_external_movement.apply(lambda x: get_std_neighbor_movement(x.node_id, df_edges, level), axis=1)
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

def get_mean_movement_stats_overtime(df_movement):

    df_inner_movement = df_movement[df_movement['start_poly_id'] == df_movement['end_poly_id']].copy()
    df_external_movement = df_movement[df_movement['start_poly_id'] != df_movement['end_poly_id']].copy()
    df_external_movement = df_external_movement.groupby(["start_poly_id", "date_time"]).sum()
    df_external_movement.reset_index(inplace=True)

    df_inner_movement.rename(columns={"movement":"inner_movement", "start_poly_id":"poly_id"}, inplace=True)
    df_inner_movement.drop(columns=["end_poly_id"], inplace=True)
    df_external_movement.rename(columns={"movement":"external_movement", "start_poly_id":"poly_id"}, inplace=True)
    df_external_movement.drop(columns=["end_poly_id"], inplace=True)

    df_movement_stats = df_inner_movement.merge(df_external_movement, on=["poly_id", "date_time"], how="outer").fillna(0)
    return df_movement_stats.groupby("poly_id").mean()

def get_std_movement_stats_overtime(df_movement):

    df_inner_movement = df_movement[df_movement['start_poly_id'] == df_movement['end_poly_id']].copy()
    df_external_movement = df_movement[df_movement['start_poly_id'] != df_movement['end_poly_id']].copy()
    df_external_movement = df_external_movement.groupby(["start_poly_id", "date_time"]).sum()
    df_external_movement.reset_index(inplace=True)

    df_inner_movement.rename(columns={"movement":"inner_movement", "start_poly_id":"poly_id"}, inplace=True)
    df_inner_movement.drop(columns=["end_poly_id"], inplace=True)
    df_external_movement.rename(columns={"movement":"external_movement", "start_poly_id":"poly_id"}, inplace=True)
    df_external_movement.drop(columns=["end_poly_id"], inplace=True)

    df_movement_stats = df_inner_movement.merge(df_external_movement, on=["poly_id", "date_time"], how="outer").fillna(0)
    return df_movement_stats.groupby("poly_id").std()

# returns dataframe with the standard deviation movement
def get_std_external_movement(df_nodes):
    return df_nodes.groupby('node_id').std()