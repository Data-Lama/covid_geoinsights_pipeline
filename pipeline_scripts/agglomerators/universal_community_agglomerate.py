# Universal Community Agglomeration script
# This script agglomerates a certain other agglomeration using the walking trap algorithm. 
# This implementation currently excecutes an outside R script for the polygon to community map.
# The attributes are agglomerated acording to the impemented attribute scheme (created in the unify step)

# Imports the necessary libraries
import pandas as pd
import numpy as np
import os
import sys

# Excecution Functions
import excecution_functions as ef

# Attribute agglomerator 
from attr_agglomeration_functions import *

# Directories
from global_config import config
data_dir = config.get_property('data_dir') # Data Directory

# Ident
ident = '      '

# Reads the parameter inputs
location_name  = sys.argv[1] # location name
location_folder_name  = sys.argv[2] # location folder name
source_agglomeration  = sys.argv[3] # Type of agglomeration to build upon (source)

# Debug

#location_name  = 'Colombia'
#location_folder_name  = 'colombia'
#source_agglomeration  = 'geometry'


# Agglomeration name
agglomeration_method = 'community' # Constant

print(ident + f'Computing Community agglomeration for: {location_name} over: {source_agglomeration}')
      
# Sets the location
location_folder = os.path.join(data_dir, 'data_stages', location_folder_name)

# Source and destination agglomeration
source_agglomeration_folder = os.path.join(location_folder, 'agglomerated/', source_agglomeration)
destination_agglomeration_folder = os.path.join(location_folder, 'agglomerated/', agglomeration_method)

# Checks if the agglomeration to build upon exists
if not os.path.exists(source_agglomeration_folder):
    raise ValueError(f'Agglomeration method: {source_agglomeration} does not exist for: {location_name}. Please compute it!')

# Creates the destination agglomeration folder
if not os.path.exists(destination_agglomeration_folder):
    os.makedirs(destination_agglomeration_folder)


# 1 -- Computes the Community map (Calls R script)
print(ident + '   Computing the Community Map (R Code)')
script_location = 'pipeline_scripts/agglomerators/'
name = 'compute_community_agglomeration_map'
code_type = 'R'
parameters = f'{location_name} {location_folder_name} {source_agglomeration}'

# Excecutes Script
resp = ef.excecute_script(script_location, name, code_type, parameters)

if resp != 0: # Error
    raise ValueError('Error when computing the Community Map. Will not agglomerate.')
    

# 2 -- Loads all the necessary files
print(ident + '   Loads Files')
# Cases
df_cases = pd.read_csv(os.path.join(source_agglomeration_folder, 'cases.csv'), parse_dates = ['date_time'], dtype = {'poly_id':str})

# Movement
df_movement = pd.read_csv(os.path.join(source_agglomeration_folder, 'movement.csv'), parse_dates = ['date_time'], dtype = {'start_poly_id':str, 'end_poly_id':str})

# Polygons
df_polygons = pd.read_csv(os.path.join(source_agglomeration_folder, 'polygons.csv'), dtype = {'poly_id':str})

# Population (Mock)
df_population = pd.read_csv(os.path.join(source_agglomeration_folder, 'population.csv'), parse_dates = ['date_time'], dtype = {'poly_id':str})

# Movement Range (if Exists)
df_movement_range = None
if os.path.exists(os.path.join(source_agglomeration_folder, 'movement_range.csv')):
    df_movement_range = pd.read_csv(os.path.join(source_agglomeration_folder, 'movement_range.csv'), parse_dates = ['date_time'], dtype = {'poly_id':str})

# Agglomeration Scheme
aggl_scheme = pd.read_csv(os.path.join(data_dir, 'data_stages', location_folder_name, 'unified/aggl_scheme.csv'))

# Community map
community_map = pd.read_csv(os.path.join(destination_agglomeration_folder, 'polygon_community_map.csv'), dtype = {'poly_id':str, 'community_id':str})

community_map = community_map[["poly_id","community_id","community_name"]].copy()



# 3 -- Agglomerates
print(ident + '   Agglomerates')

# Cases
# -----------
print(ident + '      Cases')
# Merges
df_cases_final = df_cases.merge(community_map, on = 'poly_id', how = 'left')

# Drops and renames
df_cases_final.drop(['poly_id','location'], axis = 1, inplace = True)
df_cases_final.rename(columns = {'community_id':'poly_id', 'community_name':'location'}, inplace = True)

# Groups by
groupby_cols = ['date_time','location','poly_id']
agglomerate_cols = df_cases_final.columns.drop(groupby_cols).values
df_cases_final = agglomerate(df_cases_final, aggl_scheme, groupby_cols, agglomerate_cols)


# Movement
# ---------------
print(ident + '      Movement')

# Merges
# Start id
df_movement_final = df_movement.merge(community_map[['poly_id','community_id']].rename(columns = {'poly_id':'start_poly_id'}), on = 'start_poly_id', how = 'left')
df_movement_final.drop(['start_poly_id'], axis = 1, inplace = True)
df_movement_final.rename(columns = {'community_id':'start_poly_id'}, inplace = True)

# End id
df_movement_final = df_movement_final.merge(community_map[['poly_id','community_id']].rename(columns = {'poly_id':'end_poly_id'}), on = 'end_poly_id', how = 'left')
df_movement_final.drop(['end_poly_id'], axis = 1, inplace = True)
df_movement_final.rename(columns = {'community_id':'end_poly_id'}, inplace = True)

# Groups by
groupby_cols = ['date_time','start_poly_id','end_poly_id']
agglomerate_cols = df_movement_final.columns.drop(groupby_cols).values
df_movement_final = agglomerate(df_movement_final, aggl_scheme, groupby_cols, agglomerate_cols)


# Polygons
# ------------
print(ident + '      Polygons')

# Merges
df_polygons_final = df_polygons.merge(community_map, on = 'poly_id', how = 'left')

# Drops and renames
df_polygons_final.drop(['poly_id','community_name'], axis = 1, inplace = True)
df_polygons_final.rename(columns = {'community_id':'poly_id'}, inplace = True)

# Groups by
groupby_cols = ['poly_id']
agglomerate_cols = df_polygons_final.columns.drop(groupby_cols).values
df_polygons_final = agglomerate(df_polygons_final, aggl_scheme, groupby_cols, agglomerate_cols)


# Population
# ---------------
print(ident + '      Population')

# Merges
df_population_final = df_population.merge(community_map[['poly_id','community_id']], on = 'poly_id', how = 'left')

# Drops and renames
df_population_final.drop(['poly_id'], axis = 1, inplace = True)
df_population_final.rename(columns = {'community_id':'poly_id'}, inplace = True)


# Groups by (Mock)
#groupby_cols = ['date_time','poly_id']
#agglomerate_cols = df_population_final.columns.drop(groupby_cols).values
#df_population_final = agglomerate(df_population_final, aggl_scheme, groupby_cols, agglomerate_cols)


# Movement Range
# ---------------
# Does the procedure manually, since the agglomeration does not have support for columnes outside the 
# dataframe.

df_movement_range_final = None
if df_movement_range is not None:
    
    print(ident + '      Movement Range')
    # Merges with original polygons
    df_movement_range_final = df_movement_range.merge(df_polygons, on = 'poly_id', how = 'left')

    # Merges with community map
    df_movement_range_final = df_movement_range_final.merge(community_map[['poly_id','community_id']], on = 'poly_id', how = 'left')

    # Drops and renames
    df_movement_range_final.drop(['poly_id'], axis = 1, inplace = True)
    df_movement_range_final.rename(columns = {'community_id':'poly_id'}, inplace = True)

    groupby_cols = ['date_time','poly_id']
    agglomerate_cols = ['movement_change']

    # Default option is average
    temp_scheme_dic = {'attr_name':'movement_change', 'aggl_function':'attr_average', 'secondary_attr': None, 'aggl_parameters': None}

    # Checks options
    if "attr_population" in df_movement_range_final: # By population
        temp_scheme_dic['aggl_function'] = 'attr_weighted_average'
        temp_scheme_dic['secondary_attr'] = 'attr_population'

    elif "attr_area" in df_movement_range_final: # By Area
        temp_scheme_dic['aggl_function'] = 'attr_weighted_average'
        temp_scheme_dic['secondary_attr'] = 'attr_area'

    # Declares Agglomeration Scheme
    temp_scheme = pd.DataFrame(temp_scheme_dic, index = [0])
    # Groups by
    df_movement_range_final = agglomerate(df_movement_range_final, temp_scheme, groupby_cols, agglomerate_cols)

    
# 4 -- Saves the dataframes
print(ident + '   Saves')
# Cases
df_cases_final.to_csv(os.path.join(destination_agglomeration_folder, 'cases.csv'), index = False)
# Movement
df_movement_final.to_csv(os.path.join(destination_agglomeration_folder, 'movement.csv'), index = False)
# Population
df_population_final.to_csv(os.path.join(destination_agglomeration_folder, 'population.csv'), index = False)
# Polygons
df_polygons_final.to_csv(os.path.join(destination_agglomeration_folder, 'polygons.csv'), index = False)

# Movement Change
if df_movement_range_final is not None:
    df_movement_range_final.to_csv(os.path.join(destination_agglomeration_folder, 'movement_range.csv'), index = False)
    
    
print(ident + 'Done')    