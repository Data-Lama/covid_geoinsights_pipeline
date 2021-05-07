# Script for detecting superspreading locations in Bogotá

# Loads the different libraries
import numpy as np
import pandas as pd
import igraph as ig
import seaborn as sns
import geopandas as gpd
import contextily as ctx
from datetime import timedelta
import matplotlib.pyplot as plt
from google.cloud import bigquery
import os, sys

import bigquery_functions as bqf
import graph_functions as grf


# Global Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')




# Starts the BigQuery Client
client = bigquery.Client(location="US")

# Constants
# -------------
date_format = "%Y-%m-%d"
eps = 0.001
total_places = 10
percentage = 0.1 # Percentage of top places
round_number = 4 # 15m
days_go_back = 15 # Two weeks
div = 1200 # in meters for the Soft Plus

ident = '   '

result_folder_name = "edgelist_detection"

# Constants for Bogota
location_name = "Bogotá"
location_folder_name = "bogota"
location_graph_id = "colombia_bogota"


def filter(df_geo, min_lat, max_lat, min_lon, max_lon):
    resp = df_geo[(df_geo.lat >= min_lat) & (df_geo.lat <= max_lat)].copy()
    resp = resp[(resp.lon >= min_lon) & (resp.lon <= max_lon)]
    return(resp)


# Declares the export location

# export location
export_folder_location = os.path.join(analysis_dir, 
                                        location_folder_name, 
                                        result_folder_name)

if not os.path.exists(export_folder_location):
    os.makedirs(export_folder_location)    


print(ident + f"Computing Edgelist Extraction for {location_name}")

print(ident + "   Extracts Max Date")
# Extracts the max support date
max_date = bqf.get_date_for_graph(client, location_graph_id)

# Min date will be two weeks before max_date
min_date = max_date - timedelta(days = days_go_back)


# Extracts
print(ident + "   Extracts the Distance to Infected")
df_dist_infected = bqf.get_distance_to_infected(client, location_id, min_date, max_date)
dist_infected_dates = df_dist_infected.date.unique() 


print(ident + f'   Computing Centralities from: {min_date.strftime(date_format)} to {max_date.strftime(date_format)}')


location_centrality = []

current_date = min_date

# Main Loop
while current_date <= max_date:


    # First extracts the date closest to the curren for distance to infected
    date_closest_dist_infected = dist_infected_dates[np.argmin([np.abs((current_date - d).days) for d in dist_infected_dates])]

    # Extracts the distaace to infected temporary
    df_dist_infected_temp = df_dist_infected[df_dist_infected.date = date_closest_dist_infected]
    
    print(ident + f'      Date: {current_date.strftime(date_format)}')

        
    for hour in range(6,22):
        
        print(ident + f'         Hour:{hour}')
        
        # Gets the contacts
        edges = bqf.get_contacts(dataset_id, location_id, current_date_string, hour)

        # Declares the edges
        nodes = pd.concat((edges[['id1']].rename(columns = {'id1':'identifier'}), edges[['id2']].rename(columns = {'id2':'identifier'})), ignore_index = True).drop_duplicates()
        
        # Adds distance to infected
        nodes = nodes.merge(df_dist_infected_temp[['identifier','distance_to_infected']], 
                            on = 'identifier',
                            how = 'left')

        nodes.fillna(np.inf, inplace = True)


        # Constructs weights
        # Nodes
        # Apply inverted soft_plus
        nodes['weight'] = np.log(1 + np.exp(-1*nodes.distance_to_infected/div))/np.log(2)

        # Edges
        # Groups
        compact_edges = edges[['id1','id2']].groupby(['id1','id2']).count().reset_index()
        compact_edges.columns = ['id1','id2','weight']

        # Gets the personalized pagerank
        centrality = grf.get_personalized_ppr(nodes, compact_edges, weighted_edges = True)

        # Adds it
        nodes['centrality'] = ppr

        # Sets the id 
        nodes.index = nodes.identifier

        # Sorts the original edges
        edges['centrality'] = nodes.loc[edges.id1,'centrality'].values*
                              nodes.loc[edges.id2,'centrality'].values

        # Selects the top places by percentage
        edges = edges.sort_values('centrality', ascending = False)
        
        # Extract location
        chunk = int(np.ceil(new_edges.shape[0]*percentage))
        df_temp = new_edges.iloc[0:chunk]

        # Selects lat and lon
        df_temp = df_temp[['lat','lon']].copy()
        df_temp['total'] = 1
        df_temp = df_temp.groupby(['lat','lon']).sum().reset_index()
        
        # Adds the timestamp
        df_temp['date'] = current_date
        df_temp['hour'] = hour              
        
        location_centrality.append(df_temp)

    
    current_date = current_date + timedelta(days = 1)


# Merges
df_centrality = pd.concat(location_centrality, ignore_index = True)


# NUM CONTACTS
# ---------------------------
# Calculate top places with more contacts
df_locations = get_contacs_by_location(dataset_id, 
                                        location_id, 
                                        min_date, 
                                        max_date, 
                                        round_to = 4)

df_locations = df_locations.head(total_places)

df1 = df_locations[['lat','lon','total_contacts']].rename(columns = {'total_contacts':'total'})

# Adds noise
df1.lat = df1.lat + np.random.normal(0,eps,df1.shape[0])
df1.lon = df1.lon + np.random.normal(0,eps,df1.shape[0])


geo_locations = gpd.GeoDataFrame(df1,crs =  "EPSG:4326", geometry=gpd.points_from_xy(df1.lon, df1.lat))
geo_locations = geo_locations.to_crs(epsg=3857)


# PAGERANK
# Top Places
# ---------------------------

# Calculate top places more frequently selected when removing 10% of edges
df2 = df_centrality[['lat','lon','total']].copy()
df2.lon = df2.lon.round(round_number)
df2.lat = df2.lat.round(round_number)

df2 = df2.groupby(['lat','lon']).sum().reset_index().sort_values('total', ascending = False).head(total_places)

df2.lat = df2.lat + np.random.normal(0,eps,df2.shape[0])
df2.lon = df2.lon + np.random.normal(0,eps,df2.shape[0])

geo_pagerank = gpd.GeoDataFrame(df2,crs =  "EPSG:4326", geometry=gpd.points_from_xy(df2.lon, df2.lat))
geo_pagerank = geo_pagerank.to_crs(epsg=3857)


# PAGERANK
# Traces
# ---------------------------

# Calculate places 
df3 = df_centrality[['lat','lon','total']].copy()
df3 = df3.groupby(['lat','lon']).sum().reset_index()

geo_pagerank_trace = gpd.GeoDataFrame(df3,crs =  "EPSG:4326", geometry=gpd.points_from_xy(df3.lon, df3.lat))
geo_pagerank_trace = geo_pagerank_trace.to_crs(epsg=3857)



# Plots
# ------------------
ax = geo_pagerank_trace.plot(figsize=(12, 12), alpha=0.1, markersize = 17, color = 'green', label = 'Pagrank (Traza)')
geo_pagerank.plot(alpha=1, markersize = 35, color = 'blue', ax = ax, label = 'Pagrank (Top)')
geo_locations.plot(alpha=1, markersize = 35, color = 'red', ax = ax, label = 'Contactos (Top)')
ctx.add_basemap(ax, source=ctx.providers.OpenTopoMap)
ax.set_axis_off()
ax.set_title(f'Superdispersión ({min_date.strftime(date_format)} - {max_date.strftime(date_format)})')
ax.legend()
ax.figure.savefig(os.path.join(export_folder_location, 'edge_detection.png'), dpi = 150)


df_pagerank = geo_pagerank.to_crs(epsg=4326)[['geometry']]
df_pagerank['tipo'] = 'Pagerank Top'

df_contactos = geo_locations.to_crs(epsg=4326)[['geometry']]
df_contactos['tipo'] = 'Contactos Top'    

df_pagerank_trace = geo_pagerank_trace.to_crs(epsg=4326)[['geometry']]
df_pagerank_trace['tipo'] = 'Pagerank Traza'

df_export = pd.concat((df_pagerank, df_contactos, df_pagerank_trace), ignore_index = False)
df_export['lon'] = df_export.geometry.x
df_export['lat'] = df_export.geometry.y

df_export.to_csv(os.path.join(export_folder_location, 'edge_detection.csv'), index = False)
    
    