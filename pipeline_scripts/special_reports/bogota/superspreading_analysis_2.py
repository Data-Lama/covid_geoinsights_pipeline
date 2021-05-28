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
from datetime import datetime

from matplotlib.patches import Patch
from matplotlib.lines import Line2D

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
days_go_back = 14 # Two weeks
div = 1200 # in meters for the Soft Plus

MAX_RESOLUTION = 5
MAX_DISTANCE = 3
MAX_TIME = 2

ident = '   '

result_folder_name = "edgelist_detection"

# Constants for Bogota
location_name = "Bogotá"
location_folder_name = "bogota"
location_graph_id = "colombia_bogota"

# Edgelist dataset
dataset_id = "edgelists_cities"


shapefile_folder = "shapefile"
shapefile_name = "superspreader_locations.shp"



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

# Creates the geofile export
if not os.path.exists(os.path.join(export_folder_location, shapefile_folder)):
    os.makedirs(os.path.join(export_folder_location, shapefile_folder))   


print(ident + f"Computing Edgelist Extraction for {location_name}")

print(ident + "   Extracts Max Date")
# Extracts the max support date
max_date = bqf.get_date_for_graph(client, location_graph_id)


# Min date will be two weeks before max_date
min_date = max_date - timedelta(days = days_go_back)

# Extracts
print(ident + "   Extracts the Distance to Infected")
df_dist_infected = bqf.get_distance_to_infected(client, location_graph_id, min_date, max_date)
df_dist_infected.date = pd.to_datetime(df_dist_infected.date)
dist_infected_dates = df_dist_infected.date.unique() 


print(ident + f'   Computing Centralities from: {min_date.strftime(date_format)} to {max_date.strftime(date_format)}')


location_centrality = []


# Main Loop
current_date = min_date
while current_date <= max_date:


    # First extracts the date closest to the curren for distance to infected
    date_closest_dist_infected = dist_infected_dates[np.argmin([np.abs((current_date - d).days) for d in dist_infected_dates])]
    
    # Extracts the distaace to infected temporary
    df_dist_infected_temp = df_dist_infected[df_dist_infected.date == date_closest_dist_infected]
    
    print(ident + f'      Date: {current_date.strftime(date_format)}')

        
    for hour in range(6,22):
        
        #print(ident + f'         Hour:{hour}')
        
        # Gets the contacts
        edges = bqf.get_contacts(client, 
                                 dataset_id, 
                                 location_graph_id, 
                                 current_date.strftime(date_format), 
                                 hour,
                                 max_resolution = MAX_RESOLUTION,
                                 max_distance = MAX_DISTANCE,
                                 max_time = MAX_TIME)

        # Declares the edges
        nodes_from_edges = pd.concat((edges[['id1']].rename(columns = {'id1':'identifier'}), edges[['id2']].rename(columns = {'id2':'identifier'})), ignore_index = True).drop_duplicates()
        
        # Adds distance to infected
        nodes = nodes_from_edges.merge(df_dist_infected_temp[['identifier','distance_to_infected']], 
                            on = 'identifier',
                            how = 'left')

        nodes.fillna(np.inf, inplace = True)


        # Constructs weights
        # Nodes
        # Apply inverted soft_plus
        nodes['weight'] = np.log(1 + np.exp(-1*nodes.distance_to_infected/div))/np.log(2)

        # Edges
        # Groups
        compact_edges = edges[['id1','id2']].copy()
        compact_edges['weight'] = 1
        compact_edges = compact_edges.groupby(['id1','id2']).count().reset_index()
        

        # Gets the personalized pagerank
        nodes['centrality'] = grf.get_personalized_ppr(nodes, compact_edges, weighted_edges = True)

        # Sets the id 
        nodes.index = nodes.identifier

        # Sorts the original edges
        edges['centrality'] = nodes.loc[edges.id1,'centrality'].values*nodes.loc[edges.id2,'centrality'].values

        # Selects the top places by percentage
        edges = edges.sort_values('centrality', ascending = False)
        
        # Extract location
        chunk = int(np.ceil(edges.shape[0]*percentage))
        df_temp = edges.iloc[0:chunk]

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



# --------------
# Maps

# Extracts the localities of bogota
geo_localidades = gpd.read_file(os.path.join(data_dir,'data_stages', location_folder_name,'raw/geo/','localities_shapefile.shp'),)
geo_localidades.geometry = geo_localidades.geometry.set_crs("EPSG:4326")

# Constructs boundingbox
centroids = geo_localidades.geometry.to_crs('EPSG:3395').centroid.to_crs("EPSG:4326")
min_lon = centroids.x.min()
max_lon = centroids.x.max()

min_lat = centroids.y.min()
max_lat = centroids.y.max()

geo_localidades = geo_localidades.to_crs(epsg=3857)

# Gets the superspreading polygons

# Polygons to include
# Historic superpreading
location_ids = ['colombia_bogota_super_spreading_1',
                'colombia_bogota_super_spreading_2',
                'colombia_bogota_super_spreading_4',
                'colombia_bogota_super_spreading_6',
                'colombia_bogota_super_spreading_8']

geo_ss_polygons = bqf.get_location_geometries_by_ids(client, location_ids)


# Health Polygons
location_ids = ['colombia_bogota_poligono_salud_1',
                'colombia_bogota_poligono_salud_2',
                'colombia_bogota_poligono_salud_3',
                'colombia_bogota_poligono_salud_4']

geo_health_polygons = bqf.get_location_geometries_by_ids(client, location_ids)



# NUM CONTACTS
# ---------------------------
# Calculate top places with more contacts
df_locations = bqf.get_contacs_by_location(client,
                                        dataset_id, 
                                        location_graph_id, 
                                        min_date, 
                                        max_date, 
                                        round_to = 4,
                                        max_resolution = MAX_RESOLUTION,
                                        max_distance = MAX_DISTANCE,
                                        max_time = MAX_TIME)

df_locations = df_locations.head(total_places)

df1 = df_locations[['lat','lon','total_contacts']].rename(columns = {'total_contacts':'total'})


# Maps
# -------------

# Crops to fit the localities
df1 = filter(df1, min_lat, max_lat, min_lon, max_lon)

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

# Crops
df2 = filter(df2, min_lat, max_lat, min_lon, max_lon)

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
             
# Crops
df3 = filter(df3, min_lat, max_lat, min_lon, max_lon)

geo_pagerank_trace = gpd.GeoDataFrame(df3,crs =  "EPSG:4326", geometry=gpd.points_from_xy(df3.lon, df3.lat))
geo_pagerank_trace = geo_pagerank_trace.to_crs(epsg=3857)


# Merges both
geo_top_both = pd.concat((geo_pagerank, geo_locations))



# Gets the boundary
centr = geo_localidades.unary_union.centroid
rotate = 0
markersize = 40
buffer_polygons = 500
buffer_lugares_superdispersion = 750

# Plots
print(ident + "   Plots")
# Localities
ax = geo_localidades.rotate(rotate, origin=centr).plot(figsize=(6, 10), color = "black", alpha = 0.6, zorder = 1)
geo_localidades.rotate(rotate, origin=centr).boundary.plot(ax = ax, color = "white", alpha = 0.4, zorder=1)


geo_pagerank_trace.rotate(rotate, origin=centr).plot(alpha=0.05, 
                                                     markersize = 12, 
                                                     color = 'Firebrick', 
                                                     ax = ax, 
                                                     label = 'Superdispersión')

df = pd.DataFrame({'geometry':[geo_top_both.rotate(rotate, origin=centr).buffer(buffer_lugares_superdispersion).unary_union]})
df = gpd.GeoDataFrame(df, geometry='geometry')
df.geometry = df.geometry.set_crs("EPSG:3857")

df_top_boundary = df.boundary

df_top_boundary.plot(alpha=1, 
                  color = 'red', 
                  ax = ax, 
                  label = 'Focos Superdispersión')


# Polygons
geo_health_polygons.rotate(rotate, origin=centr).buffer(buffer_polygons).plot(
                                                                        alpha=0.9,  
                                                                        color = 'blue', 
                                                                        ax = ax, 
                                                                        label = 'Pol. Salud')

geo_ss_polygons.rotate(rotate, origin=centr).buffer(buffer_polygons).plot(alpha=0.9,  
                                                                    color = 'yellow', 
                                                                    ax = ax, 
                                                                    label = 'Pol. Superdispersión Histórica')
                                                                      

ctx.add_basemap(ax, source=ctx.providers.OpenTopoMap)
ax.set_axis_off()
ax.set_title(f'Lugares de Superdispersión\n({min_date.strftime(date_format)} - {max_date.strftime(date_format)})')


legend_elements = [Line2D([0], [0], marker='o', color='w', label='Poligonos Salud', markerfacecolor='blue', markersize=10),
                   Line2D([0], [0], marker='o', color='w', label='Poligonos Superdispersión', markerfacecolor='yellow', markersize=10),
                   Line2D([0], [0], marker='o', color='w', label='Superdispersión', markerfacecolor='Firebrick', markersize=7),
                   Line2D([0], [0], color='red', lw=2, label='Focos Superdispersión')]

ax.legend(handles=legend_elements)


ax.figure.savefig(os.path.join(export_folder_location, 'superdispersion_bogota.png'), dpi = 150)


