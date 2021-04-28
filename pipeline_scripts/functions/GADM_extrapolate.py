import os
import sys
import json
import datetime
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely import wkt
import matplotlib.pyplot as plt
from shapely.geometry import Point, LineString, Polygon

# Local functions
from pipeline_scripts.functions.geo_functions import get_GADM_polygon

# Direcotries
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')

# Reads the parameters from excecution
location_name   =  sys.argv[1] # location name
location_folder =  sys.argv[2] # polygon name
date_str        = sys.argv[3] # date

# Get files
movement_range_file = os.path.join(data_dir, 'data_stages', location_name, 'raw', 'movement_range', 'Movement Range_COL_gadm_2_{}.csv'.format(date_str))
polygons = os.path.join(data_dir, 'data_stages', location_name, 'agglomerated', location_folder, 'polygons.csv')
output = '/Local/Users/andrea/Desktop/tmp/intersections_3116.json'

# Load dfs and gdfs
# polygon geodataset has crs: {epsg:4326}
df_polygons = pd.read_csv(polygons)
df_polygons['geometry'] = df_polygons['geometry'].apply(wkt.loads)
gdf_polygons = gpd.GeoDataFrame(df_polygons, geometry='geometry')
gdf_polygons.crs = 'epsg:4326'
gdf_polygons = gdf_polygons.to_crs('epsg:3116')

# GADM geodataset originally crs = {epsg:4326}
df_movement_range = pd.read_csv(movement_range_file)
df_movement_range['geometry'] = df_movement_range.apply(lambda x: get_GADM_polygon(x.external_polygon_id), axis=1)
gdf_movement_range = gpd.GeoDataFrame(df_movement_range, geometry='geometry')
gdf_movement_range.crs = 'epsg:4326'
gdf_movement_range = gdf_movement_range.to_crs('epsg:3116')

# print('{}-{}'.format(gdf_movement_range.crs, gdf_polygons.crs))
# ax = gdf_movement_range.plot(column='all_day_bing_tiles_visited_relative_change', cmap='Reds', figsize=(15,9),
#                                      legend=True, linewidth=0.5)
# gdf_polygons.plot(edgecolor='black', facecolor='none', ax=ax, zorder=1)
# plt.show()



# Receives a polygon and a list of polygons and chooses the polygons in the list 
# that intersect with the polygon. Returns list of tuples (poly_id, area_of_intersection)
def get_intersection_areas(polygon, gdf):
    polygons = gdf[['external_polygon_id', 'geometry']]
    polygons['area'] = polygons['geometry'].area
    intersections = polygons['geometry'].intersection(polygon)
    polygons['intersections'] = intersections
    df_polygons = polygons[~polygons['intersections'].is_empty]
    df_polygons['area_intersection'] = polygons['intersections'].area
    df_polygons['area_proportion'] = df_polygons['area_intersection'].divide(df_polygons['area'])
    df_polygons.drop(columns=['intersections', 'geometry', 'area_intersection', 'area'], inplace=True)
    df_polygons.dropna(inplace=True)
    return list(df_polygons.itertuples(index=False, name=None))



intersections_dict = {}
for poly_id in gdf_polygons['poly_id'].unique():
    polygon = gdf_polygons.loc[gdf_polygons['poly_id'] == poly_id, 'geometry']
    polygon = polygon.to_numpy()[0]
    intersections = get_intersection_areas(polygon, gdf_movement_range)
    if len(intersections) > 0:
        intersections_dict[np.int64(poly_id).item()] = intersections


with open(output, 'w') as out:
    json.dump(intersections, out)

