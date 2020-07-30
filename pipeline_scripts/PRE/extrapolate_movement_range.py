import os
import sys
import numpy as np
import pandas as pd
from shapely import wkt
import geopandas as gpd

# Import local modules
from pipeline_scripts.functions.geo_functions import get_GADM_polygon

# Direcotries
from global_config import config
data_dir = config.get_property('data_dir')

# Reads the parameters from excecution
location_name  =  sys.argv[1] # location name
location_folder =  sys.argv[2] # polygon name

# Constants
indent = '         '

# Get name of files
constructed_file_path = os.path.join(data_dir, 'data_stages', location_name, 'constructed', location_folder, 'daily_graphs')
movement_range_dir = os.path.join(data_dir, 'data_stages', location_name, 'raw', 'movement_range')
output_file_path = os.path.join(data_dir, 'data_stages', location_name, 'raw', 'movement_range_by_{}'.format(location_folder))
polygons = os.path.join(data_dir, 'data_stages', location_name, 'agglomerated', location_folder, 'polygons.csv')

# Check if folder exists
if not os.path.isdir(output_file_path):
    os.makedirs(output_file_path)

original_movement_range_files = [f for f in os.listdir(movement_range_dir) if (os.path.isfile(os.path.join(movement_range_dir, f))) and (f[0] == 'M')]
transformed_movement_range_files = [f for f in os.listdir(output_file_path) if os.path.isfile(os.path.join(output_file_path, f)) and (f[0] == 'M')]

movement_range_files = np.setdiff1d(original_movement_range_files,transformed_movement_range_files)
print('{}Transforming {} movement range files.'.format(indent, len(movement_range_files)))

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

def calculate_movement(intersections, df_movement_range):
    df_movement_range = df_movement_range.set_index('external_polygon_id')
    total_movement = 0
    for i in intersections:
        external_id = i[0]
        factor = i[1]
        movement = df_movement_range.at[external_id, 'all_day_bing_tiles_visited_relative_change']
        total_movement += movement * factor

    return total_movement

def convert_movement_range(f, gdf_polygons):
    movement_range_file = os.path.join(movement_range_dir, f)
    output_file_name = os.path.join(output_file_path, f)
    df_movement_range = pd.read_csv(movement_range_file)

    if df_movement_range.empty:
        df_movement_range.to_csv(output_file_name, index=False)
    else:

        # GADM geodataset originally crs = {epsg:4326}
        df_movement_range = pd.read_csv(movement_range_file)
        df_movement_range['geometry'] = df_movement_range.apply(lambda x: get_GADM_polygon(x.external_polygon_id), axis=1)
        gdf_movement_range = gpd.GeoDataFrame(df_movement_range, geometry='geometry')
        gdf_movement_range.crs = 'epsg:4326'
        gdf_movement_range = gdf_movement_range.to_crs('epsg:3116')

        # This dict stores for each node_id, the GADM nodes that intersect and the area of intersection
        # intersections_dict = {}
        with open(output_file_name, 'w') as out:
            out.write("{},{}\n".format('poly_id', 'extrapolated_relative_movement'))
            for poly_id in gdf_polygons['poly_id'].unique():
                polygon = gdf_polygons.loc[gdf_polygons['poly_id'] == poly_id, 'geometry']
                polygon = polygon.to_numpy()[0]
                intersections = get_intersection_areas(polygon, gdf_movement_range)
                extrapolated_movement = calculate_movement(intersections, df_movement_range)
                out.write("{},{}\n".format(poly_id, extrapolated_movement))

    

# Load dfs and gdfs
# polygon geodataset has crs: {epsg:4326}
df_polygons = pd.read_csv(polygons)
df_polygons['geometry'] = df_polygons['geometry'].apply(wkt.loads)
gdf_polygons = gpd.GeoDataFrame(df_polygons, geometry='geometry')
gdf_polygons.crs = 'epsg:4326'
gdf_polygons = gdf_polygons.to_crs('epsg:3116')

for f in movement_range_files:
    f = f.strip()
    print('{}{}Transforming {}.'.format(indent, indent, f))
    convert_movement_range(f, gdf_polygons)