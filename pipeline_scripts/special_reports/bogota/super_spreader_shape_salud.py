import os
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Point

# Local imports 
import special_functions.utils as butils
import special_functions.geo_functions as gfun
import superspreading_analysis as ss_analysis

# Global Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')

sources = []
indent = '   '
RADIUS = 200    # Agglomeration radius in meters

# Constants for bogota
location_folder_name = "bogota"

# From edgelist_detection_bogota
result_folder_name = "edgelist_detection"
input_folder_location = os.path.join(analysis_dir, 
                                        location_folder_name, 
                                        result_folder_name)

shapefile_folder = "shapefile_colombia_bogota"
shapefile_name = "superspreader_colombia_bogota.shp"

# Get super_spreaders
gdf_super_spreading = gpd.read_file(os.path.join(input_folder_location, shapefile_folder, shapefile_name))

# Get cuadrantes salud
cudrantes_salud_path = os.path.join(data_dir, "data_stages", "bogota", "raw", "geo", "cuadrantes_salud.shp")
gdf_salud = gpd.read_file(cudrantes_salud_path)
  

# ---------- FUNCTIONS ------------ #

def calcualte_rank(idxs, df):
    weights = {"Pagerank Top": 1,
                "Contacts Top": 1}
    rank = 0
    for idx in idxs:
        r = df.at[idx, "total"]
        t = df.at[idx, "type"]
        rank += r*weights[t]

    return rank

def aggl_geo(idxs, df):
    poly_1 = df.at[idxs[0], "geometry"]
    if len(idxs) == 1:
        return poly_1
    poly_2 = df.at[idxs[1], "geometry"]
    polygon = poly_1.union(poly_2)
    if len(idxs) == 2:
        return polygon
    else:
        for idx in idxs[2:]:
            poly_x = df.at[idx, "geometry"]
            polygon = polygon.union(poly_x)  
        return polygon


# ---------------------------------- #

print(f"{indent}Creating superspreader polygons for Salud Quadrants. Using {RADIUS} meters to agglomerate")

# Join nearby superspreaders
gdf_super_spreading = gdf_super_spreading[gdf_super_spreading["type"].isin(['Pagerank Top', 'Contacts Top'])]
gdf_super_spreading["geometry"] = gdf_super_spreading.apply(lambda x: gfun.get_radius_meters(x["lon"], x["lat"], RADIUS), axis=1)

# Constructs intersections
intersections_dict = {}
for idx in gdf_super_spreading.index:
    intersections = []
    polygon = gdf_super_spreading.at[idx, "geometry"]
    intersection = gdf_super_spreading["geometry"].intersects(polygon)
    intersection_indices = gdf_super_spreading[intersection].index.values
    for i in intersection_indices:
        if i != idx:
            intersections.append(i)  

    intersections_dict[idx] = intersections


indices_to_skip = []
ranks = []
geometries = []
for idx in intersections_dict.keys():
    if idx in indices_to_skip:
        continue
    if intersections_dict[idx] == []:
        rank = gdf_super_spreading.at[idx, "total"]
        geo = gdf_super_spreading.at[idx, "geometry"]
        ranks.append(rank)
        geometries.append(geo)
    else:
        idxs = intersections_dict[idx]
        indices_to_skip += idxs
        idxs.append(idx)              # Add itself to idxs list
        rank = calcualte_rank(idxs, gdf_super_spreading)
        geo = aggl_geo(idxs, gdf_super_spreading)
        ranks.append(rank)
        geometries.append(geo)

df_super_spreading_aggl = pd.DataFrame({"rank": ranks, "geometry": geometries})
gdf_super_spreading_aggl = gpd.GeoDataFrame(df_super_spreading_aggl, geometry="geometry")

# translate to cuadrantes
print(f"{indent}{indent}Aggregating quadrants")

intersections_dict = {}
salud_quad_dict = {}
for idx in gdf_super_spreading_aggl.index:
    intersections = []
    polygon = gdf_super_spreading_aggl.at[idx, "geometry"]
    intersections = gdf_salud["geometry"].intersects(polygon)
    idxs = gdf_salud[intersections].index
    names = gdf_salud[intersections]["Id_Geo"].unique()
    names = ", ".join(names)
    geo = aggl_geo(idxs, gdf_salud)
    intersections_dict[idx] = geo
    salud_quad_dict[idx] = names


for idx in intersections_dict.keys():
    gdf_super_spreading_aggl.at[idx, "geometry"] = intersections_dict[idx]
    gdf_super_spreading_aggl.at[idx, "Id_Geo"] = salud_quad_dict[idx]


# Saves
# Declares the export location
print(f"{indent}{indent}Saves")
out_shapefile_folder = "shapefile_salud"
out_shapefile_name = "superspreader_locations_salud.shp"
export_folder_location = os.path.join(analysis_dir, location_folder_name, result_folder_name, out_shapefile_folder)

if not os.path.exists(export_folder_location):
    os.makedirs(export_folder_location)  

file_name = os.path.join(export_folder_location, shapefile_name)
gdf_super_spreading_aggl.to_file(file_name)

# Adds to export
sources.append(file_name)

# add export file info
butils.add_export_info(os.path.basename(__file__), sources)
