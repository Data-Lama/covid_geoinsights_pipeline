# Movement range functions
import geopandas as gpd
from shapely import wkt
import pandas as pd
import numpy as np
import geopandas
import time
import sys
import os

# Direcotries
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')
key_string = config.get_property('key_string') 

ident = '         '


def get_population_density(polygon, gdf):
    polygons = gdf[['poly_id', 'attr_population', 'attr_area', 'geometry']].copy()
    polygons["area_poly"] = polygons["geometry"].area
    polygons["population_density"] = polygons["attr_population"].divide(polygons["area_poly"])
    intersections = polygons['geometry'].intersection(polygon)
    df_intersections = pd.DataFrame({"poly_id":polygons["poly_id"].unique()})
    df_intersections["intersection"] = intersections
    df_intersections["intersection_area"] = intersections.area
    df_intersections["intersection_empty"] = ~intersections.is_empty
    df_intersections = df_intersections[df_intersections["intersection_empty"] == True]
    area = df_intersections["intersection_area"].sum()
    if area == 0:
        return np.nan
    df_intersections = df_intersections.merge(polygons, how="outer", on="poly_id").dropna()
    df_intersections.drop(columns=["attr_population", "attr_area", 
                                   "intersection_empty", "area_poly",
                                  "geometry"], inplace=True)
    df_intersections["population"] = df_intersections["intersection_area"].multiply(df_intersections["population_density"])
    df_intersections = df_intersections.replace([np.inf, -np.inf], np.nan).dropna(axis=0)
    pop_tot = df_intersections["population"].sum()
    pop_density = pop_tot / area
    
    return pop_density

def get_intersection_areas_pop_density(polygon, gdf):
    polygons = gdf[['external_polygon_id', 'geometry', 'population_density']].copy()
    polygons["area"] = polygons["geometry"].area
    polygons['population_density'] = polygons['population_density']
    intersections = polygons['geometry'].intersection(polygon)
    polygons['intersections'] = intersections
    df_polygons = polygons[~polygons['intersections'].is_empty].copy()
    df_polygons['area_intersection'] = polygons['intersections'].area
    df_polygons['area_proportion'] = df_polygons['area_intersection'].multiply(df_polygons['population_density'])
    df_polygons.drop(columns=['intersections', 'geometry', 'area_intersection', 'area', 'population_density'], inplace=True)
    df_polygons.dropna(inplace=True)
    return list(df_polygons.itertuples(index=False, name=None))

def get_intersection_areas(polygon, gdf):
    polygons = gdf[['external_polygon_id', 'geometry', 'population_density']].copy()
    polygons["area"] = polygons["geometry"].area
    polygons['population_density'] = polygons['population_density']
    intersections = polygons['geometry'].intersection(polygon)
    polygons['intersections'] = intersections
    df_polygons = polygons[~polygons['intersections'].is_empty].copy()
    df_polygons['area_intersection'] = polygons['intersections'].area
    df_polygons['area_proportion'] = df_polygons['area_intersection']
    df_polygons.drop(columns=['intersections', 'geometry', 'area_intersection', 'area'], inplace=True)
    df_polygons.dropna(inplace=True)
    return list(df_polygons.itertuples(index=False, name=None))

def calculate_movement(intersections, gdf):
    gdf = gdf.set_index('external_polygon_id')
    total_movement = 0
    total_factor = 0
    if intersections == []:
        return 0
    for i in intersections:
        external_id = i[0]
        factor = i[1]
        try:
            movement = gdf.at[external_id, 'all_day_bing_tiles_visited_relative_change']
            total_factor += factor
        except KeyError: 
            movement = 0
        total_movement += movement * factor
    if total_factor == 0:
        return np.nan
    return total_movement / total_factor

def construct_movement_range_by_polygon(df_movement_range, gdf_polygons, gdf_external_ids):
    '''
    Method that constructs the movent range by polygon ID based on its geometry.

    If polygons have the  population and size attribute, it should be done by density. Else
    will only look at the volumes.

    Parameters
    df_movement_range: pd.DataFrame
        Unified movement range file
    gdf_polygons : geopandas
        Geopandas dataframe with the polygons. It is assumed to be in crs epsg:4326.
    gdf_external_ids : geopandas
        Geopandas dataframe with the geometries of external ids. It is assumed to be in crs epsg:4326.
        At least has columns:
            - geometry
            - external_polygon_id

    Must return a dataframe with columns:
        - date_time: date of the movement range
        - poly_id: The id of the polygon 
        - movement_change: Relative movement change
    '''

    print("Constructing movement range by polygon.")
    print("{}Adjusting crs to equal area projection".format(ident))

    # modify df_polygons
    gdf_polygons.crs = 'epsg:4326'
    gdf_polygons.to_crs("epsg:3410", inplace=True)  # Use equal area projection

    # modify gdf_external_ids
    gdf_external_ids.crs = 'epsg:4326'
    gdf_external_ids.to_crs("epsg:3410", inplace=True)  # Use equal area projection

    # modify movement_range
    cols_to_drop = list(set(df_movement_range.columns) - set(["ds", "external_polygon_id", "all_day_bing_tiles_visited_relative_change"]))
    df_movement_range.drop(columns=cols_to_drop, inplace=True)

    if ("attr_population" in gdf_polygons.columns) and ("attr_area" in gdf_polygons.columns):
        
        # Add population_density to external ids
        print("{}Area and population attributes detected. Calculating population density".format(ident))
        start = time.time()
        gdf_external_ids["population_density"] = gdf_external_ids.apply(lambda x: get_population_density(x.geometry, gdf_polygons), axis=1).dropna()
        end = time.time()
        print("{}{}({}seconds to build population density)".format(ident, ident, end-start))

        # Calculate intersecitons between polygons and external polygons
        print("{}{}Calculating intersections for polygons".format(ident, ident))
        start = time.time()
        gdf_polygons["intersections"] = gdf_polygons.apply(lambda x: get_intersection_areas_pop_density(x.geometry, gdf_external_ids), axis=1)
        end = time.time()
        print("{}{}({} seconds to build intersections)".format(ident,ident, end-start))


        # Use intersections to calculate movement range
        df_movement_range_by_polygon = pd.DataFrame(columns=["date_time", "poly_id", "movement"])
        for d in df_movement_range["ds"].unique():
            gdf_movement_range_sm = df_movement_range[df_movement_range["ds"] == d].copy()
            print("{}{}Calculating movement range for {}".format(ident, ident, d))
            gdf_polygons["movement"] = \
                gdf_polygons.apply(lambda x: calculate_movement(x["intersections"], gdf_movement_range_sm), axis=1)
            gdf_polygons["date_time"] = d
            df_movement_range_by_polygon = pd.concat([df_movement_range_by_polygon, gdf_polygons], ignore_index=True, join="inner")
        return df_movement_range_by_polygon.dropna()

    else:
        print("{}Only area attribute detected. Processing by area.".format(ident))

        # Calculate intersecitons between polygons and external polygons
        print("{}{}Calculating intersections for polygons".format(ident, ident))
        start = time.time()
        gdf_polygons["intersections"] = gdf_polygons.apply(lambda x: get_intersection_areas(x.geometry, gdf_external_ids), axis=1)
        end = time.time()
        print("{}{}({} seconds to build intersections)".format(ident,ident, end-start))

        # Use intersections to calculate movement range
        df_movement_range_by_polygon = pd.DataFrame(columns=["date_time", "poly_id", "movement"])
        for d in df_movement_range["ds"].unique():
            gdf_movement_range_sm = df_movement_range[df_movement_range["ds"] == d].copy()
            print("{}{}Calculating movement range for {}".format(ident, ident, d))
            gdf_polygons["movement"] = \
                gdf_polygons.apply(lambda x: calculate_movement(x["intersections"], gdf_movement_range_sm), axis=1)
            gdf_polygons["date_time"] = d
            df_movement_range_by_polygon = pd.concat([df_movement_range_by_polygon, gdf_polygons], ignore_index=True, join="inner")

        return df_movement_range_by_polygon.dropna()
