import os
import pandas as pd
from shapely import wkt
import geopandas as gpd

# Direcotries
from global_config import config
data_dir = config.get_property('data_dir')

GADM = os.path.join(data_dir, "data_stages", "colombia", "raw", "geo", "gadm36_COL_shp", "gadm36_COL_2.shp")
polygons = os.path.join(data_dir, "data_stages", "colombia", "agglomerated", "geometry", "polygons.csv")
output_path = os.path.join(data_dir, "data_stages", "colombia", "raw", "geo", "gadm36_COL_shp", "gadm36_COL_2_population_density.csv")

gdf_GADM = gpd.read_file(GADM)
gdf_GADM.crs = 'epsg:4326'
gdf_GADM.to_crs("epsg:3410", inplace=True)  # Use equal area projection
df_polygons = pd.read_csv(polygons)
df_polygons['geometry'] = df_polygons['geometry'].apply(wkt.loads)
gdf_polygons = gpd.GeoDataFrame(df_polygons, geometry='geometry')
gdf_polygons.crs = 'epsg:4326'
gdf_polygons.to_crs("epsg:3410", inplace=True)  # Use equal area projection

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
    df_intersections = df_intersections.merge(polygons, how="outer", on="poly_id").dropna()
    df_intersections.drop(columns=["attr_population", "attr_area", 
                                   "intersection_empty", "area_poly",
                                  "geometry"], inplace=True)
    df_intersections["population"] = df_intersections["intersection_area"].multiply(df_intersections["population_density"])
    pop_tot = df_intersections["population"].sum()
    pop_density = pop_tot / area
    
    return pop_density

gdf_GADM["population_density"] = gdf_GADM.apply(lambda x: get_population_density(x.geometry, gdf_polygons), axis=1)
columns_to_drop = list(set(gdf_GADM.columns) - set(["GID_2", "geometry", "population_density"]))
gdf_GADM.drop(columns=columns_to_drop, inplace=True)
gdf_GADM.to_csv(output_path, index=False)
