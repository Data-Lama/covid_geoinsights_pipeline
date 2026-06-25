from functools import partial

import pyproj
from shapely import geometry
from shapely.geometry import Point
from shapely.ops import transform
import geopandas as gpd
import pandas as pd
import numpy as np

import os


# Global Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')


def get_radius_meters(lon, lat, radius):
    '''
    Params: 
        - lon (Longitude in WGS84 projection)
        - lat (Latitude in WGS84 projection)
        - radius in meters
    '''

    local_azimuthal_projection = "+proj=aeqd +R=6371000 +units=m +lat_0={} +lon_0={}".format(
        lat, lon
    )
    wgs84_to_aeqd = partial(
        pyproj.transform,
        pyproj.Proj("+proj=longlat +datum=WGS84 +no_defs"),
        pyproj.Proj(local_azimuthal_projection),
    )
    aeqd_to_wgs84 = partial(
        pyproj.transform,
        pyproj.Proj(local_azimuthal_projection),
        pyproj.Proj("+proj=longlat +datum=WGS84 +no_defs"),
    )

    center = Point(float(lon), float(lat))
    point_transformed = transform(wgs84_to_aeqd, center)
    buffer = point_transformed.buffer(radius)

    # Get the polygon with lat lon coordinates
    circle_poly = transform(aeqd_to_wgs84, buffer)

    return circle_poly



def enrich_top_superspreading(gdf_super_spreading,
                              RADIUS = 200,
                              indent = '      '):
    '''
    Method that enriches the receives geofile with the surrounding elements, including
    landmarks, localities and UPZ.

    Supported only in Bogota
    '''

    # Reads the inner territories
    gdf_manzanas, gdf_upz, gdf_localities = get_bogota_territories()

    print(f"{indent}Creating superspreader polygons using {RADIUS} meters to agglomerate")

    # Join nearby superspreaders
    gdf_super_spreading = gdf_super_spreading[gdf_super_spreading["type"].isin(['Pagerank Top', 'Contacts Top'])]
    gdf_super_spreading["geometry"] = gdf_super_spreading.apply(lambda x: get_radius_meters(x["lon"], x["lat"], RADIUS), axis=1)

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

    # translate to manzanas
    print(f"{indent}   Aggregating cityblocks")

    intersections_dict = {}
    for idx in gdf_super_spreading_aggl.index:
        intersections = []
        polygon = gdf_super_spreading_aggl.at[idx, "geometry"]
        intersections = gdf_manzanas["geometry"].intersects(polygon)
        idxs = gdf_manzanas[intersections].index
        geo = aggl_geo(idxs, gdf_manzanas)
        intersections_dict[idx] = geo

    for idx in intersections_dict.keys():
        gdf_super_spreading_aggl.at[idx, "geometry"] = intersections_dict[idx]

    # --- LOCALIZAR (localidadesy UPZ) --- #
    print(f"{indent}   Adding information about 'localidades' and 'UPZs'")


    localities_dict = {}
    upz_dict = {}
    upz_names = {}
    for idx in gdf_super_spreading_aggl.index:
        intersections = []
        polygon = gdf_super_spreading_aggl.at[idx, "geometry"]
        intersections_locality = gdf_localities["geometry"].intersects(polygon)
        localidades = gdf_localities[intersections_locality].localidad.unique()
        localidades = ", ".join(localidades)
        localities_dict[idx] = localidades

        intersections_upz = gdf_upz["geometry"].intersects(polygon)
        upz_cod = gdf_upz[intersections_upz]["UPZ_code"].unique()
        upz_cod = ", ".join(upz_cod)
        upz_dict[idx] = upz_cod   
        upz_name = gdf_upz[intersections_upz]["UPZ_name"].unique()
        upz_name = ", ".join(upz_name)
        upz_names[idx] = upz_name 

    df_localidades = pd.DataFrame.from_dict(localities_dict, orient="index", columns=["localidad"])
    df_upz_dict = pd.DataFrame.from_dict(upz_dict, orient="index", columns=["UPZ_code"])
    df_upz_names = pd.DataFrame.from_dict(upz_names, orient="index", columns=["UPZ_name"])

    gdf_super_spreading_aggl = pd.concat([gdf_super_spreading_aggl, df_localidades, df_upz_dict, df_upz_names], axis=1)


    # ---------- CARACTERIZAR ------------ #
    print(f"{indent}   Caracterizing based on landmarks")
    
    gdf_usos_general = get_bogota_general_use()

    usos_dict = {}
    variables_usos = ["COM-IND-TURI", "FUN-PUB", "SALUD", "TRANS", "EDUC", "TRANSMI"]
    for idx in gdf_super_spreading_aggl.index:
        usos_details_dict = {}
        intersections = []
        polygon = gdf_super_spreading_aggl.at[idx, "geometry"]
        intersections = gdf_usos_general["geometry"].intersects(polygon)
        usos = gdf_usos_general[intersections]["NGeClasifi"].values
        usos_details = gdf_usos_general[intersections]["NGeNombre"].values
        for i, u in enumerate(usos):
            try:
                details = usos_details_dict[u]
                details.append(usos_details[i])
                usos_details_dict[u] = details
            except KeyError:
                usos_details_dict[u] = [usos_details[i]]
        for u in variables_usos:
            try:
                usos_details_dict[u] = ", ".join(usos_details_dict[u])
            except KeyError:
                usos_details_dict[u] = np.nan
        usos_dict[idx] = usos_details_dict

    df_usos = pd.DataFrame.from_dict(usos_dict, orient='index')

    # Concat and sort
    gdf_super_spreading_aggl_caracterized = pd.concat([gdf_super_spreading_aggl, df_usos], axis=1)
    gdf_super_spreading_aggl_caracterized.sort_values("rank", ascending=False, inplace=True)

    # Simplify rank
    gdf_super_spreading_aggl_caracterized["rank"] = range(1, gdf_super_spreading_aggl_caracterized.shape[0] + 1)
    gdf_super_spreading_aggl_caracterized["id"] = gdf_super_spreading_aggl_caracterized["rank"]

    # Save
    columns_to_save = ['id', 'rank', 'geometry', 'localidad', 'UPZ_code', 'UPZ_name',
        'COM-IND-TURI', 'FUN-PUB', 'SALUD', 'TRANS', 'EDUC', 'TRANSMI']

    
    gdf_super_spreading_aggl_caracterized.crs = "EPSG:4326"
    
    return(gdf_super_spreading_aggl_caracterized[columns_to_save])

   


# Support functions
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
    poly_2 = df.at[idxs[1], "geometry"]
    polygon = poly_1.union(poly_2)
    if len(idxs) == 2:
        return polygon
    else:
        for idx in idxs[2:]:
            poly_x = df.at[idx, "geometry"]
            polygon = polygon.union(poly_x)  
        return polygon


def get_bogota_territories():
    '''
    Returns blocks, upz and localities for bogota in geopandas format
    '''

    location_folder_name = 'bogota'

    # Get manzanas shapefile
    manzanas_path = os.path.join(data_dir,
                                "data_stages",                            
                                location_folder_name,
                                "raw",
                                "geo",
                                "manzanas_bogota_DANE.shp")

    gdf_manzanas = gpd.read_file(manzanas_path)

    # localidades y UPZs
    localidades_path = os.path.join(data_dir,
                                "data_stages",                            
                                location_folder_name,
                                "raw",
                                "geo",
                                "localities_shapefile.shp")

    gdf_localities = gpd.read_file(localidades_path)
    gdf_localities = gdf_localities[["label", "geometry"]]
    gdf_localities.rename(columns={"label": "localidad"}, inplace=True)

    upz_path = os.path.join(data_dir,
                                "data_stages",                            
                                location_folder_name,
                                "raw",
                                "geo",
                                "upz_tipo_1.shp")

    gdf_upz = gpd.read_file(upz_path)
    gdf_upz = gdf_upz[["UPlCodigo", "UPlNombre", "geometry"]]
    gdf_upz["UPlNombre"] = gdf_upz.apply(lambda x: (" ".join([f"{i[0]}{i[1:].lower()}" for i in x["UPlNombre"].split(" ")])), axis=1)
    gdf_upz.rename(columns={"UPlNombre": "UPZ_name", "UPlCodigo": "UPZ_code"}, inplace=True) 

    return(gdf_manzanas, gdf_upz, gdf_localities)


def get_bogota_general_use():
    '''
    Returns the geopandas dataframes with the general use of the territory in bogota
    '''
    location_folder_name = 'bogota'

    # Usos 
    usos_path = os.path.join(data_dir,
                                "data_stages",                            
                                location_folder_name,
                                "raw",
                                "socio_economic",
                                "Usos_Pipeline", 
                                "Usos_Pipeline.shp")

    gdf_usos = gpd.read_file(usos_path)
    gdf_usos = gdf_usos[["NGeNombre", "NGeClasifi", "geometry"]]

    # Transmi
    transmi_path = os.path.join(data_dir,
                                "data_stages",                            
                                location_folder_name,
                                "raw",
                                "socio_economic",
                                "Red_Transmilenio", 
                                "Red_Transmilenio.shp")

    gdf_transmi = gpd.read_file(transmi_path)
    gdf_transmi["NGeNombre"] = gdf_transmi.apply(lambda x: f"Estación {x.NTrNombre} - ({x.NTrCodigo})", axis=1)
    gdf_transmi["NGeClasifi"] = "TRANSMI"
    gdf_transmi = gdf_transmi[["NGeNombre", "NGeClasifi", "geometry"]]

    # Merge
    gdf_usos_general = pd.concat([gdf_usos, gdf_transmi], axis=0)    

    return(gdf_usos_general)