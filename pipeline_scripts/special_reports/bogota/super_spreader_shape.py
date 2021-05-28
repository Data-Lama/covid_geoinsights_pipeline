import os
import sys
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Point

# Local imports 
import special_functions.geo_functions as gfun

# Global Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')

indent = '   '
RADIUS = 200    # Agglomeration radius

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

# Get manzanas shapefile
manzanas_path = os.path.join(data_dir,
                            "data_stages",                            
                            location_folder_name,
                            "raw",
                            "geo",
                            "manzanas_bogota_DANE.shp")

gdf_manzanas = gpd.read_file(manzanas_path)


# Declares the export location
out_shapefile_folder = "shapefile_report"
out_shapefile_name = "superspreader_locations_report.shp"
export_folder_location = os.path.join(analysis_dir, location_folder_name, result_folder_name, out_shapefile_folder)

if not os.path.exists(export_folder_location):
    os.makedirs(export_folder_location)    

# ---------- FUNCTIONS ------------ #

def calcualte_rank(idxs, df):
    weights = {"Pagerank Top": 2,
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

# ---------------------------------- #

print(f"{indent}Creating superspreader polygons using {RADIUS} meters to agglomerate")

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

# translate to manzanas
print(f"{indent}{indent}Aggregating cityblocks")

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
print(f"{indent}{indent}Adding information about 'localidades' and 'UPZs'")
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
print(f"{indent}{indent}Caracterizing based on landmarks")
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

print(f"{indent}Saving")
out_file = os.path.join(export_folder_location, out_shapefile_name)
gdf_super_spreading_aggl_caracterized[columns_to_save].to_file(out_file, index=False)

# Heatmap
variables = ["COM-IND-TURI", "FUN-PUB", "SALUD", "TRANS", "EDUC", "TRANSMI"]
df_heatmap = gdf_super_spreading_aggl_caracterized[variables].copy()
df_heatmap = df_heatmap.fillna(0)
for col in df_heatmap.columns:
    df_heatmap[col] = df_heatmap.apply(lambda x: 0 if x[col] == 0 else 1, axis=1)

df_heatmap = df_heatmap.transpose()


fig, ax = plt.subplots(1,1, figsize=(5,5))
ax.imshow(df_heatmap, cmap='GnBu')
ax.set_xticks(np.arange(0, df_heatmap.shape[1]))
ax.xaxis.label.set_color('w')
ax.set_yticks(np.arange(0, len(variables)))
ax.yaxis.label.set_color('w')
ax.set_yticklabels(variables)
out_file = os.path.join(os.path.join(export_folder_location, "attributes.png"))
fig.savefig(out_file, bbox_inches='tight')