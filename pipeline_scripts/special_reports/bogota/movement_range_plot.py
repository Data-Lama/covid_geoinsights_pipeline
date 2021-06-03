import os
import sys
import math
import numpy as np
import pandas as pd
import networkx as nx
from shapely import wkt
import geopandas as gpd
import matplotlib.pyplot as plt

# Local imports
import bogota_constants as cons
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')

# Reads the parameters from excecution
location_name  =  sys.argv[1] # location name
agglomeration_folder = sys.argv[2] # agglomeration name

movement_path = os.path.join(data_dir, "data_stages", "colombia", "agglomerated", agglomeration_folder, "movement_range.csv")
out_folder = os.path.join(analysis_dir, location_name, agglomeration_folder, "movement_analysis")

# Check if folder exists
if not os.path.isdir(out_folder):
        os.makedirs(out_folder)

ciudades = {11001:"Bogota",
           76001:"Cali",
           8001: "Barranquilla",
           5001: "Medellin",
           13001: "Cartagena"}

ciudades_list = ["Bogota", "Cali", "Barranquilla", "Medellin", "Cartagena", "Otros"]
ciudades_colors = ["red", "green", "orange", "blue", "purple", "grey"]

df_movement_range["ciudad"] = df_movement_range.apply(lambda x: (ciudades[x.poly_id] if x.poly_id in ciudades.keys() else "Otro"), axis=1)
df_movement_range.drop(columns=["poly_id"], inplace=True)

df_movement_range_avg = df_movement_range.groupby(["ciudad", "date_time"]).mean().reset_index()
df_movement_range_avg["movement_change"] = 100*(1 + df_movement_range_avg["movement_change"])

fig, ax = plt.subplots(1, 1, figsize=(25,10))

for idx, ciudad in enumerate(ciudades_list):
    df = df_movement_range_avg[df_movement_range_avg["ciudad"] == ciudad]
    color = ciudades_colors[idx]
    if ciudad == "Bogota":
        plt.plot("date_time", "movement_change", data=df, color=color, alpha=1)
    else:
        plt.plot("date_time", "movement_change", data=df, alpha=0.6, color=color)
        
plt.legend(labels = ciudades_list, bbox_to_anchor=(1.05, 1.0), loc='upper left', fontsize=14)
plt.title("Movimiento con respecto a marzo 2020", fontsize=18)
plt.ylabel("Pocentaje (%)", fontsize=16)
plt.xticks(fontsize=16)
plt.yticks(fontsize=14)

plt.savefig(os.path.join(out_folder, "mov_range.png"), bbox_inches='tight')

