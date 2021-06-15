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

movement_path = os.path.join(data_dir, "data_stages", location_name, "agglomerated", agglomeration_folder, "movement.csv")
polygons_path = os.path.join(data_dir, "data_stages", location_name, "agglomerated", agglomeration_folder, "polygons.csv")
out_folder = os.path.join(analysis_dir, location_name, agglomeration_folder, "movement_analysis")

# Check if folder exists
if not os.path.isdir(out_folder):
        os.makedirs(out_folder)
        
# Load movement
df_movement = pd.read_csv(movement_path, parse_dates=["date_time"])

# Load geometry
df_polygons = pd.read_csv(polygons_path)
df_polygons["geometry"] = df_polygons["geometry"].apply(wkt.loads)
gdf_polygons = gpd.GeoDataFrame(df_polygons)

# ----------- CONSTANTS ----------- #

INDENT = "    "
ROL_AVG = 5
START_DATE = pd.Timestamp.today() - pd.to_timedelta(cons.WINDOW, unit='d')
if df_movement.date_time.max() < START_DATE:
    START_DATE = df_movement["date_time"].max() - pd.to_timedelta(cons.WINDOW, unit='d')
CMAP='YlOrRd'


# ----------- FUNCTIONS ----------- #

# If the combination is not recorded in the baseline, 1 is returned
def calculate_percentage(start_poly_id, end_poly_id, original_movement, df_baseline):
    try:
        mov = df_baseline.loc[(start_poly_id, end_poly_id)].movement
        return original_movement / mov
    except KeyError:
        return 1


# ----------- INTERNAL MOVEMENT ----------- #
print("\t" + INDENT + "Calculating internal movement")
df_interal_mov = df_movement[df_movement["start_poly_id"] == df_movement["end_poly_id"]].copy()
df_interal_mov.drop(columns=["end_poly_id"], inplace=True)
df_interal_mov.rename(columns={"start_poly_id": "poly_id"}, inplace=True)

# Generate baseline
baseline_date = pd.to_datetime(cons.BASELINE_DATE)
df_internal_mov_baseline = df_interal_mov[df_interal_mov["date_time"] <= baseline_date].copy()
df_internal_mov_baseline = df_internal_mov_baseline.groupby("poly_id")["movement"].mean()
df_internal_mov_baseline_dict = df_internal_mov_baseline.to_dict()

# Calculate percentage movement relative to baseline
df_interal_mov["percentage"] = df_interal_mov.apply(lambda x: (x.movement / df_internal_mov_baseline_dict[x.poly_id]), axis=1)
df_interal_mov["color"] = df_interal_mov.apply(lambda x: cons.COLORS[x.poly_id], axis=1)

# Smooth by calculating rolling average
df_interal_mov_rolavg = df_interal_mov.groupby("poly_id").rolling(ROL_AVG).mean().reset_index()
df_interal_mov_rolavg_dt = pd.merge(df_interal_mov_rolavg.set_index("level_1"), df_interal_mov[["date_time", "color"]], right_index=True, left_index=True).dropna()
df_interal_mov_rolavg_dt["percentage"] = (df_interal_mov_rolavg_dt["percentage"]*100)

# Pivot to ready df for plotting
df_interal_plot = df_interal_mov_rolavg_dt.rename(columns={"date_time":"Fecha", "poly_id":"Localidad"}).pivot(index='Fecha', columns='Localidad', values='percentage')

# Plot
print("\t" + INDENT + INDENT + "Plotting...")
df_interal_plot.rename(columns=cons.TRANSLATE, inplace=True)
df_interal_plot.plot(linewidth=1,figsize=(15,10), cmap=cons.CMAP)
plt.legend(bbox_to_anchor=(1.05, 1.0), loc='upper left')
plt.ylabel(f"Porcentaje con respecto a {cons.BASELINE_DATE}")
plt.title(f"Movimiento interno con respecto a {cons.BASELINE_DATE}")

# Savefig
plt.savefig(os.path.join(out_folder, "internal_mov_bog.png"), bbox_inches='tight')


# ----------- EXTERNAL MOVEMENT ----------- #
print("\t" + INDENT + "Calculating external movement")
df_external_mov = df_movement[df_movement["start_poly_id"] != df_movement["end_poly_id"]].copy()

# Baseline
df_external_mov_baseline = df_external_mov[df_external_mov["date_time"] <= cons.BASELINE_DATE].copy()
df_external_mov_baseline = df_external_mov_baseline.groupby(["start_poly_id", "end_poly_id"]).mean()
df_external_mov["percentage"] = df_external_mov.apply(lambda x: calculate_percentage(x.start_poly_id, x.end_poly_id, x.movement, df_external_mov_baseline), axis=1)

# Get last week data
df_external_mov_last_week = df_external_mov[df_external_mov["date_time"] >= START_DATE]
df_external_mov_last_week_avg = df_external_mov_last_week.groupby(["start_poly_id", "end_poly_id"]).mean()

# External movement distribution
localities_list = sorted(df_polygons.poly_id.unique(), key=str.lower)
movement_dict = {}
for poly_id in localities_list:
    values = []
    for end_id in localities_list:
        if end_id == poly_id:
            values.append(0)
            continue
        try:
            mov = df_external_mov_last_week_avg.at[(poly_id, end_id), "movement"]
        except KeyError:
            mov = 0
        values.append(mov)
        movement_dict[poly_id] = values
    
                       
index = [cons.TRANSLATE[x] for x in localities_list]
df_movement_dist = pd.DataFrame(movement_dict, index=index).rename(columns=cons.TRANSLATE).transpose()

# Incoming and outgoing movement distribution per polygon
df_incoming_dist = df_movement_dist.transpose()
df_incoming_dist = df_incoming_dist / df_incoming_dist.sum(axis=0)

df_outgoing_dist = df_movement_dist
df_outgoing_dist = df_outgoing_dist / df_outgoing_dist.sum(axis=0)

# Plot
print("\t" + INDENT + INDENT + "Plotting...")
fig, (ax1) = plt.subplots(1, 1, figsize=(10,10))
df_incoming_dist.transpose().plot.bar(stacked=True, ax=ax1, cmap=cons.CMAP, width=0.8)
plt.legend(bbox_to_anchor=(1.05, 1.0), loc='upper left')
plt.title("Porcentaje de movimiento incidente por localidad")

# Save figure
plt.savefig(os.path.join(out_folder, "incident_mov_bog.png"), bbox_inches='tight')

# ----------- MOVEMENT INTO AND OUT-OF ----------- #
# --------------AND STAY-IN POLYGON -------------- #
print("\t" + INDENT + "Calculating movement relative to polygon")

# Movement into polygon
df_movement_into_polygon = df_external_mov_last_week.groupby(["end_poly_id", "date_time"])["movement"].sum().reset_index()
df_movement_into_polygon.rename(columns={"end_poly_id": "poly_id"}, inplace=True)

# Movement into polygon weekday and weekend
df_movement_into_polygon["weekday"] = df_movement_into_polygon.apply(lambda x: x.date_time.weekday(), axis=1)

df_movement_into_polygon_weekday = df_movement_into_polygon[df_movement_into_polygon["weekday"].isin(cons.WEEKDAY_LIST)]
df_movement_into_polygon_weekday_avg = df_movement_into_polygon_weekday.groupby("poly_id")["movement"].mean().reset_index()
df_movement_into_polygon_weekend = df_movement_into_polygon[df_movement_into_polygon["weekday"].isin(cons.WEEKEND_LIST)]
df_movement_into_polygon_weekend_avg = df_movement_into_polygon_weekend.groupby("poly_id")["movement"].mean().reset_index()

# Movement out of polygon
df_movement_outof_polygon = df_external_mov_last_week.groupby(["start_poly_id", "date_time"])["movement"].sum().reset_index()
df_movement_outof_polygon.rename(columns={"start_poly_id": "poly_id"}, inplace=True)

# Movement out of polygon weekday adn weekedn
df_movement_outof_polygon["weekday"] = df_movement_outof_polygon.apply(lambda x: x.date_time.weekday(), axis=1)

df_movement_outof_polygon_weekday = df_movement_outof_polygon[df_movement_outof_polygon["weekday"].isin(cons.WEEKDAY_LIST)]
df_movement_outof_polygon_weekday_avg = df_movement_outof_polygon_weekday.groupby("poly_id")["movement"].mean().reset_index()
df_movement_outof_polygon_weekend = df_movement_outof_polygon[df_movement_outof_polygon["weekday"].isin(cons.WEEKEND_LIST)]
df_movement_outof_polygon_weekend_avg = df_movement_outof_polygon_weekend.groupby("poly_id")["movement"].mean().reset_index()

# Stay in polygon 
df_movement_stayin_polygon = df_interal_mov[df_interal_mov["date_time"] >= START_DATE].copy()
df_movement_stayin_polygon["weekday"] = df_movement_stayin_polygon.apply(lambda x: x.date_time.weekday(), axis=1)

df_movement_stayin_polygon_weekday = df_movement_stayin_polygon[df_movement_stayin_polygon["weekday"].isin(cons.WEEKDAY_LIST)]
df_movement_stayin_polygon_weekday_avg = df_movement_stayin_polygon_weekday.groupby("poly_id")["movement"].mean().reset_index()
df_movement_stayin_polygon_weekend = df_movement_stayin_polygon[df_movement_stayin_polygon["weekday"].isin(cons.WEEKEND_LIST)]
df_movement_stayin_polygon_weekend_avg = df_movement_stayin_polygon_weekend.groupby("poly_id")["movement"].mean().reset_index()

# Normalize by grouping per category
# mov out of polygon
max_mov = max(df_movement_outof_polygon_weekend_avg["movement"].max(), df_movement_outof_polygon_weekday_avg["movement"].max())
min_mov = min(df_movement_outof_polygon_weekend_avg["movement"].min(), df_movement_outof_polygon_weekday_avg["movement"].min())

df_movement_outof_polygon_weekend_avg["movement_norm"] = (df_movement_outof_polygon_weekend_avg["movement"] - min_mov) / (max_mov - min_mov)
df_movement_outof_polygon_weekday_avg["movement_norm"] = (df_movement_outof_polygon_weekday_avg["movement"] - min_mov) / (max_mov - min_mov)

# mov into polygon
max_mov = max(df_movement_into_polygon_weekend_avg["movement"].max(), df_movement_into_polygon_weekday_avg["movement"].max())
min_mov = min(df_movement_into_polygon_weekend_avg["movement"].min(), df_movement_into_polygon_weekday_avg["movement"].min())

df_movement_into_polygon_weekend_avg["movement_norm"] = (df_movement_into_polygon_weekend_avg["movement"] - min_mov) / (max_mov - min_mov)
df_movement_into_polygon_weekday_avg["movement_norm"] = (df_movement_into_polygon_weekday_avg["movement"] - min_mov) / (max_mov - min_mov)

# mov stay in polygon
max_mov = max(df_movement_stayin_polygon_weekend_avg["movement"].max(), df_movement_stayin_polygon_weekday_avg["movement"].max())
min_mov = min(df_movement_stayin_polygon_weekend_avg["movement"].min(), df_movement_stayin_polygon_weekday_avg["movement"].min())

df_movement_stayin_polygon_weekend_avg["movement_norm"] = (df_movement_stayin_polygon_weekend_avg["movement"] - min_mov) / (max_mov - min_mov)
df_movement_stayin_polygon_weekday_avg["movement_norm"] = (df_movement_stayin_polygon_weekday_avg["movement"] - min_mov) / (max_mov - min_mov)


# Consolidate to plot
df_movement_consolidated = pd.merge(df_movement_outof_polygon_weekend_avg.rename(columns={"movement": "out_of_mov_weekend", 
                                                                                 "movement_norm": "out_of_mov_weekend_nor"}), 
                           df_movement_into_polygon_weekend_avg.rename(columns={"movement": "into_mov_weekend", 
                                                                                 "movement_norm": "into_mov_weekend_nor"}), on="poly_id", how="outer")

df_movement_consolidated = pd.merge(df_movement_consolidated, df_movement_outof_polygon_weekend_avg.rename(columns={"movement": "out_of_mov_weekday", 
                                                                                 "movement_norm": "out_of_mov_weekday_nor"}), on="poly_id", how="outer")
                                    
df_movement_consolidated = pd.merge(df_movement_consolidated, df_movement_into_polygon_weekday_avg.rename(columns={"movement": "into_mov_weekday", 
                                                                                 "movement_norm": "into_mov_weekday_nor"}), on="poly_id", how="outer")

df_movement_consolidated = pd.merge(df_movement_consolidated, df_movement_stayin_polygon_weekday_avg.rename(columns={"movement": "stayin_mov_weekday", 
                                                                                 "movement_norm": "stayin_mov_weekday_nor"}), on="poly_id", how="outer")

df_movement_consolidated = pd.merge(df_movement_consolidated, df_movement_stayin_polygon_weekend_avg.rename(columns={"movement": "stayin_mov_weekend", 
                                                                                 "movement_norm": "stayin_mov_weekend_nor"}), on="poly_id", how="outer")
# Plot
# Plot
gdf_movement_consolidated = gdf_polygons[["poly_id", "geometry"]].merge(df_movement_consolidated, on="poly_id", how="outer")
fig, ((ax1, ax2), (ax3, ax4), (ax5, ax6)) = plt.subplots(3, 2, figsize=(5,15))
axes = [ax1, ax2, ax3, ax4, ax5, ax6]


plot_order = ["out_of_mov_weekday_nor",
                 "out_of_mov_weekend_nor",
                 "into_mov_weekday_nor",
                 "into_mov_weekend_nor",
                 "stayin_mov_weekday_nor",
                 "stayin_mov_weekend_nor",]

titles = {"out_of_mov_weekday_nor": "Entre semana",
                 "out_of_mov_weekend_nor": "Fin de semana",
                 "into_mov_weekday_nor": "Entre semana",
                 "into_mov_weekend_nor": "Fin de semana",
                 "stayin_mov_weekday_nor": "Entre semana",
                 "stayin_mov_weekend_nor": "Fin de semana"}

y_axis = {"out_of_mov_weekday_nor": "Movimiento saliente",
                 "out_of_mov_weekend_nor": "Movimiento saliente",
                 "into_mov_weekday_nor": "Movimiento incidente",
                 "into_mov_weekend_nor": "Movimiento incidente",
                 "stayin_mov_weekday_nor": "Movimiento interno",
                 "stayin_mov_weekend_nor": "Movimiento interno"}

for idx, col in enumerate(plot_order):
    ax = axes[idx]
    ax.set_title(titles[col])
    ax.axes.get_xaxis().set_visible(False)
    if idx % 2 == 0:
        gdf_movement_consolidated.plot(column=col, ax=ax, cmap=CMAP, missing_kwds=cons.MISSING_KWDS, legend=False, vmin=0, vmax=1)
        ax.axes.set_ylabel(y_axis[col])
        ax.yaxis.set_ticks([])
        ax.yaxis.set_ticklabels([])
    else:
        gdf_movement_consolidated.plot(column=col, ax=ax, cmap=CMAP, missing_kwds=cons.MISSING_KWDS, legend=True, vmin=0, vmax=1)
        ax.axes.get_yaxis().set_visible(False)


# Save figure
plt.savefig(os.path.join(out_folder, "movimiento.png"), bbox_inches='tight')