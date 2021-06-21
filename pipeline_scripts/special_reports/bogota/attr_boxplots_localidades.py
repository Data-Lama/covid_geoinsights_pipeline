import os
import sys
import pandas as pd
import matplotlib.pyplot as plt

from google.cloud import bigquery
from google.api_core.exceptions import BadRequest
from google.cloud.exceptions import NotFound

# local imports
import bogota_constants as cons
import special_functions.utils as butils

# Global Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')

# ------------- CONSTANTS -------------- #

attributes = ["pagerank_gini_index"]

sources = []
indent = '    '
window = pd.Timedelta(15, unit="d")

# Constants for plotting
WHIS = 1.5 # Determines how far boxplot whiskers extend
COLOR_CTRL = "#79c5b4"
COLOR_TRT = "#324592"
COLOR_HIHGLIGH = '#ff8c65'
PALETTE = ["#75D1BA", "#B0813B", "#A85238", "#18493F", "#32955E", 
          "#1C4054", "#265873", "#CF916E", "#DEAA9C", "#6B2624",
          "#BB3E94", "#2F8E31", "#CED175", "#85D686", "#A39FDF"]

ATTR_TRANSLATE = {"pagerank_gini_index": "Indice Pagerank GINI",
                "personalized_pagerank_gini_index": "Indice Personalized Pagerank GINI (ponderado)"}

box_plots = True

# ------------- FUNCTIONS -------------- #


def get_window(window_start_date, date):
    for start_date in window_start_date:
        if date <= start_date:
            return start_date
    return 


# -------------------------------------- #
localities = ['colombia_bogota_localidad_barrios_unidos',
        'colombia_bogota_localidad_bosa',
        'colombia_bogota_localidad_chapinero',
        'colombia_bogota_localidad_ciudad_bolivar',
        'colombia_bogota_localidad_engativa',
        'colombia_bogota_localidad_fontibon',
        'colombia_bogota_localidad_kennedy', 
        'colombia_bogota_localidad_los_martires',
        'colombia_bogota_localidad_puente_aranda',
        'colombia_bogota_localidad_rafael_uribe_uribe',
        'colombia_bogota_localidad_san_cristobal',
        'colombia_bogota_localidad_santa_fe',
        'colombia_bogota_localidad_suba',
        'colombia_bogota_localidad_teusaquillo',
        'colombia_bogota_localidad_tunjuelito',
        'colombia_bogota_localidad_usaquen',
        'colombia_bogota_localidad_usme',
        'colombia_bogota_localidad_antonio_narino',
        'colombia_bogota_localidad_candelaria']


# Get args
location_folder_name = "bogota"                 # sys.argv[1]
start_time = "2020-01-01"                       # sys.argv[2]
location_ids = localities                       # sys.argv[3:]

# Declares the export location
export_folder_location = os.path.join(analysis_dir, location_folder_name, "attr_boxplots")

if not os.path.exists(export_folder_location):
    os.makedirs(export_folder_location)    

print(f"{indent}Plotting pagerank gini for localities")

# Gets data
print(f"{indent}{indent}Getting data")
client = bigquery.Client(location="US")
job_config = bigquery.QueryJobConfig(allow_large_results = True)

where_parameters = [f'location_id = "{loc_id}"' for loc_id in location_ids]
where_parameters = " OR ".join(where_parameters)
where_parameters = f"({where_parameters.strip()})"

query = f"""
SELECT *
FROM grafos-alcaldia-bogota.graph_attributes.graph_attributes
WHERE 
    {where_parameters} AND
    date > "{start_time}"
"""

query_job = client.query(query, job_config=job_config) 

# Return the results as a pandas DataFrame
df = query_job.to_dataframe()
df["date"] = df.apply(lambda x: pd.Timestamp(x["date"]), axis=1)

# sort
df_sort = df[["location_id", "attribute_name", "attribute_value"]].copy()
df_sort = df_sort[df_sort["attribute_name"] == "personalized_pagerank_gini_index"]
df_sort = df_sort.groupby("location_id")["attribute_value"].max()
order = df_sort.sort_values(ascending=False).reset_index()["location_id"].values

localities = order

fig_1 = localities[0:7]
fig_2 = localities[7:13]     
fig_3 = localities[13:]

# Generate windows of desired length
prev_date = df.date.min()
window_start_date = []
while prev_date < df.date.max():
    window_start_date.append(prev_date)
    prev_date = prev_date + window

window_start_date.append(df.date.max())


df_graph = df.pivot_table(values='attribute_value', index=['location_id', 'date'], columns='attribute_name')
df_graph.reset_index(inplace=True)
df_graph.sort_values(by="date", inplace=True)
df_graph["window"] = df_graph.apply(lambda x: get_window(window_start_date, x["date"]), axis=1)

columns = ['location_id','window']
for attr in attributes:
    columns.append(attr)

df_graph = df_graph[columns].copy()


# Figure 1
# Create a figure instance
print(f"{indent}Plots")
fig, (ax1, ax2, ax3, ax4, ax5, ax6, ax7) = plt.subplots(7, 1, figsize=(20, 30))
fig.subplots_adjust(bottom=0.5)  
axes = [ax1, ax2, ax3, ax4, ax5, ax6, ax7]

for idx, loc in enumerate(fig_1): 
    df_tmp = df_graph[df_graph["location_id"] == loc].copy()

    ax = axes[idx]
    data_to_plot = []
    
    for window in df_tmp.window.unique():
        df_window = df_tmp[df_tmp["window"] == window]
        data_to_plot.append(df_window["pagerank_gini_index"])
        

    # Create the boxplot
    medianprops = dict(linewidth=1.5, color='royalblue')
    bp = ax.boxplot(data_to_plot, showfliers=False, medianprops=medianprops)
    if idx != (len(axes) - 1):
        ax.set_xticklabels([])
    else:
        ax.set_xticklabels([d.strftime('%Y-%m-%d') for d in window_start_date], rotation=90)
        

    ax.set_title(cons.TRANSLATE[loc])

file_name = os.path.join(export_folder_location,f"pr_gini_localities_1.png")
fig.savefig(file_name, bbox_inches='tight')  

# Adds to export
sources.append(file_name)

# Figure 2
# Create a figure instance
fig, (ax1, ax2, ax3, ax4, ax5, ax6) = plt.subplots(6, 1, figsize=(20, 30))
fig.subplots_adjust(bottom=0.5)  
axes = [ax1, ax2, ax3, ax4, ax5, ax6]

for idx, loc in enumerate(fig_2): 
    df_tmp = df_graph[df_graph["location_id"] == loc].copy()

    ax = axes[idx]
    data_to_plot = []
    
    for window in df_tmp.window.unique():
        df_window = df_tmp[df_tmp["window"] == window]
        data_to_plot.append(df_window["pagerank_gini_index"])
        

    # Create the boxplot
    medianprops = dict(linewidth=1.5, color='royalblue')
    bp = ax.boxplot(data_to_plot, showfliers=False, medianprops=medianprops)
    if idx != (len(axes) - 1):
        ax.set_xticklabels([])
    else:
        ax.set_xticklabels([d.strftime('%Y-%m-%d') for d in window_start_date], rotation=90)
        

    ax.set_title(cons.TRANSLATE[loc])
    

file_name = os.path.join(export_folder_location,f"pr_gini_localities_2.png")
fig.savefig(file_name, bbox_inches='tight')  

# Adds to export
sources.append(file_name)

# Figure 3
# Create a figure instance
fig, (ax1, ax2, ax3, ax4, ax5, ax6) = plt.subplots(6, 1, figsize=(20, 30))
fig.subplots_adjust(bottom=0.5)  
axes = [ax1, ax2, ax3, ax4, ax5, ax6]

for idx, loc in enumerate(fig_3): 
    df_tmp = df_graph[df_graph["location_id"] == loc].copy()

    ax = axes[idx]
    data_to_plot = []
    
    for window in df_tmp.window.unique():
        df_window = df_tmp[df_tmp["window"] == window]
        data_to_plot.append(df_window["pagerank_gini_index"])
        

    # Create the boxplot
    medianprops = dict(linewidth=1.5, color='royalblue')
    bp = ax.boxplot(data_to_plot, showfliers=False, medianprops=medianprops)
    if idx != (len(axes) - 1):
        ax.set_xticklabels([])
    else:
        ax.set_xticklabels([d.strftime('%Y-%m-%d') for d in window_start_date], rotation=90)
        

    ax.set_title(cons.TRANSLATE[loc])
print(f"{indent}Saves")   


file_name = os.path.join(export_folder_location,f"pr_gini_localities_3.png")
fig.savefig(file_name, bbox_inches='tight')  

# Adds to export
sources.append(file_name)

# add export file info
butils.add_export_info(os.path.basename(__file__), sources)