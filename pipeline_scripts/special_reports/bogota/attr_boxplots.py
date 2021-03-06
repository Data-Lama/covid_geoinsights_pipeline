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

attributes = ["number_of_contacts", "pagerank_gini_index", "personalized_pagerank_gini_index"]

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

ATTR_TRANSLATE = {"number_of_contacts": "Numero de contactos",
                 "pagerank_gini_index": "Indice Pagerank GINI",
                "personalized_pagerank_gini_index": "Indice Pagerank GINI (ponderado)"}

box_plots = True

# ------------- FUNCTIONS -------------- #


def get_window(window_start_date, date):
    for start_date in window_start_date:
        if date <= start_date:
            return start_date
    return 


# -------------------------------------- #

# Get args
location_folder_name = "bogota"             # sys.argv[1]
start_time = "2021-01-01"                   # sys.argv[2]
location_ids = cons.OBSERVATION_IDS         # sys.argv[3:]

# Declares the export location
export_folder_location = os.path.join(analysis_dir, location_folder_name, "attr_boxplots")

if not os.path.exists(export_folder_location):
    os.makedirs(export_folder_location)    

print(f"{indent}Plotting attributes per location id.")

# Gets data
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

print(f"{indent}Getting data")
query_job = client.query(query, job_config=job_config) 

# Return the results as a pandas DataFrame
df = query_job.to_dataframe()
df["date"] = df.apply(lambda x: pd.Timestamp(x["date"]), axis=1)

# sort
df_sort = df[["location_id", "attribute_name", "attribute_value"]].copy()
df_sort = df_sort[df_sort["attribute_name"] == "personalized_pagerank_gini_index"]
df_sort = df_sort.groupby("location_id")["attribute_value"].max()
order = df_sort.sort_values(ascending=False).reset_index()["location_id"].values



# Generate windows of desired length
prev_date = df.date.min()
window_start_date = []
while prev_date < df.date.max():
    window_start_date.append(prev_date)
    prev_date = prev_date + window

window_start_date.append(df.date.max())

# , aggfunc='first'
df_graph = df.pivot_table(values='attribute_value', index=['location_id', 'date'], columns='attribute_name')
df_graph.reset_index(inplace=True)
df_graph.sort_values(by="date", inplace=True)

df_graph["window"] = df_graph.apply(lambda x: get_window(window_start_date, x["date"]), axis=1)

columns = ['location_id','window']
for attr in attributes:
    columns.append(attr)

df_graph = df_graph[columns].copy()
for location in order:
    print(f"{indent}{indent}{location}")

    df_tmp = df_graph[df_graph["location_id"] == location].copy()

    # Create a figure instance
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(15, 15))
    fig.subplots_adjust(bottom=0.5)  

    axes = [ax1, ax2, ax3]


    for idx, attr in enumerate(attributes): 

        ax = axes[idx]
        data_to_plot = []
        
        for window in df_tmp.window.unique():
            df_window = df_tmp[df_tmp["window"] == window]
            data_to_plot.append(df_window[attr])
            

        # Create the boxplot
        medianprops = dict(linewidth=1.5, color='royalblue')
        bp = ax.boxplot(data_to_plot, showfliers=False, medianprops=medianprops)
        if idx != (len(axes) - 1):
            ax.set_xticklabels([])
        else:
            ax.set_xticklabels([d.strftime('%Y-%m-%d') for d in window_start_date], rotation=90)
            

        ax.set_title(ATTR_TRANSLATE[attr])
        
    plt.suptitle(cons.TRANSLATE[location], fontsize=18)
    
    file_name = os.path.join(export_folder_location,f"{location}_boxplot.png")
    fig.savefig(file_name, bbox_inches='tight')
    
    # Adds to export
    sources.append(file_name)    

    # add export file info
    butils.add_export_info(os.path.basename(__file__), sources)
    
