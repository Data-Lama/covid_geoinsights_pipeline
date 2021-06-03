# Script for pagerank gini in bogota

# Loads the different libraries
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from google.cloud import bigquery
import os, sys
import pandas as pd

import bigquery_functions as bqf
import graph_functions as grf

from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')


client = bigquery.Client(location="US")


ident = '         '
result_folder_name = "superspreading"

# Constants for Bogota
location_name = "Bogotá"
location_folder_name = "bogota"
location_graph_id = "colombia_bogota"


# Edgelist dataset
attribute_id = "personalized_pagerank_gini_index"




def main():
    '''
    Plots attribute 
    '''

    print(ident + f"Extracts {attribute_id} for {location_graph_id}")
    # export location
    export_folder_location = os.path.join(analysis_dir, 
                                            location_folder_name, 
                                            result_folder_name)

    if not os.path.exists(export_folder_location):
        os.makedirs(export_folder_location)    


    df = bqf.get_graph_attribute(client, location_graph_id, attribute_id)


    fig, ax = plt.subplots(figsize=(12, 4), constrained_layout=True)
    ax.set(title=f"Histórico del Personalized Pagerank Gini en {location_name}")

    plt.plot(df.date, df.value, 'bo--', linewidth=2, markersize=8, alpha = 0.4)
    plt.grid()
    plt.xticks(rotation = 45) 
    plt.xlabel(xlabel = "Fecha")
    plt.ylabel(ylabel = "Personalized Pagerank Gini")

    # Labels
    locator = mdates.AutoDateLocator()
    formatter = mdates.ConciseDateFormatter(locator)
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(formatter)
        
    ax.figure.savefig(os.path.join(export_folder_location, 'ppr_gini.png'), dpi = 150)

    print(ident + "Done!")

# Runs the script
if __name__ == "__main__":

    main()