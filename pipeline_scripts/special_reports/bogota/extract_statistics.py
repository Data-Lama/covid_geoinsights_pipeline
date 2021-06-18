# Script for statistics extraction for bogota

# Loads the different libraries
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from google.cloud import bigquery
import os, sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import special_functions.utils as butils

import bigquery_functions as bqf
import graph_functions as grf

from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')


client = bigquery.Client(location="US")


ident = '         '
result_folder_name = "statistics"

# Constants for Bogota
location_name = "Bogotá"
location_folder_name = "bogota"
location_graph_id = "colombia_bogota"



def main():
    '''
    Extracts the different statistics directly for the bogota weekly report
    '''

    print(ident + f"Extracts statistics for {location_graph_id}")

    # Export folder
    export_folder_location = os.path.join(analysis_dir, 
                                            location_folder_name, 
                                            result_folder_name)

    if not os.path.exists(export_folder_location):
        os.makedirs(export_folder_location)        

    # Number of cases previous seven days

    start_date = (datetime.today() - timedelta(days = 18)).strftime("%Y-%m-%d")
    sql_extract = f"""

            SELECT AVG(total_cases) as average_cases
            FROM 
            (
                SELECT fechadiagn as fecha, COUNT(*) AS total_cases
                FROM `servinf-unacast-prod.AlcaldiaBogota.positivos_agg_fecha`
                WHERE fechadiagn >= "{start_date}"
                GROUP BY fechadiagn
                ORDER BY fechadiagn
            )
    """
    average_cases = str(int(np.round(bqf.run_simple_query(client, sql_extract).average_cases[0])))


    # Top Localities
    sql_extract = """

            SELECT att.location_id, geo.name, date, attribute_name, attribute_value
            FROM grafos-alcaldia-bogota.graph_attributes.graph_attributes as att
            JOIN grafos-alcaldia-bogota.geo.locations_geometries as geo
            ON att.location_id = geo.location_id
            WHERE date = (SELECT MAX(date) 
                          FROM grafos-alcaldia-bogota.graph_attributes.graph_attributes
                          WHERE attribute_name = "personalized_pagerank_gini_index")
                  AND attribute_name = "personalized_pagerank_gini_index"
                  AND att.location_id LIKE "%bogota_localidad%"
            ORDER BY attribute_value DESC
    """

    df_res =  bqf.run_simple_query(client, sql_extract)
    df_res['name'] = df_res['name'].apply(lambda s: s.replace("Bogotá Localidad ","")) 
    names = df_res['name'].values

    top_localities = f"{names[0]}, {names[1]} y {names[2]}"


    # Creates CSV
    df = pd.DataFrame([{'parameter_name':"top_localities", "parameter_value":top_localities},
                       {'parameter_name':"average_cases", "parameter_value":average_cases}])

    df.to_csv(os.path.join(export_folder_location, 'statistics.csv'), index = False, sep = ";")

    butils.add_export_info(os.path.basename(__file__), [os.path.join(export_folder_location, 'statistics.csv')])

    print(ident + "Done!")

    
    

# Runs the script
if __name__ == "__main__":

    main()