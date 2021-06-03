# Script for statistics extraction for bogota

# Loads the different libraries
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from google.cloud import bigquery
import os, sys
import pandas as pd
from datetime import datetime, timedelta

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

    # Number of cases previous seven days

    start_date = (datetime.today() - timedelta(days = 10)).strftime("%Y-%m-%d")
    sql_extract = f"""

            SELECT AVG(total_cases) as average_cases
            FROM 
            (
                SELECT fechadiagn as fecha, COUNT(*) AS total_cases
                FROM `servinf-unacast-prod.AlcaldiaBogota.positivos_agg_fecha`
                WHERE fechadiagn >= "{start_date}"
                GROUP BY fechadiagn
                ORDER BY fecha
            )
    """

    average_cases = str(int(np.round(bqf.run_simple_query(client, sql).average_cases[0])))


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

    df_res =  bqf.run_simple_query(client, sql)
    df_res['name'] = df_res['name'].apply(lambda s: s.replace("Bogotá Localidad ","")) 
    names = df_res['name'].values

    top_localities = f"{names[0]}, {names[1]} y {names[2]}"


    # Creates CSV
    df = pd.dataFrame({'name', top_localities })


    

# Runs the script
if __name__ == "__main__":

    main()