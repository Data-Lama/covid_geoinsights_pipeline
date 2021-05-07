# BigQuery Functions
# Script with several bigquery functions

from google.cloud import bigquery
import pandas as pd

# Constants
project_id = "grafos-alcaldia-bogota"
bq_date_format = "%Y-%m-%d"


MAX_RESOLUTION = 30
MAX_DISTANCE = 5
MAX_TIME = 2


def get_client():
    '''
    Gets the client
    '''

    return bigquery.Client(location="US")

def run_simple_query(client, query, allow_large_results=False):
    '''
    Method that runs a simple query
    '''
    
    job_config = bigquery.QueryJobConfig(allow_large_results = allow_large_results)
    query_job = client.query(query, job_config=job_config) 

    # Return the results as a pandas DataFrame
    df = query_job.to_dataframe()
    
    return(df)


def get_date_for_graph(client, location_id):
    '''
    Gets the date untill which the given graph has been updated or None in case
    the location id is not found or no graph has been computed
    '''

    # Extracts reference table

    query = f"""
        SELECT location_id, dataset, start_date, end_date 
        FROM {project_id}.coverage_dates.graphs_coverage
        WHERE location_id = "{location_id}"
    """

    df = run_simple_query(client, query)

    # Checks if empty
    if df.shape[0] == 0:
        return None

    min_search_date = pd.to_datetime(df.loc[0,"end_date"])
    dataset = df.loc[0,"dataset"]

    # Now searches the actual table
    query = f"""

        SELECT MAX(date) as date
        FROM {project_id}.{dataset}.{location_id}
        WHERE date >= "{min_search_date.strftime(bq_date_format)}"

    """

    df = run_simple_query(client, query)

    # Checks if empty
    if df.shape[0] == 0:
        return None

    return  pd.to_datetime(df.loc[0,"date"])



def get_contacts(client, dataset_id, location_id, date, hour, max_resolution = MAX_RESOLUTION,
                                                              max_distance = MAX_DISTANCE,
                                                              max_time = MAX_TIME):
    '''
    Extracts the contacts of the given lo cation at date and hour
    '''
    
    # Declares Edges 
    query = f"""
            SELECT id1, id2, lat, lon
            FROM {project_id}.{dataset_id}.{location_id}
            WHERE date = "{date}" 
                AND hour = {hour}
                AND min_id1_device_accuracy <= {MAX_RESOLUTION}
                AND min_id2_device_accuracy <= {MAX_RESOLUTION}
                AND min_distance <= {MAX_DISTANCE}
                AND min_time_difference <= {MAX_TIME}
    """

    return run_simple_query(client, query)



def get_contacs_by_location(client,
                            dataset_id, 
                            location_id, 
                            start_date, 
                            end_date, 
                            round_to = 4,
                            max_resolution = MAX_RESOLUTION,
                            max_distance = MAX_DISTANCE,
                            max_time = MAX_TIME):
    '''
    Extracts the contacts of the given lo cation at date and hour
    '''
    
    # Query 
    query = f"""
            SELECT lat, lon, COUNT(*) total_contacts
            FROM
            (
              SELECT  ROUND(lat,{round_to}) AS lat, 
                      ROUND(lon,{round_to}) AS lon
              FROM {project_id}.{dataset_id}.{location_id}
              WHERE date >= "{start_date.strftime(bq_date_format)}" 
                    AND date <= "{end_date.strftime(bq_date_format)}"
                    AND min_id1_device_accuracy <= {MAX_RESOLUTION}
                    AND min_id2_device_accuracy <= {MAX_RESOLUTION}
                    AND min_distance <= {MAX_DISTANCE}
                    AND min_time_difference <= {MAX_TIME}

            )
            GROUP BY lat, lon
            ORDER BY total_contacts DESC

    """


    return run_simple_query(client, query)
    


def get_distance_to_infected(client, location_id, start_date, end_date):
    '''
    Gets the distance to infected computed attribute from the graph attribute table
    '''

    query = f"""
        SELECT identifier, date, attribute_value as distance_to_infected
        FROM {project_id}.graph_attributes.node_attributes
        WHERE location_id = "{location_id}"
            AND attribute_name = "distance_to_infected"
            AND date >= "{start_date.strftime(bq_date_format)}"
            AND date <= "{end_date.strftime(bq_date_format)}"
            AND attribute_value IS NOT NULL

    """

    return run_simple_query(client, query, allow_large_results = True)