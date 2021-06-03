import os
import sys
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Point

# Local imports
import bogota_constants as cons

from google.cloud import bigquery
from google.api_core.exceptions import BadRequest
from google.cloud.exceptions import NotFound

# Global Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')

indent = '   '

location_folder_name = "bogota"                                                      # sys.argv[1]
min_date = (pd.to_datetime('today') - pd.Timedelta(days=15)).strftime('%Y-%m-%d')    # sys.argv[2]
location_ids = cons.OBSERVATION_IDS                                                  # sys.argv[3]

# Declares the export location
export_folder_location = os.path.join(analysis_dir, location_folder_name, "housing_shapefile")

if not os.path.exists(export_folder_location):
    os.makedirs(export_folder_location)    

print(f"{indent}Constructing housing shapefile")
print(f"{indent}{indent}Getting data")
# Gets data
client = bigquery.Client(location="US")
job_config = bigquery.QueryJobConfig(allow_large_results = True)


where_parameters = [f'location_id = "{loc_id}"' for loc_id in location_ids]
where_parameters = " OR ".join(where_parameters)
where_parameters = f"({where_parameters.strip()})"

query = f"""
SELECT transits.location_id,
       geocodes.name,
       transits.identifier,
       transits.date,
       transits.hour,
       transits.total_transits,
       housing.type as type,
       housing.lat as housing_lat,
       housing.lon as housing_lon,
FROM 
    grafos-alcaldia-bogota.transits.hourly_transits transits INNER JOIN grafos-alcaldia-bogota.housing_location.colombia_housing_location housing
        ON (transits.identifier = housing.identifier) 
    JOIN grafos-alcaldia-bogota.geo.locations_geometries geocodes
    ON (geocodes.location_id = transits.location_id)
WHERE (transits.date >= "{min_date}") AND 
        {where_parameters} AND
        (housing.code_depto = "CO.34") AND 
        (housing.week_date >= "{min_date}")
"""

query_job = client.query(query, job_config=job_config) 

# # Return the results as a pandas DataFrame
df = query_job.to_dataframe()

df = pd.read_csv("/Users/andreaparra/Desktop/transits_housing.csv")
df["date"] = df.apply(lambda x: pd.Timestamp(x["date"]), axis=1)

df["geometry"] = df.apply(lambda x: Point(x.housing_lon, x.housing_lat), axis=1)
gdf = gpd.GeoDataFrame(df, geometry="geometry")

print(f"{indent}Saves")
gdf.to_file(os.path.join(export_folder_location, "housing_super_spreading.shp"), index=False)
