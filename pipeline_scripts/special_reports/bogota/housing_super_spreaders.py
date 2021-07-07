import os
import sys
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Point

# Local imports
import bogota_constants as cons
import special_functions.utils as butils

from google.cloud import bigquery
from google.api_core.exceptions import BadRequest
from google.cloud.exceptions import NotFound

# Global Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')

sources = []
indent = '   '

location_folder_name = "bogota"                                                      # sys.argv[1]
min_date = (pd.to_datetime('today') - pd.Timedelta(days=15)).strftime('%Y-%m-%d')    # sys.argv[2]
location_ids = cons.OBSERVATION_IDS                                                  # sys.argv[3]

# Declares the export location
export_folder_location = os.path.join(analysis_dir, location_folder_name, "housing_shapefile_salud")

if not os.path.exists(export_folder_location):
    os.makedirs(export_folder_location)    

print(f"{indent}Constructing housing shapefile for {min_date}")
print(f"{indent}{indent}Getting data")
# Gets data
client = bigquery.Client(location="US")
job_config = bigquery.QueryJobConfig(allow_large_results = True)


where_parameters = [f'(transits.location_id = "{loc_id}")' for loc_id in location_ids]
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

df["date"] = df.apply(lambda x: x["date"].strftime('%Y-%m-%d'), axis=1)
df = df[df["type"] == "HOUSE"]
df["geometry"] = df.apply(lambda x: Point(x.housing_lon, x.housing_lat), axis=1)
gdf = gpd.GeoDataFrame(df, geometry="geometry")

gdf_houses = gdf[["identifier", "geometry"]].drop_duplicates(ignore_index=True)
houses_dict = dict(zip(gdf_houses.identifier, gdf_houses.geometry))

gdf_aggr = gdf[["location_id", "name", "identifier", "date", "total_transits"]].groupby(["location_id", "name", "identifier", "date"]).sum().reset_index()
gdf_aggr["geometry"] = gdf_aggr.apply(lambda x: houses_dict[x["identifier"]], axis=1)


print(f"{indent}Saves")

esri_compliant = {"location_id":"loc_id",
                    "identifier": "identif",
                    "total_transits": "tot_trans"}

file_name = os.path.join(export_folder_location, "housing_super_spreading.shp")
gdf.rename(columns=esri_compliant).to_file(file_name, index=False)

# Adds to export
sources.append(file_name)    

# add export file info
butils.add_export_info(os.path.basename(__file__), sources)