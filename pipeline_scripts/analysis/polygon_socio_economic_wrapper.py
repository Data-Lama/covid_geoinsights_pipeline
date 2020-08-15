import os
import sys
import unidecode
import pandas as pd

# Direcotries
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')

# import scripts
import pipeline_scripts.analysis.polygon_socio_economic_analysis as polygon_socio_economic_analysis

# Import selected polygons
selected_polygons = pd.read_csv('pipeline_scripts/configuration/selected_polygons.csv')

# Get countries
countries = list(selected_polygons["location_name"].unique())

# Get polygons per country to avoid loading data over and over 
for country in countries:
    selected_polygons_sm = selected_polygons[selected_polygons["location_name"] == country].copy()

    for i in selected_polygons.index:
        poly_id = selected_polygons.at[i, "poly_id"]
        location_name = selected_polygons.at[i, "location_name"]
        location_folder = selected_polygons.at[i, "folder_name"]

         # Get polygons
        polygons = os.path.join(data_dir, "data_stages", location_name, "agglomerated", location_folder, "polygons.csv")
        try:
            df_polygons = pd.read_csv(polygons, low_memory=False)
        except:
            df_polygons = pd.read_csv(polygons, low_memory=False, encoding = 'latin-1')

        df_polygons.set_index("poly_id", inplace=True)
        poly_name = df_polygons.at[poly_id, "poly_name"]
        city_name = poly_name.split("-")[0].split(" ")  
        city_name = "_".join(city_name).lower()
        city_name = unidecode.unidecode(city_name)

        polygon_socio_economic_analysis.main(location_name, location_folder, city_name, poly_id, poly_name, ident = '         ')

    





