# Script for extracting google trends for specific keywords on the given location


# Basic imports
import os, sys
from pathlib import Path
import pandas as pd


# Imports constants
import constants as cons

# Imports google trend functions
import google_trends_functions as gtf


# Global Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')


# CONSTANTS
# ------------------
# For Printing
ident = '         '

output_file_name = "google_trends.csv"
output_folder_name = "digital_statistics"


# Search terms
# Declared in English and are translated afterwards
# TODO SAMAUEL
main_search_terms = ["covid","fever"] # MOOC


# Reads the parameters from excecution
location_name  = sys.argv[1] # Location name
location_folder_name = sys.argv[2] # location folder name 


# Location Folder
location_folder = os.path.join(data_dir, 'data_stages', location_folder_name)


# Extracts the description
df_description = pd.read_csv(os.path.join(location_folder, 'description.csv'), index_col = 0)

# Initial checks
# ---------------
# Checks if the description includes a google_trends_geo_code
if cons.DECRIPTION_ID_GOOGLE_TRENDS_GEO_CODE not in df_description.index:
    raise ValueError(f"No Google Trends Geo Code ({cons.DECRIPTION_ID_GOOGLE_TRENDS_GEO_CODE}) was found in the description of location: {location_name} ({location_folder_name}). Please add it to use this extractor.")


# Checks if the description id includes the fist case tag 
if cons.DECRIPTION_ID_FIRST_CASE not in df_description.index:
    raise ValueError(f"No First Case ({cons.DECRIPTION_ID_FIRST_CASE}) was found in the description of location: {location_name} ({location_folder_name}). Please add it to use this extractor.")


# Checks if the description id includes the main language
if cons.DECRIPTION_ID_MAIN_LANGUAGE not in df_description.index:
    raise ValueError(f"No Main Language ({cons.DECRIPTION_ID_MAIN_LANGUAGE}) was found in the description of location: {location_name} ({location_folder_name}). Please add it to use this extractor.")


# Checks if the folder exists and creates it
final_folder_location = os.path.join(location_folder, "raw", output_folder_name)
if not os.path.exists(final_folder_location):
    os.makedirs(final_folder_location)

final_file_location = os.path.join(final_folder_location, output_file_name)

# Extracts the necessary variables
# ------------------------------

# Extracts the google trends geo_code
geo_code = df_description.loc[cons.DECRIPTION_ID_GOOGLE_TRENDS_GEO_CODE,'value']

# Extracts first case date (yyyy-mm-dd) 
first_case_date = pd.to_datetime(df_description.loc[cons.DECRIPTION_ID_FIRST_CASE,'value'])

# Extracts main language
main_language = df_description.loc[cons.DECRIPTION_ID_MAIN_LANGUAGE,'value']


# TODO SAMUEL
# Todo suyo Samuel. Ahi está todo lo que creo que necesita, la idea es que guarde en final_file_location
# la tabla actualizada, sientase libre de cambiar lo que necesite para que funcione mejor.

print("NOT IMPLEMENTED YET!!!!!!")

    








