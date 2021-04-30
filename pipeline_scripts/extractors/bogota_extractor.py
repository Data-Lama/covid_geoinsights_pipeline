# Bogota Extractor
# Extracts the cases from the BigQuery Database mantained by Servinformacion

# Loads the different libraries
import numpy as np
import pandas as pd
import os, sys

import bigquery_functions as bqf
import general_functions as gf


# Global Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')
key_string = config.get_property('key_string') # For Encryption

# Starts the BigQuery Client
client = bqf.get_client()

# Constants
location_folder_name = "bogota"

# Ident for printing
ident = '         '

# Location Folder
location_folder = os.path.join(data_dir, 'data_stages', location_folder_name)


# Extracts the description
df_description = gf.get_description(location_folder_name)



# Creates the folders if the don't exist
cases_folder = os.path.join(data_dir, 'data_stages', location_folder_name, 'raw','cases')
if not os.path.exists(cases_folder):
    os.makedirs(cases_folder)

cases_file = os.path.join(cases_folder, df_description.loc['cases_file_name','value'])


# Declares the query for extraction

query = """
    
    SELECT fechainici, recuperado, ubicacion, upzgeo, nomupz_1, x, y
    FROM `servinf-unacast-prod.AlcaldiaBogota.positivos_agg_fecha`     

"""

# Gets df
print(ident + '   Extracting Cases')
df = bqf.run_simple_query(client, query, allow_large_results=True)


# Encrypts the file
print(ident + '   Encrypts and Saves')
gf.encrypt_df(df, cases_file, key_string)


print(ident + "Done")