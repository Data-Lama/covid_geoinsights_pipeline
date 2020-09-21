# Encrypts a new file
import sys
import pandas as pd
import general_functions as gf

from global_config import config
key_string = config.get_property('key_string') 


origin  =  sys.argv[1] # origin
destination =  sys.argv[2] # destination



if origin.upper().endswith('.CSV'):
    df = pd.read_csv(origin)

elif origin.upper().endswith('.XLSX'):
    df = pd.read_excel(origin)

else:
    t = origin.split('.')[-1]
    raise ValueError("Unsupported file extension: " + t)


print(f"Encrypting File: {origin.split('/')[-1]} into {destination.split('/')[-1]}")
gf.encrypt_df(df, destination, key_string)
print('Done')
