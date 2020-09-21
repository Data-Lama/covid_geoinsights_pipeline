import os
import sys
import pandas as pd

# Direcotries
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')

# Reads the parameters from excecution
location_name  =  sys.argv[1] # location name
dept_name = sys.argv[2]
polygons = []
i = 3
while i < len(sys.argv):
    polygons.append(sys.argv[i]) # polygon names
    i+=1

aggl = os.path.join(data_dir, "data_stages", location_name, "agglomerated", "community", "polygon_community_map.csv")
print("Finding community ids for: {}".format(polygons))
#Load community map database
try:  
    df_aggl = pd.read_csv(aggl, low_memory=False)
except:
    df_aggl = pd.read_csv(aggl, low_memory=False, encoding = 'latin-1')

df_community = df_aggl[df_aggl.poly_id.isin(polygons)].copy()

all_ids = df_community.community_id.unique()

ids = []
for d in all_ids:
    if str(d)[:2] == dept_name:
        ids.append(d)

ids = ' '.join(str(v) for v in ids)

print("The relevant communities for the selected poygons are: {}".format(ids))