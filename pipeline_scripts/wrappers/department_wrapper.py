# Usual imports
import os
import sys
import pandas as pd
from datetime import datetime

import excecute_all.excecute_script as excecute_script

'''
 ---------------- stages -------------

1. graph_maps       - YES
2. polygon_info_timewindow
3. generate_threshold_alerts
4. choropleth_maps
5. general_statistics
6. incidence_map
7. polygon_prediction_wrapper
8. polygon_socio_economic_wrapper
9. movement_range_plots_script       - YES

'''

#Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')

# Reads the parameters from excecution
location_folder =  sys.argv[1] # location folder name
agglomeration_method = sys.argv[2] # Aglomeration name
selected_polygons_name = sys.argv[3] # Selected polygon names

if len(sys.argv) <= 4:
	raise ValueError('No polygons ids provided!!!!')

selected_polygons = []
i = 4
while i < len(sys.argv):
	selected_polygons.append(sys.argv[i])
	i += 1

selected_polygons_parameters = " ".join(selected_polygons)

# Set paths
agglomerated_path = os.path.join(data_dir, "data_stages", location_folder, "agglomerated", agglomeration_method) 
polygons = os.path.join(agglomerated_path, "polygons.csv")
movement = os.path.join(agglomerated_path, "movement.csv")
cases = os.path.join(agglomerated_path, "cases.csv")
movement_range = os.path.join(agglomerated_path, "movement_range.csv")
scripts_location = os.path.join("pipeline_scripts", "analysis")

def set_folder_name(polygon_name):
    # NOT IMPLEMENTED YET
    return polygon_name



# Execute graph_maps
parameters = "{} {} {} {} {}".format(location_folder, 
                                    location_folder.lower(), 
                                    agglomeration_method, 
                                    set_folder_name(selected_polygons_name), 
                                    selected_polygons_parameters)

excecute_script(scripts_location, "graph_maps.R", "R", parameters)

# Execute generate_threshold_alerts
selected_polygons_parameter = "{} {}".format(set_folder_name(selected_polygons_name), selected_polygons_parameters)
parameters = "{} {} {} {}".format(location_folder,  
                                    agglomeration_method, 
                                    "min_record", 
                                    selected_polygons_parameter)

excecute_script(scripts_location, "generate_threshold_alerts.py", "python", parameters)

# Excecute choropleth_maps
selected_polygons_parameter = "{} {}".format(set_folder_name(selected_polygons_name), selected_polygons_parameters)
parameters = "{} {} {}".format(location_folder,  
                                agglomeration_method,
                                selected_polygons_parameter)

excecute_script(scripts_location, "choropleth_maps.py", "python", parameters)

# Execute incidence_map
# TODO

# Execute polygon_prediction_wrapper
# TODO

