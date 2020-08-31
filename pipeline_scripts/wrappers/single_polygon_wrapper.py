# Usual imports
import os
import sys
import pandas as pd
from datetime import datetime


import excecution_functions as ef
import general_functions as gf

#Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')

'''
 ---------------- stages -------------

1. graph_maps
2. polygon_info_timewindow
3. generate_threshold_alerts
4. choropleth_maps
5. general_statistics
6. incidence_map
7. polygon_socio_economic_analysis
8. movement_range_plots_script
9. polygon_prediction_wrapper

'''

# Constants
coverage = 0.8
num_neighbors = 20
ident = '         '



# Reads the parameters from excecution
location_folder =  sys.argv[1] # location folder name
agglomeration_method = sys.argv[2] # Aglomeration name
polygon_name = sys.argv[3] # Selected polygon names
folder_name = sys.argv[4]
poly_id =  sys.argv[5]



location_name = location_folder.replace('_',' ').title()

# Set paths
agglomerated_path = os.path.join(data_dir, "data_stages", location_folder, "agglomerated", agglomeration_method) 
polygons = os.path.join(agglomerated_path, "polygons.csv")
movement = os.path.join(agglomerated_path, "movement.csv")
cases = os.path.join(agglomerated_path, "cases.csv")
movement_range = os.path.join(agglomerated_path, "movement_range.csv")

# Scripts locations
analysis_scripts_location = os.path.join("pipeline_scripts", "analysis")
wrapper_scripts_location = os.path.join("pipeline_scripts", "wrappers")



# Extracts Neighbors
# By movement
movement_neighbors = gf.extract_connected_neighbors(location_folder, poly_id, agglomeration_method)
selected_connected_neighbors_polygons_parameter = " ".join([poly_id] + movement_neighbors)

# By Geography
geographic_neighbors = gf.get_geographic_neighbors(poly_id, location_folder, agglomeration_method)
selected_geographic_neighbors_polygons_parameter = " ".join([poly_id] + geographic_neighbors)


# # Execute graph_maps
# print()
# print("{}Excecuting graph_maps.R for {}".format(ident, polygon_name))
# parameters = "{} {} {} {} {}".format(location_name,
#                                 location_folder,  
#                                 agglomeration_method,
#                                 folder_name,
#                                 selected_connected_neighbors_polygons_parameter)

# ef.excecute_script(analysis_scripts_location, "graph_maps.R", "R", parameters)

# # Execute polygon_info_timewindow
# print()
# print("{}Excecuting polygon_info_timewindow.py for {}".format(ident, polygon_name))
# parameters = "{} {} {} {} {} {}".format(location_folder, 
#                                     agglomeration_method,
#                                     "5",
#                                     "days",
#                                     folder_name,
#                                     selected_geographic_neighbors_polygons_parameter)

# ef.excecute_script(analysis_scripts_location, "polygon_info_timewindow.py", "python", parameters)

# # Execute generate_threshold_alerts
# print()
# print("{}Excecuting generate_threshold_alerts.py for {}".format(ident, polygon_name))
# parameters = "{} {} {} {} {}".format(location_folder,  
#                                     agglomeration_method, 
#                                     "min_record",
#                                     folder_name, 
#                                     selected_geographic_neighbors_polygons_parameter)

# ef.excecute_script(analysis_scripts_location, "generate_threshold_alerts.py", "python", parameters)

# Excecute choropleth_maps
print()
print("{}Excecuting choropleth_maps.py for {}".format(ident, polygon_name))
parameters = "{} {} {} {}".format(location_folder,  
                                agglomeration_method,
                                folder_name,
                                selected_geographic_neighbors_polygons_parameter)

ef.excecute_script(analysis_scripts_location, "choropleth_maps.py", "python", parameters)

# # Excecute general_stats
# print()
# print("{}Excecuting general_statistics.py for {}".format(ident, polygon_name))
# parameters = "{} {} {} {}".format(location_folder,  
#                                 agglomeration_method,
#                                 folder_name,
#                                 poly_id)

# ef.excecute_script(analysis_scripts_location, "general_statistics.py", "python", parameters)

# # Execute incidence_map
# print()
# print("{}Excecuting incidence_map.py for {}".format(ident, polygon_name))
# parameters = "{} {} {} {} {}".format( location_name,
#                                 location_folder,  
#                                 agglomeration_method,
#                                 folder_name,
#                                 selected_geographic_neighbors_polygons_parameter)

# ef.excecute_script(analysis_scripts_location, "incidence_map.R", "R", parameters)

# # polygon_socio_economic_analysis
# print()
# print("{}Excecuting polygon_socio_economic_analysis.py for {}".format(ident, polygon_name))

# parameters = "{} {} {} {} {}".format(location_folder,  
#                                 agglomeration_method,
#                                 folder_name,
#                                 polygon_name,
#                                 poly_id)

# ef.excecute_script(analysis_scripts_location, "polygon_socio_economic_analysis.py", "python", parameters)

# # movement_range_plots_script
# print()
# print("{}Excecuting movement_range_plots_script_polygons.py for {}".format(ident, polygon_name))
# parameters = "{} {} {} {} {}".format(location_folder,  
#                                 agglomeration_method,
#                                 polygon_name,
#                                 folder_name,
#                                 selected_geographic_neighbors_polygons_parameter)

# ef.excecute_script(analysis_scripts_location, "movement_range_plots_script_polygons.py", "python", parameters)


# # movement_range_and_cases_plots
# print()
# print("{}Excecuting movement_range_and_cases_script_polygons.py.py for {}".format(ident, polygon_name))
# parameters = "{} {} {} {} {}".format(location_folder,  
#                                 agglomeration_method,
#                                 polygon_name,
#                                 folder_name,
#                                 poly_id)

# ef.excecute_script(analysis_scripts_location, "movement_range_and_cases_script_polygons.py", "python", parameters)



# # polygon union prediction wrapper
# print()
# print("{}Excecuting polygon_union_prediction_wrapper.py for {}".format(ident, polygon_name))
# parameters = "{} {} {} {} {} {} {}".format(location_folder,  
#                                 agglomeration_method,
#                                 coverage,
#                                 num_neighbors,
#                                 polygon_name,
#                                 folder_name,
#                                 poly_id)

# ef.excecute_script(wrapper_scripts_location, "polygon_union_prediction_wrapper.py", "python", parameters)

