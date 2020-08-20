# Usual imports
import os
import sys
import pandas as pd
from datetime import datetime

# import excecute_all.excecute_script as excecute_script

'''
 ---------------- stages -------------

1. graph_maps
2. polygon_info_timewindow
3. generate_threshold_alerts
4. choropleth_maps
5. general_statistics
6. incidence_map
7. polygon_prediction_wrapper
8. polygon_socio_economic_wrapper
9. movement_range_plots_script

'''
ident = '         '

#Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')

# Reads the parameters from excecution
location_folder =  sys.argv[1] # location folder name
agglomeration_method = sys.argv[2] # Aglomeration name
selected_polygons_name = sys.argv[3] # Selected polygon names
folder_name = sys.argv[4]

if len(sys.argv) <= 5:
	raise ValueError('No polygons ids provided!!!!')

selected_polygons = []
i = 5
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
progress_file = 'polygon_union_wrapper_excecution_progress.csv'

def set_folder_name(polygon_name):
    # NOT IMPLEMENTED YET
    return polygon_name

def add_progress(script_name, parameters, result):
	'''
	Resets progress
	'''

	with open(progress_file, 'a') as f:
		f.write(f'{datetime.now()},{script_name},{result}\n')

def excecute_script(script_location, name, code_type, parameters):
    '''
    Excecutes a certain type of script
    '''

    if code_type.upper() == 'PYTHON':

        #Python
        if not name.endswith('.py'):
            name = name + '.py'

        final_path = os.path.join(script_location, name)
        resp = os.system('{} {} {}'.format('python', final_path, parameters))

    elif code_type.upper() == 'R':
        #R
        if not name.endswith('.R'):
            name = name + '.R'

        final_path = os.path.join(script_location, name)
        resp = os.system('{} {} {}'.format('Rscript --vanilla', final_path, parameters))

    else:
        raise ValueError('No support for scripts in: {}'.format(code_type))

    add_progress(name, parameters, resp)    

# Execute graph_maps
print("{}Excecuting graph_maps.R for {}".format(ident, selected_polygons_name))
parameters = "{} {} {} {} {}".format(location_folder, 
                                    location_folder.lower(), 
                                    agglomeration_method, 
                                    folder_name, 
                                    selected_polygons_parameters)

# excecute_script(scripts_location, "graph_maps.R", "R", parameters)

# Execute polygon_info_timewindow
print("{}Excecuting polygon_info_timewindow.py for {}".format(ident, selected_polygons_name))
selected_polygons_parameter = "{} {}".format(folder_name, selected_polygons_parameters)
parameters = "{} {} {} {} {}".format(location_folder, 
                                    agglomeration_method,
                                    "5",
                                    "days",
                                    selected_polygons_parameter)

excecute_script(scripts_location, "polygon_info_timewindow.py", "python", parameters)

# Execute generate_threshold_alerts
print("{}Excecuting generate_threshold_alerts.py for {}".format(ident, selected_polygons_name))
selected_polygons_parameter = "{} {}".format(folder_name, selected_polygons_parameters)
parameters = "{} {} {} {}".format(location_folder,  
                                    agglomeration_method, 
                                    "min_record", 
                                    selected_polygons_parameter)

excecute_script(scripts_location, "generate_threshold_alerts.py", "python", parameters)

# Excecute choropleth_maps
print("{}Excecuting choropleth_maps.py for {}".format(ident, selected_polygons_name))
selected_polygons_parameter = "{} {}".format(folder_name, selected_polygons_parameters)
parameters = "{} {} {}".format(location_folder,  
                                agglomeration_method,
                                selected_polygons_parameter)

excecute_script(scripts_location, "choropleth_maps.py", "python", parameters)

# Execute incidence_map
# TODO

# Execute polygon_prediction_wrapper
# TODO

# movement_range_plots_script
print("{}Excecuting movement_range_plots_script_polygons.py for {}".format(ident, selected_polygons_name))
parameters = "{} {} {} {} {}".format(location_folder,  
                                agglomeration_method,
                                selected_polygons_name,
                                folder_name,
                                selected_polygons_parameters)

excecute_script(scripts_location, "movement_range_plots_script_polygons.py", "python", parameters)