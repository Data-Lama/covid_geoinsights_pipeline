# Scripts that edits and copies the website core data

import pandas as pd
import shutil
import os



#Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')
reports_dir = config.get_property('report_dir')
website_dir = config.get_property('website_dir')


def save_pandas(df, name, folder):
    '''
    Saves Pandas dataframe into CSV (without indixes)
    '''

    final_folder = os.path.join(website_dir, folder)
    # Makes the folders
    if not os.path.exists(final_folder):
        os.makedirs(final_folder)

    #print(os.path.join(final_folder, name))
    df.to_csv(os.path.join(final_folder, name), index = False)



ident = '         '


print(ident + 'Generating Website Data')

print(ident + '   National')


# Milestones
# -------------------
print(ident + '      Milestones')
file_location = os.path.join(analysis_dir,'colombia/community/movement_plots/entire_location/milestones.csv')
df = pd.read_csv(file_location)
save_pandas(df, 'milestones.csv', 'nacional/cuadro_1/')

# Incidences
# -------------------
print(ident + '      Incidences')

# Summary
file_location = os.path.join(analysis_dir,'colombia/geometry/incidence_maps/entire_location/incidences.csv')
df = pd.read_csv(file_location)
save_pandas(df, 'incidences.csv', 'nacional/cuadro_2/')

# Map
file_location = os.path.join(analysis_dir,'colombia/geometry/incidence_maps/entire_location/incidence_map_data.csv')
df = pd.read_csv(file_location)
save_pandas(df, 'incidence_map_data.csv', 'nacional/figura_6/')


# Alerts
# -------------------
print(ident + '      Alerts')

# Summary
file_location = os.path.join(analysis_dir,'colombia/geometry/alerts/entire_location/alerts.csv')
df = pd.read_csv(file_location)
save_pandas(df, 'alerts.csv', 'nacional/cuadro_3/')

# Map
file_location = os.path.join(analysis_dir,'colombia/geometry/alerts/entire_location/map_data.csv')
#df = pd.read_csv(file_location)
#save_pandas(df, 'map_data.csv', 'nacional/figura_9/')


# Movement Range
# -------------------
print(ident + '      Movement Range')

# Summary
file_location = os.path.join(analysis_dir,'colombia/community/movement_plots/entire_location/milestones.csv')
df = pd.read_csv(file_location)
save_pandas(df, 'milestones.csv', 'nacional/figura_1/')

# Plots
file_location = os.path.join(analysis_dir,'colombia/community/movement_plots/entire_location/mov_range_colombia_data.csv')
df = pd.read_csv(file_location)
save_pandas(df, 'mov_range_colombia_data.csv', 'nacional/figura_1/')

file_location = os.path.join(analysis_dir,'colombia/community/movement_plots/entire_location/movement_range_selected_polygons_colombia_data.csv')
df = pd.read_csv(file_location)
save_pandas(df, 'movement_range_selected_polygons_colombia_data.csv', 'nacional/figura_2/')



# Cloropleth Maps
# -------------------
print(ident + '      Cloropleth Maps')

# Map
file_location = os.path.join(analysis_dir,'colombia/geometry/polygon_info_window/entire_location/choropleth_map_colombia_15-day-window_data.csv')
#df = pd.read_csv(file_location)
#save_pandas(df, 'choropleth_map_colombia_15-day-window_data.csv', 'nacional/figura_3/')

file_location = os.path.join(analysis_dir,'colombia/geometry/polygon_info_window/entire_location/choropleth_map_colombia_historic_data.csv')
#df = pd.read_csv(file_location)
#save_pandas(df, 'choropleth_map_colombia_historic_data.csv', 'nacional/figura_3/')


# Situation Maps
# -------------------
print(ident + '      Situation Maps')

# Map
# First day
# Edges
file_location = os.path.join(analysis_dir,'colombia/community/graph_maps/entire_location/maps_by_day/map_by_day_edges_data.csv')
df = pd.read_csv(file_location)
# Changes to format
df = df[df.day == df.day.min()].copy()
df['path_id'] = df.apply(lambda df: f'{df.start_id}-{df.end_id}', axis = 1)
df_1 = df[['path_id','start_id','dept.x','municipio.x','lon.x','lat.x','movement']].copy()
df_1.columns = ['path_id','node_id','dept','municipio','lon','lat','movement']

df_2 = df[['path_id','end_id','dept.y','municipio.y','lon.y','lat.y','movement']].copy()
df_2.columns = ['path_id','node_id','dept','municipio','lon','lat','movement']

df_final = pd.concat((df_1,df_2), ignore_index = True).sort_values('path_id')

save_pandas(df_final, 'map_by_day_edges_data.csv', 'nacional/figura_4/')

# Nodes
file_location = os.path.join(analysis_dir,'colombia/community/graph_maps/entire_location/maps_by_day/map_by_day_nodes_data.csv')
df = pd.read_csv(file_location)
# Filters by first day
df = df.loc[df.day == df.day.min(), ['dept','municipio','lon','lat','inner_movement','num_cases']].copy()
save_pandas(df, 'map_by_day_nodes_data.csv', 'nacional/figura_4/')

# Map
# Last day
file_location = os.path.join(analysis_dir,'colombia/community/graph_maps/entire_location/maps_by_day/map_by_day_edges_data.csv')
df = pd.read_csv(file_location)
# Changes to format
df = df[df.day == df.day.max()].copy()
df['path_id'] = df.apply(lambda df: f'{df.start_id}-{df.end_id}', axis = 1)

df_1 = df[['path_id','start_id','dept.x','municipio.x','lon.x','lat.x','movement']].copy()
df_1.columns = ['path_id','node_id','dept','municipio','lon','lat','movement']

df_2 = df[['path_id','end_id','dept.y','municipio.y','lon.y','lat.y','movement']].copy()
df_2.columns = ['path_id','node_id','dept','municipio','lon','lat','movement']

df_final = pd.concat((df_1,df_2), ignore_index = True).sort_values('path_id')

save_pandas(df_final, 'map_by_day_edges_data.csv', 'nacional/figura_5/')

# Nodes
file_location = os.path.join(analysis_dir,'colombia/community/graph_maps/entire_location/maps_by_day/map_by_day_nodes_data.csv')
df = pd.read_csv(file_location)
# Filters by last day
df = df.loc[df.day == df.day.max(), ['dept','municipio','lon','lat','inner_movement','num_cases']].copy()
save_pandas(df, 'map_by_day_nodes_data.csv', 'nacional/figura_5/')



# Prediction
# -------------------
print(ident + '      Prediction')

# Predictions
file_location = os.path.join(analysis_dir,'colombia/community/prediction/polygon_unions/entire_location/prediction_entire_location_data.csv')
df = pd.read_csv(file_location)
save_pandas(df, 'prediction_entire_location_data.csv', 'nacional/figura_7/')

# Statistics
file_location = os.path.join(analysis_dir,'colombia/community/prediction/polygon_unions/entire_location/statistics.csv')
df = pd.read_csv(file_location)
save_pandas(df, 'statistics.csv', 'nacional/figura_7/')

# Simulation
file_location = os.path.join(analysis_dir,'colombia/community/prediction/polygon_unions/entire_location/simulations_entire_location_data.csv')
df = pd.read_csv(file_location)
save_pandas(df, 'simulations_entire_location_data.csv', 'nacional/figura_8/')

print(ident + '   Done!')