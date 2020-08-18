import os
import sys
import unidecode
import pandas as pd
import matplotlib.pyplot as plt

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

# Graphs
def graph_indicators(files):
    data = pd.DataFrame(columns=["vars", "Nacional"]).set_index(["vars", "Nacional"])
    for f in files:
        city_name = list(f.keys())[0]
        path = f[city_name]
        df_city = pd.read_csv(path, names=["vars", "Nacional", city_name], skiprows=1, header=None)
        df_city.set_index(["vars", "Nacional"], inplace=True)
        data = data.merge(df_city, how="outer", left_index=True, right_index=True)

    data.reset_index(inplace=True)
    print(data.head())

    # Graph 
    
    print(data.iloc[:,1:])
    
    fig = plt.figure()
    ax1 = fig.add_subplot(111)

    ax1.scatter(data.iloc[0,1:], [0, 0, 0, 0, 0, 0, 0, 0, 0, 0], s=10, marker="s", label=data.index[0])
    # ax1.scatter(data.iloc[1,1:], [1, 1, 1, 1, 1, 1, 1, 1, 1, 1], s=10, marker="s", label=data.index[1])
    # ax1.scatter(data.iloc[2,1:], [2, 2, 2, 2, 2, 2, 2, 2, 2, 2], s=10, marker="s", label=data.index[2])
    # ax1.scatter(data.iloc[3,1:], [3, 3, 3, 3, 3, 3, 3, 3, 3, 3], s=10, marker="s", label=data.index[3])
    # ax1.scatter(data.iloc[4,1:], [4, 4, 4, 4, 4, 4, 4, 4, 4, 4], s=10, marker="s", label=data.index[4])
    # ax1.scatter(data.iloc[4,1:], [5, 5, 5, 5, 5, 5, 5, 5, 5, 5], s=10, marker="s", label=data.index[5])
    plt.legend(loc='upper left')
    plt.show()

    # plt.scatter(data.iloc[:,1:], data.iloc[:,0])

    raise Exception("DONE")

# Get polygons per country to avoid loading data over and over 
for country in countries:
    selected_polygons_sm = selected_polygons[selected_polygons["location_name"] == country].copy()
    files_to_graph = []
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

        table = polygon_socio_economic_analysis.main(location_name, location_folder, city_name, poly_id, poly_name, ident = '         ')
        files_to_graph.append({city_name:table})
    # graph_indicators(files_to_graph)





