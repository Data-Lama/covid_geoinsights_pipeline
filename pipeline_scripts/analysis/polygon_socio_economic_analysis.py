import os
import sys
import pandas as pd

# Direcotries
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')


ident = '         '

# Is defined inside function for using it directly from python
def main(location, agglomeration_method, polygon_name, polygon_id, polygon_display_name, ident = '         '):
    # Constructs the export
    folder_location = os.path.join(analysis_dir, location, agglomeration_method, 'socio-economic', polygon_name)
    output_path = os.path.join(folder_location, "socio-economic_data_{}.csv".format(polygon_name))

    # Creates the folder if does not exists
    if not os.path.exists(folder_location):
        os.makedirs(folder_location)

    # get national socio-economic variables
    national_variables = os.path.join(data_dir, "data_stages", location, "raw", "socio_economic", "estadisticas_nacionales.csv")
    df_national = pd.read_csv(national_variables, names=[0, 1])

    # get socio-economic variables
    variables = os.path.join(data_dir, "data_stages", location, "raw", "socio_economic", "estadisticas_por_municipio.csv")
    df_variables = pd.read_csv(variables)

    # check if many polygons are given
    
    if type(polygon_id) is list:
        print(ident + "Building table of socio-economic variables for {}. (Polygons {} of {})".format(polygon_name, polygon_id, location))
        df_table = socio_economic_analysis_for_polygon_union(polygon_id, df_national, df_variables)
        write_table(df_table, output_path)
        return os.path.join(location, agglomeration_method, 'socio-economic', polygon_name, "socio-economic_data_{}.csv".format(polygon_name))
    else:
        print(ident + "Building table of socio-economic variables for {}. (Polygon {} of {})".format(polygon_name, polygon_id, location))
        df_table = socio_economic_analysis_for_polygon(polygon_id, df_variables, df_national)
        write_table(df_table, output_path)
        return os.path.join(location, agglomeration_method, 'socio-economic', polygon_name, "socio-economic_data_{}.csv".format(polygon_name))
    
    
def write_table(df_table, output_path):
    df_table.to_csv(output_path, float_format='%.1f')

def socio_economic_analysis_for_polygon(polygon_id, df_variables, df_national):
    # build table
    df_variables = df_variables[df_variables["node_id"] == int(polygon_id)]
    cols_to_drop = list(set(df_variables.columns) - set(["poblacion", "porcentaje_sobre_60", "ipm", "num_camas_UCI",
                                                    "porcentaje_subsidiado", "porcentaje_contributivo"]))

    df_variables.drop(columns=cols_to_drop, inplace=True)
    df_variables["porcentaje_sobre_60"] = df_variables["porcentaje_sobre_60"].multiply(100)
    df_variables["porcentaje_subsidiado"] = df_variables["porcentaje_subsidiado"].multiply(100)
    df_variables["porcentaje_contributivo"] = df_variables["porcentaje_contributivo"].multiply(100)
    # df_variables.astype({'num_camas_UCI': 'int32', 'poblacion':'int32'})
    df_variables.reset_index(inplace=True)
    df_variables = df_variables.transpose().reset_index()
    df_variables.set_index("index", inplace=True)
    df_national.set_index(0, inplace=True)
    df_variables = df_national.merge(df_variables, how="outer", left_index=True, right_index=True)
    df_variables.drop("index", inplace=True)
    df_variables.rename(columns={1:"Nacional", 0:polygon_name}, inplace=True)
    df_variables.rename({"ipm":"Indice de Pobreza Multidimensional (2018)",
                        "num_camas_UCI":"Número camas UCI",
                        "poblacion":"Población",
                        "porcentaje_contributivo":"Porcentaje en Régimen Contributivo (EPS)",
                        "porcentaje_subsidiado":"Porcentaje en Régimen Subsidiado (EPS)",
                        "porcentaje_sobre_60":"Porcentaje Población Mayor a 60"}, inplace=True)
    
    return df_variables


def socio_economic_analysis_for_polygon_union(polygon_ids, df_national, df_variables):
    # build table
    df_variables = df_variables[df_variables["node_id"].isin(polygon_ids)]
    cols_to_drop = list(set(df_variables.columns) - set(["poblacion", "porcentaje_sobre_60", "ipm", "num_camas_UCI",
                                                    "porcentaje_subsidiado", "porcentaje_contributivo"]))

    df_variables.drop(columns=cols_to_drop, inplace=True)
    # pop_total = df_variables["poblacion"].sum()
    df_variables["porcentaje_sobre_60"] = df_variables["porcentaje_sobre_60"].multiply(100).multiply(df_variables["poblacion"])
    df_variables["porcentaje_subsidiado"] = df_variables["porcentaje_subsidiado"].multiply(100).multiply(df_variables["poblacion"])
    df_variables["porcentaje_contributivo"] = df_variables["porcentaje_contributivo"].multiply(100).multiply(df_variables["poblacion"])
    print(df_variables.head())
    df_variables =  df_variables.sum()
    df_variables = df_variables.multiply(df_variables["poblacion"])
    print(df_variables.head())
    raise Exception("DONE")
    # df_variables.astype({'num_camas_UCI': 'int32', 'poblacion':'int32'})
    df_variables.reset_index(inplace=True)
    df_variables = df_variables.transpose().reset_index()
    df_variables.set_index("index", inplace=True)
    df_national.set_index(0, inplace=True)
    df_variables = df_national.merge(df_variables, how="outer", left_index=True, right_index=True)
    df_variables.drop("index", inplace=True)
    df_variables.rename(columns={1:"Nacional", 0:polygon_name}, inplace=True)
    df_variables.rename({"ipm":"Indice de Pobreza Multidimensional (2018)",
                        "num_camas_UCI":"Número camas UCI",
                        "poblacion":"Población",
                        "porcentaje_contributivo":"Porcentaje en Régimen Contributivo (EPS)",
                        "porcentaje_subsidiado":"Porcentaje en Régimen Subsidiado (EPS)",
                        "porcentaje_sobre_60":"Porcentaje Población Mayor a 60"}, inplace=True)
    return NotImplemented


if __name__ == "__main__":

	# Reads the parameters from excecution
	location  = sys.argv[1] # location name
	agglomeration_method = sys.argv[2] # Aglomeration name
	polygon_name = sys.argv[3] # polygon name
	polygon_id  = sys.argv[4] # polygon id
	polygon_display_name = sys.argv[5] # polygon display name

	# Excecution
	main(location, agglomeration_method, polygon_name, polygon_id, polygon_display_name)