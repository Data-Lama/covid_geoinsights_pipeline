# Script for Bogota 

# Necesary imports
import pandas as pd
import numpy as np
import os
import geo_functions as geo
import geopandas as geopandas
from shapely import wkt
import json
import time 

# Generic Unifier
from generic_unifier_class import GenericUnifier
import general_functions as gf

#Directories
from global_config import config



# Constants for columns
symptoms_start_col = "fechainici"
current_state_col = "recuperado"
location_col = "ubicacion"
x_coord_col = "x"
y_coord_col = "y"
geo_id_col = "upzgeo"
geo_name_col = "nomupz_1"


class Unifier(GenericUnifier):
    '''
    Unifier class
    '''

    def __init__(self):
        # Initilizes
        GenericUnifier.__init__(self, 'Bogota', 'bogota')

    def build_cases_geo(self):


        # Reads cases
        cases = gf.decrypt_df(os.path.join(self.raw_folder,'cases', self.get('cases_file_name')), config.get_property('key_string') )

        cases[symptoms_start_col] = cases[symptoms_start_col].apply(lambda x: pd.to_datetime(x, errors="coerce"))

        cases = cases[[symptoms_start_col,current_state_col,geo_id_col, geo_name_col, x_coord_col, y_coord_col, location_col]].rename(columns = {symptoms_start_col:'date_time', geo_id_col:'geo_id',geo_name_col:'location',x_coord_col:'lon',y_coord_col:'lat'})

        # Cleans the state
        cases[current_state_col].fillna('Infectado', inplace = True)
        cases[current_state_col] = cases[current_state_col].apply(lambda s: s.replace(' ',''))
        cases.loc[cases[current_state_col] == '', current_state_col] = 'Infectado'
        cases = cases[cases[current_state_col].isin(['Recuperado','Fallecido','Infectado'])].dropna()

        # Discriminates by status
        cases['num_cases'] = 1
        cases.loc[cases[current_state_col] == 'Recuperado','num_recovered'] = 1
        cases.loc[cases[current_state_col] == 'Infectado','num_infected'] = 1
        cases.loc[cases[current_state_col] == 'Fallecido','num_diseased'] = 1

        # Add in Hospital
        cases.loc[(cases[current_state_col] == 'Infectado') & (cases[location_col].isin(['Hospital UCI', 'Hospital'])),'num_infected_in_hospital'] = 1
        cases.loc[(cases[current_state_col]== 'Infectado') & (~cases[location_col].isin(['Hospital UCI', 'Hospital'])),'num_infected_in_house'] = 1

        # Add in ICU
        cases.loc[(cases[current_state_col]== 'Infectado') & (cases[location_col].isin(['Hospital UCI'])),'num_infected_in_icu'] = 1


        # Removes temporary columns
        cases = cases.fillna(0).drop([current_state_col,location_col], axis = 1)

        # Convert to numeric
        cases.lon = cases.lon.apply(lambda l: float(str(l).replace(',','.')))
        cases.lat = cases.lat.apply(lambda l: float(str(l).replace(',','.')))

        # Groups
        cases = cases.groupby(['date_time','geo_id','location','lon','lat']).sum().reset_index()

        return(cases)

    def build_polygons(self):

        # MOCK
        definition = 'localidad'

        if definition == 'manzana':
            # Polygons
            polygons = geopandas.read_file(os.path.join(self.raw_folder, 'geo', self.get('shape_manzana_file_name')))

            # Sorts and drops
            polygons = polygons.sort_values('SHAPE_Area', ascending = False).drop_duplicates(subset = ['COD_AG'], keep = 'first')
            

            # Selects columns and renames
            polygons = polygons[['COD_AG','SHAPE_Area','geometry']].rename(columns = {'COD_AG':'poly_id','SHAPE_Area':'attr_area'})
            polygons['poly_name'] = polygons.poly_id.apply(lambda i: 'Manzana {}'.format(i))


            # Extracts the center
            centroids = geo.get_centroids(polygons.geometry)
            polygons['poly_lon'] = centroids.x
            polygons['poly_lat'] = centroids.y

            # Adjusts geometry  to latiude and longitud
            polygons = polygons.to_crs('epsg:4326')
            
        elif definition == 'sector':
            # Polygons
            polygons = geopandas.read_file(os.path.join(self.raw_folder, 'geo', self.get('shape_sector_file_name')))
            # Polygon Info
            polygons_info = pd.read_csv(os.path.join(self.raw_folder, 'geo', self.get('geo_file_name')))
            polygons_info['CODIGO FINAL'] = polygons_info['CODIGO FINAL'].astype(str)
            
            # Sorts and drops
            polygons_info = polygons_info[['CODIGO FINAL','TOTAL']].groupby('CODIGO FINAL').sum().reset_index()
            polygons = polygons.sort_values('Shape_Area', ascending = False).drop_duplicates(subset = ['SECC'], keep = 'first')
            
            # Merges 
            polygons = polygons.merge(polygons_info, left_on = 'SECC', right_on = 'CODIGO FINAL')
            polygons['poly_name'] = polygons.OBJECTID.apply(lambda i: 'Sector {}'.format(i))

            # Selects columns and renames
            polygons = polygons[['SECC','poly_name', 'TOTAL','Shape_Area','geometry']].rename(columns = {'SECC':'poly_id','TOTAL':'attr_population','Shape_Area':'attr_area'})


            # Extracts the center
            centroids = geo.get_centroids(polygons.geometry)
            polygons['poly_lon'] = centroids.x
            polygons['poly_lat'] = centroids.y

            # Adjusts geometry  to latiude and longitud
            polygons = polygons.to_crs('epsg:4326')


        elif definition == 'localidad':
            # Polygons
            polygons = geopandas.read_file(os.path.join(self.raw_folder, 'geo', self.get('shape_locality_file_name')))
            
            polygons.geometry = polygons.geometry.set_crs("EPSG:4326")
            # Polygon Info
            polygons = polygons[['location_i','label','geometry']].rename(columns = {'location_i':'poly_id', 'label':'poly_name'})
                        
            polygons.poly_id == polygons.poly_id.apply(lambda s: s.replace("colombia_bogota_localidad_",""))
            
            # Extracts the center
            centroids = geo.get_centroids(polygons.geometry)
            polygons['poly_lon'] = centroids.x
            polygons['poly_lat'] = centroids.y

            # Adjusts geometry  to latiude and longitud
            polygons = polygons.to_crs('epsg:4326')     

        elif definition == 'upz':
            # Polygons
            polygons = geopandas.read_file(os.path.join(self.raw_folder, 'geo', self.get('shape_upz_file_name')))
            
            # Polygon Info
            polygons = polygons[['UPlCodigo','UPlNombre','geometry']].rename(columns = {'UPlCodigo':'poly_id', 'UPlNombre':'poly_name'})

            # Adjust names
            polygons.poly_name = polygons.poly_name.apply(lambda s: " ".join([(x[0] + x[1:].lower()) for x in s.split(" ")]))    

            # Adjust poly id
            polygons.poly_id = polygons.poly_id.apply(lambda s: s.replace("UPZ","colombia_bogota_upz_"))

            # Extracts the center
            centroids = geo.get_centroids(polygons.geometry)
            polygons['poly_lon'] = centroids.x
            polygons['poly_lat'] = centroids.y

            # Adjusts geometry  to latiude and longitud
            polygons = polygons.to_crs('epsg:4326')        


        return(polygons)

