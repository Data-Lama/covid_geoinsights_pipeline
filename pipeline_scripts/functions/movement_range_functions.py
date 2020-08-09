# Movement range functions
import pandas as pd
import numpy as np
import geopandas
import os

# Direcotries
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')
key_string = config.get_property('key_string') 


def construct_movement_range_by_polygon(df_movement_range, df_polygons):
    '''
    Method that constructs the movent range by polygon ID based on its geometry.

    If polygons have the  population and size attribute, it should be done by density. Else
    will only look at the volumes.

    Parameters
    df_movement_range: pd.DataFrame
        Unified movement range file
    df_polygons : geopandas
        Geopandas dataframe with the polygons

    Must return a dataframe with columns:
        - date_time: date of the movement range
        - poly_id: The id of the polygon 
        - movement_change: Relative movement change
    '''

    # TODO
    # Mock Implementation

    df = pd.read_csv(os.path.join(data_dir, 'data_stages/colombia/unified/movement_range_by_polygon.csv'))
    df.rename(columns = {'extrapolated_relative_movement':'movement_change'}, inplace = True)

    return(df)


