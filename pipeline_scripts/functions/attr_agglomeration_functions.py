# Attributte Aglomerator

import os
import re
import numpy as np
import pandas as pd
import scipy.stats as stats    

#Directories
from global_config import config
data_dir = config.get_property('data_dir')

'''
GONCHE:
Hice estos cambios:
    (1) Agregue al dict de response el nombre del atributo principal para poder ejecutar las funciones con atributo principal y secundario
    (2) Cuando se llegaba al merge de "Agglomerates columns with multiple parameters" fallaba si el df_response no se habia inicializado antes,
        es decir, cuando el único atributo era de este tipo. Inclui un check para ver si el df_response seguia siendo pd.series para inicializarlocomo DF.
    (3) Agregue parametros por defecto para las funciones y una funcion para setear los parametros que se llama get_params().
'''


# Identation variable
ident = '            '

# Main Method
def agglomerate(df, aggl_scheme, groupby_cols, agglomerate_cols):
    '''
    Main method.
    
    Method that agglomerates certain columns of a dataframe, when grouped by others
    according to by a certain scheme.
    
    This procedure first agglomerates at the same time all of the columns that take single parameter
    and then agglomerates the others one by one.
    
    Parameters
        df : Dataframe
        aggl_scheme : DataFrame with the agglomeration scheme
        groupby_cols : Columns to grupby
        agglomerate_cols : columns to agglomerate
        
    Returns
        Resulting DataFrame
    '''
    
    # Gets the function for each column
    functions = {}
    for col in agglomerate_cols:
        functions[col] = get_corresponding_function_declaration(col,aggl_scheme)
        
    
    # First agglomerates the columns with single parameter
    agg_dictionary = {}
    for col in agglomerate_cols:
        if functions[col]['single_attribute']:
            agg_dictionary[col] = get_corresponding_function(functions[col])
    
    # Agglomerates columns with single entry
    df_response = df[groupby_cols].drop_duplicates().copy()
    if len(agg_dictionary) > 0:
        df_response = df.groupby(groupby_cols).agg(agg_dictionary).reset_index()

        
    # Second, Agglomerates columns with multiple parameters
    for col in agglomerate_cols:
        if not functions[col]['single_attribute']:
            fun = get_corresponding_function(functions[col])
            df_temp = df.groupby(groupby_cols).apply(fun).to_frame().reset_index()
            
            # renames the new column
            df_temp.rename(columns = {0:col}, inplace = True)

            # Merges
            if isinstance(df_response, pd.Series):
                df_response = pd.DataFrame(df_response)
            df_response = df_response.merge(df_temp, on = groupby_cols)
            
    
    return(df_response)
    
    

def get_corresponding_function_declaration(col, aggl_scheme):
    '''
    Method that gets the corresponding function's declaration for a given column.
    
    Returns a dictionary with the reevant information.
    '''
    
    attr_name = col
    matches = []
    for name in aggl_scheme.attr_name:
        x = re.search(name, attr_name)
        if x:
            matches.append(name)
    
    if len(matches) == 0:
        raise ValueError(f'No match was found for: {attr_name}')
        
    if len(matches) > 1:
        print(f"WARNING: More than one possible function found. {matches}. Using first match: {matches[0]}")

    match = matches[0] # Gets the first one
    response = aggl_scheme.set_index("attr_name").loc[match].to_dict()
    
    # Check if the agglomeraton requires multiple parameters
    response['single_attribute'] = pd.isna(response['secondary_attr'])
    
    # Add name of original attr
    response['attr_name'] = col
    return(response)

# Parameter loading Mehtod
def get_params(default_params, input_params):
    for p in input_params:
        key = p.split("=")[0]
        value = p.split("=")[1]
        if key not in list(default_params.keys()):
            raise Exception(f"Parameter key {key} not found in allowed parameters")
        default_params[key] = value

    return default_params


# Organizer Method
def get_corresponding_function(function_declaration):
    '''
    Method that returns a function with a single parameter to insert into the groupby.
    
    If function is supported by the Pandas.GroupBy Scheme, will return a string for efficiency.
    '''
    
    name = function_declaration['aggl_function']

    # If the function requires parameters, they must all be specified. Parameters that are not
    # specified in this dict will raise an Exception
    default_params = {'attr_append': {"sep": "|",
                                    "null_handling":"drop_na",
                                    "fill_null":""},
                      'attr_union': {"sep": "|",
                                    "null_handling":"drop_na",
                                    "fill_null":""},
                      'attr_intersection': {"sep": "|",
                                    "null_handling":"drop_na",
                                    "fill_null":""},
                      'attr_weighted_average': {},
                    }
    

    # Sum
    if name == 'attr_addition':
        return('sum')
    
    # Average
    if name == 'attr_average':
        return('mean')
    
    # TODO:
    # Add the rest of the functions

    # Returns a list of the values separated by the indicated character.  
    if name == 'attr_append': 
        input_params = function_declaration['aggl_parameters'].split(";")
        params = get_params(default_params[name], input_params)
        if params["null_handling"] == "drop_na":
            fun = lambda s : params["sep"].join(s.dropna())
        else:
            fun = lambda s : params["sep"].join(s.fillna(params["fill_null"]))
        return(fun)

    if name == 'attr_union': 
        input_params = function_declaration['aggl_parameters'].split(";")
        params = get_params(default_params[name], input_params)
        if params["null_handling"] == "drop_na":
            fun = lambda s : attr_union(s.dropna(), params["sep"])
        else:
            fun = lambda s : attr_union(s.fillna(params["fill_null"]), params["sep"])
        return(fun)

    if name == 'attr_intersection': 
        input_params = function_declaration['aggl_parameters'].split(";")
        params = get_params(default_params[name], input_params)
        if params["null_handling"] == "drop_na":
            fun = lambda s : attr_intersection(s.dropna(), params["sep"])
        else:
            fun = lambda s : attr_intersection(s.fillna(params["fill_null"]), params["sep"])

        return(fun)
    
    if name == 'attr_weighted_average': 
        weight = function_declaration['secondary_attr']
        attr = function_declaration['attr_name']
        
        fun = lambda df : (df[weight]*df[attr]).sum() / df[weight].sum() 

        return(fun)

    if name == 'estimate_gamma_delay':
        fun = lambda s : estimate_gamma_delay(s)
        return(fun)
    
    
    raise ValueError(f'No implementation found for function: {name}')




# Support Functions
# ------------------

def attr_union(series, sep):
    '''
    Receives list of attributes as string. If sep not specified uses ","
    e.g:
        attrs = ["1", "2,1,4,5,7,2", "2,3"]
        returns ["1", "2", "4", "5", "7", "3"] 
    '''
    attrs = list(series)
    final_list = [] 
    attrs_list = [i.split(sep) for i in attrs]
    attrs_list = [i for sublist in attrs_list for i in sublist]
    union = sep.join(list(set().union(attrs_list)))
    
    return union

def attr_intersection(series, sep):
    '''
    Receives list of attributes as string. If sep not specified uses ","
    e.g:
        attrs = ["1", "2,1,4,5,7,2", "2,3,1"]
        returns ["1"] 
    '''
    attrs = list(series) 
    attrs_list = [set(i.split(sep)) for i in attrs]
    intersection = sep.join(list(set().intersection(*attrs_list)))
    return intersection

def estimate_gamma_delay(series):
    try:
        fit_alpha, fit_loc, fit_beta = stats.gamma.fit(series, floc = -1)
    except ValueError:
        return np.nan
    mean_g =  fit_alpha*fit_beta
    var_g  = fit_alpha*fit_beta**2

    x = np.linspace(-1, series.max(), 61)
    pdf_fitted = stats.gamma.pdf(x, *(fit_alpha, fit_loc, fit_beta))
    pdf_list = [str(i) for i in pdf_fitted.tolist()]
    return "|".join(pdf_list)
