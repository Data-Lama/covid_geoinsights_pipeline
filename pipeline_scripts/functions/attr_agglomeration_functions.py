# Attributte Aglomerator

import os
import re
import numpy as np
import pandas as pd
import scipy.stats as stats    

#Directories
from global_config import config
data_dir = config.get_property('data_dir')


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
    
    return(response)



# Organizer Method
def get_corresponding_function(function_declaration):
    '''
    Method that returns a function with a single parameter to insert into the groupby.
    
    If function is supported by the Pandas.GroupBy Scheme, will return a string for efficiency.
    '''
    
    name = function_declaration['aggl_function']
    
    # Sum
    if name == 'attr_addition':
        return('sum')
    
    # Average
    if name == 'attr_average':
        return('mean')
    
    # TODO:
    # Add the rest of the functions
    # Examples

    # Andre: La idea es llamar las funciones que ya hiciste dentro de
    # un lambda y devolver eso. Que este metodo no quede con funciones muy complejas adentro
    if name == 'append': # Mock
        function_declaration['other_attribute'] # sep = '|'
        sep = '|'
        
        fun = lambda s : sep.join(s)
        return(fun)
    
    if name == 'wweighted_average': # Mock
        col2 = function_declaration['col2']
        
        fun = lambda df : (df[col2]*df[col]).sum() / df[col2].sum() # <- super mock

        return(fun)
    
    
    raise ValueError(f'No implementation found for function: {name}')




# Support Functions
# ------------------


def attr_addition(self, attr_name):
    attrs = list(self.df[attr_name].dropna())
    return sum(attrs)

def attr_substraction():
    return NotImplemented

def attr_append(self, attr_name, sep=","):
    '''
    Receives list of attributes as string. If sep not specified uses ","
    e.g:
        attrs = ["1", "2,1,4,5,7,2", "2,3"]
        returns ["1", "2", "1", "4", "5", "7", "2", "2", "3"] 
    '''
    attrs = list(self.df[attr_name].dropna())
    attrs = [i.split(sep) for i in attrs]
    attrs_list = [i for sublist in attrs for i in sublist]

    return attrs_list

def attr_append_int(self, attr_name, sep=","):
    attrs = list(self.df[attr_name].dropna())
    attrs_list = self.attr_append(attr_name, sep=sep)
    attrs_list_int = [int(i) for i in attrs_list]

    return attrs_list

def attr_append_float(self, attr_name, sep=","):
    attrs = list(self.df[attr_name].dropna())
    attrs_list = self.attr_append(attr_name, sep=sep)
    attrs_list_int = [float(i) for i in attrs_list]

    return attrs_list

def attr_union(self, attr_name, sep=","):
    '''
    Receives list of attributes as string. If sep not specified uses ","
    e.g:
        attrs = ["1", "2,1,4,5,7,2", "2,3"]
        returns ["1", "2", "4", "5", "7", "3"] 
    '''
    attrs = list(self.df[attr_name].dropna())
    attrs_list = self.attr_append(attr_name, sep=sep)
    union = list(set().union(attrs_list))
    
    return union

def attr_union_int(self, attr_name):
    '''
    Receives list of attributes as string. If sep not specified uses ","
    e.g:
        attrs = ["1", "2,1,4,5,7,2", "2,3"]
        returns [1, 2, 4, 5, 7, 3] 
    '''
    attrs = list(self.df[attr_name].dropna())
    union = self.attr_union(attrs)
    union_int = [int(i) for i in union] 
    return union_int

def attr_intersection(self, attr_name, sep="|"):
    '''
    Receives list of attributes as string. If sep not specified uses ","
    e.g:
        attrs = ["1", "2,1,4,5,7,2", "2,3,1"]
        returns ["1"] 
    '''
    attrs = list(self.df[attr_name].dropna())
    attrs = [i.split(sep) for i in attrs]
    attrs_list = [i for sublist in attrs for i in sublist]
    intersection = list(set().intersection(attrs_list))
    return intersection

def attr_intersection_int(self, attr_name):
    '''
    Receives list of attributes as string. If sep not specified uses ","
    e.g:
        attrs = ["1", "2,1,4,5,7,2", "2,3,1"]
        returns [1] 
    '''
    attrs = list(self.df[attr_name].dropna())
    intersection = self.attr_intersection(attrs)
    intersection_int = [int(i) for i in intersection] 
    return intersection_int

def attr_average(self, attr_name):
    attrs = list(self.df[attr_name].dropna())
    avg = sum(attrs) / len(attrs)
    return avg

def attr_weighted_average(self, attr_name, secondary_attr):
    attrs = list(self.df[attr_name].fillna(0))
    weights = list(self.df[secondary_attr].fillna(0))
    wavg = sum(weights[i] * attrs[i] / sum(attrs) for i in range(len(weights)))
    return wavg

def is_empty(self, parameters):
    if isinstance(parameters, str):
        if parameters == "": return True
    if isinstance(parameters, float):
        if np.isnan(parameters): return True
    
def estimate_gamma_delay(self, attr_name):
    try:
        fit_alpha, fit_loc, fit_beta = stats.gamma.fit(self.df[attr_name], floc = -1)
    except ValueError:
        self.log.append(f"WARNING: estimate_gamma_delay failed to agglomerate {self.df.geo_id.unique()[0]}")
        return np.nan
    mean_g =  fit_alpha*fit_beta
    var_g  = fit_alpha*fit_beta**2

    x = np.linspace(-1, self.df[attr_name].max(), 61)
    pdf_fitted = stats.gamma.pdf(x, *(fit_alpha, fit_loc, fit_beta))
    pdf_list = [str(i) for i in pdf_fitted.tolist()]
    return "|".join(pdf_list)
    