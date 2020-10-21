import os
import re
import numpy as np
import pandas as pd

class AttrAgglomerator():

    # Identation variable
    ident = '            '

    def __init__(self, aggl_scheme):
        
        self.aggl_scheme = aggl_scheme

    def agglomerate_attrs(self):
    
        '''
        This method receives the dataframe to be aglomerated, and attribute by attribute, it
        chooses the correct agglomeration scheme and returns a dataframe with the agregated values 
        of each attribute.

        input: 
        DataFrame with at least the following columns:
            - poly_id
            - attr_* (indefinite number of colums starting with)

        output:
        dictionary where the attribute names are the keys and the values are the agglomerated new values.

        '''

        agglomerated_attr_values = {}

        for col in self.df.columns:
            if "attr" not in col:
                continue

            attr_name = col
            matches = []
            for name in self.aggl_scheme.attr_name:
                x = re.search(name, attr_name)
                if x:
                    matches.append(name)
            if len(matches) > 1:
                print(f"WARNING: More than one possible function found. {matches}. Using first match: {matches[0]}")
            
            match = matches[0]
            attr_function = self.aggl_scheme.set_index("attr_name").at[match, "aggl_function"]
            secondary_attr = self.aggl_scheme.set_index("attr_name").at[match, "secondary_attr"]
            attr_parameters = self.aggl_scheme.set_index("attr_name").at[match, "aggl_parameters"]

            if self.is_empty(secondary_attr) and self.is_empty(attr_parameters):
                expression = f"self.{attr_function}(\"{attr_name}\")"
            elif self.is_empty(secondary_attr):
                expression = f"self.{attr_function}(\"{attr_name}\", {attr_parameters})"
            elif self.is_empty(attr_parameters):
                expression = f"self.{attr_function}(\"{attr_name}\", secondary_attr=\"{secondary_attr}\")"
            else:
                expression = f"self.{attr_function}(\"{attr_name}\", {attr_parameters}, secondary_attr=\"{secondary_attr}\")"

            agglomeration_result = eval(expression)
            
            agglomerated_attr_values[attr_name] = agglomeration_result
        
        return agglomerated_attr_values

    def get_agglomerated_attrs(self, df):
        self.df = df
        aggl_result = self.agglomerate_attrs()
        return pd.Series(aggl_result)

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
        