import os
import re
import numpy as np
import pandas as pd
import scipy.stats as stats    

#Directories
from global_config import config
data_dir = config.get_property('data_dir')

class AttrAgglomerator():

    # Identation variable
    ident = '            '

    # def __init__(self, aggl_scheme):

    #     self.aggl_scheme = aggl_scheme

    def __init__(self, location_folder, stage_folder, aggl_scheme):

        self.location_folder = location_folder
        self.stage_folder = stage_folder
        self.aggl_scheme = aggl_scheme
        self.log = []

    def write_agglomeration_log(self):
        out_path = os.path.join(data_dir, "data_stages",self.location_folder, self.stage_folder, "aggl.log")
        with open(out_path, 'w') as f:
            for line in self.log:
                f.write(line + "\n")

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
            if not ("attr" in col):
                continue
            # print(f"\tAggregating {col}")
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
        self.write_agglomeration_log()
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
        