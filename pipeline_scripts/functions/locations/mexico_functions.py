

# Generic Unifier
from generic_unifier_class import GenericUnifier

class Unifier(GenericUnifier):
	'''
	Unifier class
	'''

	def __init__(self):
		# Initilizes
		GenericUnifier.__init__(self, 'Mexico', 'mexico')


    def build_cases_geo(self):
    '''
    Method that builds the geolocated cases. 

    Should return a DataFrame with at least the columns:
        - date_time (pd.date_time): time stamp of the case
        - geo_id (str): geographical id of the location
        - location (str): location name
        - lon (float): longitud of the location
        - lat (float): lattitude of the location
        - num_cases (float): the number of cases at that point and time
    '''