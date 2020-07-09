# Constants

import locations.germany_functions as germany
import locations.brazil_functions as brazil
import locations.new_south_wales_functions as new_south_wales
import locations.bogota_functions as bogota
import locations.colombia_functions as colombia
import locations.chile_functions as chile
import locations.peru_functions as peru
import locations.mexico_functions as mexico

agglomeration_methods = ['radial','community','geometry']



def get_unifier_class(location):

	if location == "germany":

		unif = germany.Unifier()
		return(unif)


	if location == "brazil":

		unif = brazil.Unifier()
		return(unif)


	if location == "new_south_wales":

		unif = new_south_wales.Unifier()
		return(unif)

	if location == "bogota":

		unif = bogota.Unifier()
		return(unif)


	if location == "colombia":

		unif = colombia.Unifier()
		return(unif)


	if location == "chile":

		unif = chile.Unifier()
		return(unif)

	if location == "peru":

		unif = peru.Unifier()
		return(unif)

	if location == "mexico":

		unif = mexico.Unifier()
		return(unif)

	raise ValueError('No unifier found for: {}. Please add it'.format(location))