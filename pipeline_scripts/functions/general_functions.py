# Module with general functions



def clean_for_publication(s):
	'''
	Function that cleans a string for publications
	Mostly converts to spansh
	'''

	subs = {}
	subs['11001'] = 'Bogotá'
	subs['colombia'] = 'Colombia'
	subs['italy'] = 'Italia'

	for k in subs:
		s = s.replace(k, subs[k])


	# Checks for the us
	s_split = s.split('-')
	if '_' in s_split[-1]:
		s = ('-'.join(s_split[0:-1])).replace(', US',', EEUU').replace(', ','-')

	return(s)
