import datetime
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

def load_README(path):
	readme = {}
	with open(path, 'r') as f:
		for line in f:
			line = line.strip()
			line = line.split(':')
			key = "_".join(line[0].split(' '))
			value = ":".join(line[1:]).strip()
			readme[key] = value
	return readme
			