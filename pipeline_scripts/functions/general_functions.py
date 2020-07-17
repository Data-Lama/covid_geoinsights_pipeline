import datetime
import cryptography
from cryptography.fernet import Fernet
import uuid
import pandas as pd
import os
import unidecode


# Module with general functions
from global_config import config
data_dir = config.get_property('data_dir')


def clean_for_publication(s):
	'''
	Function that cleans a string for publications
	Mostly converts to spansh
	'''

	subs = {}
	subs['11001'] = 'Bogotá'
	subs['colombia'] = 'Colombia'
	subs['italy'] = 'Italia'
	subs['brazil'] = 'Brasil'
	subs['peru'] = 'Perú'

	subs['Bogotá D.C.-Bogotá d C.'] = 'Bogotá'
	subs['-'] = ' - '

	for k in subs:
		s = s.replace(k, subs[k])


	# Checks for the us
	s_split = s.split('-')
	if '_' in s_split[-1]:
		s = ('-'.join(s_split[0:-1])).replace(', US',', EEUU').replace(', ','-')

	return(s)

def create_folder_name(s):
	'''
	given a string, return a valid file name
	'''
	s = unidecode.unidecode(s)

	s = s.lower()
	s = s.replace('.','')

	splitting_strings = ['-',':','_']
	for sp in splitting_strings:
		s = s.split(sp)[0]

	# Strips
	s = s.strip()
	s = s.replace('   ',' ')
	s = s.replace('  ',' ')

	s = s.replace(' ','_')

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
			

def encrypt_df(df, filename, key_string):
    
	# coverts to bytes
	key = bytes(key_string, 'utf-8')

	temp_file = '.'  + str(uuid.uuid4())

	df.to_csv(temp_file, index = False)

	#  Open the file to encrypt
	with open(temp_file, 'rb') as f:
		data = f.read()

	fernet = Fernet(key)
	encrypted = fernet.encrypt(data)

	# Write the encrypted file
	with open(filename, 'wb') as f:
		f.write(encrypted)

	os.remove(temp_file)

    
def decrypt_df(filename, key_string):
    
	# coverts to bytes
	key = bytes(key_string, 'utf-8')

	temp_file = '.'  + str(uuid.uuid4())

	#  Open the file to decrypt
	with open(filename, 'rb') as f:
	    data = f.read()

	fernet = Fernet(key)
	decrypted = fernet.decrypt(data)

	# Open the decrypted file
	with open(temp_file, 'wb') as f:
	    f.write(decrypted)

	df = pd.read_csv(temp_file, low_memory=False)

	os.remove(temp_file)

	return(df)


def get_description(location_folder_name):
	'''
	Gets the description of a place
	'''

	df_description = pd.read_csv(os.path.join(data_dir, 'data_stages', location_folder_name, 'description.csv'), index_col = 0)
	return(df_description)

def is_encrypted(location_folder_name):
	'''
	Checks if the location folder description has the encrypted tag
	'''
	tag = 'encrypted'

	df = get_description(location_folder_name)

	if tag in df.index and df.loc[tag,'value'].upper() == 'TRUE':
		return(True)

	return(False)

