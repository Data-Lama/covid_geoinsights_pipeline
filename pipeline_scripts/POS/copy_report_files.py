# Python script for copying selected chart into single folder

import pandas as pd
import shutil
import glob
import numpy as np
import os
import glob
from datetime import datetime
import re
import constants as con

#Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')
report_dir = config.get_property('report_dir')

export_file = 'pipeline_scripts/configuration/export_files.csv'



ident = '         '

df_files = pd.read_csv(export_file)

print(ident + 'Copying {} files:'.format(df_files.shape[0]))

not_found = 0
for ind, row in df_files.iterrows():

	if row['folder'] == 'analysis':		
		source = os.path.join(analysis_dir,row.source)

	elif row['folder'] == 'data':		
		source = os.path.join(data_dir,row.source)

	else:
		raise ValueError('No support for folder: {} of source of file'.format(row['folder']))


	if row['type'] == 'figure':		
		destination = os.path.join( report_dir, con.figure_folder_name, row.destination)

	elif row['type'] == 'table':		
		destination = os.path.join( report_dir, con.table_folder_name, row.destination)		

	else:
		raise ValueError('No support for type: {} of file'.format(row['type']))


	# Inserts into array
	sources = [source]
	destinations = [destination]

	# Regex
	if '*' in source:
		# Inserts into array
		sources = []
		destinations = []

		for file in glob.glob(source):
			sources.append(file)
			destinations.append(destination.replace('*',file.split('/')[-1]))
			

	# At position
	elif "[" in source:

		start = source.split('[')[0]
		end = source.split(']')[1]
		number =  int(re.search('\[(.*)\]', source).group(1))

		options = glob.glob(start + '*' + end)

		max_num = -1
		min_num = np.inf

		for file in options:
			num = int(file.replace(start,'').replace(end,''))
			max_num = max(max_num, num)
			min_num = min(min_num, num)


		if number >= 0:
			source = start + str(min_num + number) + end
		else:
			source = start + str(max_num + number) + end

		# Inserts into array
		sources = [source]
		destinations = [destination]


	for i in range(len(sources)):

		source_temp = sources[i]
		destination_temp = destinations[i]

		print(ident + '   {}'.format(source_temp))


		try:
			shutil.copy(source_temp, destination_temp)

		except FileNotFoundError:
			# File NOt found
			not_found += 1
			print(f'File Not Found \n: {source}')
			print("")


print(ident + 'Write TimeStamp')

#Saves the Statistics
with open(os.path.join(report_dir, 'README.txt'), 'w') as file:

	file.write('Last Updated: {}'.format(datetime.now()) + '\n')

if not_found > 0:
	raise ValueError(f"{not_found} files where not found.")

print(ident + 'Done')