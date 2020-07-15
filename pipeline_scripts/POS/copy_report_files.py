# Python script for copying selected chart into single folder

import pandas as pd
import shutil
import glob
import numpy as np
import os
from datetime import datetime

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


	source = os.path.join(analysis_dir,row.source)
	destination = os.path.join( report_dir, row.destination)

	if '*' in source:
		start = source.split('*')[0]
		end = source.split('*')[1]
		options = glob.glob(source)
		max_num = -1
		for file in options:
			num = int(file.replace(start,'').replace(end,''))
			if num > max_num:
				max_num = num
				source = file


	print(ident + '   {}'.format(source))


	try:
		shutil.copy(source, destination)

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