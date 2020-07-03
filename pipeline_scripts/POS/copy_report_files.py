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
	shutil.copy(source, destination)


print(ident + 'Write TimeStamp')

#Saves the Statistics
with open(os.path.join(report_dir, 'README.txt'), 'w') as file:

	file.write('Last Updated: {}'.format(datetime.now()) + '\n')

print(ident + 'Done')