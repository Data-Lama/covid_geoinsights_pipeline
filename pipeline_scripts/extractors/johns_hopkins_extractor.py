# Script that extracts the cases and geo from a git repository (they must be both single files)


# Imports all the necesary functions
# Other imports
import os, sys

import pandas as pd
import shutil

import extraction_functions as ext_fun
from pathlib import Path

#Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')

repository_name = 'JOHNS-HOPKINS-COVID-19'

ident = '         '

print(ident + 'Updates Johns Hopkins Repository')


# Updates the repository
ext_fun.update_git_repository(repository_name)

print(ident + 'Done!')

