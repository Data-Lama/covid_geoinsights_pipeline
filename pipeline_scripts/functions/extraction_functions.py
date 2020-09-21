# Extraction Functions
# Included download a specific file
# Also updates github repositories


# Necesary imports
import pandas as pd
import numpy as np
from pathlib import Path
import os

# Imports git
import git 

import requests

#Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')


# Git Repositories
git_repositories_location = os.path.join(data_dir, 'git_repositories/')

# Downloads
downloads_location = os.path.join(data_dir, 'downloads/')


def update_git_repository(respository):
	'''
	Automatically updates the given respository
	'''

	g = git.cmd.Git(os.path.join(git_repositories_location, respository))
	g.pull()
	

def download_file(url, name):
	'''
	Download a file from the given URL and saves it with the name in downloads_location
	'''

	r = requests.get(url, allow_redirects=True)
	open(os.path.join(downloads_location, name), 'wb').write(r.content)