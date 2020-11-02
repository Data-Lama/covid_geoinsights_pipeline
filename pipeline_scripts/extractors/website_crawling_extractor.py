import os
import sys
import time
import pandas as pd
from os import listdir
from pathlib import Path
from datetime import date

import fb_functions as fb

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.firefox.options import Options


MAX_WAIT = 600

INDENT = '         '

#Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')

# Reads the parameters from excecution
location_name  = sys.argv[1] # Location name
location_folder_name = sys.argv[2] # location folder name

# Get date
today = date.today()

# YYYYmmdd
date = today.strftime("%Y%m%d")

# Location Folder
location_folder = os.path.join(data_dir, 'data_stages', location_folder_name)

# Extracts the description
df_description = pd.read_csv(os.path.join(location_folder, 'description.csv'), index_col = 0)

# Extracts URL
url = df_description.loc['cases_url','value']

# Extract file name
if 'cases_file_name' in df_description.index:
    cases_file_name = df_description.loc['cases_file_name','value']
else:
    raise Exception("Please modify descrition.csv to include the cases file name")

# Creates the folders if the don't exist
cases_folder = os.path.join(data_dir, 'data_stages', location_folder_name, 'raw','cases')

if not os.path.exists(cases_folder):
	os.makedirs(cases_folder)

cases_file_location = os.path.join(cases_folder, cases_file_name)


# repository folder
print(INDENT + 'Extracts Cases for {}'.format(location_name))


print(INDENT + '   Extracting:')

def get_driver():
    # Set driver options

    driver = fb.get_driver(cases_folder)
    
    driver.get(url) 
    driver.implicitly_wait(5)

    return driver

def get_data(driver, xpath="", element_id=""):

    if xpath != "":
        element = driver.find_element_by_xpath(xpath)
    elif element_id != "":
        element = driver.find_element_by_id(element_id)
    else:
        raise Exception("Must provide xpath or element id.")

    driver.execute_script("arguments[0].click();", element)

def check_download():
    t = MAX_WAIT
    files_after = os.listdir(cases_folder)
    while t:
        if len(list(set(files_after) - set(files_before))) > 0:
            retreived = True
            return list(set(files_after) - set(files_before))[0]
        mins, secs = divmod(t, 60)
        timeformat = '{:02d}:{:02d}'.format(mins, secs)
        print(timeformat, end='\r')
        time.sleep(1)
        t -= 1
        files_after = os.listdir(cases_folder)
    if not retreived:
        raise Exception('Timeout. Data was not retreived')

fx_driver = get_driver()

files_before = os.listdir(cases_folder)

# Extracts XPath or element ID for download
if 'cases_download_xpath' in df_description.index:
    xpath = df_description.loc['cases_download_xpath', 'value']
    get_data(fx_driver, xpath=xpath)
elif 'cases_download_element_id' in df_description.index:
    element_id = df_description.loc['cases_download_element_id', 'value']
    get_data(fx_driver, element_id=element_id)
else:
    raise Exception("Please modify descrition.csv to include either an element_id or an Xpath for download")

# Get name of downloaded file
download_file_name = check_download()
print(download_file_name)
download_path = os.path.join(cases_folder, download_file_name)

print(INDENT + '   Waiting for download')


print(INDENT + '   Checking Integrity')

if os.path.exists(cases_file_location) and os.path.isfile(cases_file_location):
    try:
        old_cases = pd.read_csv(cases_file_location, low_memory=False)
        new_cases = pd.read_csv(download_path, low_memory=False)
    except:
        old_cases = pd.read_csv(cases_file_location, low_memory=False, encoding = 'latin-1')
        new_cases = pd.read_csv(download_path, low_memory=False, encoding = 'latin-1')

    ok = True

    if not old_cases.columns.equals(new_cases.columns):
        print(INDENT + '      New columns found, adding new data')
        
    os.rename(download_path, cases_file_location)

else:
    os.rename(download_path, cases_file_location)

print(INDENT + '         Done!')


fx_driver.close()
    