import os
import time
import pandas as pd
from pathlib import Path
from datetime import date

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

# Extracts download file name
if 'cases_download_file_name' in df_description.index:
    download_file_name = df_description.loc['cases_download_file_name','value']
    tmp = download_file_name.split("$")
    if len(tmp) == 2:
        download_file_name = tmp[0] + date + tmp[1]
    elif len(tmp) == 0:
        download_file_name = tmp[0]
else:
    raise Exception("Please modify descrition.csv to include the expected download file name")

# Set download path
download_path = os.path.join(cases_folder, download_file_name)

# Creates the folders if the don't exist
cases_folder = os.path.join(data_dir, 'data_stages', location_folder_name, 'raw','cases')

if not os.path.exists(cases_folder):
	os.makedirs(cases_folder)

cases_file_location = os.path.join(cases_folder, 'cases_raw.csv')


# repository folder
print(INDENT + 'Extracts Cases for {}'.format(location_name))


print(INDENT + '   Extracting:')

def get_data(xpath="", element_id=""):

    # Set driver options
    options = Options()
    options.set_preference("browser.download.folderList", 2)
    #options.set_preference("browser.download.dir", cases_folder)
    options.set_preference("browser.download.dir", "/Local/Users/andrea/Downloads")
    options.set_preference("browser.download.useDownloadDir", True)
    options.set_preference("browser.helperApps.neverAsk.saveToDisk", "text/csv")

    driver = webdriver.Firefox(options=options)
    driver.get(url) 

    driver.implicitly_wait(5)
    if xpath != "":
        element = driver.find_element_by_xpath(xpath)
    elif download_id != "":
        element = driver.find_element_by_id(element_id)
    else:
        raise Exception("Must provide xpath or element id.")

    driver.execute_script("arguments[0].click();", element)


def check_download():

    t = MAX_WAIT

    while t:
        if os.path.exists(download_path):
            retreived = True
            break
        mins, secs = divmod(t, 60)
        timeformat = '{:02d}:{:02d}'.format(mins, secs)
        print(timeformat, end='\r')
        time.sleep(1)
        t -= 1
    if not retreived:
        raise Exception('Timeout. Data was not retreived')

# Extracts XPath or element ID for download
if 'cases_download_xpath' in df_description.index:
    xpath = df_description.loc['cases_download_xpath', 'value']
    get_data(xpath=xpath)
elif 'cases_download_element_id' in df_description.index:
    element_id = df_description.loc['cases_download_element_id', 'value']
    get_data(element_id=element_id)
else:
    raise Exception("Please modify descrition.csv to include either an element_id or an Xpath for download")

print(INDENT + '   Waiting for download')
check_download()

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
    
    for col in old_cases.columns.difference(new_cases.columns):
        print(INDENT + '         {}: adding'.format(col))
        old_cases[col] = new_cases[col]

    # Remove outdated file
    os.remove(cases_file_location)

    # Save updated file
    old_cases.to_csv(cases_file_location)

else:
    os.rename(download_path, cases_file_location)



    