# Script that agglomerates by radius based on the unified data


# Imports all the necesary functions
import agglomeration_functions as agg


# Other imports
import os, sys

from pathlib import Path
import pandas as pd

from global_config import config
data_dir = config.get_property('data_dir')


# Method Name
method_name = 'radial'

# Reads the parameters from excecution
location_name  = sys.argv[1] # location namme
location_folder_name  = sys.argv[2] # location folder namme
radius  = int(sys.argv[3]) # radius

# Sets the location
location_folder = os.path.join(data_dir, 'data_stages', location_folder_name)

# Creates the folders if the don't exist
# constructed
agglomeration_folder = os.path.join(location_folder, 'agglomerated/', method_name)
if not os.path.exists(agglomeration_folder):
	os.makedirs(agglomeration_folder)



ident = '         '

print(ident + 'Agglomerates for {}'.format(location_name))
print()
print(ident + 'Builds Datasets by Radius')
print(ident + 'Parameters:')
print(ident + '   Radius: {} km'.format(radius))

# Loads Data
print()
print(ident + '   Loads Data:')

print(ident + '      Polygons')
polygons = pd.read_csv(os.path.join(location_folder, 'unified/polygons.csv'))

print(ident + '      Cases')
cases = pd.read_csv(os.path.join(location_folder, 'unified/cases.csv'))

print(ident + '      Movement')
movement = pd.read_csv(os.path.join(location_folder,  'unified/movement.csv'))

print(ident + '      Population')
population = pd.read_csv(os.path.join(location_folder,  'unified/population.csv'))

print()
print(ident + '   Agglomerates:')

final_polygons, final_cases, final_population, final_movement = agg.agglomerate_by_radius(polygons, cases, movement, population, radius)



# Builds The Agglomeration Summary
sum_ident = '   '
summary = ''
summary = summary + sum_ident + 'Initial Number of Locations: {}'.format(final_cases.location.unique().size) + '\n'
summary = summary + sum_ident + 'Final Number of Locations: {}'.format(final_cases.poly_id.unique().size) + '\n'
summary = summary + sum_ident + 'Merged Locations: {}'.format(final_cases[final_cases.poly_merged].location.unique().size) + '\n'
summary = summary + sum_ident + 'Merged Cases: {}'.format(final_cases[final_cases.poly_merged].shape[0]) + '\n'
summary = summary + sum_ident + 'Polygon Distance Summary (All):' + '\n'

poly_summary = str(final_cases.poly_distance.astype(float).describe()).split('\n')[0:-1]

for line in poly_summary:
	summary = summary + sum_ident + '   ' + line + '\n'

summary = summary + sum_ident + 'Polygon Distance Summary (Merged):' + '\n'

poly_summary = str(final_cases[final_cases.poly_merged].poly_distance.astype(float).describe()).split('\n')[0:-1]

for line in poly_summary:
	summary = summary + sum_ident + '   ' + line + '\n'




print(ident + '   Saves Data:')

print(ident + '      Cases')
final_cases = final_cases[['date_time','location','poly_id','num_cases']]
cases_date = final_cases.date_time.max()
final_cases.to_csv(os.path.join(agglomeration_folder, 'cases.csv'), index = False)




print(ident + '      Movement')
final_movement = final_movement[['date_time','start_poly_id','end_poly_id', 'n_crisis']].rename(columns = {'n_crisis':'movement'})


# Rounds to day
final_movement.date_time = pd.to_datetime(final_movement.date_time)
final_movement.date_time = final_movement.date_time.dt.round(freq = 'D')
final_movement = final_movement.groupby(['date_time','start_poly_id','end_poly_id']).sum().reset_index()

movement_date = final_movement.date_time.max()
final_movement.to_csv(os.path.join(agglomeration_folder, 'movement.csv'), index = False)


print(ident + '      Population')
final_population = final_population[['date_time','poly_id','n_crisis']].rename(columns = {'n_crisis': 'population'})
population_date = final_population.date_time.max()
final_population.to_csv(os.path.join(agglomeration_folder, 'population.csv'), index = False)


print(ident + '      Polygons')
final_polygons.to_csv(os.path.join(agglomeration_folder, 'polygons.csv'), index = False)
# poly_date = np.min([cases_date, movent_date, population_date])

print(ident + '   Saves Statistics:')

#Saves the dates
with open(os.path.join(agglomeration_folder, 'README.txt'), 'w') as file:

	file.write('Parameters For Radial Agglomeration:' + '\n')
	file.write('   Type: {}'.format('by radius') + '\n')
	file.write('   Radius: {} km'.format(radius) + '\n')
	file.write('Current max dates for databases:' + '\n')
	file.write('   Cases: {}'.format(cases_date) + '\n')
	file.write('   Movement: {}'.format(movement_date) + '\n')
	file.write('   Population: {}'.format(population_date) + '\n')
	file.write('Agglomeration Summary:' + '\n')
	file.write(summary)

	
print(ident + 'Done! Data copied to: {}/agglomerated/{}'.format(location_folder_name, method_name))
print('')