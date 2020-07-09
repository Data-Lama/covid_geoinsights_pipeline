# Agglomerates by comunnity
options(warn=-1)
# Checks if the R enviorment has the packages
list.of.packages <- c("dplyr", "igraph","proxy", "ramify")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

# Imports the libraries
suppressMessages(library("dplyr"))
suppressMessages(library('igraph'))
suppressMessages(library('proxy'))
suppressMessages(library('ramify'))


source("pipeline_scripts/functions/agglomeration_functions.R")

# Directories
source("global_config/config.R")
data_dir = get_property('data_dir')
analysis_dir = get_property('analysis_dir')


# Ident for console
ident = '         '


args = commandArgs(trailingOnly=TRUE)
location_name = args[1] # Location Name
location_folder = args[2] # Location Folder
agglomeration_method = args[3] # Agglomeration method to build on to

# For Debug
#setwd("~/Dropbox/Projects/covid_fb_pipeline/covid_geoinsights_pipeline")
#location_name = 'Colombia'
#location_folder = 'colombia'


if(is.na(agglomeration_method))
{
  # First tries geometry
  if(dir.exists(file.path(data_dir, 'data_stages',location_folder,'agglomerated','geometry')))
    agglomeration_method = 'geometry'
  else if(dir.exists(file.path(data_dir, 'data_stages',location_folder,'agglomerated','radial')))
    agglomeration_method = 'radial'
  else
    stop('No agglomeration method found to build communiies')
}



cat(paste(ident, 'Community Agglomeration for: ', location_name,  '\n', sep = ""))

# Agglomerated Folder
agglomerated_folder = file.path(data_dir, 'data_stages',location_folder,'agglomerated', agglomeration_method)

# Export Folder
# Two steps
export_folder = file.path(data_dir, 'data_stages',location_folder, 'agglomerated')
dir.create(export_folder)

export_folder = file.path(export_folder, 'community')
dir.create(export_folder)



# Loads movement
cat(paste(ident, '   Loads Movement','\n', sep = ""))
movement = read.csv(file.path(agglomerated_folder, 'movement.csv'))
polygons = read.csv(file.path(agglomerated_folder, 'polygons.csv'))
cases = read.csv(file.path(agglomerated_folder, 'cases.csv'), stringsAsFactors = FALSE)

# Extracts the cases columns
cases_col = colnames(cases)[as.vector(sapply(colnames(cases), function(col){grepl('num_', col, fixed = TRUE)}))]
# Extracts the attr columns
attr_col = colnames(polygons)[as.vector(sapply(colnames(polygons), function(col){grepl('attr_', col, fixed = TRUE)}))]


# Groups the dataframe
g_mov = movement %>% 
        group_by(start_poly_id, end_poly_id) %>% 
        summarise(movement = mean(movement)) %>%
        ungroup() %>%
        select(start_poly_id, end_poly_id, movement)

# Creates the graph
cat(paste(ident, '   Creates the Graph','\n', sep = ""))
g <- graph_from_data_frame(d = g_mov %>% rename(weight = movement), directed = FALSE, vertices = polygons$poly_id)
g = simplify(g)


cat(paste(ident, '   Finds Communities and Pagerank','\n', sep = ""))
# Clusters
wc <- cluster_walktrap(g)

# Assigns the community for each center
polygons$community_id = membership(wc)


cat(paste(ident, '   Agglomerates Cases','\n', sep = ""))


# creates the agglomerated polygons
#------------------------
cat(paste(ident, '   Agglomerates Polygons','\n', sep = ""))

# Final_comunity
final_community = polygons %>%
                  select(poly_id, community_id, num_cases) %>%
                  group_by(community_id) %>%
                  summarise(final_id = exctract_id_by_cases(poly_id, num_cases)) %>%
                  ungroup()  

polygons = polygons %>%
           inner_join(final_community, by = c('community_id' = 'community_id')) %>%
           mutate(community_id = final_id)



# Creates the community name (the one with highest frequency) for aglomeration
agg_poly_1 = polygons %>% 
  group_by(community_id) %>%
  summarise(poly_name = exctract_name_by_cases(poly_name, num_cases), agglomerated_polygons = extract_list_of_agg_polygons(poly_id), geometry = extract_geometry(poly_lon, poly_lat), poly_lon = extract_center_by_cases(poly_lon, poly_lat, num_cases)[1], poly_lat = extract_center_by_cases(poly_lon, poly_lat, num_cases)[2]) %>%
  ungroup()  

# Creates the location to community map
polygon_community_map = agg_poly_1 %>% 
                        select(community_id, poly_name) %>%
                        rename(community_name = poly_name) %>%
                        inner_join(polygons, by = c('community_id' = 'final_id')) %>%
                        select(poly_id, poly_name, community_id, community_name )
          


agg_poly = polygons %>% 
  group_by(community_id) %>%
  summarise_at(c(attr_col, cases_col), sum, na.rm = TRUE) %>%
  ungroup() %>%
  inner_join(agg_poly_1, by = c('community_id' = 'community_id')) %>%
  select(c('community_id', 'poly_name', 'agglomerated_polygons', 'geometry', 'poly_lon', 'poly_lat', all_of(c(attr_col,cases_col)))) %>%
  rename(poly_id = community_id) %>%
  arrange(desc(num_cases))



# Creates the agglomerated cases
# ------------------------------
agg_cases = cases %>% 
            inner_join(polygons %>% select(poly_id,  community_id), by = c('poly_id' = 'poly_id')) %>%
            group_by(date_time, community_id)  %>%
            summarise_at(cases_col, sum, na.rm = TRUE) %>%
            ungroup() %>%
            inner_join(agg_poly %>% select(poly_id, poly_name) , by = c('community_id' = 'poly_id')) %>%
            select(c('date_time', 'community_id', 'poly_name', all_of(cases_col))) %>%
            rename(poly_id = community_id,location = poly_name) %>%
            arrange(date_time, desc(num_cases))
            



# creates the agglomerated Movement
#------------------------
cat(paste(ident, '   Agglomerates Movement','\n', sep = ""))
agg_mov = movement %>% 
          inner_join(polygons %>% select(poly_id, community_id), by = c('start_poly_id' = 'poly_id')) %>%
          mutate(start_poly_id = community_id) %>%
          select(date_time, start_poly_id, end_poly_id, movement) %>%
          inner_join(polygons %>% select(poly_id, community_id), by = c('end_poly_id' = 'poly_id')) %>%
          mutate(end_poly_id = community_id) %>%
          select(date_time, start_poly_id, end_poly_id, movement) %>%
          group_by(date_time, start_poly_id, end_poly_id) %>%
          summarise(movement = sum(movement)) %>%
          ungroup() %>%
          arrange(date_time)



# creates the agglomerated Populations
#------------------------
cat(paste(ident, '   Agglomerates Population','\n', sep = ""))
# Loads the cases data to assign the comunity ids and name them
pop = read.csv(file.path(agglomerated_folder, 'population.csv'), stringsAsFactors = FALSE)

agg_pop = pop %>%
          inner_join(polygons %>% select(poly_id, community_id), by = c('poly_id' = 'poly_id')) %>%
          mutate(poly_id = community_id) %>%
          select(date_time, poly_id, population) %>%
          group_by(date_time, poly_id, poly_id) %>%
          summarise(population = sum(population)) %>%
          ungroup() %>%
          arrange(date_time)



cat(paste(ident, '   Saves Files','\n', sep = ""))
# Saves all the datasets
write.csv( agg_cases, file.path(export_folder,'cases.csv'), row.names = FALSE)
write.csv( agg_mov, file.path(export_folder,'movement.csv'), row.names = FALSE)
write.csv(agg_poly, file.path(export_folder,'polygons.csv'), row.names = FALSE)
write.csv(agg_pop, file.path(export_folder,'population.csv'), row.names = FALSE)
write.csv(polygon_community_map, file.path(export_folder,'polygon_community_map.csv'), row.names = FALSE)

cat(paste(ident, 'Done!','\n', sep = ""))

