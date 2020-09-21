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
source("global_config/config.R")
data_dir = get_property('data_dir')

# Ident for console
ident = '         '


args = commandArgs(trailingOnly=TRUE)
location_name = args[1] # Location Name
location_folder = args[2] # Location Folder


# For Debug
#setwd("~/Dropbox/Projects/covid_fb")
#location_name = 'Colombia'
#location_folder = 'colombia'

cat(paste(ident, 'Community Agglomeration for: ', location_name,  '\n', sep = ""))

# Unified Folder
unified_folder = file.path(data_dir, 'data_stages',location_folder,'unified')

# Export Folder
# Two steps
export_folder = file.path(data_dir, 'data_stages',location_folder, 'agglomerated')
dir.create(export_folder)

export_folder = file.path(export_folder, 'community', sep = "")
dir.create(export_folder)


# Loads movement
cat(paste(ident, '   Loads Movement','\n', sep = ""))
mov_unified = read.csv(file.path(unified_folder, 'movement.csv'))
mov = mov_unified %>%
      select(date_time, start_movement_lon, start_movement_lat, end_movement_lon, end_movement_lat, n_crisis) %>% 
      rename(start_lon = start_movement_lon) %>% 
      rename(start_lat = start_movement_lat) %>%
      rename(end_lon = end_movement_lon) %>%
      rename(end_lat = end_movement_lat) %>% 
      rename(movement = n_crisis)

#Created the centers
cat(paste(ident, '   Finds Centers','\n', sep = ""))
centers = rbind(mov %>% select(start_lon, start_lat) %>% rename(lon = start_lon, lat = start_lat),
                mov %>% select(end_lon, end_lat) %>% rename(lon = end_lon, lat = end_lat)) %>% 
          unique() %>%
          mutate(poly_id = row_number())



# Groups the dataframe
g_mov = mov %>% 
        group_by(start_lon, start_lat, end_lon, end_lat) %>% 
        summarise(movement = mean(movement)) %>%
        inner_join(centers, by = c('start_lon' = 'lon', 'start_lat' = 'lat')) %>%
        rename(poly_start_id = poly_id)  %>%
        inner_join(centers, by = c('end_lon' = 'lon', 'end_lat' = 'lat')) %>%
        rename(poly_end_id = poly_id) %>%
        ungroup() %>%
        select(poly_start_id, poly_end_id, movement)

# Creates the graph
cat(paste(ident, '   Creates the Graph','\n', sep = ""))
g <- graph_from_data_frame(d = g_mov %>% rename(weight = movement), directed = FALSE, vertices = centers$poly_id)
g = simplify(g)


cat(paste(ident, '   Finds Communities and Pagerank','\n', sep = ""))
# Clusters
wc <- cluster_walktrap(g)

# Assigns the community for each center
centers$community_id = membership(wc)

# Assigns Pagerank
centers$page_rank = page_rank(g)$vector


cat(paste(ident, '   Agglomerates Cases','\n', sep = ""))

# Loads the cases data to assign the comunity ids and name them
cases = read.csv(paste0(unified_folder, 'cases.csv'), stringsAsFactors = FALSE)

# Finds the nearest comminity for each case
dist_matrix = dist(x = cases[,c('lon','lat')], y = centers[,c('lon','lat')], method = 'Euclidean')

nearest_center = argmin(dist_matrix, rows = TRUE)

# Creates the agglomerated cases
# ------------------------------
agg_cases = cases %>% 
            select(date_time, location, num_cases) %>% 
            mutate(poly_id = sapply(nearest_center, function(ind){centers[ind,'community_id'] })) %>%
            mutate(poly_distance = sapply(seq_along(nearest_center), function(i){dist_matrix[i,nearest_center[i]]*km_constant }))



# creates the agglomerated polygons
#------------------------
cat(paste(ident, '   Agglomerates Polygons','\n', sep = ""))

# Creates the community name (the one with highest frequency) for aglomeration
poly_id_names = agg_cases %>% 
  select(location, poly_id, num_cases) %>%
  group_by(poly_id) %>%
  summarise(poly_name = exctract_name(location, num_cases), num_cases = sum(num_cases)) %>%
  ungroup() 


agg_poly = centers %>%
           select(community_id, lon, lat, page_rank) %>%
           group_by(community_id) %>%
           summarise(geometry = extract_geometry(lon,lat), poly_lon = extract_center(lon, lat, page_rank)[1], poly_lat = extract_center(lon, lat, page_rank)[2]) %>%
           ungroup() %>%
           inner_join(poly_id_names, by = c('community_id' = 'poly_id')) %>%
           rename(poly_id = community_id) %>%
           select(poly_id, poly_name, poly_lon, poly_lat, num_cases, geometry)

# creates the agglomerated polygons
#------------------------
cat(paste(ident, '   Agglomerates Movement','\n', sep = ""))
agg_mov = mov %>% 
          inner_join(centers %>% select(lon,lat, community_id), by = c('start_lat' = 'lat', 'start_lon' = 'lon')) %>%
          rename(start_poly_id = community_id) %>%
          inner_join(centers %>% select(lon,lat, community_id), by = c('end_lat' = 'lat', 'end_lon' = 'lon')) %>%
          rename(end_poly_id = community_id) %>%
          select(date_time, start_poly_id, end_poly_id, movement) %>%
          group_by(date_time, start_poly_id, end_poly_id) %>%
          summarise(movement = sum(movement)) %>%
          ungroup()



# creates the agglomerated Populations
#------------------------
cat(paste(ident, '   Agglomerates Population','\n', sep = ""))
# Loads the cases data to assign the comunity ids and name them
pop = read.csv(file.path(unified_folder, 'population.csv'), stringsAsFactors = FALSE)

agg_pop = pop %>%
          select(date_time, lon, lat, n_crisis) %>%
          inner_join(centers %>% select(lon, lat, community_id), by = c('lat' = 'lat', 'lon' = 'lon')) %>%
          rename(poly_id = community_id, population =  n_crisis) %>%
          select(date_time, poly_id, population)



cat(paste(ident, '   Saves Files','\n', sep = ""))
# Saves all the datasets
write.csv( agg_cases, file.path(export_folder,'cases.csv'), row.names = FALSE)
write.csv( agg_mov, file.path(export_folder,'movement.csv'), row.names = FALSE)
write.csv(agg_poly, file.path(export_folder,'polygons.csv'), row.names = FALSE)
write.csv(agg_pop, file.path(export_folder,'population.csv'), row.names = FALSE)

cat(paste(ident, 'Done!','\n', sep = ""))

