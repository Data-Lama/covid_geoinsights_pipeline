# Agglomerates by comunnity
options(warn=-1)

# Imports the libraries
suppressMessages(library('igraph'))
suppressMessages(library("dplyr"))


source("pipeline_scripts/functions/agglomeration_functions.R")

# Directories
source("global_config/config.R")
data_dir = get_property('data_dir')
analysis_dir = get_property('analysis_dir')


# Ident for console
ident = '            '


args = commandArgs(trailingOnly=TRUE)


# For Debug
debug = FALSE
if(debug)
{   
  cat("\n\n\n\n\n\n\n\n\n\n\n¡¡¡¡¡DEBUG IS ON!!!!\n\n\n\n\n\n\n\n\n")
  setwd("~/Dropbox/Projects/covid_fb_pipeline/covid_geoinsights_pipeline")
  args = c('Colombia','colombia','geometry')
}

location_name = args[1] # Location Name
location_folder = args[2] # Location Folder
agglomeration_method = args[3] # Agglomeration method to build on to

agglomerated_folder = file.path(data_dir, 'data_stages',location_folder,'agglomerated', agglomeration_method)

if(is.na(agglomeration_method))
{
  stop('No agglomeration method found to build communities')
  
}else if(!(dir.exists(agglomerated_folder)))
{
  stop(paste0('No agglomeration found for ',agglomeration_method, ' please compute it!'))
}
  


cat(paste(ident, 'Community Agglomeration for: ', location_name,  ' (R)\n', sep = ""))

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
movement = read.csv(file.path(agglomerated_folder, 'movement.csv'),  stringsAsFactors = FALSE)
polygons = read.csv(file.path(agglomerated_folder, 'polygons.csv'),  stringsAsFactors = FALSE)
cases = read.csv(file.path(agglomerated_folder, 'cases.csv'), stringsAsFactors = FALSE)


# Extracts the cases columns
cases_col = colnames(cases)[as.vector(sapply(colnames(cases), function(col){grepl('num_', col, fixed = TRUE)}))]
# Extracts the attr columns
attr_col = colnames(polygons)[as.vector(sapply(colnames(polygons), function(col){grepl('attr_', col, fixed = TRUE)}))]


# Groups the dataframe
g_mov = movement %>% 
  group_by(start_poly_id, end_poly_id) %>% 
  summarise(movement = mean(movement), .groups = "keep") %>%
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



# creates the agglomerated polygons
#------------------------
cat(paste(ident, '   Creates Polygon Community Map','\n', sep = ""))

# Final_comunity
# I population is found it will be used for the id
# if not, the number of cases will be used
if('attr_population' %in% colnames(polygons))
{
  final_community = polygons %>%
    select(poly_id, community_id, attr_population) %>%
    group_by(community_id) %>%
    summarise(final_id = exctract_id_by_population(poly_id, attr_population), .groups = "keep") %>%
    ungroup()  
}else
{
  final_community = polygons %>%
    select(poly_id, community_id, num_cases) %>%
    group_by(community_id) %>%
    summarise(final_id = exctract_id_by_cases(poly_id, num_cases), .groups = "keep") %>%
    ungroup()  
}


polygons = polygons %>%
  inner_join(final_community, by = c('community_id' = 'community_id')) %>%
  mutate(community_id = final_id)



# Creates the community name:
# If population exists it uses it if not, number of cases
if('attr_population' %in% colnames(polygons))
{
  agg_poly_1 = polygons %>% 
    group_by(community_id) %>%
    summarise(poly_name = exctract_name_by_population(poly_name, attr_population), agglomerated_polygons = extract_list_of_agg_polygons(poly_id), geometry = extract_geometry(geometry), poly_lon = extract_center_by_population(poly_lon, poly_lat, attr_population)[1], poly_lat = extract_center_by_population(poly_lon, poly_lat, attr_population)[2], .groups = "keep") %>%
    ungroup()  
  
}else
{
  agg_poly_1 = polygons %>% 
    group_by(community_id) %>%
    summarise(poly_name = exctract_name_by_cases(poly_name, num_cases), agglomerated_polygons = extract_list_of_agg_polygons(poly_id), geometry = extract_geometry(geometry), poly_lon = extract_center_by_cases(poly_lon, poly_lat, num_cases)[1], poly_lat = extract_center_by_cases(poly_lon, poly_lat, num_cases)[2], .groups = "keep") %>%
    ungroup()  
}


# Creates the location to community map
polygon_community_map = agg_poly_1 %>% 
  select(community_id, poly_name) %>%
  rename(community_name = poly_name) %>%
  inner_join(polygons, by = c('community_id' = 'final_id')) %>%
  select(poly_id, poly_name, community_id, community_name )


write.csv(polygon_community_map, file.path(export_folder,'polygon_community_map.csv'), row.names = FALSE)

cat(paste(ident, 'Done!','\n', sep = ""))

