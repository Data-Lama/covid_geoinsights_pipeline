# Agglomerates by comunnity
options(warn=-1)

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

# For Debug
debug = FALSE
if(debug)
{   
  cat("\n\n\n\n\n\n\n\n\n\n\n¡¡¡¡¡DEBUG IS ON!!!!\n\n\n\n\n\n\n\n\n")
  setwd("~/Dropbox/Projects/covid_fb_pipeline/covid_geoinsights_pipeline")
  args = c('Peru','peru')
}


location_name = args[1] # Location Name
location_folder = args[2] # Location Folder
agglomeration_method = args[3] # Agglomeration method to build on to



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


cat(paste(ident, '   Agglomerates Cases','\n', sep = ""))


# creates the agglomerated polygons
#------------------------
cat(paste(ident, '   Agglomerates Polygons','\n', sep = ""))

# Final_comunity
# I fpopulation is found it will be used for the id
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
          summarise(movement = sum(movement), .groups = "keep") %>%
          ungroup() %>%
          arrange(date_time)



# creates the agglomerated Populations
#------------------------
cat(paste(ident, '   Agglomerates Population','\n', sep = ""))
# Loads the cases data to assign the comunity ids and name them
pop = read.csv(file.path(agglomerated_folder, 'population.csv'), stringsAsFactors = FALSE)


if(nrow(pop) > 0)
{
  
  agg_pop = pop %>%
    inner_join(polygons %>% select(poly_id, community_id), by = c('poly_id' = 'poly_id')) %>%
    mutate(poly_id = community_id) %>%
    select(date_time, poly_id, population) %>%
    group_by(date_time, poly_id) %>%
    summarise(population = sum(population), .groups = "keep") %>%
    ungroup() %>%
    arrange(date_time)
}else
{
  agg_pop = pop
}




# Checks if movent range exits
agg_mov_range = NULL
movement_range_file = file.path(agglomerated_folder,'movement_range.csv')
if(file.exists(movement_range_file))
{
  df_movement_range = read.csv(movement_range_file)
  
  filtered_polys = polygons[,c('poly_id','community_id')]
  filtered_polys$factor = 1
  
  'attr_population' %in% colnames(polygons)
  
  if('attr_density' %in% colnames(polygons))
  {
    filtered_polys$factor = polygons$attr_density
    
  }else if('attr_population' %in% colnames(polygons) && 'attr_area' %in% colnames(polygons))
  {
    filtered_polys$factor = polygons$attr_population / polygons$attr_area
    
  }else if('attr_population' %in% colnames(polygons))
  {
    filtered_polys$factor = polygons$attr_population
    
  }else if('attr_area' %in% colnames(polygons))
  {
    filtered_polys$factor = polygons$attr_area    
  }
  
  agg_mov_range = df_movement_range %>% 
                  inner_join(filtered_polys, by = c('poly_id' = 'poly_id')) %>%
                  group_by(date_time, community_id)  %>%
                  summarise(movement_change = factor_average(movement_change, factor), .groups = "keep") %>%
                  ungroup() %>%
                  select(c('date_time', 'community_id', 'movement_change')) %>%
                  rename(poly_id = community_id) %>%
                  arrange(date_time)
                  
}



cat(paste(ident, '   Saves Files','\n', sep = ""))
# Saves all the datasets
write.csv( agg_cases, file.path(export_folder,'cases.csv'), row.names = FALSE)
write.csv( agg_mov, file.path(export_folder,'movement.csv'), row.names = FALSE)
write.csv(agg_poly, file.path(export_folder,'polygons.csv'), row.names = FALSE)
write.csv(agg_pop, file.path(export_folder,'population.csv'), row.names = FALSE)
write.csv(polygon_community_map, file.path(export_folder,'polygon_community_map.csv'), row.names = FALSE)

if(!is.null(agg_mov_range))
{
  write.csv(agg_mov_range, file.path(export_folder,'movement_range.csv'), row.names = FALSE)
}

cat(paste(ident, 'Done!','\n', sep = ""))

