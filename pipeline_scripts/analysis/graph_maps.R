#!/usr/bin/env Rscript

options(warn=-1)

# If they are placed before TDA something fails (No idea what)
suppressMessages(library('ggplot2'))
suppressMessages(library('ggmap'))
suppressMessages(library("dplyr"))


source('pipeline_scripts/functions/constants.R')

# Directories
source("global_config/config.R")
data_dir = get_property('data_dir')
analysis_dir = get_property('analysis_dir')


add_labels = FALSE

# Working Directory

# Location Folder from args
args = commandArgs(trailingOnly=TRUE)

debug = FALSE
if(debug)
{   
cat("\n\n\n\n\n\n\n\n\n\n\n¡¡¡¡¡DEBUG IS ON!!!!\n\n\n\n\n\n\n\n\n")
setwd("~/Dropbox/Projects/covid_fb_pipeline/covid_geoinsights_pipeline")
args = c('Colombia', 'colombia','community', 'norte_de_santander', '54001', '54051', '54099', '54109', '54128', '54172', '54174', '54223', '54239', '54245', '54250', '54313', '54344', '54347', '54377', '54405', '54418', '54498', '54498', '54518', '54520', '54520', '54553', '54660', '54670', '54680', '54720', '54743', '54800', '54810', '54820', '54871')
}


location_name = args[1] 
location_folder = args[2]
agglomeration_method = args[3]# Aglomeration Method

selected_polygons_name = args[4]# Selected Polygon name

if(is.na(selected_polygons_name))
{
selected_polygons_name = "entire_location"
selected_polygons = c()

}else{

selected_polygons = args[5:length(args)]
add_labels = TRUE
selected_polygons = unique(selected_polygons)

if(length(selected_polygons) == 0)
{
  stop("If a selected polygons name is given, then at least one polygon id must be given")
}

}




# Export options
width = 8
height = 8
perc_margin = 0.08

window = 14

ident = '         '


agglomerated_folder = file.path(data_dir,'data_stages',location_folder,'agglomerated', agglomeration_method)

if(!dir.exists(agglomerated_folder))
{
  stop(paste(ident, 'No data found for ', agglomeration_method, ' Agglomeration ', sep = ""))
}


# Export Folder
# Four Steps
export_folder = file.path(analysis_dir ,location_folder)
dir.create(export_folder)

# Export Folder
export_folder = file.path(export_folder ,agglomeration_method)
dir.create(export_folder)

# Export Folder
export_folder = file.path(export_folder, 'graph_maps')
dir.create(export_folder)

# Export Folder
export_folder = file.path(export_folder, selected_polygons_name)
dir.create(export_folder)


graphs_location = file.path(data_dir,'data_stages', location_folder, 'constructed', agglomeration_method, 'daily_graphs')

# Plots automatically the maps
# ----------------------

# Loads the data
nodes = read.csv(file.path(graphs_location, 'nodes.csv'), stringsAsFactors = FALSE)
nodes[is.na(nodes)] = 0

# Edges
edges = read.csv(file.path(graphs_location, 'edges.csv'), stringsAsFactors = FALSE)

locations = read.csv(file.path(graphs_location, 'node_locations.csv'), stringsAsFactors = FALSE) # To assign the geo location

# Filters the nodes and edges
if(length(selected_polygons) > 0)
{
  locations = locations[locations$node_id %in% selected_polygons, ]
}

# Split node_name in two for labling
locations <- locations %>% tidyr::separate(node_name, 
                       c("municipio", "dept"))

# Extracts the lattitude and longitud from the location datatset

nodes = merge(nodes, locations, by ='node_id')

edges = merge(edges, locations, by.x = 'start_id', by.y = 'node_id')
edges = merge(edges, locations, by.x = 'end_id', by.y = 'node_id')





# Extracts the max and mins for each scale

# Cases
cases_min = min(nodes$num_cases)
cases_max = max(nodes$num_cases)

# Internal Movement
internal_min = min(nodes$inner_movement[!is.na(nodes$inner_movement)])
internal_max = max(nodes$inner_movement[!is.na(nodes$inner_movement)])

# External Movement
external_min = min(edges$movement)
external_max = max(edges$movement)


# Extracts the map


# Map location
# Constructed from the sample, plus a defined margin

bottom = min(locations$lat) 
top = max(locations$lat)
left = min(locations$lon)
right = max(locations$lon)

margin = max((top - bottom),(right - left))*perc_margin


bottom = bottom  - margin
top = top + margin
left = left - margin
right = right + margin


cat(paste(ident, '   Downloading Map', '\n', sep = ""))
map = suppressMessages(get_map(c(left = left, bottom = bottom, right = right, top = top), maptype = 'satellite', color = 'bw'))


start_day =  min(nodes$day)
end_day = max(nodes$day)

# Creates output folder
dir.create(file.path(export_folder, 'maps_on_day'))

cat(paste(ident, '   Generating Maps on day', '\n', sep = ""))
cat(paste(ident, '   ', sep = ""))
# First for non cumulative scenarios (only cases of the day)
for(day in start_day:end_day)
{
  curr_date = nodes[nodes$day == day,]$date_time
  cat(paste(day,' ',sep =""))
  
  #Sliding
  plus = max(window/2, window - (day - start_day))
  minus = max(window/2, window - (end_day - day))
  current_graph = nodes[nodes$day <= day + plus,]
  current_graph = current_graph[current_graph$day >= day - minus,]
  
  current_graph = current_graph %>% 
    group_by(lon, lat) %>% 
    summarise(inner_movement = mean(inner_movement), num_cases = mean(num_cases),  .groups = "keep") %>%
    ungroup()
  
  current_edges = edges[edges$day <= day + plus,]
  current_edges = current_edges[current_edges$day >= day - minus,]
  
  current_edges = current_edges %>% 
    group_by(lon.x, lat.x, lon.y, lat.y) %>% 
    summarise(movement = mean(movement), .groups = "keep") %>%
    ungroup()


  # Only plots the one with at least one case
  current_graph = current_graph[current_graph$num_cases > 0, ]
  

  p =  ggmap(map)
  p = p + geom_segment(data = current_edges, aes(x = lon.x, y = lat.x, xend = lon.y, yend = lat.y, alpha = movement), color = 'yellow', size = 1.5)
  p = p + geom_point(data = current_graph, aes(x = lon, y = lat, size = inner_movement, color = num_cases))
  if(add_labels){
    p = p + geom_text(data = locations, aes(label = municipio))
  }    
  p = p + scale_color_gradient(low = "darkblue", high = "red", limits=c(cases_min, cases_max))
  p = p + scale_size(limits=c(internal_min, internal_max))
  p = p + scale_alpha(limits=c(external_min, external_max))
  p = p + labs(color = "Casos", alpha = "Mov. Externo", size = "Mov. Interno")
  p = p + ggtitle(paste0('COVID-19 Dinámicas Promedio en el Día: ', day,' (',curr_date,')'))
  
  ggsave(file.path( export_folder, "maps_on_day", paste0("map_on_",day,".jpeg")), plot = p, width = width, height = height, device = 'jpeg')
  
}

cat('\n')
cat(paste(ident, '  Done', '\n', sep = ""))

# second for  cumulative scenarios (days are accumulated)

# Creates output folder
dir.create(file.path(export_folder, 'maps_by_day'))

# Recalculates the cases_max
cases_max = -1
nodes = nodes[order(nodes$lon, nodes$lat),]
cases = rep(0, nrow(locations))
for(day in start_day:end_day)
{
  temp_nodes = nodes[nodes$day == day,]
  temp_nodes = temp_nodes[order(temp_nodes$lon, temp_nodes$lat),]
  cases = cases + temp_nodes$num_cases
  
}


cases_max = max(cases)

cases = rep(0, nrow(locations))

cat(paste(ident, '   Generating Maps by day', '\n', sep = ""))
cat(paste(ident, '   ', sep = ""))
for(day in start_day:end_day)
{   
  curr_date = nodes[nodes$day == day,]$date_time
  
  cat(paste(day,' ',sep =""))
  
  plus = max(window/2, window - (day - start_day))
  minus = max(window/2, window - (end_day - day))
  current_graph = nodes[nodes$day <= day + plus,]
  current_graph = current_graph[current_graph$day >= day - minus,]
  
  current_graph = as.tbl(current_graph)
  
  current_graph = current_graph %>% 
    group_by(lon, lat) %>% 
    summarise(inner_movement = mean(inner_movement), num_cases = mean(num_cases), .groups = "keep") %>%
    ungroup()
  
  # Adds the cases
  current_graph = current_graph[order(current_graph$lon, current_graph$lat),]
  current_graph$num_cases = current_graph$num_cases + cases
  #Adjusts to max
  current_graph$num_cases =  sapply( current_graph$num_cases, function(s){min(s, cases_max)})
  
  
  current_edges = edges[edges$day <= day + plus,]
  current_edges = current_edges[current_edges$day >= day - minus,]
  
  current_edges = current_edges %>% 
    group_by(lon.x, lat.x, lon.y, lat.y) %>% 
    summarise(movement = mean(movement), .groups = "keep") %>%
    ungroup()
  
  # Only plots the one with at least one case
  current_graph = current_graph[current_graph$num_cases > 0, ]

  
  p =  ggmap(map)
  p = p + geom_segment(data = current_edges, aes(x = lon.x, y = lat.x, xend = lon.y, yend = lat.y, alpha = movement), color = 'yellow', size = 1.5)
  p = p + geom_point(data = current_graph, aes(x = lon, y = lat, size = inner_movement, color = num_cases))
  if(add_labels){
    p = p + geom_text(data = locations, aes(label = municipio))
  }      
  p = p + scale_color_gradient(low = "darkblue", high = "red", limits=c(cases_min, cases_max))
  p = p + scale_size(limits=c(internal_min, internal_max))
  p = p + scale_alpha(limits=c(external_min, external_max))
  p = p + labs(color = "Casos", alpha = "Mov. Externo", size = "Mov. Interno")
  p = p + ggtitle(paste0('COVID-19 Dinámicas Promedio al Día: ', day,' (',curr_date,')',' (Casos Acumulados)'))
  
  ggsave(file.path( export_folder, "maps_by_day", paste0("map_by_",day,".jpeg")), plot = p, width = width, height = height, device = 'jpeg')
  
  cases = cases + nodes[nodes$day == day,]$num_cases
}

cat('\n')

cat(paste0('   Done','\n'))



cat(paste(ident, ' ','\n', sep = ""))
cat(paste(ident, '----------------','\n', sep = ""))
cat(paste(ident, 'Graph Maps Done!','\n', sep = ""))
cat(paste(ident, ' ','\n', sep = ""))

