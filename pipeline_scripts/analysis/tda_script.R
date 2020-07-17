#!/usr/bin/env Rscript

options(warn=-1)

# TDA Over daily graphs of the COVID Pandemic
suppressMessages(library('igraph'))
suppressMessages(library('ggplot2'))
suppressMessages(library('ggmap'))
suppressMessages(library("dplyr"))

# Loads constants
source('pipeline_scripts/functions/constants.R')


# Directories
source("global_config/config.R")
data_dir = get_property('data_dir')
analysis_dir = get_property('analysis_dir')
mapper_dir = get_property('mapper_dir')

# Source code
if(dir.exists(mapper_dir))
{
    suppressMessages(source(paste0(mapper_dir , '/R/mapperKD_functions.R')))
    suppressMessages(source(paste0(mapper_dir , '/R/epidemiology_module.R')))
    suppressMessages(source(paste0(mapper_dir , '/R/general_clusters.R')))
}



# Working Directory


# Location Folder from args
args = commandArgs(trailingOnly=TRUE)
location_name = args[1] 
location_folder = args[2]
agglomeration_method_parameter = args[3]# Aglomeration Method


window = 4

debug = FALSE
if(debug)
{   
    cat("\n\n\n\n\n\n\n\n\n\n\n¡¡¡¡¡DEBUG IS ON!!!!\n\n\n\n\n\n\n\n\n")
    setwd("~/Dropbox/Projects/covid_fb_pipeline/covid_geoinsights_pipeline")
    location_name = 'Colombia'
    location_folder = 'colombia'
    agglomeration_method_parameter = 'community'
}


ident = '         '


if(toupper(agglomeration_method_parameter) == "ALL")
{
    agglomeration_methods = all_agglomeration_methods
    
}else
{
    agglomeration_methods = c(agglomeration_method_parameter)
}


agg = 0
for(agglomeration_method in agglomeration_methods)
{
    
    agg = agg +1 
    # Agglomerated Folder
    agglomerated_folder = file.path(data_dir,'data_stages',location_folder,'agglomerated', agglomeration_method)
    
    if(!dir.exists(agglomerated_folder))
    {
        cat(paste(ident, 'No data found for ', agglomeration_method, ' Agglomeration (', agg, ' of ', length(agglomeration_methods), '). Skipping', '\n', sep = ""))
        next
    }

    
    cat(paste(ident, 'Excecuting TDA Script for: ', location_name,  ' ', agglomeration_method,' Agglomeration (', agg, ' of ', length(agglomeration_methods), ')', '\n', sep = ""))
    
    
    # Export Folder
    # Three Steps
    export_folder = file.path(analysis_dir ,location_folder)
    dir.create(export_folder)
    
    # Export Folder
    export_folder = file.path(export_folder ,agglomeration_method)
    dir.create(export_folder)
    
    # Export Folder
    export_folder = file.path(export_folder, 'TDA')
    dir.create(export_folder)
    
    
    graphs_location = file.path(data_dir,'data_stages', location_folder, 'constructed', agglomeration_method, 'daily_graphs')
    
    # Loads the data
    meta_df = read.csv(file.path(graphs_location, 'graph_values.csv'), stringsAsFactors = FALSE)
    distance_matrix = read.csv(file.path(graphs_location, 'distance_matrix.csv'))
    distance_matrix = as.matrix(distance_matrix[,2:dim(distance_matrix)[2]])
    
    # Export options
    width = 8
    height = 8
    perc_margin = 0.02
    
    # First scenario
    cat(paste(ident, '   Plots Movement Progress', '\n', sep = ""))
    file_name = file.path(export_folder, 'movement_progress.png')
    p = ggplot() + geom_point(data = meta_df, aes(x = inner_movement, y = external_movement, size = cases, color = day))
    p = p + labs(x = 'Movimiento Interno', y = 'Movimiento Externo', size = 'Casos', title = 'Progresión de Movimientos', color = 'Día')
    p = p + theme(plot.title = element_text(hjust = 0.5, size = 20))
    ggsave(file_name, plot = p, width = width, height = height, device = 'png')
    
    
    
    # First scenario
    cat(paste(ident, '   First Scenario: Internal vs External', '\n', sep = ""))
    
    # FIlter 2 dimensianal: internal and external movement
    filter = cbind(meta_df$inner_movement, meta_df$external_movement)
    
    # Applies the Algorithm
    one_skeleton_result = mapperKD(k = 2,
                                   distance = distance_matrix,
                                   filter = filter,
                                   interval_scheme = "FIXED",
                                   num_intervals = c(8,8),
                                   overlap = c(40,40),
                                   clustering_method = function(x) {hierarchical_clustering(x, method = 'average', height = 70)},
                                   verbose = FALSE)
    
    
    # Plots the 1 Skeleton (graph)
    file_name = file.path(export_folder, 'tda_int_vs_ext.png')
    p = plot_1_skeleton(one_skeleton_result, layout = 'FILTER', filter = filter, noise = 0.1)
    p = p + labs(x = 'Movimiento Interno', y = 'Movimiento Externo', size = '# Elem.')
    ggsave(file_name, plot = p, width = width, height = height, device = 'png')
    
    
    
    # Second scenario
    cat(paste(ident, '   Second Scenario: Internal', '\n', sep = ""))
    
    # FIlter 1 dimensianal: internal
    filter = cbind(meta_df$inner_movement)
    
    # Applies the Algorithm
    one_skeleton_result = mapperKD(k = 1,
                                   distance = distance_matrix,
                                   filter = filter,
                                   interval_scheme = "FIXED",
                                   num_intervals = c(8),
                                   overlap = c(40),
                                   clustering_method = function(x) {hierarchical_clustering(x, method = 'average', height = 70)},
                                   verbose = FALSE)
    
    
    # Plots the 1 Skeleton (graph)
    file_name = file.path(export_folder, 'tda_int.png')
    p = plot_1_skeleton(one_skeleton_result, layout = 'FILTER', filter = filter, noise = 0.1)
    p = p + labs(x = 'Movimiento Interno', size = '# Elem.')
    ggsave(file_name, plot = p, width = width, height = height, device = 'png')
    
    
    
    # Third scenario
    cat(paste(ident, '   Third Scenario: External', '\n', sep = ""))
    
    # FIlter 1 dimensianal: external
    filter = cbind(meta_df$external_movement)
    
    # Applies the Algorithm
    one_skeleton_result = mapperKD(k = 1,
                                   distance = distance_matrix,
                                   filter = filter,
                                   interval_scheme = "FIXED",
                                   num_intervals = c(8),
                                   overlap = c(40),
                                   clustering_method = function(x) {hierarchical_clustering(x, method = 'average', height = 70)},
                                   verbose = FALSE)
    
    
    # Plots the 1 Skeleton (graph)
    file_name = file.path(export_folder, 'tda_ext.png')
    p = plot_1_skeleton(one_skeleton_result, layout = 'FILTER', filter = filter, noise = 0.1)
    p = p + labs(x = 'Movimiento Externo', size = '# Elem.')
    ggsave(file_name, plot = p, width = width, height = height, device = 'png')
    
    
    
    
    
    # Forth scenario
    cat(paste(ident, '   Forth Scenario: Time', '\n', sep = ""))
    
    # FIlter 1 dimensianal: time
    filter = meta_df$day
    one_skeleton_result = mapperKD(k = 1,
                                   distance = distance_matrix,
                                   filter = meta_df$day,
                                   interval_scheme = "FIXED",
                                   num_intervals = c(10),
                                   overlap = c(30),
                                   clustering_method = function(x) {hierarchical_clustering(x, method = 'average', height = 70)},
                                   verbose = FALSE)
    
    
    # Plots the 1 Skeleton (graph)
    file_name = file.path(export_folder, 'tda_dias.png')
    p = plot_1_skeleton(one_skeleton_result, layout = 'FILTER', filter = filter, noise = 0.0)
    p = p + labs(x = 'Días', size = '# Elem.')
    ggsave(file_name, plot = p, width = width, height = height, device = 'png')
    
    
    
    
    
    
    # Plots automatically the maps
    # ----------------------
    
    # Loads the data
    nodes = read.csv(file.path(graphs_location, 'nodes.csv'), stringsAsFactors = FALSE)
    nodes[is.na(nodes)] = 0
    
    locations = read.csv(file.path(graphs_location, 'node_locations.csv'), stringsAsFactors = FALSE) # To assign the geo location
    nodes = merge(nodes, locations, by ='node_id')
    # Edges
    edges = read.csv(file.path(graphs_location, 'edges.csv'), stringsAsFactors = FALSE)
    
    # Extarcts the lattitude and longitud from the location datatset
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
        current_graph = nodes[nodes$day <= day + window/2,]
        current_graph = current_graph[current_graph$day >= day - window/2,]
        
        current_graph = current_graph %>% 
            group_by(lon, lat) %>% 
            summarise(inner_movement = mean(inner_movement), num_cases = mean(num_cases)) %>%
            ungroup()
        
        
        current_edges = edges[edges$day <= day + window/2,]
        current_edges = current_edges[current_edges$day >= day + window/2,]
        
        current_edges = current_edges %>% 
            group_by(lon.x, lat.x, lon.y, lat.y) %>% 
            summarise(movement = mean(movement)) %>%
            ungroup()
        
        # Only plots the one with at least one case
        current_graph = current_graph[current_graph$num_cases > 0, ]
    
        p =  ggmap(map)
        p = p + geom_point(data = current_graph, aes(x = lon, y = lat, size = inner_movement, color = num_cases))
        p = p + geom_segment(data = current_edges, aes(x = lon.x, y = lat.x, xend = lon.y, yend = lat.y, alpha = movement), color = 'yellow', size = 1.5)
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
    cases = rep(0, nrow(locations))
    for(day in start_day:end_day)
        cases = cases + nodes[nodes$day == day, 'num_cases']
    
    cases_max = max(cases)
    
    cases = rep(0, nrow(locations))
    
    cat(paste(ident, '   Generating Maps by day', '\n', sep = ""))
    cat(paste(ident, '   ', sep = ""))
    for(day in start_day:end_day)
    {   
        curr_date = nodes[nodes$day == day,]$date_time
        
        cat(paste(day,' ',sep =""))
       
        current_graph = nodes[nodes$day <= day + window/2,]
        current_graph = current_graph[current_graph$day >= day - window/2,]
        
        current_graph = current_graph %>% 
            group_by(lon, lat) %>% 
            summarise(inner_movement = mean(inner_movement), num_cases = mean(num_cases)) %>%
            ungroup()
        
        # Adds the cases
        current_graph$num_cases = current_graph$num_cases + cases
        
        
        current_edges = edges[edges$day <= day + window/2,]
        current_edges = current_edges[current_edges$day >= day + window/2,]
        
        current_edges = current_edges %>% 
            group_by(lon.x, lat.x, lon.y, lat.y) %>% 
            summarise(movement = mean(movement)) %>%
            ungroup()
        
        # Only plots the one with at least one case
        current_graph = current_graph[current_graph$num_cases > 0, ]
    
        p =  ggmap(map)
        p = p + geom_point(data = current_graph, aes(x = lon, y = lat, size = inner_movement, color = num_cases))
        p = p + geom_segment(data = current_edges, aes(x = lon.x, y = lat.x, xend = lon.y, yend = lat.y, alpha = movement), color = 'yellow', size = 1.5)
        p = p + scale_color_gradient(low = "darkblue", high = "red", limits=c(cases_min, cases_max))
        p = p + scale_size(limits=c(internal_min, internal_max))
        p = p + scale_alpha(limits=c(external_min, external_max))
        p = p + labs(color = "Casos", alpha = "Mov. Externo", size = "Mov. Interno")
        p = p + ggtitle(paste0('COVID-19 Dinámicas Promedio al Día: ', day,' (',curr_date,')',' (Casos Acumulados)'))
        
        ggsave(file.path( export_folder, "maps_by_day", paste0("map_by_",day,".jpeg")), plot = p, width = width, height = height, device = 'jpeg')
    
        cases = nodes[nodes$day == day,]$num_cases
    }
    
    cat('\n')

    cat(paste0('   Done','\n'))
    # Projections
    
    # # Proyection Map
    # pro = read.csv('data/daily_graphs/predictions.csv')
    # 
    # last_date = pro$Fecha[(length(pro$Fecha) - 7):length(pro$Fecha)]
    # 
    # weekly_pro = pro[pro$Fecha %in% last_date,]
    # weekly_pro = aggregate(weekly_pro[,'Casos'], list(weekly_pro$Ciudad,weekly_pro$lon,weekly_pro$lat), sum)
    # 
    # colnames(weekly_pro) = c('Ciudad','lon','lat','Casos')
    # 
    # 
    # p =  ggmap(map)
    # p = p + geom_point(data = weekly_pro, aes(x = lon, y = lat, size = Casos, color = Casos))
    # p = p + scale_color_gradient(low = "darkblue", high = "red")
    # p = p + labs(color = "Casos", size = "Casos")
    # p = p + ggtitle('Total Casos Proyectados (7 Días Siguientes)')
    # p
    #ggsave("img/projections.jpeg", width = width, height = height, device = 'jpeg')

}


cat(paste(ident, ' ','\n', sep = ""))
cat(paste(ident, '----------------','\n', sep = ""))
cat(paste(ident, 'Finished TDA!','\n', sep = ""))
cat(paste(ident, ' ','\n', sep = ""))



