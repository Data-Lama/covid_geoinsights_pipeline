#!/usr/bin/env Rscript

options(warn=-1)

suppressMessages(library("dplyr"))
suppressMessages(library('igraph'))
suppressMessages(library('proxy'))
suppressMessages(library('ramify'))

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

# If they are placed before TDA something fails (No idea what)
suppressMessages(library('ggplot2'))
suppressMessages(library('ggmap'))


# Working Directory

# Location Folder from args
args = commandArgs(trailingOnly=TRUE)
location_name = args[1] 
location_folder = args[2]
agglomeration_method_parameter = args[3]# Aglomeration Method


window = 14

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
    

}


cat(paste(ident, ' ','\n', sep = ""))
cat(paste(ident, '----------------','\n', sep = ""))
cat(paste(ident, 'Finished TDA!','\n', sep = ""))
cat(paste(ident, ' ','\n', sep = ""))



