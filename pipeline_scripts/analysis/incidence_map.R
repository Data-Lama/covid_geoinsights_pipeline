#!/usr/bin/env Rscript

options(warn=-1)

suppressMessages(library("dplyr"))
suppressMessages(library('igraph'))
suppressMessages(library('proxy'))
suppressMessages(library('ramify'))
suppressMessages(library('ggplot2'))
suppressMessages(library('ggmap'))

source('pipeline_scripts/functions/constants.R')

# Directories
source("global_config/config.R")
data_dir = get_property('data_dir')
analysis_dir = get_property('analysis_dir')


# Working Directory

# Location Folder from args
args = commandArgs(trailingOnly=TRUE)
location_name = args[1] 
location_folder = args[2]
agglomeration_method_parameter = args[3]# Aglomeration Method



# Export options
width = 8
height = 8
perc_margin = 0.02


debug = FALSE
if(debug)
{   
  cat("\n\n\n\n\n\n\n\n\n\n\n¡¡¡¡¡DEBUG IS ON!!!!\n\n\n\n\n\n\n\n\n")
  setwd("~/Dropbox/Projects/covid_fb_pipeline/covid_geoinsights_pipeline")
  location_name = 'Colombia'
  location_folder = 'colombia'
  agglomeration_method_parameter = 'geometry'
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
  
  # Export Folder
  # Three Steps
  export_folder = file.path(analysis_dir ,location_folder)
  dir.create(export_folder)
  
  # Export Folder
  export_folder = file.path(export_folder ,agglomeration_method)
  dir.create(export_folder)
  
  # Export Folder
  export_folder = file.path(export_folder, 'incidence_maps')
  dir.create(export_folder)
  
  
  data_location = file.path(data_dir,'data_stages', location_folder, 'agglomerated', agglomeration_method)
  
  
  # Plots automatically the maps
  # ----------------------
  
  # Loads the data
  polygons = read.csv(file.path(data_location, 'polygons.csv'))
  polygons['incidence'] = polygons$num_cases/polygons$attr_population
  polygons = polygons[polygons$incidence > 0,]
  
  
  per_capita = 100
  polygons$incidence = 100*polygons$incidence
  while(max(polygons$incidence) < 1)
  {
    per_capita = 10*per_capita
    polygons$incidence = 10*polygons$incidence
  }
  
  # Creates the population groups
  div = 1000
  qs = quantile(round(polygons$attr_population/div), probs = c(0.3,0.6,0.9))
  
  labels = c(paste0("< ", qs[[1]]), paste0("[",qs[[1]], ",", qs[[2]] ,")"),  paste0("[",qs[[2]], ",", qs[[3]] ,"]"), paste0("> ", qs[[3]]))
  
  polygons$group = labels[1]
  polygons$group[polygons$attr_population/div >= qs[[1]]] = labels[2]
  polygons$group[polygons$attr_population/div >= qs[[2]]] = labels[3]
  polygons$group[polygons$attr_population/div > qs[[3]]] = labels[4]
  
  polygons = polygons[order(-1*polygons$incidence),]
  
  poly_export = polygons[1:10,c('poly_name', 'incidence', 'attr_population')]
  poly_export$incidence = round(poly_export$incidence)
  # Saves
  write.csv(poly_export, file = file.path( export_folder,"incidences.csv"), quote = FALSE, row.names = FALSE)
  
  
  # Map location
  # Constructed from the sample, plus a defined margin
  
  bottom = min(polygons$poly_lat) 
  top = max(polygons$poly_lat)
  left = min(polygons$poly_lon)
  right = max(polygons$poly_lon)
  
  margin = max((top - bottom),(right - left))*perc_margin
  
  
  bottom = bottom  - margin
  top = top + margin
  left = left - margin
  right = right + margin
  
  
  cat(paste(ident, '   Downloading Map', '\n', sep = ""))
  map = suppressMessages(get_map(c(left = left, bottom = bottom, right = right, top = top), maptype = 'satellite', color = 'bw'))
  
  df_plot = polygons[order(polygons$incidence),]
  
  p =  ggmap(map)
  p = p + geom_point(data = df_plot, aes(x = poly_lon, y = poly_lat, color = incidence, size = incidence, alpha = incidence, shape = group ))
  p = p + guides(size=FALSE, alpha = FALSE)
  p = p + scale_shape(breaks = labels)
  p = p + scale_alpha_continuous(range = c(0.4, 1))
  p = p + scale_color_gradient(low = "darkblue", high = "red")
  p = p + labs(color = paste0("Incidencia\n(Casos por ",per_capita ,"\nPersonas)"), shape = 'Población\n(Miles)')
  p = p + ggtitle(paste0("COVID-19: Incidencia por Municipio (Casos por ",per_capita ," Personas)"))
  p
  
  ggsave(file.path( export_folder,"incidence_map.jpeg"), plot = p, width = width, height = height, device = 'jpeg')
  
  
}