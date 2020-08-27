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
add_labels = FALSE

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
  map_type = "terrain"

}else{
  add_labels = FALSE
  selected_polygons = args[5:length(args)]
  selected_polygons = unique(selected_polygons)
  map_type = "terrain-background"

  if(length(selected_polygons) == 0)
  {
    stop("If a selected polygons name is given, then at least one polygon id must be given")
  }
  
}



# Export options
width = 8
height = 8
perc_margin = 0.02



ident = '         '

  
# Agglomerated Folder
agglomerated_folder = file.path(data_dir,'data_stages',location_folder,'agglomerated', agglomeration_method)

if(!dir.exists(agglomerated_folder))
{
  stop(paste(ident, 'No data found for ', agglomeration_method, ' Agglomeration ', sep = ""))
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

# Export Folder
export_folder = file.path(export_folder, selected_polygons_name)
dir.create(export_folder)


data_location = file.path(data_dir,'data_stages', location_folder, 'agglomerated', agglomeration_method)


# Plots automatically the maps
# ----------------------

# Loads the data
polygons = read.csv(file.path(data_location, 'polygons.csv'))
polygons['incidence'] = polygons$num_cases/polygons$attr_population
polygons = polygons[polygons$incidence > 0,]

# Filters polygons
if(length(selected_polygons) > 0)
{
  polygons = polygons[polygons$poly_id %in% selected_polygons, ]
}


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
poly_export = drop_na(poly_export)
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
# map = suppressMessages(get_stamenmap(c(left = left, bottom = bottom, right = right, top = top), maptype = map_type, color = 'bw'))
map = suppressMessages(get_map(c(left = left, bottom = bottom, right = right, top = top), maptype = 'satellite', color = 'bw'))

df_plot = polygons[order(polygons$incidence),]

# Split poly_name in two for labling
df_plot <- df_plot %>% tidyr::separate(poly_name, 
                      c("municipio"), extra='drop', sep="-")

p =  ggmap(map)
p = p + geom_point(data = df_plot, aes(x = poly_lon, y = poly_lat, color = incidence, size = incidence, alpha = incidence, shape = group ))
p = p + guides(size=FALSE, alpha = FALSE)
p = p + scale_shape(breaks = labels)
if(add_labels){
    p = p + geom_text(data = df_plot, aes(label = municipio, x = poly_lon + 0.02, y = poly_lat - 0.01), inherit.aes = FALSE)
}
p = p + scale_alpha_continuous(range = c(0.4, 1))
p = p + scale_color_gradient(low = "darkblue", high = "red")
p = p + labs(color = paste0("Incidencia\n(Casos por ",per_capita ,"\nPersonas)"), shape = 'Población\n(Miles)')
p = p + ggtitle(paste0("COVID-19: Incidencia por Municipio (Casos por ",per_capita ," Personas)"))
p

ggsave(file.path( export_folder,"incidence_map.jpeg"), plot = p, width = width, height = height, device = 'jpeg')
  
  
