#!/usr/bin/env Rscript

options(warn=-1)

# Creates the report plots based on the mobility

suppressMessages(library('igraph'))
suppressMessages(library('ggplot2'))
suppressMessages(library('ggmap'))
suppressMessages(library('lubridate'))
suppressMessages(library('gridExtra'))

# Loads constants
source('pipeline_scripts/functions/constants.R')

# Directories
source("global_config/config.R")
data_dir = get_property('data_dir')
analysis_dir = get_property('analysis_dir')

# Ident for console
ident = '         '


# Export options
width = 8
height = 8

look_back = 7 # Days to look back
perc_margin = 0.03 # Percentage margin
alfa  = 0.001 # For outlier removal

# Months
months = c('Enero','Febrero','Marzo','Abril','Mayo','Junio','Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre')

# Location Folder from args
args = commandArgs(trailingOnly=TRUE)
location_name = args[1] 
location_folder = args[2]
agglomeration_method_parameter = args[3]
alfa_movement_plot = as.numeric(args[4]) # Plots only extreme movement
percentage_plot_cut = as.numeric(args[5]) # Plots only extreme percentage change

# Debug Variables

# setwd("~/Dropbox/Projects/covid_fb")
# location_name = 'Bogota'
# location_folder = 'bogota'
# agglomeration_method_parameter = 'radial'
# alfa_movement_plot = 0.8
# percentage_plot_cut = 150


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
  agglomerated_folder = file.path(data_dir, 'data_stages',location_folder,'agglomerated', agglomeration_method)
  
  if(!dir.exists(agglomerated_folder))
  {
    cat(paste(ident, '   No data found for ', agglomeration_method, ' Agglomeration (', agg, ' of ', length(agglomeration_methods), '). Skipping', '\n', sep = ""))
    next
  }
  
  
  cat(paste(ident, 'Ggplot Movement Plots for: ', location_name, ' with Agglomeration (', agg, ' of ', length(agglomeration_methods), ')', '\n', sep = ""))
  
  cat(paste(ident, '   General Movement Plots', '\n', sep = ""))
  
  # Export Folder
  # Three Steps
  export_folder = file.path(analysis_dir,location_folder)
  dir.create(export_folder)
  
  # Export Folder
  export_folder = file.path(export_folder ,agglomeration_method)
  dir.create(export_folder)
  
  # Export Folder
  export_folder = file.path(export_folder, 'movement_plots')
  dir.create(export_folder)
  
  
  # Unified Folder
  unified_folder = file.path(data_dir, 'data_stages',location_folder,'unified')
  # Agglomerated Folder
  agglomerated_folder = file.path(data_dir,'data_stages',location_folder,'agglomerated', agglomeration_method)
  
  cat(paste(ident, '      Loads Unified Movement', '\n', sep = ""))
  
  # Unified
  df_movement_unified = read.csv(file.path(unified_folder,'movement.csv'), stringsAsFactors = FALSE)
  
  # Converts to date
  df_movement_unified$date_time = as.Date(df_movement_unified$date_time)
  
  # Assings month
  df_movement_unified$month = month(df_movement_unified$date_time)
  
  # Assigns week
  df_movement_unified$week = floor(day(df_movement_unified$date_time)/7) + 1
  df_movement_unified$week[df_movement_unified$week == 5] = 4
  
  # Extracts the Cut
  lower_cut = quantile(df_movement_unified$n_baseline, alfa_movement_plot)
  
  
  # Map location
  # Constructed from the sample, plus a defined margin
  
  bottom = min(quantile(df_movement_unified$start_movement_lat, alfa, na.rm = TRUE), quantile(df_movement_unified$end_movement_lat, alfa, na.rm = TRUE)) 
  top = max(quantile(df_movement_unified$start_movement_lat, 1- alfa, na.rm = TRUE), quantile(df_movement_unified$end_movement_lat, 1-  alfa, na.rm = TRUE)) 
  left = min(quantile(df_movement_unified$start_movement_lon, alfa, na.rm = TRUE), quantile(df_movement_unified$end_movement_lon, alfa, na.rm = TRUE))
  right = max(quantile(df_movement_unified$start_movement_lon, 1 - alfa, na.rm = TRUE), quantile(df_movement_unified$end_movement_lon, 1- alfa, na.rm = TRUE)) 
  
  margin = max((top - bottom),(right - left))*perc_margin
  
  bottom = bottom  - margin
  top = top + margin
  left = left - margin
  right = right + margin
  
  # Gets the map
  cat(paste(ident, '      Downloading Map', '\n', sep = ""))
  map = suppressMessages(get_map(c(left = left, bottom = bottom, right = right, top = top), maptype = 'satellite', color = 'bw'))
  
  
  
  # Movement Change by Month
  # ----------------------------
  # Creates output folder
  export_folder_movement_change = file.path(export_folder,'movement_change_by_month')
  dir.create(export_folder_movement_change)
  
  cat(paste(ident, '      Movement Change Maps', '\n', sep = ""))
  
  
  # Useful columns
  useful_cols = c('start_movement_lon','start_movement_lat','end_movement_lon','end_movement_lat', 'month', 'week','percent_change','n_baseline','n_crisis')
  
  # Dimsenions
  width_change = 8
  height_change = 9
  
  # Creates plot by month and week
  for(m in sort(unique(df_movement_unified$month)))
  {
    # First by month
    df_mes = df_movement_unified[(df_movement_unified$month == m),useful_cols]
    
    for(w in sort(unique(df_mes$week)))
    {
      cat(paste(ident, '         Map for Month: ',m,', Week: ', w, '\n', sep = ""))
      # By week
      df_temp = df_mes[(df_mes$week == w),useful_cols]
      
      # Groups
      df_grouped = aggregate(cbind(df_temp$n_baseline, df_temp$n_crisis, df_temp$percent_change), list(df_temp$start_movement_lon, df_temp$start_movement_lat, df_temp$end_movement_lon, df_temp$end_movement_lat), mean)
      colnames(df_grouped) = c('lon.x','lat.x','lon.y','lat.y','n_baseline','n_crisis', 'percentage')
      
      # Frames for movement
      # -----
  
      # Baseline and crisis
      df_grouped_baseline = df_grouped[df_grouped$n_baseline >= lower_cut,]
      df_grouped_crisis = df_grouped[df_grouped$n_crisis >= lower_cut,]
      
      # Frames for percentage
      # -----
      df_grouped_percentage = df_grouped
      
      # Removes outliers
      lower = quantile(df_grouped_percentage$percentage, alfa)
      higher = quantile(df_grouped_percentage$percentage, 1 - alfa)
      
      df_grouped_percentage = df_grouped_percentage[df_grouped_percentage$percentage >= lower,]
      df_grouped_percentage = df_grouped_percentage[df_grouped_percentage$percentage <= higher,]
      
      # Selects data
      df_grouped_percentage = df_grouped_percentage[abs(df_grouped_percentage$percentage) >= percentage_plot_cut,]
      # Sorts
      df_grouped_percentage = df_grouped_percentage[order(abs(df_grouped_percentage$percentage)),]
      
      
      # Sizes
      min_size = min(df_grouped_baseline$n_baseline, df_grouped_crisis$n_crisis)
      max_size = max(df_grouped_baseline$n_baseline, df_grouped_crisis$n_crisis)
      
      #limits percentage
      rad_percentage = max(abs(df_grouped_percentage$percentage))
      
      
      # Plot
      # Baseline
      p1 =  ggmap(map)
      p1 = p1 + geom_segment(data = df_grouped_baseline, aes(x = lon.x, y = lat.x, xend = lon.y, yend = lat.y, size = n_baseline, alpha = n_baseline),  color = 'blue')
      p1 = p1 + scale_size(limits=c(min_size, max_size))
      p1 = p1 + scale_alpha(limits=c(min_size, max_size))
      p1 = p1 + theme(legend.position = "none")
      p1 = p1 + theme(axis.title = element_blank(), axis.ticks =element_blank())
      p1 = p1 + ggtitle('Un Mes Atrás')
      
      # Crisis
      p2 =  ggmap(map)
      p2 = p2 + geom_segment(data = df_grouped_crisis, aes(x = lon.x, y = lat.x, xend = lon.y, yend = lat.y, size = n_crisis, alpha = n_crisis),  color = 'red')
      p2 = p2 + scale_size(limits=c(min_size, max_size))
      p2 = p2 + scale_alpha(limits=c(min_size, max_size))
      p2 = p2 + theme(legend.position = "none")
      p2 = p2 + theme(axis.title = element_blank(), axis.ticks=element_blank())
      p2 = p2 + ggtitle(paste('Durante la Semana', w, 'de', months[m]))
      
      p3 =  ggmap(map)
      p3 = p3 + geom_segment(data = df_grouped_percentage, aes(x = lon.x, y = lat.x, xend = lon.y, yend = lat.y, color = percentage), alpha = 1, size = 0.8)
      p3 = p3 + scale_color_gradientn(colors =c("blue", "white", "red"), limits=c(-1*rad_percentage, rad_percentage))
      p3 = p3 + labs(color = "Cambio % Movimiento ")
      p3 = p3 + ggtitle(paste('Cambio Promedio Semana', w, 'de', months[m], '\nCambios Netos Mayores a', percentage_plot_cut, '%'))
      p3 = p3 + theme(axis.title = element_blank(), axis.ticks=element_blank())
      p3 = p3 + theme(legend.position="bottom")
  
      # Layout
      lay <- rbind(c(1,3), c(2,3))
      p = grid.arrange(p1,p2,p3, layout_matrix = lay)
      
      # Saves
      ggsave(file.path( export_folder_movement_change, paste0("movement_change_",m,'_', w,".jpeg")), plot = p, width = width_change, height = height_change, device = 'jpeg')
      
    }
    
  }
  
  
  # Movement Change For the last days
  # ----------------------------
  df_temp = df_movement_unified[df_movement_unified$date_time >= (max(df_movement_unified$date_time) - look_back),useful_cols]
  
  cat(paste(ident, '         Map for last ', look_back,' Days', '\n', sep = ""))
  
  # Groups
  df_grouped = aggregate(cbind(df_temp$n_baseline, df_temp$n_crisis, df_temp$percent_change), list(df_temp$start_movement_lon, df_temp$start_movement_lat, df_temp$end_movement_lon, df_temp$end_movement_lat), mean)
  colnames(df_grouped) = c('lon.x','lat.x','lon.y','lat.y','n_baseline','n_crisis', 'percentage')
  
  # Frames for movement
  # -----
  
  # Baseline and crisis
  df_grouped_baseline = df_grouped[df_grouped$n_baseline >= lower_cut,]
  df_grouped_crisis = df_grouped[df_grouped$n_crisis >= lower_cut,]
  
  # Frames for percentage
  # -----
  df_grouped_percentage = df_grouped
  
  # Removes outliers
  lower = quantile(df_grouped_percentage$percentage, alfa)
  higher = quantile(df_grouped_percentage$percentage, 1 - alfa)
  
  df_grouped_percentage = df_grouped_percentage[df_grouped_percentage$percentage >= lower,]
  df_grouped_percentage = df_grouped_percentage[df_grouped_percentage$percentage <= higher,]
  
  # Selects data
  df_grouped_percentage = df_grouped_percentage[abs(df_grouped_percentage$percentage) >= percentage_plot_cut,]
  # Sorts
  df_grouped_percentage = df_grouped_percentage[order(abs(df_grouped_percentage$percentage)),]
  
  
  # Sizes
  min_size = min(df_grouped_baseline$n_baseline, df_grouped_crisis$n_crisis)
  max_size = max(df_grouped_baseline$n_baseline, df_grouped_crisis$n_crisis)
  
  #limits percentage
  rad_percentage = max(abs(df_grouped_percentage$percentage))
  
  
  # Plot
  # Baseline
  p1 =  ggmap(map)
  p1 = p1 + geom_segment(data = df_grouped_baseline, aes(x = lon.x, y = lat.x, xend = lon.y, yend = lat.y, size = n_baseline, alpha = n_baseline),  color = 'blue')
  p1 = p1 + scale_size(limits=c(min_size, max_size))
  p1 = p1 + scale_alpha(limits=c(min_size, max_size))
  p1 = p1 + theme(legend.position = "none")
  p1 = p1 + theme(axis.title = element_blank(), axis.ticks =element_blank())
  p1 = p1 + ggtitle('Un Mes Atrás')
  
  # Crisis
  p2 =  ggmap(map)
  p2 = p2 + geom_segment(data = df_grouped_crisis, aes(x = lon.x, y = lat.x, xend = lon.y, yend = lat.y, size = n_crisis, alpha = n_crisis),  color = 'red')
  p2 = p2 + scale_size(limits=c(min_size, max_size))
  p2 = p2 + scale_alpha(limits=c(min_size, max_size))
  p2 = p2 + theme(legend.position = "none")
  p2 = p2 + theme(axis.title = element_blank(), axis.ticks=element_blank())
  p2 = p2 + ggtitle(paste('Durante Últimos', look_back, 'Días'))
  
  p3 =  ggmap(map)
  p3 = p3 + geom_segment(data = df_grouped_percentage, aes(x = lon.x, y = lat.x, xend = lon.y, yend = lat.y, color = percentage), alpha = 1, size = 0.8)
  p3 = p3 + scale_color_gradientn(colors =c("blue", "white", "red"), limits=c(-1*rad_percentage, rad_percentage))
  p3 = p3 + labs(color = "Cambio % Movimiento ")
  p3 = p3 + ggtitle(paste('Cambio Promedio Últimos', look_back, 'Días\nCambios Netos Mayores a', percentage_plot_cut, '%'))
  p3 = p3 + theme(axis.title = element_blank(), axis.ticks=element_blank())
  p3 = p3 + theme(legend.position="bottom")
  
  # Layout
  lay <- rbind(c(1,3), c(2,3))
  p = grid.arrange(p1,p2,p3, layout_matrix = lay)
  
  # Saves
  ggsave(file.path( export_folder, "movement_change_last_days.jpeg"), plot = p, width = width_change, height = height_change, device = 'jpeg')
  
  
  cat(paste(ident, '   Done!', '\n\n', sep = ""))

}


