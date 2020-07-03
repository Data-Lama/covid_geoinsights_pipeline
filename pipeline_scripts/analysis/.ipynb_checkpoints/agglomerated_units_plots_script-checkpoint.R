#!/usr/bin/env Rscript

options(warn=-1)

# Creates the report plots based on the mobility

suppressMessages(library('igraph'))
suppressMessages(library('ggplot2'))
suppressMessages(library('ggmap'))
suppressMessages(library('lubridate'))
suppressMessages(library('gridExtra'))
suppressMessages(library('dplyr'))
suppressMessages(library('stringr'))


# Loads constants
source('pipeline_scripts/functions/constants.R')


# Ident for console
ident = '         '


# Export options
width = 8
height = 8

look_back = 7 # Days to look back
perc_margin = 0.03 # Percentage margin
alfa  = 0.001 # For outlier removal

# Top Polygons
top_polygons = 15

# Months
months = c('Enero','Febrero','Marzo','Abril','Mayo','Junio','Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre')

# Location Folder from args
args = commandArgs(trailingOnly=TRUE)
location_name = args[1] 
location_folder = args[2]
agglomeration_method_parameter = args[3]

# Debug Variables

#setwd("~/Dropbox/Projects/covid_fb")
#location_name = 'Colombia'
#location_folder = 'colombia'
#agglomeration_method_parameter = 'community'


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
	agglomerated_folder = paste('data/data_stages/',location_folder,'/agglomerated/', agglomeration_method, '/', sep = "")

	if(!dir.exists(agglomerated_folder))
	{
		cat(paste(ident, '   No data found for ', agglomeration_method, ' Agglomeration (', agg, ' of ', length(agglomeration_methods), '). Skipping', '\n', sep = ""))
		next
	}

	# Movement Selected Polygons (Agglomerated Control Units)
	# ----------------------------

	cat(paste(ident, '   Movement For Polygons by ', agglomeration_method, ' Agglomeration (', agg, ' of ', length(agglomeration_methods),')', '\n', sep = ""))


	# Export Folder
	# Three Steps
	export_folder = paste('analysis/',location_folder,'/', sep = "")
	dir.create(export_folder)

	# Export Folder
	export_folder = paste(export_folder ,agglomeration_method,'/', sep = "")
	dir.create(export_folder)

	# Export Folder
	export_folder = paste(export_folder, 'agglomerated_polygons_plots/', sep = "")
	dir.create(export_folder)



	



	cat(paste(ident, '      Loads Agglomerated Movement', '\n', sep = ""))


	# Agglomerated
	df_movement_agg = read.csv(paste(agglomerated_folder,'movement.csv', sep = ''), stringsAsFactors = FALSE)
	df_poly = read.csv(paste(agglomerated_folder,'polygons.csv', sep = ''), stringsAsFactors = FALSE)

	# Renames the oher places
	df_poly = df_poly[order(-1*df_poly$num_cases),]
	df_poly$poly_name[(min(top_polygons + 1, dim(df_poly)[1])):dim(df_poly)[1]] = 'Otros'

	df_movement_agg = merge(df_movement_agg, df_poly, by.y = 'poly_id',  by.x = 'start_poly_id')
	df_movement_agg = merge(df_movement_agg, df_poly, by.y = 'poly_id',  by.x = 'end_poly_id')

	# Drops movement between other places
	#df_movement_agg = df_movement_agg %>% filter(poly_name.y != 'Otros' | poly_name.x != 'Otros')

	# Moves the "other" movement to end polygon ID (shifts)
	df_others = df_movement_agg %>% 
	          filter(poly_name.x == 'Otros') %>%
	          rename(end_poly_id = start_poly_id, start_poly_id = end_poly_id, poly_name.x = poly_name.y, poly_lon.x = poly_lon.y,  poly_lat.x = poly_lat.y,  num_cases.x =  num_cases.y, poly_name.y = poly_name.x, poly_lon.y = poly_lon.x,  poly_lat.y = poly_lat.x,  num_cases.y =  num_cases.x ) %>%
	          select(colnames(df_movement_agg))


	df_movement_agg[df_movement_agg$poly_name.x == 'Otros', ] = df_others


	# Converts to date
	df_movement_agg$date_time = as.Date(df_movement_agg$date_time)

	# Assings month
	df_movement_agg$month = month(df_movement_agg$date_time)

	# Assigns week
	df_movement_agg$week = floor(day(df_movement_agg$date_time)/7) + 1
	df_movement_agg$week[df_movement_agg$week == 5] = 4


	# Maps the Control Units
	cat(paste(ident, '      Location Selected Polygons', '\n', sep = ""))

	# Creates the dataframe for each of the multipoints
	df_units = NULL
	for(i in 1:dim(df_poly)[1])
	{
	  unit_name = df_poly[i,'poly_name']
	  df_unit_temp = data.frame('unit_name' = unit_name, 'lon' = df_poly[i,'poly_lon'], 'lat' = df_poly[i,'poly_lat'])
	  geometry = df_poly[i,'geometry']
	  
	  # Cleans Geometry
	  geometry = gsub("MULTIPOINT","", geometry)
	  geometry = gsub("(","", geometry,fixed = TRUE)
	  geometry = gsub(")","", geometry, fixed = TRUE)
	  points = strsplit(geometry, ",")[[1]]
	  
	  for(p in points)
	  {
	    p = str_trim(p)
	    coor = strsplit(p, " ")[[1]]
	    df_unit_temp = rbind(df_unit_temp, data.frame('unit_name' = unit_name, 'lon' = as.numeric(coor[1]), 'lat' =as.numeric(coor[2])))
	  }
	  
	  if(is.null(df_units))
	  {
	    df_units = df_unit_temp
	  }else
	  {
	    df_units = rbind(df_units, df_unit_temp)
	  }
	    
	}


	# Gets the map
	cat(paste(ident, '          Downloading Map', '\n', sep = ""))

	# By Month

	# Map location
	# Constructed from the sample, plus a defined margin
	alfa  = 0
	bottom = min(df_units$lat) 
	top = max(df_units$lat) 
	left = min(df_units$lon) 
	right = max(df_units$lon) 

	margin = max((top - bottom),(right - left))*perc_margin

	bottom = bottom  - margin
	top = top + margin
	left = left - margin
	right = right + margin


	map = suppressMessages(get_map(c(left = left, bottom = bottom, right = right, top = top), maptype = 'satellite', color = 'bw'))


	cat(paste(ident, '         Saving Map', '\n', sep = ""))

	p =  ggmap(map)
	p = p + geom_point(data = df_units %>% filter(unit_name != 'Otros'), aes(x = lon, y = lat, color = unit_name))
	p = p + scale_color_discrete( name = "Unidad de Control" )
	#p = p + guides(size = FALSE)
	p = p + ggtitle(paste('Unidades de Control Principales'))
	p = p + theme(axis.title = element_blank(), axis.ticks=element_blank())
	#p = p + theme(legend.position = "none")

	# Saves
	ggsave(paste( export_folder, "selected_polygons_", agglomeration_method,".jpeg", sep = ""), plot = p, width = width, height = height, device = 'jpeg')




	cat(paste(ident, '      Movement Selected Polygons Maps', '\n', sep = ""))

	# Gets the map
	cat(paste(ident, '         Downloading Map', '\n', sep = ""))

	# By Month

	# Map location
	# Constructed from the sample, plus a defined margin
	alfa  = 0
	bottom = min(quantile(df_movement_agg$poly_lat.x, alfa), quantile(quantile(df_movement_agg$poly_lat.y, alfa), alfa)) 
	top = max(quantile(df_movement_agg$poly_lat.x, 1- alfa), quantile(df_movement_agg$poly_lat.x, 1-  alfa)) 
	left = min(quantile(df_movement_agg$poly_lon.x, alfa), quantile(quantile(df_movement_agg$poly_lon.y, alfa), alfa)) 
	right = max(quantile(df_movement_agg$poly_lon.x, 1- alfa), quantile(df_movement_agg$poly_lon.x, 1-  alfa)) 

	margin = max((top - bottom),(right - left))*perc_margin

	bottom = bottom  - margin
	top = top + margin
	left = left - margin
	right = right + margin


	map = suppressMessages(get_map(c(left = left, bottom = bottom, right = right, top = top), maptype = 'satellite', color = 'bw'))

	# boundaries
	min_mov = min(df_movement_agg$movement)
	max_mov = max(df_movement_agg$movement)


	# Creates output folder
	export_folder_movement_selected = paste(export_folder,'/selected_polygons_movement_by_month_', agglomeration_method, '/', sep = "")
	dir.create(export_folder_movement_selected)



	# Useful columns
	useful_cols = c('poly_name.x', 'poly_lon.x','poly_lat.x', 'poly_name.y','poly_lon.y','poly_lat.y', 'month', 'week','movement')

	# Dimsenions
	width_selected = 8
	height_selected = 8


	# Creates plot by month and week
	for(m in sort(unique(df_movement_agg$month)))
	{
		# First by month
		df_mes = df_movement_agg[(df_movement_agg$month == m),useful_cols]

		for(w in sort(unique(df_mes$week)))
		{
		  
		  cat(paste(ident, '         Map for Month: ',m,', Week: ', w, '\n', sep = ""))
		  # By week
		  df_temp = df_mes[(df_mes$week == w),useful_cols]
		  
		  # Groups
		  df_grouped = aggregate(cbind(df_temp$movement), list(df_temp$poly_name.x, df_temp$poly_lon.x, df_temp$poly_lat.x, df_temp$poly_name.y, df_temp$poly_lon.y, df_temp$poly_lat.y), mean)
		  colnames(df_grouped) = c('poly_name.x', 'poly_lon.x','poly_lat.x', 'poly_name.y','poly_lon.y','poly_lat.y', 'movement')
		  
		  p =  ggmap(map)
		  p = p + geom_segment(data = df_grouped, aes(x = poly_lon.x, y = poly_lat.x, xend = poly_lon.y, yend = poly_lat.y, color = poly_name.x, size = movement))
		  p = p + scale_size(limits=c(min_mov, max_mov) )
		  p = p + scale_color_discrete( name = "Unidad de Control" )
		  p = p + guides(size = FALSE)
		  #p = p + scale_alpha(limits=c(min_mov, max_mov))
		  p = p + ggtitle(paste('Mov. Promedio Unidades de Control\nSemana', w, 'de', months[m]))
		  p = p + theme(axis.title = element_blank(), axis.ticks=element_blank())
		  #p = p + theme(legend.position = "none")
		  
		  # Saves
		  ggsave(paste( export_folder_movement_selected, "movement_selected_", agglomeration_method, "_", m,'_', w,".jpeg", sep = ""), plot = p, width = width_selected, height = height_selected, device = 'jpeg')
		  
		}
	}


	# ----------------------------
	# Last Days

	# Extract final date
	final_date =  as.POSIXlt(max(df_movement_agg$date_time))
	final_date$mday = final_date$mday - look_back
	df_temp = df_movement_agg[as.POSIXlt(df_movement_agg$date_time) >= final_date, useful_cols]

	cat(paste(ident, '         Map for last ', look_back,' Days', '\n', sep = ""))

	# Groups
	df_grouped = aggregate(cbind(df_temp$movement), list(df_temp$poly_name.x, df_temp$poly_lon.x, df_temp$poly_lat.x, df_temp$poly_name.y, df_temp$poly_lon.y, df_temp$poly_lat.y), mean)
	colnames(df_grouped) = c('poly_name.x', 'poly_lon.x','poly_lat.x', 'poly_name.y','poly_lon.y','poly_lat.y', 'movement')

	p =  ggmap(map)
	p = p + geom_segment(data = df_grouped, aes(x = poly_lon.x, y = poly_lat.x, xend = poly_lon.y, yend = poly_lat.y, color = poly_name.x, size = movement))
	p = p + scale_size(limits=c(min_mov, max_mov))
	p = p + scale_color_discrete( name = "Unidad de Control" )
	p = p + guides(size = FALSE)
	p = p + ggtitle(paste('Mov. Promedio Unidades de Control\nÚltimos', look_back, 'Días'))
	p = p + theme(axis.title = element_blank(), axis.ticks=element_blank())
	#p = p + theme(legend.position = "none")

	# Saves
	ggsave(paste( export_folder, "movement_selected_", agglomeration_method, "_last_days.jpeg", sep = ""), plot = p, width = width_selected, height = height_selected, device = 'jpeg')


	cat(paste(ident, '   Done!', '\n', sep = ""))


	cat(paste(ident, 'Done!', '\n', sep = ""))


}


