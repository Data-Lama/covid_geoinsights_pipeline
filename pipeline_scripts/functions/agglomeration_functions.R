# Community Agglomeration Functions

km_constant = 110.567 # For extarcting distances

# Function that extracts the name of the community
exctract_name = function(location, num_cases)
{
  df = data.frame(location = location, num_cases = num_cases) %>%
    group_by(location) %>%
    summarise(num_cases = sum(num_cases)) %>%
    ungroup() %>% arrange(desc(num_cases))
  
  return(df$location[1])
  
}



# Function that create the geometry (colection of points)
extract_geometry = function(lon, lat)
{
  points = paste( sapply(seq_along(lon), function(i) paste(lon[i], lat[i])), collapse = ", ")
  geom_string = paste("MULTIPOINT (", points, ")")
  
  return(geom_string)
}

# Function that create the geometry (colection of points)
extract_list_of_agg_polygons = function(poly_id)
{
  ids = paste(poly_id, collapse = ", ")
  
  return(ids)
}

# Function that extracts the center
extract_center= function(lon, lat, page_rank)
{
  return(c(lon[which.max(page_rank)], lat[which.max(page_rank)]))
}

# Function that extracts the center
extract_center_by_cases = function(lon, lat, num_cases)
{
  return(c(lon[which.max(num_cases)], lat[which.max(num_cases)]))
}

# Function that extracts the name of the community (by _cases)
exctract_name_by_cases = function(poly_name, num_cases)
{
  return(poly_name[num_cases == max(num_cases)][1])
}

# Function that extracts the id of the community (by _cases)
exctract_id_by_cases = function(poly_id, num_cases)
{
  return(poly_id[num_cases == max(num_cases)][1])
}

exctract_name_by_population = function(poly_name, population)
{
  return(poly_name[population == max(population)][1])
  
}

exctract_id_by_population = function(poly_id, population)
{
  return(poly_id[population == max(population)][1])
}

