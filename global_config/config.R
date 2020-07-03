# Config file for R


config_file = 'global_config/config_file.csv'
if(file.exists(config_file))
{
  df_config_file = read.csv(config_file, stringsAsFactors = FALSE)
  
}else
{
  stop(paste0('Config file: ', config_file, ' not found'))
}

get_property = function(name)
{
  if(name %in% df_config_file$name)
  {
    value = df_config_file$value[df_config_file$name == name]

    # Majority of scripts won't work if directories include the last /
    if(grepl('_dir', name))
    {
      if(substr(value, nchar(value), nchar(value)) == "/")
      {
        value = substr(value, 1, nchar(value) - 1)
      }
    }
    
    return(value)
    
  }else
  {
    stop(paste0('Property: ', name, ' not found in the configuration file'))
  }
}