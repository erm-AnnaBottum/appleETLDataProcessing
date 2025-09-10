# functions ####
get_gen_id <- function(str_name){
  out <- paste(
    unlist(
      str_split(str_name, "[.]")[[1]][1:4]
    ),
    collapse = " "
  )
  return(out)
}

get_gen_idx <- function(str_name){
  print(str_name)
  str_split(str_name, "[.]")[[1]][[4]] %>% as.numeric()
}
