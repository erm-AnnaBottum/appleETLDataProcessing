if(!require(pacman)){ install.packages("pacman") } else { library(pacman) }
# import ####
p_load(
  here, 
  dplyr, 
  readxl,
  stringr,
  tidyr,
  openxlsx,
  janitor,
  lubridate,
  svDialogs
)
options(scipen = 999)

filename <- "ULA Generator Tracking Workbook v3.1_for PG"
facility <- "Ulanqab"
distinct_date_structure <- TRUE

fol_main <- here::here()
fol_data <- file.path(fol_main, "data")

sheet_names <- excel_sheets(file.path(fol_data, paste0(filename, ".xlsx")))
data_sheet <- dlg_list(choices = sheet_names, title = "Please select input tab")$res #$res gets result value of dlg_list()

if (!distinct_date_structure){
  df_in <- read_excel(
    file.path(fol_data, paste0(filename, ".xlsx")),
    sheet = data_sheet,
    skip = 3,
    col_names = FALSE,
    col_types = "text" # do this so number formatting doesn't get messed up
  )
  # prep columns & tables
  df_prep <- df_in %>%
    t() %>% # transpose so that 'right' becomes 'down', in order to use fill(). this returns a matrix
    as.data.frame() %>% # fill() works on dfs, so convert
    fill(V1, .direction = "down") %>%
    t() %>% # transpose back to original state
    as.data.frame() # convert back to df
  
  generator_info <- df_prep[1, ]
  param_info <- df_prep[2, ]
  new_colnames <- paste(generator_info, param_info, sep = "|")
  
  # get distinct list of generator and parameter codes, export to create mapping tables
  gen_codes <- generator_info %>% pivot_longer(cols = -c(...1)) %>% select(value) %>% distinct()
  param_codes <- param_info %>% pivot_longer(cols = -c(...1)) %>% select(value) %>% distinct()
} else {
  df_in <- read_excel(
    file.path(fol_data, paste0(filename, ".xlsx")),
    sheet = data_sheet,
    skip = 2,
    col_names = FALSE,
    col_types = "text" # do this so number formatting doesn't get messed up
  )
  
  # prep columns & tables
  df_prep <- df_in %>%
    t() %>% # transpose so that 'right' becomes 'down', in order to use fill(). this returns a matrix
    as.data.frame() %>% # fill() works on dfs, so convert
    fill(V1, .direction = "down") %>%
    t() %>% # transpose back to original state
    as.data.frame() # convert back to df
  
  generator_info <- df_prep[1, ]
  param_info <- df_prep[2, ]
  new_colnames <- paste(generator_info, param_info, sep = "|")
  
  # get distinct list of generator and parameter codes, export to create mapping tables
  gen_codes <- generator_info %>% pivot_longer(cols = -c(...1)) %>% select(value) %>% distinct()
  param_codes <- param_info %>%
    pivot_longer(cols = -c(...1)) %>%
    select(value) %>%
    mutate(value = str_replace_all(str_replace_all(value, "[\U4E00-\U9FFF\U3000-\U303F]|（|）", ""), "\\r?\\n|\\r", " ")) %>%
    distinct()
}

list_codes_out <- list("gen-codes" = gen_codes,
                       "param-codes" = param_codes)

write.xlsx(list_codes_out, paste0(facility, "_tag_code_IDs.xlsx"))
