# Apple ETL Data Automation
# Summer 2025 - Anna Bottum
#
# Process Emissions Tracking Workbooks into format used for loading into
# Cority database.

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

# user input ####
filename <- "20250917_FINAL_WKE Emissions Tracking v2.1"
facility <- "Waukee"
collector <- "mikayla"
rows_to_skip <- 2

fol_main <- here::here()
fol_data <- file.path(fol_main, "data")
fol_ref <- file.path(fol_main, "reference")

# read in data ####
sheet_names <- excel_sheets(file.path(fol_data, paste0(filename, ".xlsx")))
data_sheet <- dlg_list(choices = sheet_names, title = "Please select input tab")$res #$res gets result value of dlg_list()

tag_mappings <- read_excel(
  file.path(fol_ref, "Apple_ETL_Remaps.xlsx"),
  sheet = "TagEndings"
) %>%
  mutate(
    original_tag = str_replace_all(sub(".*\\|", "", original_tag), "\\r?\\n|\\r", " ")
  ) %>%
  distinct()

generator_mappings <- read_excel(
  file.path(fol_ref, "generator_tag_mappings_MT.xlsx"),
  sheet = facility
) %>%
  select(
    generator_name,
    generator_tag
  )

df_in <- read_excel(
  file.path(fol_data, paste0(filename, ".xlsx")),
  sheet = data_sheet,
  skip = rows_to_skip,
  col_names = FALSE#
  # col_types = "text" # do this so number formatting doesn't get messed up
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

if(FALSE){
  gen_info <- generator_info %>% pivot_longer(cols = -c(...1)) %>% select(value) %>% distinct()
  write.xlsx(gen_info, paste0(facility, "_generator_IDs.xlsx"))
}

names(df_prep) <- new_colnames
colnames(df_prep)[1] <- "param_date"
names(df_prep)[2] <- "param_year"
df_clean <- df_prep[-c(1,2), ]

df_pvt <- df_clean %>%
  pivot_longer(
    cols = -c(param_date, param_year)
  ) %>%
  mutate(
    param_value = case_when(
      value == "<--Starting hour meter reading (tenth of hr)" ~ NA_character_
    )
  ) %>%
  filter(
    !str_detect(name, "Month/Year")
  )

# take out the starting_hrs portion of df to fill down columns, then join back in
df_meta_hrs <- df_pvt %>%
  filter(
    param_date == "Starting Hrs"
  ) %>%
  fill(
    param_value,
    .direction = "down"
  )

df_data_full <- df_pvt %>%
  filter(
    param_date != "Starting Hrs", 
    !is.na(value),
    !str_detect(name, "Month Year")
  ) %>%
  mutate(
    first_string = str_replace_all(as.character(as.numeric(value)), "\\..*", ""),
    second_string = str_replace_all(as.character(as.numeric(value)), "^.*?\\.", ""),
    # second_string_fmt = substr(second_string, 1, 10),
    param_value = case_when(
      tolower(value) == "maint. & testing" ~ "1",
      tolower(value) == "emergency" ~ "2",
      tolower(value) == "other" ~ "3",
      tolower(value) == "commissioning" ~ "4",
      tolower(value) == "y" ~ "1",
      tolower(value) == "n" ~ "0",
      is.na(as.numeric(value)) ~ value,
      nchar(second_string) > 10 ~ paste(first_string, substr(second_string, 1, 10), sep = "."), #as.character(round(as.numeric(value), digits = 3)),
      TRUE ~ as.character(as.numeric(value))
    ),
    # check_nchar = nchar(check),
    # chec_val = as.character(value),
    generator_name = sub("\\|.*", "", name),
    param_type = str_replace_all(sub(".*\\|", "", name), "\\r?\\n|\\r", " ")
  ) %>%
  left_join(
    tag_mappings,
    by = c("param_type" = "original_tag")
  ) %>%
  left_join(
    generator_mappings,
    by = c("generator_name")
  ) %>%
  distinct()

# this should be zero:
table(df_data_full$value[is.na(df_data_full$param_value)], useNA = "always")

# for some sticky cases:
df_data_full$output_tag[df_data_full$param_type == "Fuel Use\r\n(gal/hr)"] <- "_FUELUSEGALHR-NOTAG"
df_data_full$output_tag[df_data_full$param_type == "Total Fuel Usage\r\n(gal/run)"] <- "_FUELUSEGALRUN-NOTAG"

# this should be a df of 0
check <- df_data_full %>% filter(is.na(output_tag))

# prep full dataset for export ####
# if duplicates, increment minutes by one
# bin by tag/no tag, write to separate tabs of out
df_out_prep <- df_data_full %>%
  mutate(
    Date = format(janitor::convert_to_datetime(param_date), "%m/%d/%Y %I:%M %p"), #format(openxlsx::convertToDate(param_date), "%m/%d/%Y %H:%M"), #paste0(format(openxlsx::convertToDate(param_date), "%m/%d/%Y"), " 12:00 AM"),
    Tag = paste(generator_tag, output_tag, sep = "\\"),
    Value = param_value,
    Collector = collector,
    `Engine Mode CF` = NA_character_ # for now
  ) %>%
  group_by(
    Date,
    Tag
  ) %>%
  mutate(
    group_n = n(),
    row = row_number(),
    Date = case_when(
      group_n > 1 ~ format(as.Date(Date, "%m/%d/%Y %I:%M %p") + minutes(row_number() - 1), "%m/%d/%Y %I:%M %p"),
      TRUE ~ Date #format(as.Date(Date, "%m/%d/%Y %I:%M %p"), "%m/%d/%Y %I:%M %p")
    )
  )

  # move rfr comment into last column
splt_prep <- split(df_out_prep, f = list(df_out_prep$generator_tag, df_out_prep$Date), drop = TRUE) # drop = TRUE forces to drop levels that don't occur

df_final <- lapply(splt_prep, function(splt){
  if (sum(str_detect(splt$name, "(?i)Reason for Run Comment")) > 0){
    # grab rfr comment value from the correct row, 'value' column
    rfr_comment <- splt[str_detect(splt$name, "(?i)Reason for Run Comment"), ]$value
    
    # put it into the Engine Mode CF column of the rfr row
    splt[splt$param_type == "Reason for Run", ]$`Engine Mode CF` <- rfr_comment
    
    return(splt)
  } else {
    return(splt)
  }
}) %>%
  bind_rows() %>%
  select(
    Date,
    Tag,
    Value,
    Collector,
    `Engine Mode CF`
  ) %>%
  mutate(
    sort_date = format(as.Date(Date, "%m/%d/%Y"), "%Y-%m-%d")
  ) %>%
  arrange(sort_date) %>%
  select(-c(sort_date))

df_out_wtag <- df_final %>%
  filter(
    !str_detect(Tag, "(?i)NOTAG")
  )

df_out_notag <- df_final %>%
  filter(
    str_detect(Tag, "(?i)NOTAG")
  )

start_date <- format(min(as.Date(df_final$Date, "%m/%d/%Y %H:%M")), "%Y%m%d")
end_date <- format(max(as.Date(df_final$Date, "%m/%d/%Y %H:%M")), "%Y%m%d")

write_out <- list(
  "Data Import" = df_out_wtag,
  "Data Import - No Tags" = df_out_notag
  )

write.xlsx(write_out, file.path("output", paste0(facility, "_ETL_Data_", start_date, "-", end_date, ".xlsx")))







