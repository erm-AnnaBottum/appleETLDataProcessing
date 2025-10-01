# this script is sourced by the process_raw_data script. that script must be run
# before running this one.

# read in template file
et_template <- read_excel(
  file.path(fol_main, "reference", "et_workbook_template.xlsx"),
  col_types = "text"
)

# format column names in both so they can bind
names(et_template) <- tolower(names(et_template))
names(write_prep) <- tolower(names(write_prep))

write_template <- write_prep %>%
  bind_rows(
    et_template
  )

# get date info to create filename
date_str <- paste0(lubridate::month(write_template$date[[1]], label = TRUE), # specify lubridate to use label argument
                   "-",
                   year(write_template$date[[1]]))

write.xlsx(write_template, file.path(fol_out, paste0(
  facility, "_",
  date_str, "_",
  str_replace(csv_filename, " \\(last month\\)", ""), "_",
  format(Sys.Date(), "%Y%m%d"), ".xlsx")
  )
)
