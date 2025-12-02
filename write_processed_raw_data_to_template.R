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

# write_template <- write_prep %>%
#   bind_rows(
#     et_template
#   ) %>%
  # mutate(
  #   date_new = as.POSIXct(date)#format(as.POSIXct(date, format = "%Y-%m-%d %I:%M:%S"), "%m/%d/%Y %H:%M %p")#format(as.Date(date, tryFormats = c("%Y-%m-%d %H:%M:%S")), "%m/%d/%Y %H:%M %p")
  # )
write_template <- et_template %>% bind_rows(write_prep)

write_lst <- list("data" = write_template,
                  "records_missing_rfrs" = df_no_rfr,
                  "unused_rfrs" = df_unused_rfrs)


write.xlsx(write_lst, file.path(fol_out, paste0(
  facility, "_",
  month_folder, "_Run_Logs_",
  format(Sys.Date(), "%Y%m%d"), ".xlsx")
  )
)
