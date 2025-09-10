# Apple ETL Data Automation
# Summer 2025 - Anna Bottum
#
# process raw datasets into Emissions Tracking Workbook template format
# user will copy and paste output from this code into Emissions Tracking Workbook
# for ultimate transformation into Cority loading format
# in order to run:
# 1) copy word document table into a new excel file.
#    be sure to use template column headers (see past files for examples)
#    Use this excel file as the input table for RFR codes.

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
  svDialogs,
  docxtractr,
  purrr,
  tidyverse
)
options(scipen = 999)
source("source/function.R")

# user input ####
csv_filename <- "1500 Gen 1-5 Run Log (last month)" # user input
docx_tbl_filename <- "Generator Run log for August 2025" # word doc table
facility <- "Mesa"

# read in data ####
fol_main <- here::here()
fol_data <- file.path(fol_main, "raw_data")
fol_out <- file.path(fol_main, "output")
list_files <- list.files(fol_data, ".csv")

df_gen_log_in <- read.csv(file.path(fol_data, paste0(csv_filename, ".csv")))

# read in word doc table, containing reason for run info
# do a bit of cleanup in order to be able to join it to other datasets
df_doc_tbl <- read_excel(file.path(fol_data, paste0(docx_tbl_filename, ".xlsx"))) %>%
  mutate(
    gen_date_key = str_replace_all(paste0(str_replace(generator, "(?i)Gen.*", "GEN"), "_", as.Date(date)), " ", "-"),
    gen_range_val = str_replace(str_replace_all(str_extract(generator, "(?i)Gen.*"), "(?i)Gen-|(?i)Gen| ", ""), "-|&", ":"),
    gen_start_rng = str_replace_all(gen_range_val, ":.*", ""),
    gen_end_rng = str_replace_all(gen_range_val, ".*:", ""),
    building = str_replace_all(generator, "(?i)Gen.*|-", ""),
    load_status = case_when(
      str_detect(rfr, "(?i)no load") ~ 0,
      TRUE ~ 1
    ),
    reason_for_run = case_when(
      str_detect(rfr, "(?i)Block") ~ "Maint. & Testing",
      str_detect(rfr, "(?i)Load bank") ~ "Maint. & Testing",
      str_detect(rfr, "(?i)Switchgear") ~ "Other",
      str_detect(rfr, "(?i)Transformer") ~ "Other",
      str_detect(rfr, "(?i)Quarterly PM|Q") ~ "Maint. & Testing",
      str_detect(rfr, "(?i)3 YR PM") ~ "Maint. & Testing",
      str_detect(rfr, "(?i)Test") ~ "Maint. & Testing",
      str_detect(rfr, "(?i)Verification") ~ "Maint. & Testing",
      str_detect(rfr, "(?i)Failure|(?i)Utility") ~ "Emergency",
      str_detect(rfr, "(?i)PM Maintenance") ~ "Maint. & Testing",
      TRUE ~ rfr
    )
  ) %>%
  rename(
    op_mode_comment = rfr
  ) #%>%
  # rowwise() %>%
  # mutate(
  #   gen_range = list(seq(start_val, end_val))
  #   # gen_range = case_when(
  #   #   !str_detect(gen_range_val, ":") ~ as.character(gen_range_val),
  #   #   TRUE ~ as.character(unlist(map2(start_val, end_val, seq)))
  #   # )
  # ) %>%
  # ungroup()

if(nrow(df_doc_tbl) > 0){
  df_doc_tbl_exists <- TRUE
} else{
  df_doc_tbl_exists <- FALSE
}

# prep data
# split df_gen_log_in by na rows, since each section of rows needs to be
# loaded as its own sub dataset

# gen blank rows
# blank_rws <- apply(df_gen_log_in, 1, function(rw) all(is.na(rw) | rw == ""))

# get column groupings, split by them
# create a new dataframe which consists of gen info & original column names
timestamps <- df_gen_log_in[, 1]
df_gen_log_in <- df_gen_log_in[, 2:ncol(df_gen_log_in)]
gen_identifier <- data.frame(
  lapply(names(df_gen_log_in),
         get_gen_id)
) %>%
  setNames(names(df_gen_log_in))

# bind gen info df to the original df, to split by
df_prep <- gen_identifier %>%
  rbind(df_gen_log_in)

# get split index info
rw_idxs <- lapply(names(df_gen_log_in), get_gen_idx) %>% unlist
rw_diffs <- diff(rw_idxs)
rw_switch_ends <- which(rw_diffs != 0)
rw_switch_starts <- rw_switch_ends[1:length(rw_switch_ends)] + 1
rw_switch_starts <- c(1, rw_switch_starts)
rw_switch_ends <- c(rw_switch_ends, ncol(df_gen_log_in))
switches <- Map(c, rw_switch_starts, rw_switch_ends) # pair start & end index for each

# get list of separated dataframes
rw_dfs <- switches %>% lapply(function(switch_coords){
  switch_start <- switch_coords[[1]]
  switch_end <- switch_coords[[2]]
  cbind(timestamps, df_gen_log_in[, switch_start:switch_end])
})

# manage each dataframe, then combine
# if this errors, it means there is an issue in the input file column names
rw_dfs_prep <- rw_dfs %>% lapply(function(df){
  print(df)
  gen_info <- get_gen_id(colnames(df[2]))
  colname_extr <- str_replace_all(gen_info, " ", ".")
  colnames(df) <- str_replace_all(str_replace_all(str_replace_all(str_replace(colnames(df), colname_extr, ""), "[0-9]", ""), "\\.{2,}", ""), "[.]", "_")
  colnames(df) <- str_replace_all(colnames(df), "(?i)SCR_SCR", "SCR")
  df_out <- df %>%
    filter(rowSums(is.na(select(., -1))) < (ncol(.) - 1)) %>%
    mutate(
      update_point = (Engine_Running == 1 & lag(Engine_Running, default = 0) == 0),
      group_num = cumsum(update_point),
      generator_info = gen_info
    ) %>%
    group_by(group_num) %>%
    mutate(
      group_size = n()
    )
}) %>%
  bind_rows()

# now that we've got a 'cleaned' dataset, split by generator, then split by group
# number to create each individual new row
colnames(rw_dfs_prep) <- tolower(colnames(rw_dfs_prep))
gen_splt_lst <- split(rw_dfs_prep, rw_dfs_prep$generator_info)

new_rws <- gen_splt_lst %>% lapply(function(gen_df){
  grp_splt_lst <- split(gen_df, gen_df$group_num)
  
  new_rws_df <- grp_splt_lst %>% lapply(function(grp){
    grp_size <- grp$group_size[1]
    dstemp_colname <- colnames(grp[, str_detect(names(grp), "downstream")])
    print(grp)
    if (grp_size == 2){
      # get values in place, then create a new, 1-line df
      generator_val <- str_replace(grp$generator_info[1], "genlogs ", "")
      gen_range_val <- str_replace_all(str_extract(str_replace(generator_val, "genlogs", ""), "(?i)Gen.*"), "(?i)Gen-|(?i)Gen| ", "")
      date_val <- grp$timestamps[1]
      hr_meter_val <- grp$runtime_hr_[2]
      power_ld_val <- grp$max_power_kw_[2]
      # downstrm_temp_val <- paste0(round(as.numeric(grp[, str_detect(names(grp), "downstream")][1]), 3), " to ", round(as.numeric(grp[, str_detect(names(grp), "downstream")][2]), 3))
      downstrm_temp_val <- paste0(
        round(as.numeric(pull(grp[1, str_detect(names(grp), "downstream")])), 3),
        " to ",
        round(as.numeric(pull(grp[2, str_detect(names(grp), "downstream")])), 3)
      )
      building_val = str_replace_all(str_replace(generator_val, "genlogs", ""), "(?i)Gen.*|-| ", "")
      
      # get vals in place to determine controlled status
      check_temp_val <- min(grp[, str_detect(names(grp), "downstream")])
      check_pwrld_val <- min(grp$max_power_kw_)
      
      # get values in place to determine which reason for run value to assign to record
      check_load_status = case_when(
        power_ld_val == 0 ~ 0,
        TRUE ~ 1
      )
      gen_number <- str_split(generator_val, " ")[[1]][3]
      
      # add in logic to check for existence of SCR columns
      # if they don't exist, control_yn will be NO
      # if they do exist, run the following existing logic
      if (sum(str_detect(names(grp), "scr")) == 0){
        control_yn_val <- "N"
      } else{
        if (!is.na(grp$scr_treated_run[1]) & grp$scr_treated_run[1] == 1){ # changed from grp$scr_treated_run[2] to grp$scr_treated_run[1]
          if (grp$scr_shutdown[2] == 0){
            if (as.numeric(check_temp_val) > 572 & check_pwrld_val != 0){
              control_yn_val <- "Y" # controlled
            } else if (as.numeric(check_temp_val) < 572 | check_pwrld_val == 0){
              control_yn_val <- "N" # uncontrolled
            }
          }
        } else{
          control_yn_val <- "N"
        }
      }
      
      new_rw_prep_a <- data.frame(
        generator = as.character(generator_val),
        date = as.character(date_val),
        hour_meter = as.character(hr_meter_val),
        downstream_temp_F = as.character(downstrm_temp_val),
        controlled_yn = as.character(control_yn_val),
        power_load_kW = as.character(power_ld_val),
        gen_date_key = as.character(str_replace_all(paste0(str_replace(generator_val, "(?i)Gen.*", "GEN"), "_", as.Date(date_val, format = "%Y-%m-%d %H:%M:%S")), " ", "-")) # for 1500 file
        # gen_date_key = as.character(str_replace_all(paste0(str_replace(generator_val, "(?i)Gen.*", "GEN"), "_", as.Date(date_val, format = "%m/%d/%Y %H:%M")), " ", "-")) # for 1100 file
        # gen_date_key = as.character(str_replace_all(paste0(str_replace(generator_val, "(?i)Gen.*", "GEN"), "_",as.Date(date_val, "%Y-%m-%d %H:%M:%S")), " ", "-"))
      )
      
      if(df_doc_tbl_exists){
        doc_tbl_filt_prep <- df_doc_tbl %>%
          mutate(
            gen_match = case_when(
              !is.na(as.numeric(gen_number)) & (gen_number > gen_start_rng & gen_number < gen_end_rng) ~ "match", # cases where gen # is a number value
              gen_number == gen_start_rng ~ "match", # cases where it's a number or character and equals start range
              TRUE ~ "no match"
            )
          )
        doc_tbl_filt <- doc_tbl_filt_prep %>%
          filter(
            str_detect(generator, as.character(building_val)) & check_load_status == load_status &
              gen_match == "match" & #str_detect(gen_range, gen_number) &
              # as.Date(date_val, format = "%m/%d/%Y %H:%M") == date #1100 files
              as.Date(date_val, format = "%Y-%m-%d %H:%M:%S") == date #1500 files
          ) %>%
          select(
            gen_date_key,
            reason_for_run,
            op_mode_comment,
            load_status
          )
        
        # handle cases where load statuses don't match
        if(nrow(doc_tbl_filt) == 0){
          doc_tbl_filt <- doc_tbl_filt_prep %>%
            filter(
              str_detect(generator, as.character(building_val)) &
                gen_match == "match" & #str_detect(gen_range, gen_number) &
                # as.Date(date_val, format = "%m/%d/%Y %H:%M") == date #1100 files
                as.Date(date_val, format = "%Y-%m-%d %H:%M:%S") == date #1500 files
            ) %>%
            select(
              gen_date_key,
              reason_for_run,
              op_mode_comment,
              load_status
            )
        }
        
        new_rw_out_a <- new_rw_prep_a %>%
          left_join(
            doc_tbl_filt,
            by = "gen_date_key"
          ) %>%
          select(
            generator,
            date,
            hour_meter,
            reason_for_run,
            downstream_temp_F,
            controlled_yn,
            power_load_kW,
            op_mode_comment
          )
      } else {
        new_rw_out_a <- new_rw_prep_a %>%
          mutate(
            reason_for_run = NA_character_,
            op_mode_comment = NA_character_
          ) %>%
          select(
            generator,
            date,
            hour_meter,
            reason_for_run,
            downstream_temp_F,
            controlled_yn,
            power_load_kW,
            op_mode_comment
          )
        }
      } else { # if group size is >2, will need to handle row creation slightly differently
        
        grp_add_rw_count <- grp %>%
          mutate(row_num = row_number())
        grp_split_rws <- split(grp_add_rw_count, grp_add_rw_count$row_num)
        
        new_rw <- lapply(1:(length(grp_split_rws) - 1), function(curr_rw_num){
          
          curr_df <- grp_split_rws[[curr_rw_num]]
          next_df <- grp_split_rws[[curr_rw_num + 1]]


          # get values in place, then create a new, 1-line df
          generator_val <- str_replace(curr_df$generator_info, "genlogs ", "")
          gen_range_val <- str_replace_all(str_extract(str_replace(generator_val, "genlogs", ""), "(?i)Gen.*"), "(?i)Gen-|(?i)Gen| ", "")
          date_val <- curr_df$timestamps
          hr_meter_val <- next_df$runtime_hr_
          power_ld_val <- next_df$max_power_kw_
          downstrm_temp_val <- paste0(round(as.numeric(curr_df[, str_detect(names(curr_df), "downstream")]), 3), " to ", round(as.numeric(next_df[, str_detect(names(next_df), "downstream")]), 3))
          building_val = str_replace_all(str_replace(generator_val, "genlogs", ""), "(?i)Gen.*|-| ", "")

          # get vals in place to determine controlled status
          check_temp_val <- min(curr_df[, str_detect(names(curr_df), "downstream")], next_df[, str_detect(names(next_df), "downstream")])
          check_pwrld_val <- min(curr_df$max_power_kw_, next_df$max_power_kw_)
          
          # get vals in place to determine which reason for run value to grab
          check_load_status = case_when(
            power_ld_val == 0 ~ 0,
            TRUE ~ 1
          )
          gen_number <- str_split(generator_val, " ")[[1]][3]

          # add in logic to check for existence of SCR columns
          # if they don't exist, control_yn will be NO
          # if they do exist, run the following existing logic
          control_yn_val <- "N"
          if (sum(str_detect(names(grp), "scr")) == 0){
            control_yn_val <- "N"
          } else{
            if (!is.na(curr_df$scr_treated_run) & curr_df$scr_treated_run == 1){ # changed from next_df$scr_treated_run to curr_df$scr_treated_run
              if (next_df$scr_shutdown == 0){ #grp$scr_shutdown[2] == 0
                if (check_temp_val > 572 & check_pwrld_val > 0){
                  control_yn_val <- "Y" # controlled
                } else if (check_temp_val < 572 | check_pwrld_val == 0){
                  control_yn_val <- "N" # uncontrolled
                }
              }
            } else{
              control_yn_val <- "N"
            }
          }
          
          new_rw_prep_b <- data.frame(
            generator = as.character(generator_val),
            date = as.character(date_val),
            hour_meter = as.character(hr_meter_val),
            downstream_temp_F = as.character(downstrm_temp_val),
            controlled_yn = as.character(control_yn_val),
            power_load_kW = as.character(power_ld_val),
            # gen_date_key = as.character(str_replace_all(paste0(str_replace(generator_val, "(?i)Gen.*", "GEN"), "_", as.Date(date_val, format = "%m/%d/%Y %H:%M")), " ", "-")) # for 1100 file
            gen_date_key = as.character(str_replace_all(paste0(str_replace(generator_val, "(?i)Gen.*", "GEN"), "_",as.Date(date_val, format = "%Y-%m-%d %H:%M:%S")), " ", "-")) # for 1500 file
            # gen_date_key = as.character(str_replace_all(paste0(str_replace(generator_val, "(?i)Gen.*", "GEN"), "_",as.Date(date_val, "%Y-%m-%d %H:%M:%S")), " ", "-"))
          )

          if(df_doc_tbl_exists){
            doc_tbl_filt_prep <- df_doc_tbl %>%
              mutate(
                gen_match = case_when(
                  !is.na(as.numeric(gen_number)) & (gen_number > gen_start_rng & gen_number < gen_end_rng) ~ "match", # cases where gen # is a number value
                  gen_number == gen_start_rng ~ "match", # cases where it's a number or character and equals start range
                  TRUE ~ "no match"
                )
              )
            
            doc_tbl_filt <- doc_tbl_filt_prep %>%
              filter(
                str_detect(generator, as.character(building_val)) &
                  gen_match == "match" & #check_load_status == load_status & str_detect(gen_range, gen_number) &
                  # as.Date(date_val, format = "%m/%d/%Y %H:%M") == date #1100 files
                  as.Date(date_val, format = "%Y-%m-%d %H:%M:%S") == date #1500 files
              ) %>%
              select(
                gen_date_key,
                reason_for_run,
                op_mode_comment
              )
            
            # handle cases where load statuses don't match
            if(nrow(doc_tbl_filt) == 0){
              doc_tbl_filt <- doc_tbl_filt_prep %>%
                filter(
                  str_detect(generator, as.character(building_val)) &
                    gen_match == "match" & #str_detect(gen_range, gen_number)
                    # as.Date(date_val, format = "%m/%d/%Y %H:%M") == date #1100 files
                    as.Date(date_val, format = "%Y-%m-%d %H:%M:%S") == date #1500 files
                ) %>%
                select(
                  gen_date_key,
                  reason_for_run,
                  op_mode_comment,
                  load_status
                )
            }

            new_rw_out <- new_rw_prep_b %>%
              left_join(
                doc_tbl_filt,
                by = "gen_date_key"
              ) %>%
              select(
                generator,
                date,
                hour_meter,
                reason_for_run,
                downstream_temp_F,
                controlled_yn,
                power_load_kW,
                op_mode_comment
              )
          } else {
            new_rw_out <- new_rw_prep_b %>%
              mutate(
                reason_for_run = NA_character_,
                op_mode_comment = NA_character_
              ) %>%
              select(
                generator,
                date,
                hour_meter,
                reason_for_run,
                downstream_temp_F,
                controlled_yn,
                power_load_kW,
                op_mode_comment
              )
            }

        }) %>% # end of new_rw building lapply
          bind_rows()
        
      } # end of larger group size else statement
    
    
  }) %>%
    bind_rows()
  
}) %>% # end of full new_rws building lapply
  bind_rows() %>% # end final product of long format output table
  mutate(
    fmt_date = date#format(as.POSIXct(date, format = "%m/%d/%Y %H:%M:%S"), "%m/%d/%Y %H:%M %p")
  ) %>%
  select(
    generator,
    fmt_date,
    hour_meter,
    reason_for_run,
    downstream_temp_F,
    controlled_yn,
    power_load_kW,
    op_mode_comment
  ) %>%
  rename(
    date = fmt_date
  )

# widen table so that gens are next to eachother, bind rows based on datetime
final_gen_splt <- split(new_rws, new_rws$generator) %>%
  lapply(function(df_final){
  gen_id <- df_final$generator[1]
  colnames(df_final)[3:ncol(df_final)] <- paste0(gen_id, "_", colnames(df_final)[3:ncol(df_final)])
  df_final %>% select(-c(generator))
})

write_prep <- final_gen_splt %>%
  purrr::reduce(
    full_join,
    by = "date"
  ) %>%
  # arrange(as.POSIXct(date, format = "%m/%d/%Y %H:%M")) #1100 files
  arrange(as.POSIXct(date, format = "%Y-%m-%d %H:%M:%S")) #1500 files

write.xlsx(write_prep, file.path(fol_out, paste0(facility, "_", csv_filename, "_.xlsx")))
