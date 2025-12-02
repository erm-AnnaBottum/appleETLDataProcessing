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
  tidyverse, 
  data.table,
  lubridate
)
options(scipen = 999)
source("source/function.R")

# user input ####
# csv_filename <- "2100 Gen 1-5 Run Log (last month)" # user input
gen_run_log_filename <- "Generator Run log for October 2025" # generator run log word doc table
fpump_run_log_filename <- "Fire Pump Run log for October 2025" # fire pump run log word doc table
facility <- "Mesa"
month_folder <- "Oct-2025"

# read in data ####
fol_main <- here::here()
fol_data <- file.path(fol_main, "raw_data", month_folder)
fol_out <- file.path(fol_main, "output")
list_files <- list.files(fol_data, ".csv")

# set up run logs ####
# the contents of these two logs will be used as a spine
# to process the rest of the data against. Any records in these run logs
# that don't find corresponding data in the generator logs will be flagged.

# generator run log
# read in word doc table, containing reason for run info
# do a bit of cleanup in order to be able to join it to other datasets
# originally df_run_log
df_gen_run_log <- read_excel(file.path(fol_data, paste0(gen_run_log_filename, ".xlsx")))

# fire pump run log
df_fpump_run_log <- read_excel(file.path(fol_data, paste0(fpump_run_log_filename, ".xlsx"))) %>%
  mutate(
    generator = "Fire Pump"
  ) %>%
  select(
    generator,
    date,
    hour_meter_b
  ) %>%
  rename(
    fire_pump_hr_meter = hour_meter_b
  )

# combine the two tables into one run log reference table
df_run_log <- df_gen_run_log %>%
  bind_rows(df_fpump_run_log) %>%
  mutate(
    gen_date_key = str_replace_all(paste0(str_replace(str_replace_all(generator, "-|_| ", ""), "(?i)Gen.*", "GEN"), "_", as.Date(date)), " ", "-"),
    gen_range_val = str_replace(str_replace_all(str_extract(generator, "(?i)Gen.*"), "(?i)Gen-|(?i)Gen| ", ""), "-|&", ":"),
    gen_start_rng = str_replace_all(gen_range_val, ":.*", ""),
    gen_end_rng = str_replace_all(gen_range_val, ".*:", ""),
    building = str_replace_all(generator, "(?i)Gen.*|-", ""),
    load_status = case_when(
      str_detect(generator, "(?i)MGen|M-Gen|M Gen") ~ 0,
      str_detect(rfr, "(?i)no load") ~ 0,
      TRUE ~ 1
    ),
    reason_for_run = case_when(
      str_detect(rfr, "(?i)Block") ~ "Maint. & Testing",
      str_detect(rfr, "(?i)Load bank") ~ "Maint. & Testing",
      str_detect(rfr, "(?i)Switchgear") ~ "Other",
      str_detect(rfr, "(?i)Transformer") ~ "Other",
      str_detect(rfr, "UPS") ~ "Other",
      str_detect(rfr, "(?i)Quarterly PM|Q") ~ "Maint. & Testing",
      str_detect(rfr, "(?i)3 YR PM") ~ "Maint. & Testing",
      str_detect(rfr, "(?i)Test") ~ "Maint. & Testing",
      str_detect(rfr, "(?i)Verification") ~ "Maint. & Testing",
      str_detect(rfr, "(?i)Failure|(?i)Utility") ~ "Emergency",
      str_detect(rfr, "(?i)PM Maintenance") ~ "Maint. & Testing",
      TRUE ~ rfr
    ),
    record_use_column = NA_character_
  ) %>%
  rename(
    op_mode_comment = rfr
  )




if(nrow(df_run_log) > 0){
  df_run_log_exists <- TRUE
} else{
  df_run_log_exists <- FALSE
}

# generator logs ####
# loop through all logs in the folder, process each and finally export together
gen_logs_list <- list.files(fol_data, pattern = "*.csv", full.names = TRUE)

combined_gen_logs <- lapply(gen_logs_list, function(input_file){
  df_gen_log_in <- read.csv(input_file)
  
  if (nrow(df_gen_log_in) <  1){
    print("No data to process in this file! Moving on to the next one.")
    return(NULL)
  } else {
    # prep data
    # split df_gen_log_in by na rows, since each section of rows needs to be
    # loaded as its own sub dataset
    
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
    # rw_diffs <- diff(rw_idxs)
    rw_idxs_change_indx <- data.frame(grp = rw_idxs) %>% group_by(grp) %>% mutate(grp_id = cur_group_id()) %>% ungroup() %>% select(grp_id)
    rw_idxs_vec <- rw_idxs_change_indx[["grp_id"]] # turn back into vector to complete process
    rw_diffs <- diff(rw_idxs_vec) # rw_diffs <- diff(rw_idxs)
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
      
      gen_info <- get_gen_id(colnames(df[2]))
      colname_extr <- str_replace_all(gen_info, " ", ".")
      colnames(df) <- str_replace_all(str_replace_all(str_replace_all(str_replace(colnames(df), colname_extr, ""), "[0-9]", ""), "\\.{2,}", ""), "[.]", "_")
      colnames(df) <- str_replace_all(colnames(df), "(?i)SCR_SCR", "SCR")
      df_out <- df %>%
        dplyr::filter(rowSums(is.na(select(., -1))) < (ncol(.) - 1)) %>%
        mutate(
          update_point = (Engine_Running == 1 & dplyr::lag(Engine_Running, default = 0) == 0),
          group_num = cumsum(update_point),
          generator_info = gen_info
        ) %>%
        group_by(group_num) %>%
        mutate(
          group_size = n()
        )
    }) %>%
      bind_rows() %>%
      mutate(
        #treating dates as characters, remove seconds
        date_cleaned = str_extract(timestamps, "[^:]*:[^:]*") #[^:] matches anything other than :
      )
    
    # now that we've got a 'cleaned' dataset, split by generator, then split by group
    # number to create each individual new row
    colnames(rw_dfs_prep) <- tolower(colnames(rw_dfs_prep))
    
    # update 3 cases of building names, so that they will match the rfr doc table for joining
    rw_dfs_prep <- rw_dfs_prep %>%
      mutate(
        generator_info = str_replace(generator_info, "\\b419\\b", "M"),
        generator_info = str_replace(generator_info, "\\b426\\b", "X"),
        generator_info = str_replace(generator_info, "\\b500\\b", "M") # \\b is a word boundary, to ensure only these standalone numbers get replaced (ie. 500 vs 4500)
      )
    
    # iterate over each individual generator ID ####
    gen_splt_lst <- split(rw_dfs_prep, rw_dfs_prep$generator_info)
    count <- 0
    new_rws <- gen_splt_lst %>% lapply(function(gen_df){
    #HEREREERERE  
      count <- count + 1
      print(count)
      # before moving on to next step, clean out unnecessary records:
      # where engine running = 0
      gen_df_rw_count <- cbind(row_number = seq_len(nrow(gen_df)), gen_df) # add row count for checks down the line
      df_len <- nrow(gen_df_rw_count)
      
      gen_df_clean_er <- gen_df_rw_count %>%
        mutate(
          er_group_id = rleid(engine_running) # generate group ID for each consecutive set of identical values
        ) %>%
        group_by(er_group_id) %>%
        mutate(
          er_status_count = case_when(
            engine_running == 0 ~ row_number(),
            TRUE ~ 0
          )
        ) %>%
        ungroup() %>%
        dplyr::filter(
          er_status_count != row_number(), # for cases of 0's starting the dataset (0 must follow a 1 to be used)
          er_status_count < 2, # only take the first zero when there are multiple
          engine_running <= 1, # if any aberrant reading values in this column, omit those records
          !(engine_running == 1 & row_number == nrow(gen_df_rw_count)) # if an engine startup record is the last record, omit
        ) %>%
        select(-c(er_group_id, er_status_count))
      
      # iterate over each record group (usually of size 2) ####
      grp_splt_lst <- split(gen_df_clean_er, gen_df_clean_er$group_num) # a list of all row-group dataframes for a generator
      
      new_rws_df <- grp_splt_lst %>% lapply(function(grp){
        grp_size <- grp$group_size[1]
        dstemp_colname <- colnames(grp[, str_detect(names(grp), "downstream")])
        
        if (grp_size == 2){
          # get values in place, then create a new, 1-line df
          generator_val <- str_replace(grp$generator_info[1], "genlogs ", "")
          gen_range_val <- str_replace_all(str_extract(str_replace(generator_val, "genlogs", ""), "(?i)Gen.*"), "(?i)Gen-|(?i)Gen| ", "")
          date_val <- grp$date_cleaned[1] #grp$timestamps[1]
          hr_meter_val <- grp$runtime_hr_[2]
          power_ld_val <- grp$max_power_kw_[2]
          
          building_val = str_replace_all(str_replace(generator_val, "genlogs", ""), "(?i)Gen.*|-| ", "")
          
          # get vals in place to determine controlled status, account for files without downstream temp info
          if (sum(str_detect(names(grp), "downstream")) > 0){
            check_temp_val <- min(grp[, str_detect(names(grp), "downstream")])
            
            # get downstream temp min & max to concat min-max
            dst_vector <- c(round(as.numeric(pull(grp[1, str_detect(names(grp), "downstream")])), 3),
                            round(as.numeric(pull(grp[2, str_detect(names(grp), "downstream")])), 3))
            downstrm_temp_val <- paste0(
              min(dst_vector),
              " to ",
              max(dst_vector)
            )
            
          } else {
            check_temp_val <- NA_real_
            downstrm_temp_val <- NA_character_
          }
          
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
          
          # if (sum(str_detect(names(grp), "scr")) == 0 | is.na(check_temp_val)){ 
          #   control_yn_val <- "N"
          # } else if (grp$engine_running[1] == 1 & grp$scr_treated_run[1] == 1 & grp$scr_shutdown[1] == 1){ # new logic added here 10/2
          #   control_yn_val <- "N"
          # } else{
          #   if (!is.na(grp$scr_treated_run[1]) & grp$scr_treated_run[1] == 1){ # changed from grp$scr_treated_run[2] to grp$scr_treated_run[1]
          #     if (grp$scr_shutdown[2] == 0){
          #       if (as.numeric(check_temp_val) > 572 & check_pwrld_val != 0){
          #         control_yn_val <- "Y" # controlled
          #       } else if (as.numeric(check_temp_val) < 572 | check_pwrld_val == 0){
          #         control_yn_val <- "N" # uncontrolled
          #       }
          #     }
          #   } else{
          #     control_yn_val <- "N"
          #   }
          # }
          
          # new control status check workflow 11/7
          # for any groups of 2 rows, it will never be a controlled run
          control_yn_val <- "N"
          
          # new logic to add:
          # if engine_running == 1 & scr_treated_run == 1 & scr_shutdown == 1: control_yn_val <- "N"
          
          new_rw_prep_a <- data.frame(
            generator = as.character(generator_val),
            date = as.character(date_val),
            hour_meter = as.character(hr_meter_val),
            downstream_temp_F = as.character(downstrm_temp_val),
            controlled_yn = as.character(control_yn_val),
            power_load_kW = as.character(power_ld_val),
            gen_date_key = as.character(str_replace_all(paste0(str_replace(str_replace_all(generator_val, "-|_| ", ""), "(?i)Gen.*", "GEN"), "_", as.Date(date_val, tryFormats = c("%m/%d/%Y %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"))), " ", "-"))
          )
          
          if(df_run_log_exists){
            doc_tbl_filt_prep <- df_run_log %>%
              mutate(
                gen_match = case_when(
                  !is.na(as.numeric(gen_number)) & (as.numeric(gen_number) >= as.numeric(gen_start_rng) & as.numeric(gen_number) <= as.numeric(gen_end_rng)) ~ "match", # cases where gen # is a number value
                  gen_number == gen_start_rng ~ "match", # cases where it's a number or character and equals start range
                  TRUE ~ "no match"
                )
              )
            # before cleaving off extra columns, keep a copy of this to compare against run log df and track which records are used
            doc_tbl_filt_allcol <- doc_tbl_filt_prep %>%
              dplyr::filter(
                (str_detect(generator, as.character(building_val)) &
                   # check_load_status == load_status & # commenting out 11/7/25
                   gen_match == "match" &
                   as.Date(date_val, tryFormats = c("%m/%d/%Y %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M")) == date) |
                  (str_detect(generator, as.character(building_val)) &
                     str_detect(op_mode_comment, "(?i)Test for report generated") & # if this comment exists, load status doesn't matter
                     gen_match == "match" &
                     as.Date(date_val, tryFormats = c("%m/%d/%Y %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M")) == date)
              )

            # compare against df_run_log and update the record use column so that
            # we can track which records of run log get used throughout processing steps
            df_run_log <- df_run_log %>%
              bind_rows(doc_tbl_filt_allcol) %>%
              group_by(across(-c(gen_match))) %>%
              mutate(record_use_column = case_when(
                n() > 1 ~ "matched & used",
                TRUE ~ record_use_column)
              ) %>%
              distinct(across(-c(gen_match)))

            doc_tbl_filt <- doc_tbl_filt_allcol %>%
              select(
                gen_date_key,
                reason_for_run,
                op_mode_comment,
                load_status
              )

            # # handle cases where load statuses don't match
            if(nrow(doc_tbl_filt) == 0){
              doc_tbl_filt <- doc_tbl_filt_prep %>%
                dplyr::filter(
                  str_detect(generator, as.character(building_val)) &
                    gen_match == "match" &
                    as.Date(date_val, tryFormats = c("%m/%d/%Y %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M")) == date
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
            date_val <- curr_df$date_cleaned
            hr_meter_val <- next_df$runtime_hr_
            power_ld_val <- next_df$max_power_kw_
            building_val = str_replace_all(str_replace(generator_val, "genlogs", ""), "(?i)Gen.*|-| ", "")
            check_pwrld_val <- min(curr_df$max_power_kw_, next_df$max_power_kw_) # this isn't written to df, but used to check controlled status
            
            # get vals in place to determine controlled status, account for files without downstream temp info
            if (sum(str_detect(names(curr_df), "downstream")) > 0){
              check_temp_val <- min(curr_df[, str_detect(names(curr_df), "downstream")], next_df[, str_detect(names(next_df), "downstream")])
              
              # get downstream temp min & max to concat min-max
              dst_vector <- c(round(as.numeric(curr_df[, str_detect(names(curr_df), "downstream")]), 3),
                              round(as.numeric(next_df[, str_detect(names(next_df), "downstream")]), 3))
              downstrm_temp_val <- paste0(
                min(dst_vector),
                " to ",
                max(dst_vector)
              )
            } else {
              check_temp_val <- NA_real_
              downstrm_temp_val <- NA_character_
            }
            
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
            if (sum(str_detect(names(grp), "scr")) == 0 | is.na(check_temp_val)){ # if no downstream temp info, assume uncontrolled
              control_yn_val <- "N"
            } else if (curr_df$engine_running == 1 & curr_df$scr_treated_run == 1 & curr_df$scr_shutdown == 1 |
                       curr_df$engine_running == 1 & curr_df$scr_treated_run == 1 & next_df$scr_shutdown == 1){
              control_yn_val <- "N"
            } else{
              if (!is.na(curr_df$scr_treated_run) & curr_df$scr_treated_run == 1){ # changed from next_df$scr_treated_run to curr_df$scr_treated_run
                if (next_df$scr_shutdown == 0){
                  if (check_temp_val >= 572 & check_pwrld_val > 0){
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
              gen_date_key = as.character(str_replace_all(paste0(str_replace(str_replace_all(generator_val, "-|_| ", ""), "(?i)Gen.*", "GEN"), "_", as.Date(date_val, tryFormats = c("%m/%d/%Y %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"))), " ", "-")) # for 1100 file
            )
            
            if(df_run_log_exists){
              doc_tbl_filt_prep <- df_run_log %>%
                mutate(
                  gen_match = case_when(
                    # !is.na(as.numeric(gen_number)) & (gen_number > gen_start_rng & gen_number < gen_end_rng) ~ "match", 
                    !is.na(as.numeric(gen_number)) & (as.numeric(gen_number) >= as.numeric(gen_start_rng) & as.numeric(gen_number) <= as.numeric(gen_end_rng)) ~ "match", # cases where gen # is a number value
                    gen_number == gen_start_rng ~ "match", # cases where it's a number or character and equals start range
                    TRUE ~ "no match"
                  )
                )
              
              # before cleaving off extra columns, keep a copy of this to compare against run log df and track which records are used
              doc_tbl_filt_allcol <- doc_tbl_filt_prep %>%
                dplyr::filter(
                  (str_detect(generator, as.character(building_val)) &
                     # check_load_status == load_status &
                     gen_match == "match" &
                     as.Date(date_val, tryFormats = c("%m/%d/%Y %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M")) == date) |
                    (str_detect(generator, as.character(building_val)) &
                       str_detect(op_mode_comment, "(?i)Test for report generated") & # if this comment exists, load status doesn't matter
                       gen_match == "match" &
                       as.Date(date_val, tryFormats = c("%m/%d/%Y %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M")) == date)
                )
              
              # compare against df_run_log and update the record use column so that
              # we can track which records of run log get used throughout processing steps
              df_run_log <- df_run_log %>%
                bind_rows(doc_tbl_filt_allcol) %>%
                group_by(across(-c(gen_match))) %>%
                mutate(record_use_column = case_when(
                  n() > 1 ~ "matched & used",
                  TRUE ~ record_use_column)
                ) %>%
                distinct(across(-c(gen_match)))
              
              doc_tbl_filt <- doc_tbl_filt_allcol %>%
                select(
                  gen_date_key,
                  reason_for_run,
                  op_mode_comment,
                  load_status
                )
              
              # # updated to this 9/30:
              # doc_tbl_filt <- doc_tbl_filt_prep %>%
              #   dplyr::filter(
              #     (str_detect(generator, as.character(building_val)) &
              #        check_load_status == load_status &
              #        gen_match == "match" &
              #        as.Date(date_val, tryFormats = c("%m/%d/%Y %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M")) == date) |
              #       (str_detect(generator, as.character(building_val)) &
              #          str_detect(op_mode_comment, "(?i)Test for report generated") & # if this comment exists, load status doesn't matter
              #          gen_match == "match" &
              #          as.Date(date_val, tryFormats = c("%m/%d/%Y %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M")) == date)
              #   ) %>%
              #   select(
              #     gen_date_key,
              #     reason_for_run,
              #     op_mode_comment,
              #     load_status
              #   )
              
              # handle cases where load statuses don't match
              if(nrow(doc_tbl_filt) == 0){
                doc_tbl_filt <- doc_tbl_filt_prep %>%
                  dplyr::filter(
                    str_detect(generator, as.character(building_val)) &
                      gen_match == "match" &
                      as.Date(date_val, tryFormats = c("%m/%d/%Y %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M")) == date
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
        # fmt_date = format(as.POSIXct(date, tryFormats = c("%m/%d/%Y %H:%M:%S", "%m/%d/%Y %I:%M", "%y-%m-%d %H:%M", "%y-%m-%d %I:%M")), "%m/%d/%Y %H:%M %p")
        fmt_date = format(as.POSIXct(date, tryFormats = c("%m/%d/%Y %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M")), "%m/%d/%Y %H:%M %p"),
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
      ) %>%
      # added next group_by and mutate to get rid of duplicate rows where there are multiple comments. concat comments & return singel row
      group_by(
        generator,
        date
      ) %>%
      mutate(
        op_mode_comment = paste0(op_mode_comment, collapse = "; ")
      ) %>%
      ungroup() %>%
      distinct() # at some point go back and determine why some datasets are getting duplicate records in outputs.
    
    # widen table so that gens are next to each other, bind rows based on datetime
    # final_gen_splt <- split(new_rws, new_rws$generator) %>%
    #   lapply(function(df_final){
    #     gen_id <- df_final$generator[1]
    #     colnames(df_final)[3:ncol(df_final)] <- paste0(gen_id, "_", colnames(df_final)[3:ncol(df_final)])
    #     df_final %>% select(-c(generator))
    #   }) %>% bind_rows()
    
    # handle duplicate comments
  }
  # return(list(final_gen_splt))
  # return(list(final_gen_splt, df_run_log))
}) %>% bind_rows() # end of combined gen logs (main) loop

# set up fire pump dataset, to bind in
fire_pump_log <- df_fpump_run_log %>%
  rename(
    hour_meter = fire_pump_hr_meter
  ) %>%
  mutate(
    date = format(as.POSIXct(date, tryFormats = c("%m/%d/%Y %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M")), "%m/%d/%Y %H:%M %p"),
    reason_for_run = "Maint. & Testing",
    power_load_kW = "0",
    hour_meter = as.character(hour_meter)
  )

full_dataset_logs <- combined_gen_logs %>%
  bind_rows(fire_pump_log) %>%
  mutate(
    generator = str_replace(generator, " Gen", "-Gen")
  )

# next create all the fields that exist in run log, to join them up and determine which run log records have NOT been used
processed_qc_cols <- full_dataset_logs %>%
  mutate(
    gen_date_key = str_replace_all(paste0(str_replace(str_replace_all(generator, "-|_| ", ""), "(?i)Gen.*", "GEN"), "_", format(as.POSIXct(date, tryFormats = c("%m/%d/%Y %I:%M %p", "%m/%d/%Y %H:%M %p", "%m/%d/%Y")), "%Y-%m-%d")), " ", "-"),
    gen_range_val = str_replace(str_replace_all(str_extract(generator, "(?i)Gen.*"), "(?i)Gen-|(?i)Gen| ", ""), "-|&", ":"),
    gen_start_rng = str_replace_all(gen_range_val, ":.*", ""),
    gen_end_rng = str_replace_all(gen_range_val, ".*:", ""),
    building = str_replace_all(generator, "(?i)Gen.*|-", ""),
    load_status = case_when(
      power_load_kW == 0 ~ 0,
      TRUE ~ 1
    )
  )

# get list of rfrs that were not used
df_unused_rfrs <- df_run_log %>%
  left_join(
    processed_qc_cols,
    by = c("gen_date_key",
           # "gen_start_rng",
           # "gen_end_rng",
           "load_status"),
    keep = FALSE
  ) %>%
  filter(
    is.na(generator.y),
    generator.x != "Fire Pump"
  ) %>%
  select(
    generator.x,
    date.x,
    op_mode_comment.x,
    load_status,
    gen_date_key,
    reason_for_run.x
  )
names(df_unused_rfrs) <- str_replace_all(names(df_unused_rfrs), ".x", "")
unused_rfr_dates <- df_unused_rfrs %>%
  select(date) %>%
  mutate(date = as.character(date)) %>%
  distinct()

df_no_rfr <- combined_gen_logs %>%
  filter(
    is.na(reason_for_run)
  )


# grab all unique dates from full dataset, then join each individual set back together based on date
all_gen_dates <- full_dataset_logs %>%
  # bind_rows() %>%
  select(date) %>%
  distinct()

# out <- bind_cols(c(all_gen_dates, combined_gen_logs))

# widen table so that gens are next to each other, bind rows based on datetime
final_gen_splt <- split(full_dataset_logs, full_dataset_logs$generator) %>%
  lapply(function(df_final){
    print(df_final)
    gen_id <- df_final$generator[1]
    colnames(df_final)[3:ncol(df_final)] <- paste0(gen_id, "_", colnames(df_final)[3:ncol(df_final)])
    df_final %>% select(-c(generator)) %>% distinct()
  })

# THIS WORKS
for (i in 1:length(final_gen_splt)) {
  print(i)
  df <- final_gen_splt[[i]]
  all_gen_dates <- all_gen_dates %>% left_join(df, by = "date")
  print(dim(all_gen_dates))
}



write_prep <- all_gen_dates %>%
  # pull unused rfr dates in for team to QC easier
  bind_rows(
    unused_rfr_dates
  ) %>%
  # set date in 24-hr time first, for proper ordering
  mutate(
    write_date = format.Date(parse_date_time(date, orders = c("%m/%d/%Y %I:%M", "%m/%d/%Y %H:%M", "%Y-%m-%d %I:%M", "%Y-%m-%d %H:%M", "%Y-%m-%d")), "%m/%d/%Y %H:%M %p")
  ) %>%
  select(
    write_date,
    everything()
  ) %>%
  # need to un-tibble the date field
  group_by(
    date
  ) %>%
  ungroup() %>%
  select(
    -c(date)
  ) %>%
  rename(
    date = write_date
  ) %>%
  arrange(date) %>%
  mutate(
    date = as.character(format.Date(parse_date_time(date, orders = c("%m/%d/%Y %I:%M", "%m/%d/%Y %H:%M", "%Y-%m-%d %I:%M", "%Y-%m-%d %H:%M", "%Y-%m-%d")), "%m/%d/%Y %I:%M %p"))
  )


source("source/write_processed_raw_data_to_template.R")


