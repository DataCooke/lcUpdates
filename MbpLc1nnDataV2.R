#############################################################################################################################################
# STEP ZERO: PACKAGES & DIRECTORY #
# You may have to install the below packages if you don't already have them. I would just go ahead and run lines 3 through 11 (remove #'s)

#install.packages("bigrquery")
#install.packages("curl")
#install.packages("jsonlite")
#install.packages("Rcpp")
#install.packages("readr")
#install.packages("dplyr")
#install.packages("bigQueryR")
#devtools::install_github("r-dbi/bigrquery")
#install.packages("lubridate")
library(ggplot2)
library(bigrquery)
library(dplyr)
library(class)
library(lubridate)

# you will also have to change your working directory.  Create a folder for this to run in and use that as your working directory
setwd("C:/Users/mmphan/OneDrive - Nu Skin/Project Files/2019/Digital Products/Learning Center")
#############################################################################################################################################


#############################################################################################################################################
# STEP ONE: Get Data #
  # The function to get data is in get_LC_data.R
  # !IMPORTANT!: You need to format the arugment EXACTLY as follows: "'YYYY-MM-DD'" 
source('get_LC_data.R') #if this files isn't in the directory you set above, you will need to add the file path here
query_results <- get_lc_data(strt_dt = "'2019-06-01'", end_dt = "'2019-06-30'") 
  #write.table(query_results, file = 'june_LC_data.csv', row.names = FALSE, sep = ',') # write to a file so I don't need to re-run the query
#############################################################################################################################################


#############################################################################################################################################
# STEP TWO: CLEAN DATA #
  # 2a: Defining Participants 
tasks <- 8 # used for more strict participant definition where they needed to include at least 8 tasks
LC_dat <- subset(query_results, tasks_completed >= tasks) ## Learning Center Participants
#LC_dat <- subset(query_results, lc_flg == 1) # based on whether they ever logged into the learning center
nonLC_dat <- subset(query_results, lc_flg == 0) ## Non-Learning Center AND NO TASKS

#does LOI flag matter?
query_results %>% group_by(lc_flg) %>% summarize(mean_size=mean(mth_loi_flg))
query_results %>% group_by(lc_flg) %>% summarize(mean_size=mean(mth_pv_amt))
#############################################################################################################################################


#############################################################################################################################################
# STEP THREE: PAIRING #
  # The function called 'pairup.onek' is in the get_LC_data.R file
    # The function matches every LC person with a non-LC person (not every non-LC person is matched). A non-LC can matched only once
matched_ind <- pairup_one_crit(LC_dat[c('mth_pv_amt', 'lc_flg')], nonLC_dat[c('mth_pv_amt', 'lc_flg')]) # Returns indices of non-LC people who are matched
names(matched_ind) <- 'nonLC_matched_ind' #rename column name for convenience

# How well does the pairing do?
matched_pv <- cbind(LC_dat[,c(23)],nonLC_dat[matched_ind,c(23)]) #make a data set of LC PV and paired nonLC PV
summary(round(apply(matched_pv, 1, function(x) x[1] - x[2]), 2)) 
#############################################################################################################################################


#############################################################################################################################################
# STEP FOUR: AGGREGATE INTO NEW DATA SET #
  #### ONLY NEED THIS GROUP IF USING TASKS TO DETERMINE PARTICIPANTS! ###
midLC <- subset(query_results, tasks_completed < tasks) 
  # If you use the more narrow definition of participants, you will exclude people who enrolled in LC, but aren't considered
  # a participants. This will affect total signup count which we are reporting in the dashboard

# Need to set control group flag for those non-LC users who were matched
nonLC_dat$cntrl_group_flg <- 0 # setting control group flag to zero and then we'll overwrite the zeroes for those who were matched
nonLC_dat$cntrl_group_flg[seq.int(nrow(nonLC_dat)) %in% as.list(matched_ind[,1])] <- 1 # only set the control group flag if we matched them (not everyone in the nonLC group will be matched)

data_final <- rbind(nonLC_dat, LC_dat, midLC)
#############################################################################################################################################


#############################################################################################################################################
# STEP FIVE: UPLOAD TO BIGQUERY #
#Prep data for upload
data_final$mth2_pv_amt <- as.numeric(data_final$mth2_pv_amt) #will be zero until we set values
data_final$lftv_amt <- as.numeric(data_final$lftv_amt)
data_final$cntrl_group_flg <- as.integer(data_final$cntrl_group_flg)
data_final$lc_flg <- as.integer(data_final$lc_flg)

  ##### This will append data #######
library(bigQueryR)
bqr_auth(token = NULL, new_user = FALSE, no_auto = FALSE)
# Commented out so it isn't accidentally run 
#bqr_upload_data(projectId = "nu-skin-corp", datasetId = "REPORTING",tableId   = "SUMMARY_MONTH_ONBOARDING_8TASKS",
#                upload_data = data_final,overwrite = FALSE)

#############################################################################################################################################


#############################################################################################################################################
# STEP SIX: 2ND MONTH PV, 2ND MONTH ACTIVE, LTV VALUE #
  ###!!!THE FUNCTIONS ARE FOR 8 TASKS TABLE!!!!!###
  # This will overwrite the empty columns from above #

#add next month's PV
  # The arguments are for comm_month_dt, so it always needs to be the first day ofa month
  # If you're only updating one month, the start and end date will be the same
  # If you want to update May and June, the start date is "'2019-05-01'" and end date is "'2019-06-01'"
#update_2ndmonth_pv(strt_date = "'2019-06-01'", end_date = "'2019-06-01'")


#Set Active flag (based on results from above query; i.e. if you don't run the above function first, it won't update active flag)
#update_2nd_month_active(strt_date = "'2019-06-01'", end_date = "'2019-06-01'")

# Build Lifetime Value
#update_LTV()


#############################################################################################################################################

