#install.packages("bigrquery")
#install.packages("curl")
#install.packages("jsonlite")
#install.packages("Rcpp")
#install.packages("readr")
#install.packages("dplyr")
#install.packages("bigQueryR")
#devtools::install_github("r-dbi/bigrquery")
#install.packages("lubridate")
setwd("C:/Users/jcooke/Documents/projects/learningCenter")
rm(list=ls())
library(ggplot2)
library(bigrquery)
library(dplyr)
library(class)
library(lubridate)

#updatedMonth <- as.Date('2019-03-01')
#lastDayUpdatedMonth <- updatedMonth %m+% months(1) - 1
#previousMonth <- updatedMonth %m-% months(1)

#STEP 1: update summary_month_onboarding with new month data joining lc data to other data

project_id <- "nu-skin-corp"
sql_string <- "SELECT 
                  first.*
, ifnull(mth_orders,0) mth_ord_cnt
, ifnull(mth_pv,0) mth_pv_amt
, ifnull(mth_spon_all_cnt,0) mth_spon_all_cnt
, ifnull(mth_spon_ba_cnt,0) mth_spon_ba_cnt
, 0 cntrl_group_flg
, 0  mth2_pv_ret_flg
, cast(0.00 as float64) mth2_pv_amt
, cast(0.00 as float64) lftv_amt
, cast(0 as int64) lfspon_cnt
, cast(0 as int64) submt_loi_flg_cnt
FROM
(SELECT
kfd.comm_month_dt
, kfd.dist_id
, rg.cntry_cd
, rg.cntry_desc
, rg.region
, kfd.mth_pv_act_flg
, new_signup_flg
, ttl_rank
, case when ttl_rank = 6 then 'BA'
when ttl_rank in (7,8) then 'QBR/BR'
when ttl_rank between 9 and 14 then 'BP+'
else 'Unknown'
end ttl_cat
, case when ob.sap_id is not null then 1 else 0 end lc_flg
, kfd.submt_loi_flg mth_loi_flg
, case when badge_id = 18320 then 1 else 0 end graduated_flg
, case when tasks_completed >= 7 then 1 else 0 end Tasks7_flg
, case when tasks_completed >= 1 then 1 else 0 end Tasks1_flg
, ob.*


FROM (select comm_month_dt, dist_id, tov_amt, new_signup_flg, ttl_cd, submt_loi_flg, dist_cntry_cd, mth_pv_act_flg
from `nu-skin-corp.EDW.KPIR_FLAG_DTL`
where ttl_cd is not null --remove any retail accounts
and ttl_cd <> 55 -- remove PFC accounts
and comm_month_dt =  DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL -1 MONTH)
and dist_cntry_cd in (1,2,6,13)
and new_signup_flg = 1) kfd --US/CAN/AS/NZ only
JOIN `nu-skin-corp.EDW.TTL` ttl ON kfd.ttl_cd = ttl.ttl_cd
JOIN `nu-skin-corp.EDW.COMM_PER` cp ON kfd.comm_month_dt = cp.strt_dt
LEFT JOIN (SELECT rw_id, sap_id, acct_create_dt, last_login_date, tasks_started, tasks_completed, badges_received
FROM `nu-skin-corp.ONBOARDING.SUMMARY`
where file_date = DATE_ADD(CURRENT_DATE(), INTERVAL -EXTRACT(DAY FROM CURRENT_DATE()) DAY)) ob ON kfd.dist_id = ob.sap_id 
LEFT JOIN `nu-skin-corp.EDW.CNTRY_REGION` rg ON kfd.dist_cntry_cd = rg.cntry_cd
LEFT JOIN (select ba.rw_id, ba.badge_id, badge_desc.badge_title
from `nu-skin-corp.ONBOARDING.BADGES_ACHIEVED` ba 
JOIN `nu-skin-corp.ONBOARDING.BADGES` badge_desc on ba.badge_id = badge_desc.badge_id
where ba.badge_id = 18320 and achieved_dt BETWEEN DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL -1 MONTH) AND DATE_ADD(CURRENT_DATE(), INTERVAL -EXTRACT(DAY FROM CURRENT_DATE()) DAY)) grad 
ON cast(ob.rw_id as string) = cast(grad.rw_id as string)) as first

LEFT JOIN (select dist_id, count(ord_id) mth_orders, round(sum(ord_pv),0) mth_pv
from
(SELECT ord_id, buyer_id dist_id, sum(lin_itm_pv_amt * lin_itm_qty) as ord_pv
FROM `nu-skin-corp.EDW.ORDER_LIN_ITM_KITS`
WHERE comm_month_dt = DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL -1 MONTH)
AND on_order_flg = 1
group by dist_id, ord_id)
group by dist_id) as second ON first.dist_id = second.dist_id

LEFT JOIN (SELECT kfd.comm_month_dt spon_mth_dt, spon_dist_id dist_id, sum(new_signup_flg) mth_spon_all_cnt, sum(case when ttl.ttl_rank between 6 and 14 then 1 else 0 end) mth_spon_ba_cnt
FROM `nu-skin-corp.EDW.DIST_CUST` dc
JOIN (select comm_month_dt, dist_id, ttl_cd, new_signup_flg
from `nu-skin-corp.EDW.KPIR_FLAG_DTL`
where new_signup_flg = 1
and mth_pv_act_flg = 1
and comm_month_dt = DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL -1 MONTH)) kfd on dc.dist_cust_id = kfd.dist_id 
LEFT JOIN `nu-skin-corp.EDW.TTL` ttl ON kfd.ttl_cd = ttl.ttl_cd
group BY spon_dist_id, kfd.comm_month_dt) as spon ON first.dist_id = spon.dist_id AND first.comm_month_dt = spon.spon_mth_dt
"
query_results <- query_exec(sql_string, project = project_id, use_legacy_sql = FALSE)

#done bringing data into R.  Next step is to preprocess data for knn

dat <- query_results

#STEP 4: next step is to organize data for KNN and run KNN

train_dat <- subset(dat, lc_flg == 1)
test_dat <- subset(dat, lc_flg == 0)

train_dat <- train_dat[c('mth_pv_amt', 'lc_flg', 'lc_flg')]
head(train_dat)
test_dat <- test_dat[c('mth_pv_amt', 'lc_flg', 'lc_flg')]
head(train_dat)

train_labels <- train_dat[c('lc_flg')]
test_labels <- test_dat[c('lc_flg')]

pairup <- function(list1, list2){
  keep = 1:nrow(list2)
  used = c()
  for(i in 1:nrow(list1)){
    nearest = FNN::get.knnx(list2, list1[i,,drop=FALSE], 1)$nn.index[1,1]        
    used = c(used, keep[nearest])
    keep = keep[-nearest]
    list2 = list2[-nearest,,drop=FALSE]
  }
  used
}

dat_pred2 <- as.data.frame(pairup(train_dat, test_dat))
data.frame(table(dat_pred2))

t1 <-  dat_pred2 

t2 <- cbind(train_dat, t1)

names(t2) [4] <- "1" 

dat_list <- as.list(t1[,1])

lc <- subset(dat, lc_flg == 1)
noLc <- subset(dat, lc_flg == 0)

noLc$cntrl_group_flg = 0
noLc$index <- seq.int(nrow(noLc))
noLc$cntrl_group_flg[noLc$index %in% dat_list] <- 1
lc$cntrl_group_flg = 0
lc$index <- seq.int(nrow(lc))

datEnd <- rbind(noLc, lc)  
datEnd$index <- NULL
datEnd$cntrl_group_flg <- as.integer(datEnd$cntrl_group_flg)

#STEP 5: upload data from knn into bigquery test table
#change mth2_pv_amt to numeric

datEnd$mth2_pv_amt <- as.numeric(datEnd$mth2_pv_amt)
datEnd$lftv_amt <- as.numeric(datEnd$lftv_amt)

library(bigQueryR)
bqr_auth(token = NULL, new_user = FALSE, no_auto = FALSE)

bqr_upload_data(projectId = "nu-skin-corp", 
                datasetId = "REPORTING",
                tableId   = "SUMMARY_MONTH_ONBOARDING",
                upload_data = datEnd,
                overwrite = FALSE)

#below I'm just organizing some daters

t2$index <- seq.int(nrow(t2))

#columns needed are index, 1, mth_pv_amt.y

output <- as.data.frame(merge(t2, noLc, by.x = '1', by.y = 'index') [, c(5,1,29)])
output <- output[order(output$index),] 
output <- output[c(3)]
t2 <- cbind(t2, output)

t2$var <- round(apply(t2, 1, function(x) x[1] - x[6]), 2)
summary(t2$var)

project_id <- "nu-skin-corp"

sql_string <- "UPDATE `nu-skin-corp.REPORTING.SUMMARY_MONTH_ONBOARDING` t1
    SET mth2_pv_amt = (SELECT cast(round(tov_amt,0) as int64)
    FROM `nu-skin-corp.EDW.KPIR_FLAG_DTL` kfd
    WHERE kfd.dist_id = t1.dist_id 
        and kfd.comm_month_dt = date_add(kfd.comm_month_dt, interval -1 month))
WHERE t1.comm_month_dt = DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL -2 MONTH)"


query_results_tov <- query_exec(sql_string, project = project_id, use_legacy_sql = FALSE)

sql_string <- "UPDATE `nu-skin-corp.REPORTING.SUMMARY_MONTH_ONBOARDING`
SET mth2_pv_ret_flg = 1 
WHERE mth2_pv_amt > 0 AND comm_month_dt = DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL -2 MONTH)" 


query_results_tov <- query_exec(sql_string, project = project_id, use_legacy_sql = FALSE)

# Build Lifetime Value

sql_string <- "update `nu-skin-corp.REPORTING.SUMMARY_MONTH_ONBOARDING` smo
                  set smo.lftv_amt = (SELECT ltv.lftv_amt
                    FROM `nu-skin-corp.ONBOARDING.SUMMARY_NEW_6MO_LTDAT` ltv
                    where smo.dist_id = ltv.dist_id
                    )
                    , smo.lfspon_cnt = (SELECT ltv.lfspon_cnt
                    FROM `nu-skin-corp.ONBOARDING.SUMMARY_NEW_6MO_LTDAT` ltv
                    where smo.dist_id = ltv.dist_id
                    )
                    , smo.submt_loi_flg_cnt = (SELECT ltv.submt_loi_flg_cnt
                    FROM `nu-skin-corp.ONBOARDING.SUMMARY_NEW_6MO_LTDAT` ltv
                    where smo.dist_id = ltv.dist_id
                    )
                    where smo.dist_id IN (SELECT dist_id FROM `nu-skin-corp.ONBOARDING.SUMMARY_NEW_6MO_LTDAT`)"

query_results_lftv_amt <- query_exec(sql_string, project = project_id, use_legacy_sql = FALSE)


#datEnd$f1f2 <- interaction(datEnd$lc_flg, datEnd$cntrl_group_flg)

#boxplot <- ggplot(datEnd, aes(x = f1f2, y = mth_pv_amt)) +
#  geom_boxplot(outlier.colour='red') +
#  coord_cartesian(ylim = c(0, 1000)) +
#  stat_summary(fun.y=mean, geom="point", shape=8, size=4)

#boxplot <- ggplot(datEnd, aes(x = f1f2, y = mth_spon_all_cnt)) +
 # geom_boxplot(outlier.colour='red') +
  # coord_cartesian(ylim = c(0, 1000)) +
  #stat_summary(fun.y=mean, geom="point", shape=8, size=4)


#boxplot
