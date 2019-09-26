# This function access BigQuery, queries it, and returns the data for the Learning center
  # You need to add the starting and end date for the data. This is designed to run one month at a time!
get_lc_data <- function(strt_dt,end_dt){
  project_id <- "nu-skin-corp"
  sql_string <- paste("SELECT 
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
                      and comm_month_dt =",  strt_dt,"
                      and dist_cntry_cd in (1,2,6,13)
                      and new_signup_flg = 1) kfd --US/CAN/AS/NZ only
                      JOIN `nu-skin-corp.EDW.TTL` ttl ON kfd.ttl_cd = ttl.ttl_cd
                      JOIN `nu-skin-corp.EDW.COMM_PER` cp ON kfd.comm_month_dt = cp.strt_dt
                      LEFT JOIN (SELECT rw_id, sap_id, acct_create_dt, last_login_date, tasks_started, tasks_completed, badges_received
                      FROM `nu-skin-corp.ONBOARDING.SUMMARY`
                      where file_date =",end_dt,") ob ON kfd.dist_id = ob.sap_id 
                      LEFT JOIN `nu-skin-corp.EDW.CNTRY_REGION` rg ON kfd.dist_cntry_cd = rg.cntry_cd
                      LEFT JOIN (select ba.rw_id, ba.badge_id, badge_desc.badge_title
                      from `nu-skin-corp.ONBOARDING.BADGES_ACHIEVED` ba 
                      JOIN `nu-skin-corp.ONBOARDING.BADGES` badge_desc on ba.badge_id = badge_desc.badge_id
                      where ba.badge_id = 18320 and achieved_dt BETWEEN", strt_dt  ,"AND", end_dt,") grad 
                      ON cast(ob.rw_id as string) = cast(grad.rw_id as string)) as first
                      LEFT JOIN (select dist_id, count(ord_id) mth_orders, round(sum(ord_pv),0) mth_pv
                      from
                      (SELECT ord_id, buyer_id dist_id, sum(lin_itm_pv_amt * lin_itm_qty) as ord_pv
                      FROM `nu-skin-corp.EDW.ORDER_LIN_ITM_KITS`
                      WHERE comm_month_dt =", strt_dt, "
                      AND on_order_flg = 1
                      group by dist_id, ord_id)
                      group by dist_id) as second ON first.dist_id = second.dist_id
                      LEFT JOIN (SELECT kfd.comm_month_dt spon_mth_dt, spon_dist_id dist_id, sum(new_signup_flg) mth_spon_all_cnt, sum(case when ttl.ttl_rank between 6 and 14 then 1 else 0 end) mth_spon_ba_cnt
                      FROM `nu-skin-corp.EDW.DIST_CUST` dc
                      JOIN (select comm_month_dt, dist_id, ttl_cd, new_signup_flg
                      from `nu-skin-corp.EDW.KPIR_FLAG_DTL`
                      where new_signup_flg = 1
                      and mth_pv_act_flg = 1
                      and comm_month_dt =",strt_dt,") kfd on dc.dist_cust_id = kfd.dist_id 
                      LEFT JOIN `nu-skin-corp.EDW.TTL` ttl ON kfd.ttl_cd = ttl.ttl_cd
                      group BY spon_dist_id, kfd.comm_month_dt) as spon ON first.dist_id = spon.dist_id AND first.comm_month_dt = spon.spon_mth_dt
                      ")
  query_results <- query_exec(sql_string, project = project_id, use_legacy_sql = FALSE)
  return(query_results)
  
}

##############################################
# Pair up function for 1 matching 
##############################################
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


# 2nd month PV function 
update_2ndmonth_pv <- function(strt_dt,end_dt){
  sql_string <- paste("UPDATE `nu-skin-corp.REPORTING.SUMMARY_MONTH_ONBOARDING_3KnnMatch` t1
                      SET mth2_pv_amt = (SELECT cast(round(tov_amt,0) as int64)
                      FROM `nu-skin-corp.EDW.KPIR_FLAG_DTL` kfd
                      WHERE kfd.dist_id = t1.dist_id 
                      and kfd.comm_month_dt = date_add(t1.comm_month_dt, interval 1 month))
                      WHERE t1.comm_month_dt between", strt_dt, "and",end_dt)
  
    query_results_tov <- query_exec(sql_string, project = project_id, use_legacy_sql = FALSE)
}

# end month active flag function 
update_2nd_month_active <- function(strt_dt,end_dt){
  sql_string <- paste("UPDATE `nu-skin-corp.REPORTING.SUMMARY_MONTH_ONBOARDING_3KnnMatch`
                      SET mth2_pv_ret_flg = 1 
                      WHERE mth2_pv_amt > 0 AND comm_month_dt between", strt_dt, "and",  end_dt)
  
  query_results_tov <- query_exec(sql_string, project = project_id, use_legacy_sql = FALSE)
}

# LTV update
update_LTV <- function(){
  sql_string <- "update `nu-skin-corp.REPORTING.SUMMARY_MONTH_ONBOARDING_3KnnMatch` smo
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
}  