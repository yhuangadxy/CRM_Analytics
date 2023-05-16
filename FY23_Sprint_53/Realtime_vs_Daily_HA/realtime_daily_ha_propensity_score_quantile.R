library(tidyverse)
library(DBI)
library(noctua)
library(dbplyr)
library(ggplot2)
library(microsynth)
library(data.table)
library(stringr)
library(lubridate)
library(gsynth)
library(cowplot)
library(MatchIt)
library(party)
library(partykit)
library(twang)
library(CBPS)
library(quantreg)
library(pbmcapply)

setwd("~/Project/home_alert_causal_analysis")

### Connect to Snowflake
conn_snowflake <- dbConnect(odbc::odbc(),
                            driver = 'SnowflakeDSIIDriver',
                            database = "USER_SANDBOX",
                            warehouse = "CRM_ANALYTICS_WH", 
                            uid = "ying-kai.huang@move.com",
                            role = "PRODUCER_CRM_ANALYTICS_ROLE",
                            server = 'oxa55065.snowflakecomputing.com',
                            authenticator = 'externalbrowser')

### Download data for control and treatment group
data_rt_daily <- tbl(conn_snowflake, in_schema("YINGKAI_HUANG", "HA_RT_DAILY_USERS_AND_METRICS_AND_DATA_SCIENCE_MODEL")) %>% collect()
## data <- distinct(data, RDC_VISITOR_ID, .keep_all = TRUE)

### Create variable to distinguish control and treatment 
### Consider realtime and daily only
data_rt_daily$TREATMENT = 1
data_rt_daily[data_rt_daily$FREQUENCY == 'Daily',c('TREATMENT')] = 0
data_rt_daily[data_rt_daily$FREQUENCY == 'RT_and_Daily',c('TREATMENT')] = 2

data_rt_daily <- data_rt_daily[data_rt_daily$TREATMENT != 2,]

### Fill 0 in null values
for (i in colnames(data_rt_daily)[!(colnames(data_rt_daily) %in% c('CUSTOMER_ID','FREQUENCY','TREATMENT','SNAPSHOT_END_DATE'))])
{
  data_rt_daily[is.na(data_rt_daily[,i]),i] = 0
}

### Create dummy variable for lead submission uu
data_rt_daily$TOTAL_LEADS <- data_rt_daily$FOR_SALE_LEADS_SUBMITTED + data_rt_daily$FOR_RENT_LEADS_SUBMITTED 
data_rt_daily$LEAD_SUBMISSION_OR_NOT = 0
data_rt_daily[data_rt_daily$TOTAL_LEADS > 0,c('LEAD_SUBMISSION_OR_NOT')] = 1

data_rt_daily$TOTAL_LEADS_AFTER <- data_rt_daily$FOR_SALE_LEADS_SUBMITTED_AFTER  + data_rt_daily$FOR_RENT_LEADS_SUBMITTED_AFTER  
data_rt_daily$LEAD_SUBMISSION_OR_NOT_AFTER  = 0
data_rt_daily[data_rt_daily$TOTAL_LEADS_AFTER  > 0,c('LEAD_SUBMISSION_OR_NOT_AFTER')] = 1

### Remove na data
data_rt_daily = data_rt_daily[is.na(data_rt_daily$CUSTOMER_ID) == 0,]


### Remove Duplicated Data
data_rt_daily = data_rt_daily[!duplicated(data_rt_daily[,c('CUSTOMER_ID')]),]


##################################################
### Do propensity score matching #################
##################################################
##################################################

### GLM, ipw
fit = glm(TREATMENT ~ OPEN_RATE_BEFORE+TOTAL_FOR_SALE_SRP  + MINUTES_PER_VIST + FR_SEARCHES + FS_SEARCHES + FR_SEARCHES + SAVED_SEARCH_CNT + FR_PHOTO_GALLERY_CLICKS + FS_PRICE_FILTER_SEARCHES + VISIT_CNT + MEDIA_CLICKS + TOTAL_LEADS + HANDRAISER_SCORE + NOT_FOR_ME_UNREGISTERED_SCORE + BROWSE_A_LOT_SCORE + BUYER_SCORE, 
          data = data_rt_daily, 
          family = 'binomial')
p_score = predict(fit, data_rt_daily, type = "response")   
p_score = ifelse(p_score<0.025,0.025,ifelse(p_score>0.85,0.85,p_score))
data_rt_daily$logit_ipw_weight  = ifelse(data_rt_daily$TREATMENT==1,1,p_score/(1-p_score))

### quick matching with gbm
m.out_gbm <- matchit(TREATMENT ~ OPEN_RATE_BEFORE+TOTAL_FOR_SALE_SRP  + MINUTES_PER_VIST + FR_SEARCHES + FS_SEARCHES + FR_SEARCHES + SAVED_SEARCH_CNT + FR_PHOTO_GALLERY_CLICKS + FS_PRICE_FILTER_SEARCHES + VISIT_CNT + MEDIA_CLICKS + TOTAL_LEADS + HANDRAISER_SCORE + NOT_FOR_ME_UNREGISTERED_SCORE + BROWSE_A_LOT_SCORE + BUYER_SCORE + TOTAL_RSP + FS_PHOTO_GALLERY_CLICKS + TOTAL_NEW_CONSTRUCTION_SRP, 
                     method = 'quick', data = data_rt_daily, estimand = "ATT", distance = 'gbm')


### quick matching
m.out <- matchit(TREATMENT ~ OPEN_RATE_BEFORE+TOTAL_FOR_SALE_SRP  + MINUTES_PER_VIST + FR_SEARCHES + FS_SEARCHES + FR_SEARCHES + SAVED_SEARCH_CNT + FR_PHOTO_GALLERY_CLICKS + FS_PRICE_FILTER_SEARCHES + VISIT_CNT + MEDIA_CLICKS + TOTAL_LEADS + HANDRAISER_SCORE + NOT_FOR_ME_UNREGISTERED_SCORE + BROWSE_A_LOT_SCORE + BUYER_SCORE + TOTAL_RSP + FS_PHOTO_GALLERY_CLICKS + TOTAL_NEW_CONSTRUCTION_SRP, 
                 method = 'quick', data = data_rt_daily, estimand = "ATT")

m.data <- match.data(m.out)

library("marginaleffects")

fit <- lm(TOTAL_CONSUMER_VALUE_AFTER ~ TREATMENT * (OPEN_RATE_BEFORE+TOTAL_FOR_SALE_SRP  + MINUTES_PER_VIST + FR_SEARCHES + FS_SEARCHES + FR_SEARCHES + SAVED_SEARCH_CNT + FR_PHOTO_GALLERY_CLICKS + 
                                                      FS_PRICE_FILTER_SEARCHES + VISIT_CNT + MEDIA_CLICKS + TOTAL_LEADS + 
                                                      HANDRAISER_SCORE + NOT_FOR_ME_UNREGISTERED_SCORE + BROWSE_A_LOT_SCORE + 
                                                      BUYER_SCORE), data = m.data, weights = weights)

avg_comparisons(fit,
                variables = "TREATMENT",
                vcov = ~subclass,
                newdata = subset(m.data, TREATMENT == 1),
                wts = "weights")

### Dealing with outliers
threshold_1 <- quantile(data_rt_daily[data_rt_daily$TREATMENT==0,]$FS_SEARCHES,0.997)
threshold_2 <- quantile(data_rt_daily[data_rt_daily$TREATMENT==0,]$FR_SEARCHES,1)
data_rt_daily_outlier_removed <- data_rt_daily[data_rt_daily$TREATMENT == 1 | (data_rt_daily$FS_SEARCHES <threshold_1 & data_rt_daily$FR_SEARCHES <threshold_2), ]


### Use new data to do the matching again
### glm with ipw
fit_no_outlier = glm(TREATMENT ~ OPEN_RATE_BEFORE+TOTAL_FOR_SALE_SRP  + MINUTES_PER_VIST + SAVED_SEARCH_CNT + SAVED_LISTING_CNT + FS_PHOTO_GALLERY_CLICKS + FR_PHOTO_GALLERY_CLICKS + FS_PRICE_FILTER_SEARCHES + MWEB_LEADS_SUBMITTED + ANDROID_LEADS_SUBMITTED + TOTAL_CONSUMER_VALUE+ LEAD_SALES_REVENUE +VISIT_CNT + MEDIA_CLICKS + TOTAL_LEADS + HANDRAISER_SCORE + NOT_FOR_ME_UNREGISTERED_SCORE + BROWSE_A_LOT_SCORE + BUYER_SCORE, 
                     data = data_rt_daily_outlier_removed, 
                     family = 'binomial')
p_score = predict(fit_no_outlier, data_rt_daily_outlier_removed, type = "response")   
p_score = ifelse(p_score<0.025,0.025,ifelse(p_score>0.83,0.83,p_score))
data_rt_daily_outlier_removed$logit_ipw_weight  = ifelse(data_rt_daily_outlier_removed$TREATMENT==1,1,p_score/(1-p_score))
fit_tcv_no_outlier <- lm(TOTAL_CONSUMER_VALUE_AFTER ~ TREATMENT,data = data_rt_daily_outlier_removed, weights = logit_ipw_weight)
### quick match
m.out_no_outlier <- matchit(TREATMENT ~ OPEN_RATE_BEFORE+TOTAL_FOR_SALE_SRP  + MINUTES_PER_VIST + SAVED_SEARCH_CNT + SAVED_LISTING_CNT + FS_PHOTO_GALLERY_CLICKS + FR_PHOTO_GALLERY_CLICKS + FS_PRICE_FILTER_SEARCHES + MWEB_LEADS_SUBMITTED + ANDROID_LEADS_SUBMITTED + TOTAL_CONSUMER_VALUE+ LEAD_SALES_REVENUE +VISIT_CNT + MEDIA_CLICKS + TOTAL_LEADS + HANDRAISER_SCORE + NOT_FOR_ME_UNREGISTERED_SCORE + BROWSE_A_LOT_SCORE + BUYER_SCORE, 
                            method = 'quick', data = data_rt_daily_outlier_removed, estimand = "ATT")

m.data_no_outlier <- match.data(m.out_no_outlier)

library("marginaleffects")

fit_quick_tcv_no_outlier <- lm(TOTAL_CONSUMER_VALUE_AFTER ~ TREATMENT * (OPEN_RATE_BEFORE+TOTAL_FOR_SALE_SRP  + 
                                                                           MINUTES_PER_VIST + FR_SEARCHES + FS_SEARCHES + FR_SEARCHES + SAVED_SEARCH_CNT + FR_PHOTO_GALLERY_CLICKS + 
                                                                           FS_PRICE_FILTER_SEARCHES + VISIT_CNT + MEDIA_CLICKS + TOTAL_LEADS + HANDRAISER_SCORE + NOT_FOR_ME_UNREGISTERED_SCORE + 
                                                                           BROWSE_A_LOT_SCORE + BUYER_SCORE + TOTAL_RSP + FS_PHOTO_GALLERY_CLICKS + TOTAL_NEW_CONSTRUCTION_SRP),
                               data = m.data_no_outlier, weights = weights)

avg_comparisons(fit_quick_tcv_no_outlier,
                variables = "TREATMENT",
                vcov = ~subclass,
                newdata = subset(m.data_no_outlier, TREATMENT == 1),
                wts = "weights")

fit_quick_tcv_no_outlier_easy <- lm(TOTAL_CONSUMER_VALUE_AFTER ~ TREATMENT,
                                    data = m.data_no_outlier, weights = weights)

avg_comparisons(fit_quick_tcv_no_outlier_easy,
                variables = "TREATMENT",
                vcov = ~subclass,
                newdata = subset(m.data_no_outlier, TREATMENT == 1),
                wts = "weights")


fit_quick_visit_cnt_no_outlier <- lm(VISIT_CNT_AFTER ~ TREATMENT * (OPEN_RATE_BEFORE+TOTAL_FOR_SALE_SRP  + 
                                                                      MINUTES_PER_VIST + FR_SEARCHES + FS_SEARCHES + FR_SEARCHES + SAVED_SEARCH_CNT + FR_PHOTO_GALLERY_CLICKS + 
                                                                      FS_PRICE_FILTER_SEARCHES + VISIT_CNT + MEDIA_CLICKS + TOTAL_LEADS + HANDRAISER_SCORE + NOT_FOR_ME_UNREGISTERED_SCORE + 
                                                                      BROWSE_A_LOT_SCORE + BUYER_SCORE + TOTAL_RSP + FS_PHOTO_GALLERY_CLICKS + TOTAL_NEW_CONSTRUCTION_SRP),
                                     data = m.data_no_outlier, weights = weights)

avg_comparisons(fit_quick_visit_cnt_no_outlier,
                variables = "TREATMENT",
                vcov = ~subclass,
                newdata = subset(m.data_no_outlier, TREATMENT == 1),
                wts = "weights")

fit_quick_min_visit_no_outlier <- lm(MINUTES_PER_VIST_AFTER ~ TREATMENT * (OPEN_RATE_BEFORE+TOTAL_FOR_SALE_SRP  + 
                                                                             MINUTES_PER_VIST + FR_SEARCHES + FS_SEARCHES + FR_SEARCHES + SAVED_SEARCH_CNT + FR_PHOTO_GALLERY_CLICKS + 
                                                                             FS_PRICE_FILTER_SEARCHES + VISIT_CNT + MEDIA_CLICKS + TOTAL_LEADS + HANDRAISER_SCORE + NOT_FOR_ME_UNREGISTERED_SCORE + 
                                                                             BROWSE_A_LOT_SCORE + BUYER_SCORE + TOTAL_RSP + FS_PHOTO_GALLERY_CLICKS + TOTAL_NEW_CONSTRUCTION_SRP),
                                     data = m.data_no_outlier, weights = weights)

avg_comparisons(fit_quick_min_visit_no_outlier,
                variables = "TREATMENT",
                vcov = ~subclass,
                newdata = subset(m.data_no_outlier, TREATMENT == 1),
                wts = "weights")

fit_quick_total_fs_srp_no_outlier <- lm(TOTAL_FOR_SALE_SRP_AFTER ~ TREATMENT * (OPEN_RATE_BEFORE+TOTAL_FOR_SALE_SRP  + 
                                                                                  MINUTES_PER_VIST + FR_SEARCHES + FS_SEARCHES + FR_SEARCHES + SAVED_SEARCH_CNT + FR_PHOTO_GALLERY_CLICKS + 
                                                                                  FS_PRICE_FILTER_SEARCHES + VISIT_CNT + MEDIA_CLICKS + TOTAL_LEADS + HANDRAISER_SCORE + NOT_FOR_ME_UNREGISTERED_SCORE + 
                                                                                  BROWSE_A_LOT_SCORE + BUYER_SCORE + TOTAL_RSP + FS_PHOTO_GALLERY_CLICKS + TOTAL_NEW_CONSTRUCTION_SRP),
                                        data = m.data_no_outlier, weights = weights)

avg_comparisons(fit_quick_total_fs_srp_no_outlier,
                variables = "TREATMENT",
                vcov = ~subclass,
                newdata = subset(m.data_no_outlier, TREATMENT == 1),
                wts = "weights")

fit_quick_total_fr_srp_no_outlier <- lm(TOTAL_RENT_SRP_AFTER ~ TREATMENT * (OPEN_RATE_BEFORE+TOTAL_FOR_SALE_SRP  + 
                                                                              MINUTES_PER_VIST + FR_SEARCHES + FS_SEARCHES + FR_SEARCHES + SAVED_SEARCH_CNT + FR_PHOTO_GALLERY_CLICKS + 
                                                                              FS_PRICE_FILTER_SEARCHES + VISIT_CNT + MEDIA_CLICKS + TOTAL_LEADS + HANDRAISER_SCORE + NOT_FOR_ME_UNREGISTERED_SCORE + 
                                                                              BROWSE_A_LOT_SCORE + BUYER_SCORE + TOTAL_RSP + FS_PHOTO_GALLERY_CLICKS + TOTAL_NEW_CONSTRUCTION_SRP),
                                        data = m.data_no_outlier, weights = weights)

avg_comparisons(fit_quick_total_fr_srp_no_outlier,
                variables = "TREATMENT",
                vcov = ~subclass,
                newdata = subset(m.data_no_outlier, TREATMENT == 1),
                wts = "weights")

fit_quick_total_lead_no_outlier <- lm(TOTAL_LEADS_AFTER ~ TREATMENT * (OPEN_RATE_BEFORE+TOTAL_FOR_SALE_SRP  + 
                                                                         MINUTES_PER_VIST + FR_SEARCHES + FS_SEARCHES + FR_SEARCHES + SAVED_SEARCH_CNT + FR_PHOTO_GALLERY_CLICKS + 
                                                                         FS_PRICE_FILTER_SEARCHES + VISIT_CNT + MEDIA_CLICKS + TOTAL_LEADS + HANDRAISER_SCORE + NOT_FOR_ME_UNREGISTERED_SCORE + 
                                                                         BROWSE_A_LOT_SCORE + BUYER_SCORE + TOTAL_RSP + FS_PHOTO_GALLERY_CLICKS + TOTAL_NEW_CONSTRUCTION_SRP),
                                      data = m.data_no_outlier, weights = weights)

avg_comparisons(fit_quick_total_lead_no_outlier,
                variables = "TREATMENT",
                vcov = ~subclass,
                newdata = subset(m.data_no_outlier, TREATMENT == 1),
                wts = "weights")

fit_quick_saved_listing_no_outlier <- lm(SAVED_LISTING_CNT_AFTER ~ TREATMENT * (OPEN_RATE_BEFORE+TOTAL_FOR_SALE_SRP  + 
                                                                                  MINUTES_PER_VIST + FR_SEARCHES + FS_SEARCHES + FR_SEARCHES + SAVED_SEARCH_CNT + FR_PHOTO_GALLERY_CLICKS + 
                                                                                  FS_PRICE_FILTER_SEARCHES + VISIT_CNT + MEDIA_CLICKS + TOTAL_LEADS + HANDRAISER_SCORE + NOT_FOR_ME_UNREGISTERED_SCORE + 
                                                                                  BROWSE_A_LOT_SCORE + BUYER_SCORE + TOTAL_RSP + FS_PHOTO_GALLERY_CLICKS + TOTAL_NEW_CONSTRUCTION_SRP),
                                         data = m.data_no_outlier, weights = weights)

avg_comparisons(fit_quick_saved_listing_no_outlier,
                variables = "TREATMENT",
                vcov = ~subclass,
                newdata = subset(m.data_no_outlier, TREATMENT == 1),
                wts = "weights")

fit_quick_saved_search_no_outlier <- lm(SAVED_SEARCH_CNT_AFTER ~ TREATMENT * (OPEN_RATE_BEFORE+TOTAL_FOR_SALE_SRP  + 
                                                                                MINUTES_PER_VIST + FR_SEARCHES + FS_SEARCHES + FR_SEARCHES + SAVED_SEARCH_CNT + FR_PHOTO_GALLERY_CLICKS + 
                                                                                FS_PRICE_FILTER_SEARCHES + VISIT_CNT + MEDIA_CLICKS + TOTAL_LEADS + HANDRAISER_SCORE + NOT_FOR_ME_UNREGISTERED_SCORE + 
                                                                                BROWSE_A_LOT_SCORE + BUYER_SCORE + TOTAL_RSP + FS_PHOTO_GALLERY_CLICKS + TOTAL_NEW_CONSTRUCTION_SRP),
                                        data = m.data_no_outlier, weights = weights)

avg_comparisons(fit_quick_saved_search_no_outlier,
                variables = "TREATMENT",
                vcov = ~subclass,
                newdata = subset(m.data_no_outlier, TREATMENT == 1),
                wts = "weights")

fit_quick_fs_search_no_outlier <- lm(FS_SEARCHES_AFTER ~ TREATMENT * (OPEN_RATE_BEFORE+TOTAL_FOR_SALE_SRP  + 
                                                                        MINUTES_PER_VIST + FR_SEARCHES + FS_SEARCHES + FR_SEARCHES + SAVED_SEARCH_CNT + FR_PHOTO_GALLERY_CLICKS + 
                                                                        FS_PRICE_FILTER_SEARCHES + VISIT_CNT + MEDIA_CLICKS + TOTAL_LEADS + HANDRAISER_SCORE + NOT_FOR_ME_UNREGISTERED_SCORE + 
                                                                        BROWSE_A_LOT_SCORE + BUYER_SCORE + TOTAL_RSP + FS_PHOTO_GALLERY_CLICKS + TOTAL_NEW_CONSTRUCTION_SRP),
                                     data = m.data_no_outlier, weights = weights)

avg_comparisons(fit_quick_fs_search_no_outlier,
                variables = "TREATMENT",
                vcov = ~subclass,
                newdata = subset(m.data_no_outlier, TREATMENT == 1),
                wts = "weights")

fit_quick_fr_search_no_outlier <- lm(FR_SEARCHES_AFTER ~ TREATMENT * (OPEN_RATE_BEFORE+TOTAL_FOR_SALE_SRP  + 
                                                                        MINUTES_PER_VIST + FR_SEARCHES + FS_SEARCHES + FR_SEARCHES + SAVED_SEARCH_CNT + FR_PHOTO_GALLERY_CLICKS + 
                                                                        FS_PRICE_FILTER_SEARCHES + VISIT_CNT + MEDIA_CLICKS + TOTAL_LEADS + HANDRAISER_SCORE + NOT_FOR_ME_UNREGISTERED_SCORE + 
                                                                        BROWSE_A_LOT_SCORE + BUYER_SCORE + TOTAL_RSP + FS_PHOTO_GALLERY_CLICKS + TOTAL_NEW_CONSTRUCTION_SRP),
                                     data = m.data_no_outlier, weights = weights)

avg_comparisons(fit_quick_fr_search_no_outlier,
                variables = "TREATMENT",
                vcov = ~subclass,
                newdata = subset(m.data_no_outlier, TREATMENT == 1),
                wts = "weights")

### Try quantile regression
m.data_no_outlier_tcv <- m.data_no_outlier[c('TOTAL_CONSUMER_VALUE_AFTER','TREATMENT','weights')]
m.data_no_outlier_tcv$TOTAL_CONSUMER_VALUE_AFTER<-round(m.data_no_outlier_tcv$TOTAL_CONSUMER_VALUE_AFTER,3)
m.data_no_outlier_tcv_agg <- m.data_no_outlier_tcv %>% group_by(TOTAL_CONSUMER_VALUE_AFTER, TREATMENT) %>% 
  summarise(quantile_weights=sum(weights))

quantile_est<-function(start_date, q)
{
  weekly_data_table <- paste0("sc-bq-gcs-billingonly.shiny_tool_dump.",project,
                              "_g2s_latency_start_type_same_os_diff_app_version_all_did_random_sample_subsample_transformed_",start_date,"_",q,
                              sep="")
  
  pull_sql <- paste("
  SELECT *
  FROM
  `",weekly_data_table,"`
                  ",sep="")
  
  res_1 = bq_project_query('sc-dig', pull_sql, use_legacy_sql=FALSE)
  res_1 = bq_table_download(res_1)
  res_1 = as.data.frame(res_1)
  rqreg_1 <- suppressWarnings(rq(transformed_latency~ factor(week)*factor(app_version_change), data = res_1, tau =c(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9), weights = res_1$quantile_weight))
  ans_sub <- as.data.frame(rqreg_1$coefficients)
  
  
  
  weekly_data_table <- paste0("sc-bq-gcs-billingonly.shiny_tool_dump.",project,
                              "_g2s_latency_start_type_same_os_diff_app_version_all_did_random_sample_subsample_resample_transformed_",start_date,"_",q,
                              sep="")
  
  pull_sql <- paste("
  SELECT *
  FROM
  `",weekly_data_table,"`
                  ",sep="")
  
  res_2 = bq_project_query('sc-dig', pull_sql, use_legacy_sql=FALSE)
  res_2 = bq_table_download(res_2)
  res_2 = as.data.frame(res_2)
  rqreg_2 <- suppressWarnings(rq(transformed_latency~ factor(week)*factor(app_version_change), data = res_2, tau =c(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9), weights = res_2$quantile_weight))
  ans_re <- as.data.frame(rqreg_2$coefficients)
  
  return(ans_re[,]-ans_sub[,])
}

while(TRUE){
  options(warn=2)
  job <- try(pbmclapply(c(1:100),quantile_est, start_date = start_date_1, mc.cores=10,mc.preschedule = FALSE), silent=TRUE)
  if(!is(job, 'try-error')) break
  options(warn=1)
}




ptm <- proc.time()
proc.time() - ptm

### Use random forest to do the propensity score estimation
ps_formula_rf <- as.formula('TREATMENT ~ .')
set.seed(2014)
mycontrols <- cforest_unbiased(ntree = 10,mtry = 8)
fit_random_forest = cforest(ps_formula_rf, data =  data_rt_daily[c(6:55,71,73:80,81,82)], control=mycontrols)
p_score_rf = predict(fit_random_forest, type = "prob")   
p_score_rf = ifelse(p_score<0.025,0.025,ifelse(p_score>0.85,0.85,p_score))
data_rt_daily$logit_ipw_weight  = ifelse(data_rt_daily$TREATMENT==1,1,p_score_rf/(1-p_score_rf))

### Use GBM to do the propensity score estimation
data_gpm <- as.data.frame(data_rt_daily[c(6:55,71,73:80,81,82)])
myGPM <- ps(ps_formula_rf, data = data_gpm, n.trees = 100, interaction.depth =4,
            shrinkage = 0.01, stop.method = c("es.max"),estimand ="ATT",verbose =TRUE)
##p_score_gpm <- myGPM$ps
data_rt_daily$gpm_ipw_weight <- unlist(myGPM$ps)
data_rt_daily$gpm_ipw_weight  = ifelse(data_rt_daily$TREATMENT==1,1,data_rt_daily$gpm_ipw_weight/(1-data_rt_daily$gpm_ipw_weight))

### Balance Check

result_unmatched = c()
for (i in colnames(data_rt_daily)[c(3:55,71,73:79,81,82)]) {
  control = weighted.mean(unlist(data_rt_daily[data_rt_daily$TREATMENT == 0 ,i]))
  treatment = weighted.mean(unlist(data_rt_daily[data_rt_daily$TREATMENT == 1 ,i]))
  if(is.null(result_unmatched)){
    result_unmatched = c(i, control, treatment)
  }
  else {
    result_unmatched = rbind(result_unmatched, c(i, control, treatment))
  }
}

result = c()
for (i in colnames(data_rt_daily)[c(3:55,71,73:79,81,82)]) {
  control = weighted.mean(unlist(data_rt_daily[data_rt_daily$TREATMENT == 0 ,i]),unlist(data_rt_daily[data_rt_daily$TREATMENT == 0,'logit_ipw_weight']))
  treatment = weighted.mean(unlist(data_rt_daily[data_rt_daily$TREATMENT == 1 ,i]),unlist(data_rt_daily[data_rt_daily$TREATMENT == 1,'logit_ipw_weight']))
  if(is.null(result)){
    result = c(i, control, treatment)
  }
  else {
    result = rbind(result, c(i, control, treatment))
  }
}

result_quick_match = c()
for (i in colnames(m.data)[c(3:55,71,73:79,81,82)]) {
  control = weighted.mean(unlist(m.data[m.data$TREATMENT == 0 ,i]),unlist(m.data[m.data$TREATMENT == 0,'weights']))
  treatment = weighted.mean(unlist(m.data[m.data$TREATMENT == 1 ,i]),unlist(m.data[m.data$TREATMENT == 1,'weights']))
  if(is.null(result_quick_match)){
    result_quick_match = c(i, control, treatment)
  }
  else {
    result_quick_match = rbind(result_quick_match, c(i, control, treatment))
  }
}

result_no_outlier = c()
for (i in colnames(data_rt_daily_outlier_removed)[c(3:55,71,73:79,81,82)]) {
  control = weighted.mean(unlist(data_rt_daily_outlier_removed[data_rt_daily_outlier_removed$TREATMENT == 0 ,i]),unlist(data_rt_daily_outlier_removed[data_rt_daily_outlier_removed$TREATMENT == 0,'logit_ipw_weight']))
  treatment = weighted.mean(unlist(data_rt_daily_outlier_removed[data_rt_daily_outlier_removed$TREATMENT == 1 ,i]),unlist(data_rt_daily_outlier_removed[data_rt_daily_outlier_removed$TREATMENT == 1,'logit_ipw_weight']))
  if(is.null(result_no_outlier)){
    result_no_outlier = c(i, control, treatment)
  }
  else {
    result_no_outlier = rbind(result_no_outlier, c(i, control, treatment))
  }
}

result_quick_match_no_outlier = c()
for (i in colnames(m.data_no_outlier)[c(3:55,71,73:79,81,82)]) {
  control = weighted.mean(unlist(m.data_no_outlier[m.data_no_outlier$TREATMENT == 0 ,i]),unlist(m.data_no_outlier[m.data_no_outlier$TREATMENT == 0,'weights']))
  treatment = weighted.mean(unlist(m.data_no_outlier[m.data_no_outlier$TREATMENT == 1 ,i]),unlist(m.data_no_outlier[m.data_no_outlier$TREATMENT == 1,'weights']))
  if(is.null(result_qui ck_match_no_outlier)){
    result_quick_match_no_outlier = c(i, control, treatment)
  }
  else {
    result_quick_match_no_outlier = rbind(result_quick_match_no_outlier, c(i, control, treatment))
  }
}

result_gpm = c()
for (i in colnames(data_rt_daily)[c(3:55,71,73:79,81,82)]) {
  control = weighted.mean(unlist(data_rt_daily[data_rt_daily$TREATMENT == 0 ,i]),unlist(data_rt_daily[data_rt_daily$TREATMENT == 0,'gpm_ipw_weight']))
  treatment = weighted.mean(unlist(data_rt_daily[data_rt_daily$TREATMENT == 1 ,i]),unlist(data_rt_daily[data_rt_daily$TREATMENT == 1,'gpm_ipw_weight']))
  if(is.null(result_gpm)){
    result_gpm = c(i, control, treatment)
  }
  else {
    result_gpm = rbind(result_gpm, c(i, control, treatment))
  }
}
### Check treatment effect
fit_visit_14DAY <- lm(VISIT_COUNT_14DAY_AFTER~TREATMENT, data = data_rt_daily_outlier_removed, weights = data_rt_daily_outlier_removed$logit_ipw_weight)
fit_fs_search <- lm(FS_SEARCHES_AFTER~TREATMENT+FS_SEARCHES+FR_SEARCHES, data = data_rt_daily_outlier_removed, weights = data_rt_daily_outlier_removed$logit_ipw_weight)
fit_fr_search <- lm(FR_SEARCHES_AFTER~TREATMENT+FS_SEARCHES+FR_SEARCHES, data = data_rt_daily_outlier_removed, weights = data_rt_daily_outlier_removed$logit_ipw_weight)
fit_lead_submission <- lm(LEAD_SUBMISSION_OR_NOT_AFTER~TREATMENT, data = data_rt_daily_outlier_removed, weights = data_rt_daily_outlier_removed$logit_ipw_weight)
fit_consumer_value <- lm(TOTAL_CONSUMER_VALUE_AFTER~TREATMENT, data = data_rt_daily_outlier_removed, weights = data_rt_daily_outlier_removed$logit_ipw_weight)

