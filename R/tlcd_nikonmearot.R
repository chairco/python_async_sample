################################################################################
# Fucntion: TLCD_Nikon_MEA_ROT
# Description: Shift and rotate the measurement data
# Arguments:
#   mea_raw_dat: data.frame. A data frame of measurement data in Operation '1360' and 'DA60'.
#   verbose: logical. If TRUE the function will state procedure details.
# Output:
#   rot_by_prodt: list. A list contains the number of MEA ROT glasses of each product.
################################################################################
# if want should warnings and setting warn = 0 or options(warn = oldw)
oldw <- getOption("warn")
options(warn = -1)

library(dplyr)
library(reshape2)
library(logging)

# load necessary function
source("env.R")
source("ora_db.R")
source("pg_db.R")
source("basic_fun.R")

# setting logging
logReset()
basicConfig(level='FINEST')

tlcd_nikonrotmea_flow <- function(update_starttime, update_endtime, verbose = TRUE) {
    rawdata <- get_mearawdata(update_starttime, update_endtime)
    if (nrow(rawdata) == 0) {
        logwarn("No data to rotate")
        return (NULL)
    }
    if (verbose) {
        loginfo("Check if there exists design values")
    }
    mea_datacleaned <- clean_data(rawdata)
    
    product_list <- get_productlist(mea_datacleaned)
    prod_withdv <- get_prodwithdv()
    prod_no_dv <- product_list[!(product_list %in% prod_withdv)]

    mea_datacleaned <- check_designvalue(mea_datacleaned, prod_withdv, product_list, prod_no_dv)
    if (nrow(mea_datacleaned) == 0) {
        logwarn(sprintf("No design values for ROT"))
        return (NULL)
    }
    product_list <- unique(mea_datacleaned$product)

    rot_starttime <- Sys.time()
    loginfo(sprintf("Start %s", rot_starttime))

    # ROT BY Product
    if (verbose) {
        loginfo(sprintf("%s products: %s",
            length(product_list), 
            paste(product_list, collapse = ", ")))
    }

    rot_by_prodt <- lapply(product_list, function(prodt){
        # Reading Design value with "tlcd_nikon_mea_dv_ct"
        mea_dv <- get_designvalue(prodt, "tlcd_nikon_mea_dv_ct")
        if (verbose) {
            loginfo("Check the positions of MEA design values")
        }
        if (nrow(coord_checking(mea_dv)) != 0) {
            mea_dv <- coord_checking(mea_dv)
        } else{
            logwarn(sprintf("product: %s, #Distinct X * #Distinct Y != #Rows", prodt))
            rot_error_record <- paste(sprintf("(%s, -3, '''#Distinct X * #Distinct Y != #Rows'' in Product %s')", 
                lapply(seq(nrow(unique(mea_datacleaned[, 1:4]))), function(num) {
                    paste("'", mea_datacleaned[num, 1:4], "'", sep = "", collapse = ", ")
                }), prodt), 
            collapse = ", ")
            loginfo(insert_error(rot_error_record))
            retunr (NULL)
        }
        # Check how many the glasses are in the data
        uni_comb <- unique(mea_datacleaned[, c("tstamp", "glassid")])
        glass_count <- get_glasscount(uni_comb, mea_datacleaned, mea_dv, prodt)
    })
    names(rot_by_prodt) <- product_list
    rot_endtime <- Sys.time()
    rot_end_time <- Sys.time()
    loginfo(sprintf("END: %s", rot_endtime))
    loginfo(sprintf("Elapsed_time: %s", rot_endtime - rot_starttime))
    return (rot_by_prodt)
}


clean_data <- function(rawdata) {
    loginfo('Clean data')
    mea_datacleaned <- rawdata %>%
        arrange(GLASS_START_TIME, SITE_NAME) %>%
        mutate(tstamp = strftime(GLASS_START_TIME, format = "%Y-%m-%d %H:%M:%S"),
            product = sprintf("TL%s", substring(PARAM_COLLECTION, 5, nchar(PARAM_COLLECTION))),
            site_name = as.numeric(SITE_NAME)) %>%
        filter(site_name <= 48) %>%
        select(tstamp, glassid = GLASS_ID, operation = STEP_ID, product, site_name,
            param_name = PARAM_NAME, param_value = PARAM_VALUE) %>%
        reshape2::dcast(tstamp + glassid + operation + product + site_name ~ param_name, 
            value.var = "param_value", fill = NA_real_)
    return (mea_datacleaned)
}


get_productlist <- function(mea_datacleaned) {
    return (unique(mea_datacleaned$product))
}


check_designvalue <- function(mea_datacleaned, prod_withdv, product_list, prod_no_dv) {
    loginfo('Check designvalue')
    if (length(prod_no_dv) > 0) {
        lapply(product_list[!(product_list %in% prod_withdv)], function(no_dv) {
            dat_no_dv <- mea_datacleaned %>% 
                filter(product == no_dv) %>% 
                select(tstamp, glassid, operation, product) %>% 
                unique() 
            rot_error_record <- paste(sprintf("(%s, -2, 'No Design Values in Product %s')", 
                lapply(seq(nrow(dat_no_dv)), function(num) {
                    paste("'", dat_no_dv[num, 1:4], "'", sep = "", collapse = ", ")
                }), no_dv), collapse = ", ")
        
            if (DEBUG == FALSE) {
                loginfo('Insert rot data.')
                ret <- loginfo(insert_error(rot_error_record))
                loginfo(sprintf('Return: %s', ret))
            } else{
                loginfo('DEBUG MODE, not insert DATA')
            }    
        })
    }
    mea_datacleaned <- mea_datacleaned %>% filter(!(product %in% prod_no_dv))
    return (mea_datacleaned)
}


get_glasscount <- function(uni_comb, mea_datacleaned, mea_dv, prodt) {
    loginfo('Get glasscount')
    glass_count <- 0
    for (comb in seq(nrow(uni_comb))) {
        loginfo(comb)
        # Reformat the data
        mea_sub_by_gid <- get_gidsub(mea_datacleaned, comb, uni_comb)
        # Give the new labels
        mea_sub_by_gid_new <- mea_label_new_id(mea_sub_by_gid)

        if (nrow(mea_sub_by_gid_new) == 0) {
            logwarn(sprintf("glassid: %s, Raw data has missing values", uni_comb$glassid[comb]))
            rot_error_record <- sprintf("(%s, -1, 'Missing Values')", 
                paste("'", unique(mea_sub_by_gid[, 1:4]), "'", 
                    sep = "", collapse = ", "))
            
            if (DEBUG == FALSE) {
                loginfo('Insert rot data.')
                ret <- loginfo(insert_error(rot_error_record))
                loginfo(sprintf('Return: %s', ret))
            } else{
                loginfo('DEBUG MODE, not insert DATA')
            }
            return (data.frame())
        }
        mea_sub_by_gid_new <- get_tpmea(mea_sub_by_gid_new, mea_dv)
        # Long table to wide table
        Diff_X <- reshape2::dcast(mea_sub_by_gid_new[, c("tstamp", "glassid", "operation", "product", "item_id", "Diff_X")],
                                tstamp + glassid + operation + product ~ item_id,
                                value.var = "Diff_X")
        Diff_Y <- reshape2::dcast(mea_sub_by_gid_new[, c("tstamp", "glassid", "operation", "product", "item_id", "Diff_Y")],
                                tstamp + glassid + operation + product ~ item_id,
                                value.var = "Diff_Y")
        # rot
        loginfo(sprintf("Opt_product: %s glassid: %s", prodt, Diff_X$glassid))
        tryCatch({
            Diff_X <- na.omit(Diff_X)
            Diff_Y <- na.omit(Diff_Y)
            
            opt_output <- optim(c(0, 0, 0), 
                min_res_squared, 
                gr = NULL, 
                mat_x = Diff_X[, -1:-4], 
                mat_y = Diff_Y[, -1:-4], 
                mat_dx = mea_dv$x, 
                mat_dy = mea_dv$y, 
                method = "L-BFGS-B")

            rot_log_ht_record <- sprintf("(%s, 1)", paste("'", Diff_X[, 1:4], "'", sep = "", collapse = ", "))

            mea_rs_x <- Diff_X[, -1:-4] + opt_output$par[1] - mea_dv$y * tan(opt_output$par[3] * 0.000001)
            colnames(mea_rs_x) <- sprintf("X_%s", seq(ncol(mea_rs_x)))
            mea_rs_y <- Diff_Y[, -1:-4] + opt_output$par[2] + mea_dv$x * tan(opt_output$par[3] * 0.000001)
            colnames(mea_rs_y) <- sprintf("Y_%s", seq(ncol(mea_rs_y)))
            mea_rs_xy <- cbind(glassid = Diff_X$glassid, mea_rs_x, mea_rs_y)
            
            mea_rs_xy_long <-
              reshape2::melt(mea_rs_xy, id.vars = c("glassid"),
                             variable.name = "item_name", value.name = "rot_rs") %>%
              mutate_at(c("glassid", "item_name"), funs(as.character)) %>%
              select(item_name, rot_rs)

              rot_data_record <- paste(sprintf("(%s, (SELECT rot_id FROM insert_rot_ht))",
                lapply(seq(nrow(mea_rs_xy_long)), function(num) {
                    paste("'", mea_rs_xy_long[num, c("item_name", "rot_rs")], "'", sep = "", collapse = ", ")
                })), collapse = ", ")
              
            if (DEBUG == FALSE) {
                loginfo('Insert rot data.')
                ret <- loginfo(insert_error(rot_error_record))
                loginfo(sprintf('Return: %s', ret))
            } else{
                loginfo('DEBUG MODE, not insert DATA')
            }

        }, error = function(e) {
            logerror(sprintf("product: %s glassid: %s Error: %s", prodt, Diff_X$glassid[comb], e))
            rot_error_record <- sprintf("(%s, -4, 'ROT Error: %s')",
                paste("'", Diff_X[, 1:4], "'", sep = "", collapse = ", "), e)
        
            if (DEBUG == FALSE) {
                loginfo('Insert rot data.')
                ret <- loginfo(insert_error(rot_error_record))
                loginfo(sprintf('Return: %s', ret))
            } else{
                loginfo('DEBUG MODE, not insert DATA')
            }
        }, finally = {
            glass_count <- glass_count + 1
        })
    }
    return (glass_count)
}


get_gidsub <- function(mea_datacleaned, comb, uni_comb) {
    loginfo("Get sub gid")
    mea_sub_by_gid <- mea_datacleaned %>%
        filter(glassid == uni_comb$glassid[comb] & tstamp == uni_comb$tstamp[comb]) %>%
        select(tstamp, glassid, operation, product, site_name, x = TP_X, y = TP_Y)
    return (mea_sub_by_gid)
}


get_tpmea <- function(mea_sub_by_gid_new, mea_dv) {
    loginfo("Get tpma")
    mea_sub_by_gid_new <- mea_sub_by_gid_new %>%
        select(tstamp, glassid, operation, product, item_id, TP_X = x, TP_Y = y) %>%
        full_join(mea_dv, by = "item_id") %>%
        group_by(item_id) %>%
        mutate(Diff_X = TP_X - x, Diff_Y = TP_Y - y) %>%
        select(tstamp, glassid, operation, product, item_id, Diff_X, Diff_Y) %>%
        as.data.frame()
    return (mea_sub_by_gid_new)
}


main <- function(update_starttime, update_endtime) {
    tryCatch({
        #stop("demo error")
        loginfo(sprintf("start_time: %s, end_time: %s", update_starttime, update_endtime))
        tlcd_nikonrotmea_flow(update_starttime=update_starttime, update_endtime=update_endtime)
    }, error = function(e) {
        # 這就會是 demo error
        conditionMessage(e)
    }, finally = {
        loginfo('Disable dbconnect')
        ora_disconnectdb()
    })
}