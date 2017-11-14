########################################################################################
# Funciton: tlcd_rot implement TLCD_NIKON_ROT.R 
# Description: Shift and rotate the Nikon TP data
# Arguments:
#   raw_data: data.frame. A data frame of Nikon rawdata in Operation '2300', and 'D300'.
#   verbose; logical, if TRUE the function will state procedure details
# Output:
#   rot_by_prodt: list. A list contains the number of ROT glasses of each product.
########################################################################################
# if want should warnings and setting warn = 0 or options(warn = oldw)
oldw <- getOption("warn")
options(warn = -1)

library(dplyr)
library(logging)

# set relative path
PATH <- file.path(getwd(),'R')
setwd(PATH)

# load nessecy function
source("rot_db.R")


# setting logging
logReset()
basicConfig(level='FINEST')


tlcd_nikonrot_flow <- function(toolid, update_starttime, update_endtime, verbose = TRUE) {
    rawdata <- get_rawdatas(toolid, update_starttime, update_endtime)
    if (nrow(rawdata) == 0) {
        logwarn("No data to rotate")
        return (NULL)
    }
    if (verbose) {
        loginfo("Obtain needed PLFN columns")
    }

    rot_cols <- get_rotcols()
    dat_log <- clean_data(rawdata)

    prod_with_dv <- get_prodwithdv()
    product_list <- get_productlist(dat_alg)
    prod_no_dv <- product_list[!(product_list %in% prod_with_dv)]
    dat_alg <- check_designvalue(dat_alg, prod_with_dv, product_list, prod_no_dv)
    
    if (nrow(dat_alg) == 0) {
        logwarn(sprintf("No design values for ROT"))
        return (NULL)
    }
    
    product_list <- get_productlist(dat_alg)

    if (verbose) {
        loginfo("Check if raw data has missing data")
    }

    missing_data <- get_missingdata(dat_alg)
    dat_alg <- check_missingvalue(dat_alg, missing_data)

    if (nrow(dat_alg) == 0) {
        logwarn(sprintf("No data after removing missing values"))
        return(NULL)
    }

    rot_start_time <- Sys.time()
    loginfo(sprintf("START: %s", rot_start_time))

    if (verbose) {
        loginfo(sprintf("%s products: %s", 
            length(product_list), 
            paste(product_list, collapse = ", ")))
    }

    # ROT_PRODUCT
}


clean_data <- function(rawdatas) {
    dat_alg <- rawdatas %>%
        arrange(tstamp) %>%
        select(tstamp, glassid, toolid, operation, product, rot_cols) %>%
        mutate_at(rot_cols, funs(as.numeric)) %>%
        mutate(tstamp = strftime(tstamp, format = "%Y-%m-%d %H:%M:%S"))
    return (dat_alg)
}


get_productlist <- function(dat_alg) {
    return (unique(dat_alg$product))
}


get_prodwithdv <- function() {
    sql = sprintf( 
    "
    SELECT DISTINCT product 
    FROM %s 
    ","tlcd_nikon_main_v"
    )
    prod_with_dv <- dbGetQuery(con_psql.etl_rot, sql)
    return (prod_with_dv)
}


check_designvalue <- function(dat_alg, prod_with_dv, product_list, prod_no_dv) {
    if (length(prod_no_dv) > 0) {
        lapply(product_list[!(product_list %in% prod_with_dv)], function(no_dv) {
            dat_no_dv <- dat_alg %>% filter(product == no_dv)
            rot_error_record <- paste(sprintf("(%s, -2, 'No Design Values in Product %s')",
                                                lapply(seq(nrow(dat_no_dv)), function(num) {
                                                paste("'", dat_no_dv[num, 1:5], "'", sep = "", collapse = ", ")
                                                }), no_dv), collapse = ", ")
            loginfo(insert_error)
    })
    dat_alg <- dat_alg %>% filter(!(product %in% prod_no_dv))
    product_list <- get_productlist(dat_log)
    return (dat_alg)
}


get_missingdata <- function(dat_alg) {
    missing <- data.frame(which(is.na(dat_alg), 
        arr.ind = TRUE), stringsAsFactors = FALSE)
    return (missing)
}


check_missingvalue <- function(dat_alg, missing_data) {
    if (nrow(missing_data) > 0) {
        rot_error_record <- paste(sprintf("(%s, -1, 'Missing Values in Nikon PLFN %s')",
          lapply(unique(missing_data$row), function(num) {
            paste("'", dat_alg[num, 1:5], "'", sep = "", collapse = ", ")
          }),
          paste(colnames(dat_alg)[unique(missing_data$col)], collapse = ", ")),
            collapse = ", ")
    ret <- insert_error(rot_error_record)
    }
    dat_alg <- dat_alg[setdiff(seq(nrow(dat_alg)), unique(check_missing_data$row)), ]
    return (dat_alg)
}

