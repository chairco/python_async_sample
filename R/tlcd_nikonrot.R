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
    if (verbose) {
        loginfo("Check the positions of Nikon design values")
    }

    rot_by_prodt <- lapply(product_list, function(prodt)){
        rot_by_product_start_time <- Sys.time()
        loginfo(sprintf("product: %s, %s", prodt, rot_by_product_start_time))
        
        # Seperate rawdata by x and y
        ALG_x <- grep("^plfn_al\\d[x]\\d_x", rot_cols, value = TRUE)
        ALG_x <- ALG_x[order(substring(ALG_x, 10, 10), substring(ALG_x, 8, 8))]
        ALG_y <- grep("^plfn_al\\d[y]\\d_x", rot_cols, value = TRUE)
        ALG_y <- ALG_y[order(substring(ALG_y, 10, 10), substring(ALG_y, 8, 8))]

        dat_ALG_x <- dat_alg %>% 
            filter(product == prodt) %>% 
            select(tstamp, glassid, toolid, operation, product, ALG_x)

        dat_ALG_y <- dat_alg %>% 
            filter(product == prodt) %>% 
            select(tstamp, glassid, toolid, operation, product, ALG_y)

        # reading Design values: 
        DV_coord <- get_designvalue(prodt)

        if (verbose) {
            loginfo("Check the positions of Nikon design values")
        }
        # check the position
        DV_coord <- check_position(DV_coord, prodt)
        if (is.null(DV_coord)) {
            return (NULL)
        }
        glass.count <- opt(prodt, DV_coord, dat_ALG_x, dat_ALG_y)
    }

    names(rot_by_prodt) <- product_list
    rot_endtime <- Sys.time()
    loginfo(sprintf("END: %s", rot_endtime))
    loginfo(sprintf("Elapsed time: %s", rot_endtime - rot_starttime))
    return (rot_by_prodt)
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


check_position <- function(DV_coord, prodt) {
    if (nrow(coord_checking(DV_coord)) != 0) {
        DV_coord <- coord_checking(DV_coord)
    } else{
        logwarn(sprintf("product: %s, #Distinct X * #Distinct Y != #Rows", prodt))
        rot_error_record <- paste(sprintf("(%s, -3, '''#Distinct X * #Distinct Y != #Rows'' in Product %s')",
                                    lapply(seq(nrow(dat_ALG_x)), function(num) {
                                      paste("'", dat_ALG_x[num, 1:5], "'", sep = "", collapse = ", ")
                                    }), prodt),
                            collapse = ", ")
        insert_errormsg <- insert_error(rot_error_record)
        return (NULL)
    }
    return (DV_coord)
}


opt <- function(prodt, DV_coord, dat_ALG_x, dat_ALG_y) {
    glass_count <- 0
    for (i in seq_along(dat_ALG_x$glassid)) {
        loginfo(sprintf("product: %s, glassid: %s]", prodt, dat_ALG_x$glassid[i]))
        loginfo(i)
        tryCatch({
            opt_output <- optim(c(0, 0, 0),
                min_res_squared
                gr = NULL,
                mat_x = dat_ALG_x[i, ALG_x],
                mat_y = dat_ALG_y[i, ALG_y],
                mat_dx = DV_coord$x,
                mat_dy = DV_coord$y,
                method = "L-BFGS-B")
            rot_log_ht_record <- sprintf("(%s, 1)",
                paste("'", dat_ALG_x[i, 1:5], "'", sep = "", collapse = ", "))
            nikon_rs_x <- dat_ALG_x[i, ALG_x] + opt_output$par[1] - DV_coord$y * tan(opt_output$par[3] * 0.000001)
            nikon_rs_y <- dat_ALG_y[i, ALG_y] + opt_output$par[2] + DV_coord$x * tan(opt_output$par[3] * 0.000001)
            nikon_rs_xy <- cbind(glassid = dat_ALG_x$glassid[i], nikon_rs_x, nikon_rs_y)
            nikon_rs_xy_long <-
              reshape2::melt(nikon_rs_xy, id.vars = c("glassid"), measure.vars = rot_cols,
                             variable.name = "item_name", value.name = "rot_rs") %>%
              mutate_at(c("glassid", "item_name"), funs(as.character)) %>%
              select(item_name, rot_rs)
            rot_data_record <- paste(sprintf("(%s, (SELECT rot_id FROM insert_rot_ht))",
                                         lapply(seq(nrow(nikon_rs_xy_long)), function(num) {
                                           paste("'", nikon_rs_xy_long[num, c("item_name", "rot_rs")], "'", sep = "", collapse = ", ")
                                         })),
                                 collapse = ", ")
            insert_data <-
        }, error = function(e) {
            logerror(sprintf("product: %s, glassid: %s, Error: %s", prodt, dat_ALG_x$glassid[i], e))
            rot_error_record <- sprintf("(%s, -4, 'ROT Error: %s')",
                                    paste("'", dat_ALG_x[i, 1:5], "'", sep = "", collapse = ", "), e)
            ret <- insert_error(rot_error_record)
        }, finally = {
            glass_count <- glass_count + 1
        })
    }
    return (glass_count)
}


main <- function(toolid, update_starttime, update_endtime) {
    tryCatch({
        stop("demo error")
        ret = tlcd_nikonrot_flow(toolid, update_starttime, update_endtime)
    }, error = function(e) {
        # 這就會是 demo error
        conditionMessage(e)
    }, finally = {
        disconnectdb()
    })
    return (ret)
}
