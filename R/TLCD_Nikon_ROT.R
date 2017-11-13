################################################################################
# Fucntion: TLCD_Nikon_ROT
# Description: Shift and rotate the Nikon TP data
# Arguments:
#   raw_dat: data.frame. A data frame of Nikon rawdata in Operation '2300' and 'D300'.
#   verbose: logical. If TRUE the function will state procedure details.
# Output:
#   rot_by_prodt: list. A list contains the number of ROT glasses of each product.
################################################################################
TLCD_Nikon_ROT <- function(raw_dat, verbose = FALSE) {

  if (nrow(raw_dat) == 0) {
    logwarn("[FDC_TLCD_Nikon_ROT] No data to rotate")
    return(NULL)
  }

  # Set up DB connection
  drv_psql.etl_rot <- dbDriver("PostgreSQL")
  con_psql.etl_rot <- dbConnect(drv_psql.etl_rot, dbname = psql.dbname, host = psql.host, port = psql.port, user = psql.username, password = psql.password)

  # Obtain needed PLFN columns
  if (verbose) loginfo("[FDC_TLCD_Nikon_ROT] Obtain needed PLFN columns")
  rot_cols <- dbGetQuery(con_psql.etl_rot, sprintf("SELECT col_name FROM %s WHERE 1 = 1 AND category = 'tp_al'", PSQL.TLCD_NIKON_COLNAME_TBL_NAME))[, 1]
  rot_cols <- rot_cols[order(substring(rot_cols, 10, 10), substring(rot_cols, 8, 8))]

  # Raw data cleaning
  dat_alg <- raw_dat %>%
    arrange(tstamp) %>% # Sort by tstamp
    select(tstamp, glassid, toolid, operation, product, rot_cols) %>%
    mutate_at(rot_cols, funs(as.numeric)) %>%
    mutate(tstamp = strftime(tstamp, format = "%Y-%m-%d %H:%M:%S"))

  # 1) Design values checking: check if there exists design values
  if (verbose) loginfo(sprintf("[FDC_TLCD_Nikon_ROT] Check if there exists design values"))
  product_list <- unique(dat_alg$product)
  prod_with_dv <- dbGetQuery(con_psql.etl_rot, sprintf("SELECT DISTINCT product FROM %s", PSQL.TLCD_NIKON_MAIN_TBL_NAME))[, 1]

  prod_no_dv <- product_list[!(product_list %in% prod_with_dv)]
  if (length(prod_no_dv) > 0) {
    lapply(product_list[!(product_list %in% prod_with_dv)], function(no_dv) {
      dat_no_dv <- dat_alg %>% filter(product == no_dv)
      rot_error_record <- paste(sprintf("(%s, -2, 'No Design Values in Product %s')",
                                        lapply(seq(nrow(dat_no_dv)), function(num) {
                                          paste("'", dat_no_dv[num, 1:5], "'", sep = "", collapse = ", ")
                                        }), no_dv), collapse = ", ")
      dbGetQuery(con_psql.etl_rot,
                 pg_sql.etl_process.insert_error_msg(
                   PSQL.TLCD_NIKON_ROT_LOG_TBL_NAME,
                   "tstamp, glassid, toolid, operation, product, flag, descr",
                   rot_error_record
                 ))
    })

    dat_alg <- dat_alg %>% filter(!(product %in% prod_no_dv))
    product_list <- unique(dat_alg$product)

    if (nrow(dat_alg) == 0) {
      logwarn(sprintf("[FDC_TLCD_Nikon_ROT] No design values for ROT"))
      return(NULL)
    }
  }

  # 2) Raw data checking
  if (verbose) loginfo(sprintf("[FDC_TLCD_Nikon_ROT] Check if raw data has missing data"))
  check_missing_data <- data.frame(which(is.na(dat_alg), arr.ind = TRUE), stringsAsFactors = FALSE)

  # Write the error message into DB and remove the rows of which has missing values
  if (nrow(check_missing_data) > 0) {
    rot_error_record <- paste(sprintf("(%s, -1, 'Missing Values in Nikon PLFN %s')",
                                      lapply(unique(check_missing_data$row), function(num) {
                                        paste("'", dat_alg[num, 1:5], "'", sep = "", collapse = ", ")
                                      }),
                                      paste(colnames(dat_alg)[unique(check_missing_data$col)], collapse = ", ")),
                              collapse = ", ")
    dbGetQuery(con_psql.etl_rot,
               pg_sql.etl_process.insert_error_msg(
                 PSQL.TLCD_NIKON_ROT_LOG_TBL_NAME,
                 "tstamp, glassid, toolid, operation, product, flag, descr",
                 rot_error_record
               ))
    dat_alg <- dat_alg[setdiff(seq(nrow(dat_alg)), unique(check_missing_data$row)), ]
  }

  # No action if no data after data cleaning
  if (nrow(dat_alg) == 0) {
    logwarn(sprintf("[FDC_TLCD_Nikon_ROT] No data after removing missing values"))
    return(NULL)
  }

  rot_start_time <- Sys.time()
  loginfo(sprintf("[FDC_TLCD_Nikon_ROT][Start] %s", rot_start_time))

  # ROT by product
  if (verbose) loginfo(sprintf("[FDC_TLCD_Nikon_ROT] %s products: %s", length(product_list), paste(product_list, collapse = ", ")))
  rot_by_prodt <- lapply(product_list, function(prodt) {
    rot_by_product_start_time <- Sys.time()
    loginfo(sprintf("[FDC_TLCD_Nikon_ROT][Start][product: %s] %s", prodt, rot_by_product_start_time))

    # Seperate rawdata by x and y
    ALG_x <- grep("^plfn_al\\d[x]\\d_x", rot_cols, value = TRUE)
    ALG_x <- ALG_x[order(substring(ALG_x, 10, 10), substring(ALG_x, 8, 8))]
    ALG_y <- grep("^plfn_al\\d[y]\\d_x", rot_cols, value = TRUE)
    ALG_y <- ALG_y[order(substring(ALG_y, 10, 10), substring(ALG_y, 8, 8))]

    dat_ALG_x <- dat_alg %>% filter(product == prodt) %>% select(tstamp, glassid, toolid, operation, product, ALG_x)
    dat_ALG_y <- dat_alg %>% filter(product == prodt) %>% select(tstamp, glassid, toolid, operation, product, ALG_y)

    # Reading design values
    DV_coord <- dbGetQuery(con_psql.etl_rot, pg_sql.etl_process.get_design_value(PSQL.TLCD_NIKON_DV_TBL_NAME, PSQL.TLCD_NIKON_MAIN_TBL_NAME, prodt))

    # Design values: check the positon
    if (verbose) loginfo("[FDC_TLCD_Nikon_ROT] Check the positions of Nikon design values")
    if (nrow(coord_checking(DV_coord)) != 0) {
      DV_coord <- coord_checking(DV_coord)
    } else {
      logwarn(sprintf("[FDC_TLCD_Nikon_ROT][Nikon Design Value][product: %s] #Distinct X * #Distinct Y != #Rows", prodt))
      rot_error_record <- paste(sprintf("(%s, -3, '''#Distinct X * #Distinct Y != #Rows'' in Product %s')",
                                        lapply(seq(nrow(dat_ALG_x)), function(num) {
                                          paste("'", dat_ALG_x[num, 1:5], "'", sep = "", collapse = ", ")
                                        }), prodt),
                                collapse = ", ")
      insert_error_msg <- dbGetQuery(con_psql.etl_rot,
                                     pg_sql.etl_process.insert_error_msg(
                                       PSQL.TLCD_NIKON_ROT_LOG_TBL_NAME,
                                       "tstamp, glassid, toolid, operation, product, flag, descr",
                                       rot_error_record
                                     ))
      return(NULL)
    }

    # Opt
    glass_count <- 0
    for (i in seq_along(dat_ALG_x$glassid)) {
      loginfo(sprintf("[FDC_TLCD_Nikon_ROT][Opt_Insert_Data][product: %s][glassid: %s]", prodt, dat_ALG_x$glassid[i]))
      print(i)
      tryCatch({
        opt_output <- optim(c(0, 0, 0),
                            min_res_squared,
                            gr = NULL,
                            mat_x = dat_ALG_x[i, ALG_x],
                            mat_y = dat_ALG_y[i, ALG_y],
                            mat_dx = DV_coord$x,
                            mat_dy = DV_coord$y,
                            method = "L-BFGS-B")

        rot_log_ht_record <- sprintf("(%s, 1)", paste("'", dat_ALG_x[i, 1:5], "'", sep = "", collapse = ", "))

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

        insert_data <- dbGetQuery(con_psql.etl_rot,
                                  pg_sql.etl_process.insert_rot_value(
                                    PSQL.TLCD_NIKON_ROT_LOG_TBL_NAME,
                                    "tstamp, glassid, toolid, operation, product, flag",
                                    rot_log_ht_record,
                                    PSQL.TLCD_NIKON_ROT_TBL_NAME,
                                    rot_data_record
                                  ))
      }, error = function(e) {
        logerror(sprintf("[FDC_TLCD_Nikon_ROT][Opt_Insert_Data][product: %s][glassid: %s][Error]: %s", prodt, dat_ALG_x$glassid[i], e))

        rot_error_record <- sprintf("(%s, -4, 'ROT Error: %s')",
                                    paste("'", dat_ALG_x[i, 1:5], "'", sep = "", collapse = ", "), e)
        dbGetQuery(con_psql.etl_rot,
                   pg_sql.etl_process.insert_error_msg(
                     PSQL.TLCD_NIKON_ROT_LOG_TBL_NAME,
                     "tstamp, glassid, toolid, operation, product, flag, descr",
                     rot_error_record
                   ))
      }, finally = {
        glass_count <- glass_count + 1
      })
    }

    return(glass_count)
  })
  names(rot_by_prodt) <- product_list

  rot_end_time <- Sys.time()
  loginfo(sprintf("[FDC_TLCD_Nikon_ROT][End] %s", rot_end_time))
  loginfo(sprintf("[FDC_TLCD_Nikon_ROT][Elapsed_time] %s", rot_end_time - rot_start_time))

  on.exit(dbDisconnect(con_psql.etl_rot, force = TRUE))
  return(rot_by_prodt)
}
