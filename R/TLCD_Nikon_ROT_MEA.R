################################################################################
# Fucntion: TLCD_Nikon_MEA_ROT
# Description: Shift and rotate the measurement data
# Arguments:
#   mea_raw_dat: data.frame. A data frame of measurement data in Operation '1360' and 'DA60'.
#   verbose: logical. If TRUE the function will state procedure details.
# Output:
#   rot_by_prodt: list. A list contains the number of MEA ROT glasses of each product.
################################################################################
TLCD_Nikon_MEA_ROT <- function(mea_raw_dat, verbose = FALSE) {

  if (nrow(mea_raw_dat) == 0) {
    logwarn("[FDC_TLCD_MEA_ROT] No data to rotate")
    return(NULL)
  }

  # Set up DB connection
  drv_psql.etl_rot <- dbDriver("PostgreSQL")
  con_psql.etl_rot <- dbConnect(drv_psql.etl_rot, dbname = psql.dbname, host = psql.host, port = psql.port, user = psql.username, password = psql.password)

  # Raw data cleaning
  mea_dat_cleaned <- mea_raw_dat %>%
    arrange(GLASS_START_TIME, SITE_NAME) %>%
    mutate(tstamp = strftime(GLASS_START_TIME, format = "%Y-%m-%d %H:%M:%S"),
           product = sprintf("TL%s", substring(PARAM_COLLECTION, 5, nchar(PARAM_COLLECTION))),
           site_name = as.numeric(SITE_NAME)) %>%
    filter(site_name <= 48) %>%
    select(tstamp, glassid = GLASS_ID, operation = STEP_ID, product, site_name,
           param_name = PARAM_NAME, param_value = PARAM_VALUE) %>%
    reshape2::dcast(tstamp + glassid + operation + product + site_name ~ param_name,
                    value.var = "param_value", fill = NA_real_)

  # Design values checking: check if there exists design values
  if (verbose) loginfo(sprintf("[FDC_TLCD_MEA_ROT] Check if there exists design values"))
  product_list <- unique(mea_dat_cleaned$product)
  prod_with_dv <- dbGetQuery(con_psql.etl_rot, sprintf("SELECT DISTINCT product FROM %s", PSQL.TLCD_NIKON_MAIN_TBL_NAME))[, 1]

  prod_no_dv <- product_list[!(product_list %in% prod_with_dv)]
  if (length(prod_no_dv) > 0) {
    lapply(product_list[!(product_list %in% prod_with_dv)], function(no_dv) {
      dat_no_dv <- mea_dat_cleaned %>%
        filter(product == no_dv) %>%
        select(tstamp, glassid, operation, product) %>%
        unique()
      rot_error_record <- paste(sprintf("(%s, -2, 'No Design Values in Product %s')",
                                        lapply(seq(nrow(dat_no_dv)), function(num) {
                                          paste("'", dat_no_dv[num, 1:4], "'", sep = "", collapse = ", ")
                                        }), no_dv), collapse = ", ")
      dbGetQuery(con_psql.etl_rot,
                 pg_sql.etl_process.insert_error_msg(
                   PSQL.TLCD_NIKON_ROT_LOG_TBL_NAME,
                   "tstamp, glassid, operation, product, flag, descr",
                   rot_error_record
                 ))
    })

    mea_dat_cleaned <- mea_dat_cleaned %>% filter(!(product %in% prod_no_dv))
    product_list <- unique(mea_dat_cleaned$product)

    if (nrow(mea_dat_cleaned) == 0) {
      logwarn(sprintf("[FDC_TLCD_MEA_ROT] No design values for ROT"))
      return(NULL)
    }
  }

  rot_start_time <- Sys.time()
  loginfo(sprintf("[FDC_TLCD_MEA_ROT][Start] %s", rot_start_time))

  # ROT by product
  if (verbose) loginfo(sprintf("[FDC_TLCD_MEA_ROT] %s products: %s", length(product_list), paste(product_list, collapse = ", ")))
  rot_by_prodt <- lapply(product_list, function(prodt) {

    # Reading design values
    mea_dv <- dbGetQuery(con_psql.etl_rot, pg_sql.etl_process.get_design_value(PSQL.TLCD_NIKON_MEA_DV_TBL_NAME, PSQL.TLCD_NIKON_MAIN_TBL_NAME, prodt))

    # Design values: check the positon
    if (verbose) loginfo("[FDC_TLCD_MEA_ROT] Check the positions of MEA design values")
    if (nrow(coord_checking(mea_dv)) != 0) {
      mea_dv <- coord_checking(mea_dv)
    } else {
      logwarn(sprintf("[FDC_TLCD_MEA_ROT][MEA Design Value][product: %s] #Distinct X * #Distinct Y != #Rows", prodt))
      rot_error_record <- paste(sprintf("(%s, -3, '''#Distinct X * #Distinct Y != #Rows'' in Product %s')",
                                        lapply(seq(nrow(unique(mea_dat_cleaned[, 1:4]))), function(num) {
                                          paste("'", mea_dat_cleaned[num, 1:4], "'", sep = "", collapse = ", ")
                                        }), prodt),
                                collapse = ", ")
      insert_error_msg <- dbGetQuery(con_psql.etl_rot,
                                     pg_sql.etl_process.insert_error_msg(
                                       PSQL.TLCD_NIKON_ROT_LOG_TBL_NAME,
                                       "tstamp, glassid, operation, product, flag, descr",
                                       rot_error_record
                                     ))
      return(NULL)
    }

    # Check how many the glasses are in the data
    uni_comb <- unique(mea_dat_cleaned[, c("tstamp", "glassid")])

    glass_count <- 0
    for (comb in seq(nrow(uni_comb))) {
      print(comb)
      # Reformat the data
      mea_sub_by_gid <- mea_dat_cleaned %>%
        filter(glassid == uni_comb$glassid[comb] & tstamp == uni_comb$tstamp[comb]) %>%
        select(tstamp, glassid, operation, product, site_name, x = TP_X, y = TP_Y)

      # Give the new labels
      mea_sub_by_gid_new <- mea_label_new_id(mea_sub_by_gid)
      if (nrow(mea_sub_by_gid_new) == 0) {
        logwarn(sprintf("[FDC_TLCD_MEA_ROT][Raw Data][glassid: %s] Raw data has missing values", uni_comb$glassid[comb]))

        # Insert the error message into the log table
        rot_error_record <- sprintf("(%s, -1, 'Missing Values')", paste("'", unique(mea_sub_by_gid[, 1:4]), "'", sep = "", collapse = ", "))

        dbGetQuery(con_psql.etl_rot,
                   pg_sql.etl_process.insert_error_msg(
                     PSQL.TLCD_NIKON_ROT_LOG_TBL_NAME,
                     "tstamp, glassid, operation, product, flag, descr",
                     rot_error_record
                   ))
        return(data.frame())
      }

      # TP_MEA - 'Design value'
      mea_sub_by_gid_new <- mea_sub_by_gid_new %>%
        select(tstamp, glassid, operation, product, item_id, TP_X = x, TP_Y = y) %>%
        full_join(mea_dv, by = "item_id") %>%
        group_by(item_id) %>%
        mutate(Diff_X = TP_X - x, Diff_Y = TP_Y - y) %>%
        select(tstamp, glassid, operation, product, item_id, Diff_X, Diff_Y) %>%
        as.data.frame()

      # Long table to wide table
      Diff_X <- reshape2::dcast(mea_sub_by_gid_new[, c("tstamp", "glassid", "operation", "product", "item_id", "Diff_X")],
                                tstamp + glassid + operation + product ~ item_id,
                                value.var = "Diff_X")
      Diff_Y <- reshape2::dcast(mea_sub_by_gid_new[, c("tstamp", "glassid", "operation", "product", "item_id", "Diff_Y")],
                                tstamp + glassid + operation + product ~ item_id,
                                value.var = "Diff_Y")

      # ROT
      loginfo(sprintf("[FDC_TLCD_MEA_ROT][Opt_Insert_Data][product: %s][glassid: %s]", prodt, Diff_X$glassid))
      tryCatch({
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
                                         })),
                                 collapse = ", ")

        insert_data <- dbGetQuery(con_psql.etl_rot,
                                  pg_sql.etl_process.insert_rot_value(
                                    PSQL.TLCD_NIKON_ROT_LOG_TBL_NAME,
                                    "tstamp, glassid, operation, product, flag",
                                    rot_log_ht_record,
                                    PSQL.TLCD_NIKON_ROT_TBL_NAME,
                                    rot_data_record
                                  ))
      }, error = function(e) {
        logerror(sprintf("[FDC_TLCD_MEA_ROT][Opt_Insert_Data][product: %s][glassid: %s][Error]: %s", prodt, Diff_X$glassid[comb], e))

        rot_error_record <- sprintf("(%s, -4, 'ROT Error: %s')",
                                    paste("'", Diff_X[, 1:4], "'", sep = "", collapse = ", "), e)
        dbGetQuery(con_psql.etl_rot,
                   pg_sql.etl_process.insert_error_msg(
                     PSQL.TLCD_NIKON_ROT_LOG_TBL_NAME,
                     "tstamp, glassid, operation, product, flag, descr",
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
  loginfo(sprintf("[FDC_TLCD_MEA_ROT][End] %s", rot_end_time))
  loginfo(sprintf("[FDC_TLCD_MEA_ROT][Elapsed_time] %s", rot_end_time - rot_start_time))

  on.exit(dbDisconnect(con_psql.etl_rot, force = TRUE))
  return(rot_by_prodt)
}
