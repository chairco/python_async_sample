# rot_db.R
# Query and Insert Postgresql's command

# load nessecy package
library(RPostgreSQL)


# load nessecy function
source("config.R")


drv_psql.etl_rot <- dbDriver("PostgreSQL")
con_psql.etl_rot <- dbConnect(drv_psql.etl_rot, 
    dbname = psql.dbname, host = psql.host, 
    port = psql.port, user = psql.username, 
    password = psql.password)


get_rawdatas <- function(toolid, update_starttime, update_endtime) {
    sql <- sprintf(
        "
        SELECT * 
        FROM %s 
        WHERE tstamp >= '%s' 
        AND tstamp < '%s'
        ",paste0(toolid,"_rawdata"), 
            update_starttime, 
            update_endtime
    )
    rawdata <- dbGetQuery(con_psql.etl_rot, sql)
    return (rawdata)
}


get_rotcols <- function() {
    sql <- sprintf(
        "
        SELECT col_name
        FROM %s 
        WHERE 1=1
        AND category='tp_al'  
        ", "tlcd_avm_col"
    )
    rot_cols <- dbGetQuery(con_psql.etl_rot, sql)[, 1]
    rot_cols <- rot_cols[order(substring(rot_cols, 10, 10), substring(rot_cols, 8, 8))]
    return (rot_cols)
}


insert_error <- function(rot_error_record) {
    sql <- sprintf(
    "
    INSERT INTO %s(%s)
    Values
    %s
    ","tlcd_nikon_rot_log_ht", 
    "tstamp, glassid, toolid, operation, product, flag, descr", 
    rot_log_record
    )
    ret <- dbGetQuery(con_psql.etl_rot, sql)
    return (ret)
}