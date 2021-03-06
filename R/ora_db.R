# mea_db.R
# Query and Insert Postgresql's command

# load necessary package
library(ROracle)

# load necessary function
source("env.R")


drv_ora.etl_rot <- dbDriver("Oracle")
con_oracle.etl_rot <- dbConnect(drv_ora.etl_rot, 
    username = ora.eda.username, 
    password = ora.eda.password, 
    dbname = ora.eda.tns)
#on.exit(dbDisconnect(con_oracle.etl_rot, force = TRUE))

get_mearawdata <- function(update_starttime, update_endtime) {
    sql = sprintf( 
        "
        SELECT
        a.step_id, a.glass_id, a.glass_start_time, a.update_time, a.product_id, a.lot_id, a.equip_id,
        b.PARAM_COLLECTION, b.PARAM_NAME, b.PARAM_VALUE, b.SITE_NAME
        FROM 
        lcdsys.array_glass_v a,
        lcdsys.array_result_v b 
        WHERE 1=1
        AND a.STEP_ID in ('DA60','1360')
        AND a.UPDATE_TIME >= to_date('%s','yyyy-mm-dd hh24:mi:ss')
        AND a.UPDATE_TIME <= to_date('%s','yyyy-mm-dd hh24:mi:ss')
        AND b.PARAM_NAME in ('TP_X','TP_Y')
        AND b.GLASS_ID = a.GLASS_ID
        AND b.STEP_ID = a.STEP_ID
        AND b.GLASS_START_TIME = a.GLASS_START_TIME
        ", update_starttime, update_endtime
    )
    mea_rawdata <- dbGetQuery(con_oracle.etl_rot, sql)
    return (mea_rawdata)
}


ora_disconnectdb <- function() {
    #dbDisconnect(con_oracle.etl_rot, force = TRUE)
    on.exit(dbDisconnect(con_oracle.etl_rot, force = TRUE))
}