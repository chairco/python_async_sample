# Postgresql DB config.
psql.type <- "PostgreSQL"
psql.dbname <- ""
psql.host <- ""
psql.port <- 5432
psql.username <- ""
psql.password <- ""


# Oracle DB config.
ora.eda.username <- "L6EDA_FDC"
ora.eda.password <- ""
ora.eda.dbname <- ""
ora.eda.host <- ""
ora.eda.port <- 1521
ora.eda.tns <- sprintf("(DESCRIPTION = (ADDRESS_LIST = (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = %s)))
                       (CONNECT_DATA = (SERVICE_NAME = %s)) )", ora.eda.host, ora.eda.port, ora.eda.dbname)


# config
DEBUG <- TRUE