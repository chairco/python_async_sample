########################################################################################
# Funciton: tlcd_rot implement TLCD_NIKON_ROT.R 
# Description: Shift and rotate the Nikon TP data
# Arguments:
#   raw_data: data.frame. A data frame of Nikon rawdata in Operation '2300', and 'D300'.
#   verbose; logical, if TRUE the function will state procedure details
# Output:
#   rot_by_prodt: list. A list contains the number of ROT glasses of each product.
########################################################################################
library(dplyr)
library(stringr)

tlcd_rot <- function(raw_data, verbose = TRUE) {
    if (nrow(rawdata) == 0) {
        return (NULL)
    }

}


data_clean <- function(rawdata, tstamp, glassid, toolid, operatin, product, rot_columns, as.numeric) {
    data_alg <- rawdata %>%
        arrange(tstamp) %>%
        select(tstamp, glassid, toolid, operatin, product, rot_columns) %>%
        mutate_at(rot_columns, func(as.numeric)) %>%
        mutate(tstamp = strftime(tstamp, format = "%Y-%m-%d %H:%M%S"))
    return (data_alg)
}


design_value_check <- function(dat_alg, product) {
    product_list <- unique(dat_alg$product)
    prod_with_dv <- product
    prod_no_dv <- product_list[!(product_list %in% prod_with_dv)]

    if (length(prod_no_dv) > 0) {
        lapply(product_list[!(product_list %in% prod_with_dv)], 
                            function(no_dv)) {
            data_no_dv <- dat_alg %>% filter(product == no_dv)
            rot_error_record <- paste(sprint())
        }
        dat_alg <- dat_alg %>% filter(!(product %in% prod_no_dv))
        product_list <- unique(dat_alg$product)
    }
}


