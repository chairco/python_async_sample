################################################################################
# Environment setting (load 'plyr' first, then 'dplyr')
#if (sum(grepl("package:dplyr", search()))) detach("package:dplyr")
#if (sum(grepl("package:plyr", search())) == 0) library(plyr)
#library(dplyr)
################################################################################


################################################################################
# Fucntion: min_res_squared
# Description: The sum of squared residuals to be minimized.
#             It is the objective function of the ROT optimization.
# Arguments:
#   x: vector of parameters over which minimization is to take place. Here we denote that
#     - x[1]: shiftX [um]; the shift value of x direction, and
#     - x[2]: shiftY [um]; the shift value of y direction, and
#     - x[3]: Rot [urad]; the radius of rotation.
#   mat_x: Matrix of X coordinate [um]. The default number of rows is 1.
#   mat_y: Matrix of Y coordinate [um]. The default number of rows is 1.
#   mat_dx: Matrix of X coordinate of design values [um]. The default number of rows is 1.
#   mat_dy: Matrix of Y coordinate of design values [um]. The default number of rows is 1.
# Output:
#   result_sumsq: numeric. The sum of squared residuals.
################################################################################
min_res_squared <- function(x, mat_x, mat_y, mat_dx, mat_dy) {
  mat_shift_x <- matrix(x[1], ncol = ncol(mat_x))
  mat_shift_y <- matrix(x[2], ncol = ncol(mat_x))
  result_sumsq <- sum(
    (mat_x + mat_shift_x - mat_dy * tan(x[3] * 0.000001))^2 +
      (mat_y + mat_shift_y + mat_dx * tan(x[3] * 0.000001))^2
  )
  return(result_sumsq)
}


################################################################################
# Fucntion: coord_checking
# Desription: Justify the coordinates of design values
# Arguments:
#   dat: data.frame. Data frame of design values.
# Output:
#   new_coord: data.frame. Data frame of design values after reorganizing the postions.
#              (Empty data frame if something wrong with the coordinates.)
################################################################################
coord_checking <- function(dat) {

  if(length(unique(dat$x)) * length(unique(dat$y)) != nrow(dat)) {
    logwarn("#Distinct X * #Distinct Y != #rows")
    return(data.frame())
  }

  x_coord <- sort(unique(dat$x))
  y_coord <- sort(unique(dat$y))

  new_coord <- data.frame(expand.grid(x_coord, y_coord), stringsAsFactors = FALSE)
  colnames(new_coord) <- c("x", "y")
  new_coord <- new_coord %>% arrange(x, y) %>% mutate(item_id = seq(nrow(dat)))

  return(new_coord)
}


################################################################################
# Fucntion: mea_label_new_id
# Desription: Provide a new position id
# Arguments:
#   mea_sub: data.frame. Data frame of design values or raw data.
#                        Colnames: x, y
# Output:
#   mea_sub_new: data.frame. Data frame of design values or raw data after reorganizing the postions.
#               (Empty data frame if something wrong with the coordinates.)
################################################################################
mea_label_new_id <- function(mea_sub) {

  # Check the data
  check_missing_data <- data.frame(which(is.na(mea_sub), arr.ind = TRUE), stringsAsFactors = FALSE)
  if (nrow(check_missing_data) > 0) {
    logwarn(sprintf("The glassid %s has missing values", unique(mea_sub$glassid)))
    return(data.frame())
  }

  # Label the correct id
  mea_sub_x_z <- scale(mea_sub$x, center = mean(mea_sub$x), scale = sd(mea_sub$x))
  mea_sub_x_z_dist <- dist(mea_sub_x_z, method = "euclidean")
  mea_sub_x_z_clust <- hclust(mea_sub_x_z_dist, method = "ward.D")

  group_num <- nrow(mea_sub) / 6
  if (group_num %% 1 != 0) {
    logwarn("The number of group is not the multiple of the number of alignments")
    return(data.frame())
  }

  sep_group <- cutree(mea_sub_x_z_clust, k = group_num)

  mea_sub_new <- mea_sub %>% mutate(x_id = sep_group) %>%
    group_by(x_id) %>%
    mutate(item_id = (x_id - 1) * 6 + order(y)) %>%
    ungroup() %>%
    mutate(x_id = NULL) %>%
    arrange(item_id) %>%
    as.data.frame()

  return(mea_sub_new)
}
