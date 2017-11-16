# rot.R
# Fetch command line arguments and into tlcd_rot()

# if want should warnings and setting warn = 0 or options(warn = oldw)
oldw <- getOption("warn")
options(warn = -1)

# load necessary package
library(optparse)

# set relative path
#PATH <- file.path(getwd(),'R')
#setwd(PATH)

# load necessary function
source("tlcd_nikonrot.R")

# setting logging
logReset()
basicConfig(level='FINEST')

# commmand line args and test if there is at least one argument: if not, return an error
option_list = list(
  make_option(c("-t", "--tid"), type="character", default=NULL, 
              help="toolid, ex: 0501", metavar="character"),
  make_option(c("-s", "--start"), type="character", default=NULL, 
              help="start time, ex: 2017-07-13 20:00:27", metavar="character"),
  make_option(c("-e", "--end"), type="character", default=NULL, 
              help="end time, ex: 2017-07-14 20:00:27", metavar="character")
); 



opt_parser = OptionParser(option_list=option_list);
opt = parse_args(opt_parser);

if (is.null(opt$tid) || is.null(opt$start) || is.null(opt$end)){
  print_help(opt_parser)
  psql_disconnectdb()
  stop("At least one argument must be supplied (input start and end time).\n", call.=FALSE)
} else{
  loginfo('execute...')
  tid <- sprintf('tlcd%s', opt$tid)
  ret <- main(tid, opt$start, opt$end)
  loginfo(sprintf('ret: %s', ret))
  loginfo('disable dbconnect')
  psql_disconnectdb()
}


tlcd_rot <- function(raw_data='Hello', verbose = TRUE) {
    loginfo('This is a test functuion.')
    loginfo('start execute.')
    loginfo('end execute.')
    return (raw_data)
}