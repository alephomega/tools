#!/usr/bin/Rscript --vanilla

source("common.R")
source("config.R")


args.filter <- function() {
  args <- commandArgs(TRUE)
  
  f <- NA
  m <- match("--filter", args, 0L)
  if (m) {
    f <- args[m + 1]
  }
  
  f
}


basedate <- args.basedate()
offset <- args.offset()
filter <- args.filter()

conf <- config(args.basetime())
tz.basedir <- job.tz.basedir(conf, basedate, offset)


task <- conf$job$tasks$daily_summary
args <- c(
  "--base-date", 
  basedate, 
  
  "--database", 
  task$input$database, 
  
  "--table", 
  task$input$table,
  
  "--columns", 
  paste(task$input$columns, collapse = ","),
  
  "--filter", 
  filter,
  
  "--output", 
  sprintf("%s/daily_summary", tz.basedir),
  
  "--intermediate", 
  sprintf("%s/daily_summary.tmp", tz.basedir)
)

if (task$overwrite) {
  args <- c(args, "--overwrite")
}

cat(print.timestamp(), "* Running daily-summary.\n")

cat("properties:\n")
print(task$properties)

cat("args:\n")
cat(paste(args, collapse = ", "), "\n")


mr.run(
  fs = conf$fs, 
  jt = conf$jt, 
  jar = file.path(getwd(), "lib", conf$jar), 
  class = task$main,
  args = args,
  props = task$properties
)
