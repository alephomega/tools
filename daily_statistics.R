#!/usr/bin/Rscript --vanilla

source("common.R")
source("config.R")

basedate <- args.basedate()
offset <- args.offset()

conf <- config(args.basetime())
tz.basedir <- job.tz.basedir(conf, basedate, offset)


task <- conf$job$tasks$daily_statistics 
args <- c(
  "--base-date", 
  basedate, 
  
  "--input", 
  sprintf("%s/attributes/part-*", tz.basedir),
  
  "--output", 
  sprintf("%s/daily_statistics", tz.basedir)
)

if (task$overwrite) {
  args <- c(args, "--overwrite")
}


cat(print.timestamp(), "* Running daily-statistics.\n")

cat("properties:\n")
print(task$properties)

cat("args:\n")
print(args)


mr.run(
  fs = conf$fs,
  jt = conf$jt,
  jar = file.path(getwd(), "lib", conf$jar),
  class = task$main,
  args = args,
  props = task$properties
)
