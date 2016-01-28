#!/usr/bin/Rscript --vanilla

source("common.R")
source("config.R")

basedate <- args.basedate()
offset <- args.offset()

conf <- config(args.basetime())
tz.basedir <- job.tz.basedir(conf, basedate, offset)


task <- conf$job$tasks$attribution
args <- c(
  "--base-date", 
  basedate, 
  
  "--input", 
  paste(sprintf("%s/daily_summary/*", tz.basedir), sprintf("%s/%s/**/part-*", task$attribution_hdfs, basedate), sep = ","),
  
  "--output", 
  sprintf("%s/%s/%s/daily_summary/attribution", conf$job$base_dir, basedate, offset)
)

if (task$overwrite) {
  args <- c(args, "--overwrite")
}

cat("\n", print.timestamp(), "* Running attribution.\n")

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