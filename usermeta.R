#!/usr/bin/Rscript --vanilla

source("common.R")
source("config.R")

basedate <- args.basedate()
offset <- args.offset()

conf <- config(args.basetime())
tz.basedir <- job.tz.basedir(conf, basedate, offset)


task <- conf$job$tasks$usermeta 
args <- c(
  "--base-date", 
  basedate, 
  
  "--input", 
  sprintf("%s/attributes/{NEW,ACTIVE,ATRISK,WINBACK,UNKNOWN}-*", tz.basedir),
  
  "--database", 
  task$output$database,
  
  "--table", 
  task$output$table
)

cat("\n", print.timestamp(), " * Running usermeta.\n", sep = "")

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