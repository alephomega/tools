#!/usr/bin/Rscript --vanilla

source("common.R")
source("config.R")

basedate <- args.basedate()
offset <- args.offset()

conf <- config(args.basetime())
basedir <- job.basedir(conf)
tz.basedir <- job.tz.basedir(conf, basedate, offset)

input = NA
if (fs.exists(conf$fs, sprintf("%s/attributes/{NEW,ACTIVE,ATRISK,WINBACK,UNKNOWN}-*", tz.basedir))) {
  input = sprintf("%s/attributes/{NEW,ACTIVE,ATRISK,WINBACK,UNKNOWN}-*"
} else if (fs.exists(conf$fs, sprintf("%s/attributes/%s", basedir, offset))) {
  input = sprintf("%s/attributes/%s/*", basedir, offset)
}

if (!is.na(input)) {
  task <- conf$job$tasks$periodic_statistics 
  args <- c(
    "--base-date", 
    basedate, 
    
    "--input", 
    input,
    
    "--output", 
    sprintf("%s/periodic_statistics", tz.basedir)
  )
  
  cat("\n", print.timestamp(), "* Running periodic-statistics.\n")
  
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
}