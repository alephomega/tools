#!/usr/bin/Rscript --vanilla

source("common.R")
source("config.R")

basedate <- args.basedate()
offset <- args.offset()

conf <- config(args.basetime())
tz.basedir <- job.tz.basedir(conf, basedate, offset)


cat(print.timestamp(), "* Running daily-statistics.\n")

task <- conf$job$tasks$daily_statistics 
input <- sprintf("%s/attributes/part-*", tz.basedir)
if (fs.exists(conf$fs, input)) {
  args <- c(
    "--base-date", 
    basedate, 
    
    "--input", 
    input,
    
    "--output", 
    sprintf("%s/daily_statistics", tz.basedir)
  )
  
  if (task$overwrite) {
    args <- c(args, "--overwrite")
  }
  
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
} else {
  cat(print.timestamp(), "No input to process.\n") 
}
