#!/usr/bin/Rscript --vanilla

source("common.R")
source("config.R")

basedate <- args.basedate()
offset <- args.offset()

conf <- config(args.basetime())
basedir <- job.basedir(conf)
tz.basedir <- job.tz.basedir(conf, basedate, offset)

input <- NA
path <- sprintf("%s/attributes/{NEW,ACTIVE,ATRISK,WINBACK,UNKNOWN}-*", tz.basedir)

if (fs.exists(conf$fs, path)) {
  input <- path
} else {
  path <- sprintf("%s/attributes/%s", basedir, offset)
  if (fs.exists(conf$fs, path)) {
    input <- path
  }
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

  if (task$overwrite) {
    args <- c(args, "--overwrite")
  }
  

  cat(print.timestamp(), "* Running periodic-statistics.\n")
  
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
