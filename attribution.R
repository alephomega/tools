#!/usr/bin/Rscript --vanilla

source("common.R")
source("config.R")

basedate <- args.basedate()
offset <- args.offset()

conf <- config(args.basetime())
tz.basedir <- job.tz.basedir(conf, basedate, offset)

cat(print.timestamp(), " * Running attribution.\n")

if (fs.exists(conf$fs, sprintf("%s/daily_summary", tz.basedir))) {
  task <- conf$job$tasks$attribution

  input.dsumm <- sprintf("%s/daily_summary/*", tz.basedir)
  input.attr <- sprintf("%s/%s/*/part-*", task$attribution_hdfs, basedate)

  if (fs.exists(conf$fs, input.attr)) {
    input <- paste(input.dsumm, input.attr, sep = ",")
  } else {
    input <- input.dsumm
  }

  args <- c(
    "--base-date", 
    basedate, 
    
    "--input", 
    input,
    
    "--output", 
    sprintf("%s/%s/%s/daily_summary/attribution", conf$job$base_dir, basedate, offset)
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
