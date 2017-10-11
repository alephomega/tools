#!/usr/bin/Rscript --vanilla

source("common.R")
source("config.R")

basedate <- args.basedate()
offset <- args.offset()

conf <- config(args.basetime())
tz.basedir <- job.tz.basedir(conf, basedate, offset)


cat(print.timestamp(), "* Running usermeta.\n")

task <- conf$job$tasks$usermeta 
input <- sprintf("%s/attributes/{NEW,ACTIVE,ATLISK,ATRISK,WINBACK,UNKNOWN}-*", tz.basedir)
if (fs.exists(conf$fs, input)) {
  args <- c(
    "--base-date", 
    basedate, 
    
    "--input", 
    input,
    
    "--database", 
    task$output$database,
    
    "--table", 
    task$output$table
  )
  
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
