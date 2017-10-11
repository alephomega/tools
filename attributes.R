#!/usr/bin/Rscript --vanilla

source("common.R")
source("config.R")

basedate <- args.basedate()
offset <- args.offset()

conf <- config(args.basetime())
basedir <- job.basedir(conf)
tz.basedir <- job.tz.basedir(conf, basedate, offset)


task <- conf$job$tasks$attributes

inputs <- character(0)
path <- sprintf("%s/daily_summary/attribution", tz.basedir)
if (fs.exists(conf$fs, path)) {
  inputs <- sprintf("%s/daily_summary/attribution/*", tz.basedir)
}

path <- sprintf(
  "%s/attributes/{NEW,ACTIVE,ATRISK,WINBACK,UNKNOWN}-*", 
  job.tz.basedir(conf, format(as.Date(basedate, format = "%Y%m%d") - 1, format = "%Y%m%d"), offset)
)

if (fs.exists(conf$fs, path)) {
  inputs <- c(inputs, path)
}

cat(print.timestamp(), "* Running customer-attributes.\n")

if (length(inputs) > 0) {
  args <- c(
    "--base-date", 
    basedate, 
  
    "--defection", 
    task$defection,
  
    "--risk", 
    task$risk,
  
    "--input", 
    paste(inputs, collapse = ","),
  
    "--output", 
    sprintf("%s/attributes", tz.basedir)
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
