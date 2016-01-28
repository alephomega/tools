#!/usr/bin/Rscript --vanilla

source("common.R")
source("config.R")

basedate <- args.basedate()
offset <- args.offset()

conf <- config(args.basetime())
basedir <- job.basedir(conf)
tz.basedir <- job.tz.basedir(conf, basedate, offset)


task <- conf$job$tasks$attributes

inputs <- sprintf("%s/daily_summary/attribution/*", tz.basedir)
a.path <- sprintf(
  "%s/attributes/{NEW,ACTIVE,ATLISK,ATRISK,WINBACK,UNKNOWN}-*", 
  job.tz.basedir(conf, format(as.Date(basedate, format = "%Y%m%d") - 1, format = "%Y%m%d"), offset)
)

if (fs.exists(conf$fs, a.path)) {
  inputs <- c(inputs, a.path)
}

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

cat("\n", print.timestamp(), " * Running customer-attributes.\n", sep = "")

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
