#!/usr/bin/Rscript --vanilla

source("common.R")
source("config.R")

basedate <- args.basedate()
offset <- args.offset()

conf <- config(args.basetime())
basedir <- job.basedir(conf)
tz.basedir <- job.tz.basedir(conf, basedate, offset)

reduce.tasks <- function(tz.basedir) {
  du <- dfs.du(conf$fs, sprintf("%s/attributes/{NEW,ACTIVE,ATRISK,LOST,WINBACK,UNKNOWN}-*", tz.basedir))
  as.integer(sum(du$length, na.rm = TRUE) / (128 * 1024 * 1024)) + 1
}


task <- conf$job$tasks$balancer
args <- c(
  "--base-date", 
  basedate, 
  
  "--input", 
  sprintf("%s/attributes/{NEW,ACTIVE,ATRISK,WINBACK,UNKNOWN}-*", tz.basedir),
  
  "--output", 
  sprintf("%s/attributes/%s", basedir, offset)
)

if (task$overwrite) {
  args <- c(args, "--overwrite")
}

props = task$properties 
props$mapred.reduce.tasks = reduce.tasks(conf, basedate, offset)

cat("\n", print.timestamp(), "* Running balancer.\n")

cat("properties:\n")
print(props)

cat("args:\n")
print(args)

dfs.rename(
  conf$fs, 
  sprintf("%s/attributes/%s", basedir, offset), 
  sprintf("%s/attributes/_%s.%s", basedir, offset, basedate)
)

tryCatch(
  {
    mr.run(
      fs = conf$fs, 
      jt = conf$jt, 
      jar = file.path(getwd(), "lib", conf$jar), 
      class = task$main,
      args = args,
      props = props
    )
  }, error = function(e) {
    dfs.rename(
      conf$fs, 
      sprintf("%s/attributes/_%s.%s", basedir, offset, basedate), 
      sprintf("%s/attributes/%s", basedir, offset)
    )
    
    stop(e)
  }
)

dfs.rm(
  conf$fs, 
  sprintf("%s/attributes/_%s.%s", basedir, offset, basedate)
)

