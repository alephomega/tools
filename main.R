#!/usr/bin/Rscript --vanilla

library(RPostgreSQL)

basedir <- function() {
  args <- commandArgs(trailingOnly = FALSE)
  needle <- "--file="
  m <- grep(needle, args)
  if (length(m) > 0) {
    f <- normalizePath(sub(needle, "", args[m]))
  } else {
    f <- normalizePath(sys.frames()[[1]]$ofile)
  }
  
  dirname(f)
}

run.task <- function(command, args, wait = TRUE) {
  status <- system2(command, args, wait = wait)
  if (status != 0) {
    stop(sprintf("%s failed.", command))
  }
}


setwd(basedir())

source("common.R")
source("config.R")


cat(print.timestamp(), "Reading configurations.\n")

basetime <- args.basetime()
cat(sprintf("basetime: %s\n\n", basetime))

conf <- config(basetime)
print(conf)

cat(print.timestamp(), "Querying target clients.\n")
conn <- dbConnect(dbDriver(conf$filter$driver),
                  user = conf$filter$user,
                  password = conf$filter$password,
                  dbname = conf$filter$dbname,
                  host = conf$filter$host,
                  port = as.integer(conf$filter$port))

d <- dbGetQuery(conn, conf$filter$SQL)
invisible(dbDisconnect(conn))

if (nrow(d) > 0) {
  d$basedate <- format(as.Date(d$basedate, format = "%Y%m%d") - 1, format = "%Y%m%d")
  
  d <- aggregate(client_id ~ (basedate + offset), FUN = c, data = d)
  for (i in 1:nrow(d)) {
    basedate <- d$basedate[i]
    
    if (grepl("^-", d$offset[i])) {
      offset <- sprintf("B%04d", as.integer(substr(d$offset[i], 2, nchar(d$offset[i]))))  
    } else {
      offset <- sprintf("A%04d", as.integer(d$offset[i]))
    }
    
    d$client_id <- as.matrix(d$client_id)
    filter <- sprintf("\"p_dt = '%s' and (%s)\"", basedate, paste(sprintf("p_clientid = '%s'", d$client_id[i, ]), collapse = " or "))
    clients <- paste(d$client_id[i, ], collapse = ",")
    
    cat("\n", print.timestamp(), "** Running analytics job.\n")
    cat(sprintf("basedate: %s\n", basedate))
    cat(sprintf("timezone offset: %s\n", offset))
    cat("clients:\n")
    print(clients)

    command <- file.path(getwd(), "daily_summary.R")
    args <- c("--base-time", basetime, "--base-date", basedate, "--offset", offset, "--filter", filter)
    run.task(command, args)

    tz.basedir <- job.tz.basedir(conf, basedate, offset)
    z <- fs.exists(conf$fs, sprintf("%s/daily_summary", tz.basedir))
    if (z) {
      command <- file.path(getwd(), "attribution.R")
      args <- c("--base-time", basetime, "--base-date", basedate, "--offset", offset)
      run.task(command, args)
    }
    
    if (z) {
      command <- file.path(getwd(), "attributes.R")
      args <- c("--base-time", basetime, "--base-date", basedate, "--offset", offset)
      run.task(command, args)
    }
    
    if (z) {
      command <- file.path(getwd(), "balancer.R")
      args <- c("--base-time", basetime, "--base-date", basedate, "--offset", offset)
      run.task(command, args, wait = FALSE)
    }
    
    if (z) {
      command <- file.path(getwd(), "daily_statistics.R")
      args <- c("--base-time", basetime, "--base-date", basedate, "--offset", offset)
      run.task(command, args, wait = FALSE)
    }

    command <- file.path(getwd(), "periodic_statistics.R")
    args <- c("--base-time", basetime, "--base-date", basedate, "--offset", offset)
    run.task(command, args, wait = FALSE)

    if (z) {
      command <- file.path(getwd(), "usermeta.R")
      args <- c("--base-time", basetime, "--base-date", basedate, "--offset", offset)
      run.task(command, args)
    }
  }
} else {
  cat(print.timestamp(), "No clients to process.\n")
}
