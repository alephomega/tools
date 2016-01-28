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

setwd(basedir())
source("common.R")
source("config.R")

input.size <- function(conf, basedate, clients) {
  du <- dfs.du(conf$fs, sprintf("/user/hive/warehouse/valuepotion_real.db/rc_track_daily/*/p_dt=%s", basedate), summary = TRUE)
  du$client <- gsub("^.*p_clientid=([^/]*)/p_dt=.*$", "\\1", du$file)
  
  du <- merge(du, data.frame(client = clients, stringsAsFactors = FALSE), by = "client")
  sum(du$length, na.rm = TRUE)
}

run.task <- function(command, args, wait = TRUE) {
  status <- system2(command, args, wait = wait)
  if (status != 0) {
    stop(sprintf("%s failed.", command))
  }
}


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
    clients <- d$client_id[i, ]
    
    cat("\n", print.timestamp(), " ** Running analytics job.\n", sep = "")
    cat(sprintf("basedate: %s\n", basedate))
    cat(sprintf("timezone offset: %s\n", offset))
    cat("clients:\n")
    cat(paste(clients, collapse = ","), "\n")
    
    s <- input.size(conf, basedate, clients)
    if (s == 0) {
      cat(sprintf("input size: %f\n", s))
      cat(print.timestamp(), "No input to process.\n")
      next
    }
    
    args <- c(
      "--base-time", basetime, 
      "--base-date", basedate, 
      "--offset", offset
    )

    command <- file.path(getwd(), "daily_summary.R")
    run.task(command, c(args, "--filter", filter))

    tz.basedir <- job.tz.basedir(conf, basedate, offset)
    z <- fs.exists(conf$fs, sprintf("%s/daily_summary", tz.basedir))
    if (z) {
      command <- file.path(getwd(), "attribution.R")
      run.task(command, args)
    
      command <- file.path(getwd(), "attributes.R")
      run.task(command, args)
    
      command <- file.path(getwd(), "balancer.R")
      run.task(command, args, wait = FALSE)
    
      command <- file.path(getwd(), "daily_statistics.R")
      run.task(command, args, wait = FALSE)
    }

    command <- file.path(getwd(), "periodic_statistics.R")
    run.task(command, args, wait = FALSE)

    if (z) {
      command <- file.path(getwd(), "usermeta.R")
      run.task(command, args)
    }
  }
} else {
  cat(print.timestamp(), "No clients to process.\n")
}
