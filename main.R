#!/usr/bin/Rscript --vanilla

library(RPostgreSQL)
library(MR1.CDH4.7.1)


print.timestamp <- function() {
  strftime(Sys.time(), format="%Y-%m-%d %H:%M:%S") 
}

is.empty <- function(x) {
  identical(x, NA) || is.null(x) || length(x) == 0
}

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

basetime <- function() {
  args <- commandArgs(TRUE)
  
  t0 <- NA
  m <- match("--base-time", args, 0L)
  if (m) {
    t0 <- args[m + 1]
  }
  
  t0
}


setwd(basedir())
source("config.R")

cat(print.timestamp(), "Reading configurations.\n")
print(basetime())
conf <- config(basetime())
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
      offset <- sprintf("B%04s", substr(d$offset[i], 2, length(d$offset[i])))  
    } else {
      offset <- sprintf("A%04s", d$offset[i])
    }

    d$client_id <- as.matrix(d$client_id)
    filter <- sprintf("\"p_dt = '%s' and (%s)\"", basedate, paste(sprintf("p_clientid = '%s'", d$client_id[i, ]), collapse = " or "))
    clients <- paste(d$client_id[i, ], collapse = ",")
    
    cat(print.timestamp(), "Running analytics job.\n")
    cat(sprintf("basedate: %s\n", basedate))
    cat(sprintf("timezone offset: %s\n", offset))
    cat("clients:\n")

    print(clients)
    
    task <- conf$job$tasks$daily_summary
    args <- c(
      "--base-date", basedate, 
      "--database", task$input$database, 
      "--table", task$input$table,
      "--columns", paste(task$input$columns, collapse = ","),
      "--filter", filter,
      "--output", sprintf("%s/%s/%s/daily_summary", conf$job$base_dir, basedate, offset),
      "--intermediate", sprintf("%s/%s/%s/daily_summary.tmp", conf$job$base_dir, basedate, offset)
    )
    
    if (task$overwrite) {
      args <- c(args, "--overwrite")
    }
    
    cat(print.timestamp(), "Running daily-summary task.\n")
    cat("properties:\n")
    print(task$properties)
    cat("args:\n")
    cat(paste(args, collapse = ", "), "\n")
    
    mr.run(
      fs = conf$fs, 
      jt = conf$jt, 
      jar = file.path(getwd(), "lib", conf$jar), 
      class = task$main,
      args = args,
      props = task$properties
    )
    
    task <- conf$job$tasks$attribution
    args <- c(
      "--base-date", basedate, 
      "--input", paste(sprintf("%s/%s/%s/daily_summary/*", conf$job$base_dir, basedate, offset), sprintf("%s/%s/**/part-*", task$attribution_base_dir, basedate), sep = ","),
      "--output", sprintf("%s/%s/%s/daily_summary/attribution", conf$job$base_dir, basedate, offset)
    )
    
    if (task$overwrite) {
      args <- c(args, "--overwrite")
    }
    
    cat(print.timestamp(), "Running attribution task.\n")
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

    task <- conf$job$tasks$attributes

    offset.exists <- (nrow(dfs.ls(conf$fs, sprintf("%s/customer_attributes/%s", conf$job$base_dir, offset))) > 0)
    inputs <- sprintf("%s/%s/%s/daily_summary/attribution/*", conf$job$base_dir, basedate, offset)
    if (offset.exists) {
      inputs <- c(inputs, sprintf("%s/customer_attributes/%s/*", conf$job$base_dir, offset))
    }

    args <- c(
      "--base-date", basedate, 
      "--defection", task$defection,
      "--risk", task$risk,
      "--input", paste(inputs, collapse = ","),
      "--output", sprintf("%s/%s/%s/customer_attributes", conf$job$base_dir, basedate, offset)
    )
    
    if (task$overwrite) {
      args <- c(args, "--overwrite")
    }
    
    cat(print.timestamp(), "Running customer-attributes task.\n")
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
    
    task <- conf$job$tasks$balancer
    args <- c(
      "--base-date", basedate, 
      "--input", sprintf("%s/%s/%s/customer_attributes/{NEW,ACTIVE,ATLISK,WINBACK,UNKNOWN}-*", conf$job$base_dir, basedate, offset),
      "--output", sprintf("%s/customer_attributes/%s", conf$job$base_dir, offset)
    )
    
    if (task$overwrite) {
      args <- c(args, "--overwrite")
    }

    du <- dfs.du(conf$fs, sprintf("%s/%s/%s/customer_attributes/{NEW,ACTIVE,ATLISK,LOST,WINBACK,UNKNOWN}-*", conf$job$base_dir, basedate, offset))
    if (nrow(du) == 0) {
      reduce.tasks <- 10L
    } else {
      reduce.tasks <- as.integer(sum(du$length, na.rm = TRUE) / (128 * 1024 * 1024)) + 1
    }
   
    props = task$properties 
    props$mapred.reduce.tasks = reduce.tasks

    cat(print.timestamp(), "Running balancer task.\n")
    cat("properties:\n")
    print(props)
    cat("args:\n")
    print(args)
    
    mr.run(
      fs = conf$fs, 
      jt = conf$jt, 
      jar = file.path(getwd(), "lib", conf$jar), 
      class = task$main,
      args = args,
      props = props
    )

    task <- conf$job$tasks$usermeta 
    args <- c(
      "--base-date", basedate, 
      "--input", sprintf("%s/%s/%s/customer_attributes/{NEW,ACTIVE,ATLISK,WINBACK,UNKNOWN}-*", conf$job$base_dir, basedate, offset),
      "--database", task$output$database,
      "--table", task$output$table
    )

    cat(print.timestamp(), "Running usermeta task.\n")
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

} else {
  cat(print.timestamp(), "No clients to process.\n")
}
