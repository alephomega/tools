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

fs.exists <- function(fs, input) {
  du <- subset(dfs.du(fs, input), subset = !grepl("/[_\\.][^/]*$", file), select = length)
  sum(du$length, na.rm = TRUE) > 0
}


setwd(basedir())
source("config.R")

cat(print.timestamp(), "Reading configurations.\n")
cat(sprintf("basetime: %s\n\n", basetime()))

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
      offset <- sprintf("B%04d", as.integer(substr(d$offset[i], 2, nchar(d$offset[i]))))  
    } else {
      offset <- sprintf("A%04d", as.integer(d$offset[i]))
    }
    
    d$client_id <- as.matrix(d$client_id)
    filter <- sprintf("\"p_dt = '%s' and (%s)\"", basedate, paste(sprintf("p_clientid = '%s'", d$client_id[i, ]), collapse = " or "))
    clients <- paste(d$client_id[i, ], collapse = ",")
    
    cat("\n")
    cat(print.timestamp(), "** Running analytics job.\n")
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
    
    cat("\n")
    cat(print.timestamp(), "* Running daily-summary.\n")
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
    
    z <- fs.exists(conf$fs, sprintf("%s/%s/%s/daily_summary", conf$job$base_dir, basedate, offset))
    if (z) {
      task <- conf$job$tasks$attribution
      args <- c(
        "--base-date", basedate, 
        "--input", paste(sprintf("%s/%s/%s/daily_summary/*", conf$job$base_dir, basedate, offset), sprintf("%s/%s/**/part-*", task$attribution_hdfs, basedate), sep = ","),
        "--output", sprintf("%s/%s/%s/daily_summary/attribution", conf$job$base_dir, basedate, offset)
      )
      
      if (task$overwrite) {
        args <- c(args, "--overwrite")
      }
      
      cat("\n")
      cat(print.timestamp(), "* Running attribution.\n")
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
    
    if (z) {
      task <- conf$job$tasks$attributes
      exists <- (nrow(dfs.ls(conf$fs, sprintf("%s/attributes/%s", conf$job$base_dir, offset))) > 0)
      inputs <- sprintf("%s/%s/%s/daily_summary/attribution/*", conf$job$base_dir, basedate, offset)
      if (exists) {
        inputs <- c(inputs, sprintf("%s/attributes/%s/*", conf$job$base_dir, offset))
      }
      
      args <- c(
        "--base-date", basedate, 
        "--defection", task$defection,
        "--risk", task$risk,
        "--input", paste(inputs, collapse = ","),
        "--output", sprintf("%s/%s/%s/attributes", conf$job$base_dir, basedate, offset)
      )
      
      if (task$overwrite) {
        args <- c(args, "--overwrite")
      }
      
      cat("\n")
      cat(print.timestamp(), "* Running customer-attributes.\n")
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
    
    if (z) {
      task <- conf$job$tasks$balancer
      args <- c(
        "--base-date", basedate, 
        "--input", sprintf("%s/%s/%s/attributes/{NEW,ACTIVE,ATRISK,WINBACK,UNKNOWN}-*", conf$job$base_dir, basedate, offset),
        "--output", sprintf("%s/attributes/%s", conf$job$base_dir, offset)
      )
      
      if (task$overwrite) {
        args <- c(args, "--overwrite")
      }
      
      du <- dfs.du(conf$fs, sprintf("%s/%s/%s/attributes/{NEW,ACTIVE,ATRISK,LOST,WINBACK,UNKNOWN}-*", conf$job$base_dir, basedate, offset))
      reduce.tasks <- as.integer(sum(du$length, na.rm = TRUE) / (128 * 1024 * 1024)) + 1
      
      props = task$properties 
      props$mapred.reduce.tasks = reduce.tasks
      
      cat("\n")
      cat(print.timestamp(), "* Running balancer.\n")
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
    }
    
    if (z) {
      task <- conf$job$tasks$usermeta 
      args <- c(
        "--base-date", basedate, 
        "--input", sprintf("%s/%s/%s/attributes/{NEW,ACTIVE,ATRISK,WINBACK,UNKNOWN}-*", conf$job$base_dir, basedate, offset),
        "--database", task$output$database,
        "--table", task$output$table
      )
      
      cat("\n")
      cat(print.timestamp(), "* Running usermeta.\n")
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
    
    if (z) {
      task <- conf$job$tasks$daily_statistics 
      args <- c(
        "--base-date", basedate, 
        "--input", sprintf("%s/%s/%s/attributes/part-*", conf$job$base_dir, basedate, offset),
        "--output", sprintf("%s/%s/%s/daily_statistics", conf$job$base_dir, basedate, offset)
      )
      
      cat("\n")
      cat(print.timestamp(), "* Running daily-statistics.\n")
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
    
    z <- fs.exists(conf$fs, sprintf("%s/attributes/%s", conf$job$base_dir, offset))
    if (z) {
      task <- conf$job$tasks$periodic_statistics 
      args <- c(
        "--base-date", basedate, 
        "--input", sprintf("%s/attributes/%s/*", conf$job$base_dir, offset),
        "--output", sprintf("%s/%s/%s/periodic_statistics", conf$job$base_dir, basedate, offset)
      )
      
      cat("\n")
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
  }
} else {
  cat(print.timestamp(), "No clients to process.\n")
}
