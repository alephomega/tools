
config <- function(basetime) { 

  list (
    fs = "hdfs://on-hadoop-master1.daum.net:8020",
    jt = "on-hadoop-master1.daum.net:8021",
    jar = "analytics.jar",
    
    filter = list (
      driver = "PostgreSQL",
      user = "front4vp",
      password = "onproject!1",
      dbname = "valuepotion",
      host = "pg-dashboard.cv0lwhza2lbz.ap-southeast-1.rds.amazonaws.com",
      port = 5432L,
      SQL = sprintf(
        "SELECT a.client_id, a.timezone, TO_CHAR(((TO_TIMESTAMP('%s', 'YYYYMMDDHH24MI')::TIMESTAMP WITHOUT TIME ZONE AT TIME ZONE 'Asia/Seoul') AT TIME ZONE b.name), 'YYYYMMDD') AS basedate, TO_CHAR(b.utc_offset, 'HH24MI') AS offset FROM apps a, pg_timezone_names b WHERE a.company_id <> 5 AND a.status_id = 2 AND a.timezone = b.name AND TO_CHAR(((TO_TIMESTAMP('%s', 'YYYYMMDDHH24MI')::TIMESTAMP WITHOUT TIME ZONE AT TIME ZONE 'Asia/Seoul') AT TIME ZONE b.name), 'HH24MI') BETWEEN '0000' AND '0029'",
        basetime, basetime
      )
    ),
    
    job = list (
      base_dir = "/user/hdfs/analytics",
      tasks = list (
        daily_summary = list (
          main = "com.valuepotion.analytics.DailySummaryAnalytics",
          properties = list (
            hive.metastore.uris = "thrift://on-hadoop-master2.daum.net:9083",
            mapred.reduce.tasks = 60,
            mapred.task.timeout = 0,
            mapred.child.java.opts = "-Xmx1024m",
            io.file.buffer.size = 65536,
            io.sort.mb = 512,
            mapred.compress.map.output = TRUE,
            mapred.map.output.compression.codec = "org.apache.hadoop.io.compress.SnappyCodec",
            mapred.output.compress = TRUE,
            mapred.output.compression.codec = "org.apache.hadoop.io.compress.SnappyCodec"
          ),
          
          input = list (
            database = "valuepotion_real",
            table = "rc_track_daily",
            columns = c("p_clientid", "deviceid", "session", "revenueamount", "currency", "eventname", "eventid", "dt", "deviceos", "deviceosversion", "appversion", "devicemodelname", "country", "district", "userinfo_birth", "userinfo_gender", "userinfo_level")
          ),
          overwrite = TRUE
        ),
        
        attribution = list (
          main = "com.valuepotion.analytics.AttributionJoinAnalytics",
          properties = list (
            mapred.reduce.tasks = 15,
            mapred.task.timeout = 0,
            mapred.max.split.size = 67108864,
            mapred.child.java.opts = "-Xmx1024m",
            io.file.buffer.size = 65536,
            io.sort.mb = 512,
            mapred.compress.map.output = TRUE,
            mapred.map.output.compression.codec = "org.apache.hadoop.io.compress.SnappyCodec",
            mapred.output.compress = TRUE,
            mapred.output.compression.codec = "org.apache.hadoop.io.compress.SnappyCodec"
          ),
          attribution_hdfs = "/user/hdfs/attribution/by_hour",
          overwrite = TRUE
        ),
    
        attributes = list (
          main = "com.valuepotion.analytics.AttributesAnalytics",
          properties = list (
            mapred.reduce.tasks = 60,
            mapred.task.timeout = 0,
            mapred.job.shuffle.input.buffer.percent = 0.3,
            mapred.max.split.size = 67108864,
            mapred.child.java.opts = "-Xmx1024m",
            io.file.buffer.size = 65536,
            io.sort.mb = 512,
            mapred.compress.map.output = TRUE,
            mapred.map.output.compression.codec = "org.apache.hadoop.io.compress.SnappyCodec",
            mapred.output.compress = TRUE,
            mapred.output.compression.codec = "org.apache.hadoop.io.compress.SnappyCodec"
          ),
          
          defection = 365L,
          risk = 30L,
          overwrite = TRUE
        ),
        
        balancer = list (
          main = "com.valuepotion.analytics.DoNothing",
          properties = list (
            mapred.max.split.size = 67108864,
            mapred.child.java.opts = "-Xmx1024m",
            io.file.buffer.size = 65536,
            io.sort.mb = 512
          ),
          overwrite = TRUE
        ),

        drop = list(
          properties = list(
            hive.host = "on-hadoop-master2.daum.net",
            hive.port = 10000L,
            hive.db = "valuepotion_real",
            hive.table = "usermeta"
          )
        ),

        usermeta = list (
          main = "com.valuepotion.analytics.legacy.Usermeta",
          properties = list (
            mapred.reduce.tasks = 60,
            mapred.task.timeout = 0,
            mapred.job.shuffle.input.buffer.percent = 0.3,
            mapred.max.split.size = 67108864L,
            mapred.child.java.opts = "-Xmx1024m",
            io.file.buffer.size = 65536,
            io.sort.mb = 512,
            mapred.compress.map.output = TRUE,
            mapred.map.output.compression.codec = "org.apache.hadoop.io.compress.SnappyCodec",
            mapred.reduce.tasks.speculative.execution = FALSE,
            hive.metastore.uris = "thrift://on-hadoop-master2.daum.net:9083"
          ), 

          output = list (
            database = "valuepotion_real",
            table = "usermeta"
          )
        ),

        daily_statistics = list (
          main = "com.valuepotion.analytics.DailyAnalytics",
          properties = list (
            mapred.reduce.tasks = 40,
            mapred.task.timeout = 0,
            mapred.max.split.size = 67108864,
            mapred.child.java.opts = "-Xmx1024m",
            io.file.buffer.size = 65536,
            io.sort.mb = 512
          ),

          overwrite = TRUE
        ),

        periodic_statistics = list (
          main = "com.valuepotion.analytics.PeriodicAnalytics",
          properties = list (
            mapred.reduce.tasks = 5,
            mapred.task.timeout = 0,
            mapred.max.split.size = 67108864,
            mapred.child.java.opts = "-Xmx1024m",
            io.file.buffer.size = 65536,
            io.sort.mb = 512
          ),

          overwrite = TRUE
        )
      )
    )
  )
}
