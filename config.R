
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
        "SELECT a.client_id, a.timezone, TO_CHAR(((TO_TIMESTAMP('%s', 'YYYYMMDDHH24MI')::TIMESTAMP WITHOUT TIME ZONE AT TIME ZONE 'Asia/Seoul') AT TIME ZONE b.name), 'YYYYMMDD') AS basedate, TO_CHAR(b.utc_offset, 'HH24MI') AS offset FROM apps a, pg_timezone_names b WHERE a.company_id <> 5 AND (a.status_id = 2 or a.id in (SELECT DISTINCT app_id FROM tracking_links)) AND a.timezone = b.name AND TO_CHAR(((TO_TIMESTAMP('%s', 'YYYYMMDDHH24MI')::TIMESTAMP WITHOUT TIME ZONE AT TIME ZONE 'Asia/Seoul') AT TIME ZONE b.name), 'HH24MI') BETWEEN '0000' AND '0059'",
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
            mapred.reduce.tasks = 30
          ),
          
          input = list (
            database = "valuepotion_real",
            table = "rc_track_daily",
            columns = c("p_clientid", "deviceid", "session", "revenueamount", "currency", "eventname", "eventid", "dt", "deviceos", "deviceosversion", "appversion", "devicemodelname", "country", "userinfo_birth", "userinfo_gender", "userinfo_level")
          ),
          overwrite = TRUE
        ),
        
        attribution = list (
          main = "com.valuepotion.analytics.AttributionJoinAnalytics",
          properties = list (
            mapred.reduce.tasks = 10,
            mapred.max.split.size = 67108864
          ),
          attribution_hdfs = "/user/hdfs/attribution/by_hour",
          overwrite = TRUE
        ),
    
        attributes = list (
          main = "com.valuepotion.analytics.AttributesAnalytics",
          properties = list (
            mapred.reduce.tasks = 50,
            mapred.job.shuffle.input.buffer.percent = 0.3,
            mapred.max.split.size = 67108864
          ),
          
          defection = 365L,
          risk = 30L,
          overwrite = TRUE
        ),
        
        balancer = list (
          main = "com.valuepotion.analytics.DoNothing",
          properties = list (
            mapred.max.split.size = 67108864
          ),
          overwrite = TRUE
        ),
        
        usermeta = list (
          main = "com.valuepotion.analytics.legacy.Usermeta",
          properties = list (
            mapred.reduce.tasks = 50,
            mapred.max.split.size = 67108864L,
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
            mapred.reduce.tasks = 30,
            mapred.max.split.size = 67108864
          ),

          overwrite = TRUE
        ),

        periodic_statistics = list (
          main = "com.valuepotion.analytics.PeriodicAnalytics",
          properties = list (
            mapred.reduce.tasks = 5,
            mapred.max.split.size = 67108864
          ),

          overwrite = TRUE
        )
      )
    )
  )
}
