mr.run <- function(fs, jt, jar, class, args = character(0), props = list(0), files = character(0), libjars = character(0), archives = character(0)) {
  properties <- .jnew("java/util/Properties")
  properties$setProperty("fs.default.name", fs)
  properties$setProperty("mapred.job.tracker", jt)
  
  if (length(props) > 0) {
    for (name in names(props)) {
      properties$setProperty(name, as.character(props[[name]])) 
    }
  }
  
  HadoopJar <- J("com.valuepotion.mrtools.HadoopJar")
  HadoopJar$run(jar, class, properties, args, files, libjars, archives)
}
