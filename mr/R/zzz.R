library(utils)

.onLoad <- function(libname, pkgname) {
  pkdesc <- packageDescription(pkgname, lib.loc = libname, fields = "Version", drop = TRUE)
  .jpackage(pkgname)

  invisible()
}
