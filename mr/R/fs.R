rexplist2list <- function(rexplist) {
	if (is.jnull(rexplist)) {
		return(list())
	}
	
	rlist <- rexplist$asList()
	size <- rlist$size()
	if (size == 0) {
		return(list())
	}

	rexps <- lapply(0:(size-1), function(i) { rlist$get(i) })
	lst <- lapply(rexps, 
			function(rexp) {
				if (rexp$isString()) {
					return(rexp$asStrings())
				} else if (rexp$isNumeric()) {
					return(rexp$asDoubles())
				} else if (rexp$isInteger()) {
					return(rexp$asIntegers())
				} else if (rexp$isFactor()) {
					return(rexp$asFactor())
				} else if (rexp$isLogical()) {
					return(rexp$isTrue())
				} else {
					return(rexp$asStrings())
				}
			})

	names <- rlist$keys()
	if (!is.jnull(names) && length(names) > 0) {
		return(setNames(lst, names))
	} else {
		return(lst)
	}
}

dfs.ls <- function(fs.default, path = "/") {
	fs.utils <- J("com/valuepotion/mrtools/FileSystemUtils")
	rexplist <- fs.utils$ls(path, fs.default, Sys.info()["user"])
	
	lst <- rexplist2list(rexplist)
	return(as.data.frame(lst, stringsAsFactors = FALSE))
}

dfs.du <- function(fs.default, path="/", summary=FALSE) {
	fs.utils <- J("com/valuepotion/mrtools/FileSystemUtils")

	rexplist <- .jnull()
	if (summary) {
		rexplist <- fs.utils$dus(path, fs.default, Sys.info()["user"])
	} else {
		rexplist <- fs.utils$du(path, fs.default, Sys.info()["user"])
	}

	lst <- rexplist2list(rexplist)
	return(as.data.frame(lst, stringsAsFactors = FALSE))
}

dfs.put <- function(fs.default, src, dst, src.del = FALSE, overwrite = FALSE) {
	check.fs.default(fs.default)

	fs.utils <- J("com/valuepotion/mrtools/FileSystemUtils")
	fs.utils$copyFromLocal(src.del, overwrite, src, dst, fs.default, Sys.info()["user"])
}

dfs.get <- function(fs.default, src, dst, src.del = FALSE) {
	check.fs.default(fs.default)

	fs.utils <- J("com/valuepotion/mrtools/FileSystemUtils")
	fs.utils$copyToLocal(src.del, src, dst, fs.default, Sys.info()["user"])
}

dfs.rm <- function(fs.default, ...) {
	check.fs.default(fs.default)

	fs.utils <- J("com/valuepotion/mrtools/FileSystemUtils")
	res <- c()
	for (target in c(...)) {
		res <- c(res, fs.utils$delete(target, fs.default, Sys.info()["user"]))
	}
	return(res)
}

dfs.rename <- function(fs.default, src, dst) {
	check.fs.default(fs.default)

	fs.utils <- J("com/valuepotion/mrtools/FileSystemUtils")
	return(fs.utils$rename(src, dst, fs.default, Sys.info()["user"]))
}

dfs.exists <- function(fs.default, path) {
	check.fs.default(fs.default)

	fs.utils <- J("com/valuepotion/mrtools/FileSystemUtils")
	return(fs.utils$exists(path, fs.default, Sys.info()["user"]))
}

dfs.mkdirs <- function(fs.default, path) {
	check.fs.default(fs.default)

	fs.utils <- J("com/valuepotion/mrtools/FileSystemUtils")
	return(fs.utils$mkdirs(path, fs.default, Sys.info()["user"]))
}

dfs.cat <- function(fs.default, path) {
	check.fs.default(fs.default)

	fs.utils <- J("com/valuepotion/mrtools/FileSystemUtils")
	fs.utils$cat(path, fs.default, Sys.info()["user"])
	invisible()
}

dfs.tail <- function(fs.default, path, kB = 1L) {
	check.fs.default(fs.default)

	fs.utils <- J("com/valuepotion/mrtools/FileSystemUtils")
	fs.utils$tail(path, as.integer(kB), fs.default, Sys.info()["user"])
	invisible()
}

dfs.chmod <- function(fs.default, option, path, recursive = FALSE) {
	check.fs.default(fs.default)

	fs.utils <- J("com/valuepotion/mrtools/FileSystemUtils")
	fs.utils$chmod(path, option, recursive, fs.default, Sys.info()["user"])
	invisible()
}

dfs.chown <- function(fs.default, option, path, recursive = FALSE) {
	check.fs.default(fs.default)

	fs.utils <- J("com/valuepotion/mrtools/FileSystemUtils")
	fs.utils$chown(path, option, recursive, fs.default, Sys.info()["user"])
	invisible()
}

dfs.chgrp <- function(fs.default, option, path, recursive = FALSE) {
	check.fs.default(fs.default)

	fs.utils <- J("com/valuepotion/mrtools/FileSystemUtils")
	fs.utils$chgrp(path, option, recursive, fs.default, Sys.info()["user"])
	invisible()
}
