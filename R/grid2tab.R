read_tile <- function(ds, tile, size) {
  do.call(cbind,
                 lapply(seq_len(size[3]), \(band)
                        ds$read(band, tile$offset_x, tile$offset_y, tile$ncol, tile$nrow,tile$ncol, tile$nrow)))
}

chunk_size <- function(size, at_a_time) {
    if (at_a_time >= prod(size)) chunk <- prod(size) else chunk <- at_a_time
    chunk
}
parquet_i <- function(root, i, pattern = "file%03i_") {
   tempfile(tmpdir = root, pattern = sprintf(pattern, i), fileext = ".parquet")
}
grid2parquet <- function(x, root, at_a_time = 1e7) {
  tablename <- dirname(root)
  ds <- new(gdalraster::GDALRaster, x)
  size <- ds$dim()

  chunk <- chunk_size(size, at_a_time)
  crs <- ds$getProjectionRef()

  extent <- ds$bbox()[c(1, 3, 2, 4)]
#browser()
  idx <- grout::tile_index(grout::grout(extent, dimension = size[1:2], projection = crs,
                                        blocksize = c(size[1], ceiling(prod(size[1:2])/chunk))))


  print(idx)
  return(NULL)
  filename <- character(nrow(idx))

  for (i in seq_along(idx$tile)) {
    tile <- idx[i, ]

    v <- read_tile(ds, tile, size)
    colnames(v) <- paste0("V", seq_len(size[3]))
    v <- tibble::as_tibble(v)
    file <- parquet_i(root, i)
    arrow::write_parquet(v, file)
   filename[i] <- file
  }

  filename
}


grid2duck <- function(x, filename, at_a_time = 1e7) {
  tablename <- tools::file_path_sans_ext(basename(filename))
  ds <- new(gdalraster::GDALRaster, x)
  size <- ds$dim()
  con <- DBI::dbConnect(duckdb::duckdb(), filename)

  df <- as.data.frame(replicate(1, size[3], simplify = F))[0, , drop  = F]
  DBI::dbWriteTable(con,  tablename, df)
  qm <- paste0(rep("?", ncol(df)), collapse = ", ")

  if (at_a_time > prod(size)) chunk <- prod(size[1:2]) else chunk <- ceiling(prod(size) / at_a_time)
  crs <- ds$getProjectionRef()

  extent <- ds$bbox()[c(1, 3, 2, 4)]

  idx <- grout::tile_index(grout::grout(extent, dimension = size[1:2], projection = crs,
                                        blocksize = c(size[1], as.integer(chunk))))
  for (i in seq_along(idx$tile)) {
    tile <- idx[i, ]

    v <- do.call(cbind,
                 lapply(seq_len(size[3]), \(band)
                        ds$read(band, tile$offset_x, tile$offset_y, tile$ncol, tile$nrow,tile$ncol, tile$nrow)))

    colnames(v) <- names(df)
    v <- tibble::as_tibble(v)

    DBI::dbWriteTable(con, tablename, v, append = TRUE)
   print(i)
  }

  rm(con)
  filename
}
