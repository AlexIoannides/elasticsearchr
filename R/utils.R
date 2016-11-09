cleaned_field_names <- function(colnames) {
  tolower(gsub("\\.", "_", colnames))
}


create_metadata <- function(action, index, doc_type, id = NULL) {
  if (is.null(id)) {
    metadata <- paste0('{"', action, '": {"_index": "', index, '", "_type": "', doc_type, '"}}')
  } else {
    metadata <- paste0('{"', action, '": {"_index": "', index, '", "_type": "', doc_type, '", "_id": "', id, '"}}')
  }

  metadata
}


create_bulk_upload_file <- function(metadata, df) {

  jsonlite::stream_out(df, temp_conn1 <- file(temp_filename1 <- tempfile()), POSIXt = "ISO8601",
                       pagesize = 1000, verbose = FALSE, digits = 8, always_decimal = TRUE)

  json_data <- readLines(temp_filename1)
  file.remove(temp_filename1)

  final_upload_data <- rep(NA, length(json_data) * 2)
  idx_even <- seq(2, length(json_data) * 2, 2)
  idx_odd <- seq(1, length(json_data) * 2, 2)
  final_upload_data[idx_odd] <- metadata
  final_upload_data[idx_even] <- json_data

  writeLines(final_upload_data, temp_conn2 <- file(temp_filename2 <- tempfile()))
  closeAllConnections()

  temp_filename2
}
