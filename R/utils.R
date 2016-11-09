#' Title
#'
#' @param url
#'
#' @return
#' @export
#'
#' @examples
valid_url <- function(url) {
  TRUE
}


#' Title
#'
#' @param colnames
#'
#' @return
#' @export
#'
#' @examples
cleaned_field_names <- function(colnames) {
  tolower(gsub("\\.", "_", colnames))
}


#' Title
#'
#' @param action
#' @param index
#' @param doc_type
#' @param id
#' @param n
#'
#' @return
#' @export
#'
#' @examples
create_metadata <- function(action, index, doc_type, id = NULL, n = NULL) {
  if (!is.null(id)) {
    metadata <- paste0('{"', action, '": {"_index": "', index, '", "_type": "', doc_type, '", "_id": "', id, '"}}')
  } else if (!is.null(n)) {
    metadata_line <- paste0('{"', action, '": {"_index": "', index, '", "_type": "', doc_type, '"}}')
    metadata <- rep(metadata_line, n)
  } else {
    metadata <- paste0('{"', action, '": {"_index": "', index, '", "_type": "', doc_type, '"}}')
  }

  metadata
}


#' Title
#'
#' @param metadata
#' @param df
#'
#' @return
#' @export
#'
#' @examples
create_bulk_upload_file <- function(metadata, df) {

  jsonlite::stream_out(df, file(temp_filename1 <- tempfile()), POSIXt = "ISO8601",
                       pagesize = 1000, verbose = FALSE, digits = 8, always_decimal = TRUE)

  json_data <- readLines(temp_filename1)
  file.remove(temp_filename1)

  final_upload_data <- rep(NA, length(json_data) * 2)
  idx_even <- seq(2, length(json_data) * 2, 2)
  idx_odd <- seq(1, length(json_data) * 2, 2)
  final_upload_data[idx_odd] <- metadata
  final_upload_data[idx_even] <- json_data

  writeLines(final_upload_data, file(temp_filename2 <- tempfile()))
  closeAllConnections()

  temp_filename2
}


#' Title
#'
#' @param rescource
#' @param api_call_payload
#'
#' @return
#' @export
#'
#' @examples
from_size_search <- function(rescource, api_call_payload) {

  response <- httr::POST(rescource$search_url, body = api_call_payload)
  if (httr::status_code(response) != 200) {
    stop(paste0("response from server: ", httr::status_code(response)))
  }

  parsed_response <- jsonlite::fromJSON(httr::content(response, as = 'text'))
  if ("aggregations" %in% names(parsed_response)) {
    if (length(parsed_response$aggregations[[1]]$buckets) == 0) stop("empty response to request")
    return_data <- jsonlite::flatten(parsed_response$aggregations[[1]]$buckets)
  } else {
    if (length(parsed_response$hits$hits$`_source`) == 0) stop("empty response to request")
    return_data <- jsonlite::flatten(parsed_response$hits$hits$`_source`)
  }

  return_data
}
