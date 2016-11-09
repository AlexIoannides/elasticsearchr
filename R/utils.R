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
  check_http_code_throw_error(response)

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


scroll_search <- function(rescource, api_call_payload) {
  scroll_search_url <- paste0(rescource$cluster_url, "/_search/scroll")
  scroll_results <- list()

  initial_scroll_search_url <- paste0(rescource$search_url, "?scroll=1m")
  initial_response <- httr::POST(initial_scroll_search_url, body = api_call_payload)
  check_http_code_throw_error(initial_response)

  scroll_results[[1]] <- extract_query_results(initial_response)
  next_scroll_id <- httr::content(initial_response)$`_scroll_id`
  has_next <- TRUE
  n <- 2
  while (has_next) {
    cat("...")
    next_api_payload <- paste0('{"scroll": "1m", "scroll_id": "', next_scroll_id, '"}')
    next_response <- httr::POST(scroll_search_url, body = next_api_payload)
    check_http_code_throw_error(next_response)
    if(length(content(next_response)$hits$hits) > 0) {
      scroll_results[[n]] <- extract_query_results(next_response)
      next_scroll_id <- httr::content(next_response)$`_scroll_id`
      n <- n + 1
    } else {
      has_next <- FALSE
    }
  }

  do.call(rbind, scroll_results)
}


extract_query_results <- function(response) {
  jsonlite::flatten(jsonlite::fromJSON(httr::content(response, as = 'text'))$hits$hits$`_source`)
}


extract_aggs_results <- function(response) {
  jsonlite::flatten(jsonlite::fromJSON(httr::content(response, as = 'text'))$aggregations[[1]]$buckets)
}


check_http_code_throw_error <- function(response) {
  if (httr::status_code(response) != 200) {
    stop(paste("Elasticsearch returned a status code of", httr::status_code(response), "\n"),
         jsonlite::prettify(response))
  }
}
