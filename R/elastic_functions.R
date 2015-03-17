#' elasticr: A package for making working with Elasticsearch easy from R.
#'
#' @section elasticr functions:
#' elastic_conn
#'
#' @docType package
#' @name elasticr
NULL

#' Define a 'connection' to an Elasticsearch server and test it.
#'
#' @export
#'
#' @param url location of the Elasticsearch server that contains the index and type we're interestd in working with (defaults to expected local machine port 9200)
#' @param index name of the index we want to work with.
#' @param type name of the type we want to work with (optional).
#' @param test_rescource should the Elasticsearch server be pinged at the specified index and type?
#' @return A list containing the the location of the Elasticsearch server, the index and type that we want to work with (as well as the index/type path combined).
#' @examples
#' elastic_conn('http://localhost:9200', 'my_index', 'my_type', TRUE)
elastic_conn <- function (url = 'http://localhost:9200', index = '', type = '', test_rescource = FALSE) {
  tryCatch({
    stopifnot(is.character(url),
              is.character(index),
              is.character(type),
              !stringr::str_detect(url, '([a-zA-Z0-9]/|[a-zA-Z0-9]/[a-zA-Z0-9])'),
              !stringr::str_detect(index, '/'),
              !stringr::str_detect(type, '/'))
  }, error = function(e) stop('Elasticsearch details not specified correctly', call. = FALSE))

  index_and_type = if (type != '') paste(index, type, sep = '/') else index

  if (test_rescource) {
    try({
      if (httr::status_code(httr::HEAD(url)) != 200) {
        warning('failed to connect to Elasticsearch', call. = FALSE)
      } else if (httr::status_code(httr::HEAD(url, path = index)) != 200) {
        warning('failed to ping specified index', call. = FALSE)
      } else if (httr::status_code(httr::HEAD(url, path = index_and_type)) != 200) {
        warning('failed to ping specified type', call. = FALSE)
      }
    })
  }

  list(elastic_url = url, elastic_index = index, elastic_type = type, elastic_index_type = index_and_type)
}

#' Create a valid Elasticsearch date-based index name.
#'
#' @export
#'
#' @param elastic_index_name the base (or stem) part of the index name - e.g. the 'logs' part of 'logs-2015.01.01'.
#' @param date POSIXct object or a string describing the date with the format YYYY-MM-DD.
#' @return A charcter string representing a valid Elasticsearch date-based index name.
#' @examples
#' elastic_create_dated_index_name('logs', '2015-01-31')
#' elastic_create_dated_index_name('logs', Sys.time())
elastic_create_dated_index_name <- function(elastic_index_name, date) {
  tryCatch({
    stopifnot(is.character(elastic_index_name),
              any(!is.na(lubridate::ymd(date, quiet = TRUE)), !is.na(lubridate::ymd_hms(date, quiet = TRUE))))
  }, error = function(e) stop('index and/or date in not specified correctly', call. = FALSE))
  if ("POSIXct" %in% class(date)) {
    date_only <- as.character(lubridate::round_date(lubridate::ymd_hms(date), unit = 'day'))
  } else {
    date_only <- date
  }
  date_only <- stringr::str_replace_all(date_only, '-', '.')
  paste0(elastic_index_name, '-', date_only)
}

#' Facilitate interaction with Elasticsearch's bulk API for CRUD operations.
#'
#' Elasticsearch's bulk API can be awkward to use directly due to it's peculiar format. However, it remains the only way to interact with Elasticsearch
#' documents at scale. These functions help by allowing documents, document fields, and document metadata (e.g. index = 'my_index', type = 'my_type', id = '001'),
#' to be described using data frames, upon which a single bulk action can be performed ('index', 'update', or 'delete').
#'
#' The heavy-lifting is performed by \code{elastic_bulk_file}, which generates the required JSON-based representation of the data in a text file.
#' All \code{elastic_bulk} does is to upload this text file to to Elasticsearch's bulk API endpoint. \code{elastic_bulk_index_batched} splits data to be indexed into
#' pieces first, and then applies \code{elastic_bulk} individually to each piece - the idea being that for large data each bulk upload operation can be kept to
#' under 15mb, which is the largest size recommended by Elasticsearch for use with the bulk API.
#'
#' @export
#'
#' @param elastic_conn connection details for the Elasticsearch server we want to access (and optionally the index and type too).
#' @param action one of 'index', 'update' or 'delete'.
#' @param data data.frame object with multiple columns representing document field values.
#' @param metadata data.frame object containing document metadata (index, type, id). If omitted (or NULL), then documents will automatically be indexed (i.e. created).
#' @return \code{elastic_bulk}: TRUE or FALSE depending on the success of the http request.
elastic_bulk <- function(elastic_conn, action, data, metadata = NULL) {
  bulk_data_file_name <- elastic_bulk_file(action, data, metadata)
  request <- httr::PUT(url = elastic_conn$elastic_url, path = paste0(elastic_conn$elastic_index_type, '/_bulk'), body = httr::upload_file(bulk_data_file_name))
  closeAllConnections()
  file.remove(bulk_data_file_name)
  if (httr::status_code(request) == 200) {
    return(TRUE)
  } else {
    return(FALSE)
  }
}

#' @describeIn elastic_bulk
#' @param file_name the name and path (relative to the working directory), of the text file that will be produced. If NULL then a temporary file will be produced.
#' @return \code{elastic_bulk_file}: a string with the relative path and name of the text file containing the data ready to be uploaded to the Elasticsearch bulk API.
elastic_bulk_file <- function(action, data = NULL, metadata = NULL, file_name = NULL) {

  # initial checks
  tryCatch({
    stopifnot(action %in% c('index', 'update', 'delete'))
  }, error = function(e) stop('action not specified correctly', call. = FALSE))

  tryCatch({
    stopifnot(any(!is.null(data), !is.null(metadata)))
    if (is.null(data)) {
      number_actions <- nrow(metadata)
    } else {
      number_actions <- nrow(data)
    }
  }, error = function(e) stop('cannot have data and metadata both NULL', call. = FALSE))

  if (is.null(data)) {
    tryCatch({
      stopifnot(action == 'delete')
    }, error = function(e) stop('cannot have data NULL with action is "index" or "update"', call. = FALSE))
  } else {
    tryCatch({
      stopifnot(class(data) == 'data.frame', action != 'delete')
    }, error = function(e) stop('data is not specified correctly', call. = FALSE))
  }

  if(is.null(metadata)) {
    tryCatch({
      stopifnot(action == 'index')
    }, error = function(e) stop('cannot have metadata NULL when action is "update" or "delete', call. = FALSE))
  } else {
    tryCatch({
      stopifnot(class(metadata) == 'data.frame')
    }, error = function(e) stop('metadata is not specified correctly', call. = FALSE))
  }

  if (all(!is.null(data), !is.null(metadata))) {
    tryCatch({
      stopifnot(nrow(data) == nrow(metadata))
    }, error = function(e) stop('metadata must have the same number of rows as data', call. = FALSE))
  }

  # create action and metadata header lines
  json_metadata <- helper_bulk_create_metadata_json(rep(action, number_actions), metadata)

  if (action != 'delete') {
    # stream data converted to bulk JSON format to a temporary file where each JSON line can then be re-read as the single element of a character vector
    jsonlite::stream_out(data, temp_con_1 <- file(temp_file_name_1 <- tempfile()), POSIXt="ISO8601", pagesize = 1000, verbose = FALSE)
    json_data <- readLines(temp_file_name_1)
    file.remove(temp_file_name_1)

    # combine vectors using even and odd indices of an empty container
    final_upload_data <- rep(NA, length(json_data) * 2)
    idx_even <- seq(2, length(json_data) * 2, 2)
    idx_odd <- seq(1, length(json_data) * 2, 2)
    final_upload_data[idx_odd] <- json_metadata
    final_upload_data[idx_even] <- json_data
  } else {
    final_upload_data <- json_metadata
  }

  # write final output to a file and return the file's path and name
  if (is.null(file_name)) file_name = paste0(tempfile(), '.txt')
  writeLines(final_upload_data, temp_con_2 <- file(file_name))
  closeAllConnections()
  return(file_name)
}

#' @describeIn elastic_bulk
#' @param num_pieces the number of pieces to break the input data into.
#' @return \code{elastic_bulk_batched}: TRUE or FALSE depending on the success of all http requests made to the bulk API.
elastic_bulk_index_batch <- function(elastic_conn, data, num_pieces = 1) {
  stopifnot(nrow(data) >= num_pieces)
  requests = lapply( X = split(data, rep(1:num_pieces, each = ceiling(nrow(data) / num_pieces))), FUN = function (x) { elastic_bulk(elastic_conn, 'index', x) } )
  return(all(unlist(requests)))
}


# elastic_bulk_reingest <- function ( elastic_conn, mapping = '' ) {
#
#   # initiate a scroll search for all documents (this will persist after deletion)
#   scroll_filter <- '{"query": {"match_all": {} } }'
#   scroll_id <- content( POST( elastic_conn$elastic_url, path = paste( elastic_conn$elastic_index, '_search?scroll=1m&search_type=scan&size=2000', sep = '/'), body = scroll_filter ) )$`_scroll_id`
#
#   # load custom mapping if one was supplied (should review template mappings to make this cleaner/unnecessary)
#   DELETE( elastic_conn$elastic_url, path = elastic_conn$elastic_index )
#   if ( mapping != '' ) {
#     PUT( elastic_conn$elastic_url, path = elastic_conn$elastic_index, body = mapping )
#   }
#
#   # keep retreiving data from the scroll search and uploading each batch until there is no more
#   scroll_response <- POST( elastic_conn$elastic_url, path = '_search/scroll?scroll=10s', body = scroll_id )
#   scroll_all_data <- fromJSON( content( scroll_response, 'text' ) )$hits$hits
#   scroll_metadata <- scroll_all_data[, c( '_index', '_type', '_id' )]
#   scroll_data <- scroll_all_data$`_source`
#
#   while ( !is.null( scroll_data ) ) {
#     # upload data to new index
#     scroll_metadata <- scroll_all_data[, c( '_index', '_type', '_id' )]
#     bulk_operations <- bind_cols( data_frame( actions = rep( 'index', nrow( scroll_data ) ) ), scroll_metadata )
#     elastic_bulk( elastic_conn, bulk_operations['actions'], bulk_operations[, c('_index', '_type', '_id')], scroll_data )
#
#     # retreive another page of search results and convert to data frame
#     scroll_response <- POST( elastic_conn$elastic_url, path = '_search/scroll?scroll=10s', body = scroll_id )
#     scroll_all_data <- fromJSON( content( scroll_response, 'text' ) )$hits$hits
#     scroll_data <- scroll_all_data$`_source`
#   }
#
# }
#
# elastic_bulk_reingest_split_index <- function ( elastic_conn, timestamp_field, mapping = '' ) {
#
#   # get a list of days in the index using an aggregation
#   agg_days_in_index <- paste0( '{"aggs": {"requests_per_day": {"date_histogram": {"field": "', timestamp_field, '", "interval": "day"} } } }' )
#   data_days_count <- fromJSON( content( POST( elastic_conn$elastic_url, path = paste( elastic_conn$elastic_index, '_search?search_type=count', sep = '/'), body = agg_days_in_index ), 'text' ) )$aggregations$requests_per_day$buckets
#
#   # loop over days returned from aggregation
#   for ( day in data_days_count$key_as_string ) {
#     # date/day
#     current_day <- ymd_hms( day )
#     next_day <- current_day + days( 1 )
#
#     # index name
#     current_index_name <- paste0( elastic_conn$elastic_index, '-', str_replace_all( as.character( current_day ), '-', '.') )
#
#     # create custom mapping if one was supplied (should review template mappings to make this cleaner/unnecessary)
#     DELETE( elastic_conn$elastic_url, path = current_index_name )
#     if ( mapping != '' ) {
#       PUT( elastic_conn$elastic_url, path = current_index_name, body = mapping )
#     }
#
#     # initiate a scroll search for a given date
#     scroll_filter <- paste0('{"query": {"filtered": {"filter": {"range": {"', timestamp_field, '": {"gte": "', as.character(current_day), '", "lt": "', as.character(next_day), '"} } } } } }')
#     scroll_id <- content( POST( elastic_conn$elastic_url, path = paste( elastic_conn$elastic_index, '_search?scroll=1m&search_type=scan&size=2000', sep = '/'), body = scroll_filter) )$`_scroll_id`
#
#     # keep retreiving data from the scroll search and uploading each batch until there is no more
#     scroll_response <- POST( elastic_conn$elastic_url, path = '_search/scroll?scroll=5s', body = scroll_id)
#     scroll_data <- fromJSON( content( scroll_response, 'text' ) )$hits$hits$`_source`
#
#     while ( !is.null(scroll_data) ) {
#       # upload data to new index
#       bulk_operations <- data_frame( actions = rep( 'index', nrow( scroll_data ) ), `_index` = current_index_name, `_type` = elastic_conn$elastic_type)
#       elastic_bulk( elastic_conn, bulk_operations['actions'], bulk_operations[, c('_index', '_type')], scroll_data)
#
#       # retreive another page of search results and convert to data frame
#       scroll_response <- POST( elastic_conn$elastic_url, path = '_search/scroll?scroll=5s', body = scroll_id )
#       scroll_data <- fromJSON( content( scroll_response, 'text') )$hits$hits$`_source`
#     }
#
#   }
#
# }
#
# elastic_create_new_index <- function( elastic_conn, mapping ) {
#
#   # does index exist
#   index_exists <- if ( status_code( HEAD( elastic_conn$elastic_url, path = elastic_conn$elastic_index ) ) == 404 ) FALSE else TRUE
#
#   # if index doesn't exist then create it with the mapping provided
#   if ( !index_exists ) {
#     response <- PUT( elastic_conn$elastic_url, path = elastic_conn$elastic_index, body = mapping )
#     if ( status_code(response) == 200 ) index_exists = FALSE
#   }
#
#   # return whether or not a new index was created
#   index_exists
# }
#
# elastic_query <- function ( elastic_conn, query_def, to_file = FALSE, file_name = paste0('elastic_query--', Sys.time(), '.csv'), to_file_and_df = FALSE ) {
#
#   # create queryDSL json
#   query_def_json <- helper_list_to_json( query_def )
#
#   # run query
#   helper_search_scroll_retreive_all( elastic_conn, query_def_json, to_file = to_file, file_name = paste0( 'elastic_query--', Sys.time(), '.csv' ), to_file_and_df = to_file_and_df )
#
# }
#
# elastic_aggregation <- function ( elastic_conn, agg_def, agg_metric, query_def= list('query' = list('match_all' = list() ) ) ) {
#
#   # create full aggregation json (including query component)
#   agg_json <- helper_create_agg_json( agg_def, agg_metric, query_def )
#
#   # execute aggregation
#   response <- POST( elastic_conn$elastic_url, path = paste0( elastic_conn$elastic_index_type, '/_search?search_type=count' ), body = agg_json )
#   results <- fromJSON( content( response, "text" ) )$aggregations$agg_time$buckets[, -2]
#
#   # format output (map from nested data frame structure to single data_frame) and return
#   as_data_frame( lapply( results, FUN = unlist, recursive = FALSE ) )
#
# }
