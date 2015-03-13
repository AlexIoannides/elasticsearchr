# functions for using elasticsearch that are to be exposed ----

# elastic_conn                        ::  list that contains the Elasticsearch url, index, and type we want to work with
# elastic_create_dated_index_name     ::  given a date and in index name, create an Elasticsearch friendly dated index name
# elastic_bulk                        ::  CRUD operations for Elasticsearch documents using the bulk API
# elastic_bulk_batched                ::  split bulk operations into batches as not to kill Elasticsearch (~ 15MB per-batch is maximum recommended)
# elastic_bulk_reingest               ::  re-ingest an index with the option to supply a new mapping (warning: no fail-safes are in place if the process stalls before it is finished!)
# elastic_bulk_reingest_split_index   ::  re-ingest an index by splitting it into seperate indices containing a single day's worth of data (useful for log file analysis, etc)
# elastic_create_new_index            ::  create a new index (given a mapping) so long as the index does not already exist
# elastic_agg_time                    ::  run a date-histogram aggregation
# elastic_query                       ::  wrapper for the Elasticsearch QueryDSL API that supports both query and filter operations (for reference see: http://www.elasticsearch.org/guide/en/elasticsearch/guide/current/_most_important_queries_and_filters.html )

#' elasticr: A package for making working with Elasticsearch easy from R.
#'
#' @section elasticr functions:
#' elastic_conn
#'
#' @docType package
#' @name elasticr
NULL

#' Define a 'connection' to an Elasticsearch server
#' @export
#' @importFrom dplyr bind_rows
#'
#' @param url Location of the Elasticsearch serves that contains the index and type we're interestd in working with.
#' @param index Name of the index we want to work with.
#' @param type Name of the type we want to work with (optional).
#' @return A list containing the the location of the Elasticsearch server, the index and type (option) that we want to work with (as well as the index/type path).
#' @examples
#' elastic_conn('http://localhost:9200', 'analytics', 'users')
elastic_conn <- function ( url = 'http://localhost:9200', index = '', type = '' ) {
  list( elastic_url = url, elastic_index = index, elastic_type = type, elastic_index_type = ( if ( type != '' ) paste( index, type, sep = '/' ) else index ) )
}

elastic_create_dated_index_name <- function ( elastic_index_name, date ) {
  date_parsed_rounded <- round_date( ymd_hms( date ), unit = 'day' )
  date_formatted <- str_replace_all( as.character( date_parsed_rounded ), '-', '.' )
  paste0( elastic_index_name, '-', date_formatted )
}

elastic_bulk <- function ( elastic_conn, actions, metadata, data ) {

  # stream data in JSON format to a temporary test file
  stream_out( data, temp_con_1 <- file( temp_file_name_1 <- tempfile() ), POSIXt="ISO8601", pagesize = 1000, verbose = FALSE )

  # create metadata and json records in seperate vectors
  json_data <- readLines( temp_file_name_1 )
  json_metadata <- helper_bulk_create_metadata_json( actions, metadata )

  # combine vectors using even and odd indices of an empty container
  final_upload_data <- rep( NA, length(json_data) * 2 )
  idx_even <- seq( 2, length(json_data) * 2, 2 )
  idx_odd <- seq( 1, length(json_data) * 2, 2 )
  final_upload_data[idx_odd] <- json_metadata
  final_upload_data[idx_even] <- json_data

  # write merged file to a temporary location and bulk load to elasticsearch
  writeLines( final_upload_data, temp_con_2 <- file( temp_file_name_2 <- paste0(tempfile(), ".txt") ) )
  PUT( url = elastic_conn$elastic_url, path = paste0( elastic_conn$elastic_index_type, '/_bulk'), body = upload_file( temp_file_name_2 ) )

  # clean up temp files (don't need to close temp_con_2 as this will be done automatically as it is only open for writing once)
  closeAllConnections()
  file.remove( temp_file_name_1 )
  file.remove( temp_file_name_2 )
  return( NULL )
}

elastic_bulk_batched <- function( elastic_conn, data_df ) {
  lapply( X = split( data_df, rep(1:100, each = ceiling( nrow(data_df) / 100) ) ), FUN = function (x) { elastic_bulk( elastic_conn, rep('index', nrow(x) ), NULL, x) } )
  return( NULL )
}

elastic_bulk_reingest <- function ( elastic_conn, mapping = '' ) {

  # initiate a scroll search for all documents (this will persist after deletion)
  scroll_filter <- '{"query": {"match_all": {} } }'
  scroll_id <- content( POST( elastic_conn$elastic_url, path = paste( elastic_conn$elastic_index, '_search?scroll=1m&search_type=scan&size=2000', sep = '/'), body = scroll_filter ) )$`_scroll_id`

  # load custom mapping if one was supplied (should review template mappings to make this cleaner/unnecessary)
  DELETE( elastic_conn$elastic_url, path = elastic_conn$elastic_index )
  if ( mapping != '' ) {
    PUT( elastic_conn$elastic_url, path = elastic_conn$elastic_index, body = mapping )
  }

  # keep retreiving data from the scroll search and uploading each batch until there is no more
  scroll_response <- POST( elastic_conn$elastic_url, path = '_search/scroll?scroll=10s', body = scroll_id )
  scroll_all_data <- fromJSON( content( scroll_response, 'text' ) )$hits$hits
  scroll_metadata <- scroll_all_data[, c( '_index', '_type', '_id' )]
  scroll_data <- scroll_all_data$`_source`

  while ( !is.null( scroll_data ) ) {
    # upload data to new index
    scroll_metadata <- scroll_all_data[, c( '_index', '_type', '_id' )]
    bulk_operations <- bind_cols( data_frame( actions = rep( 'index', nrow( scroll_data ) ) ), scroll_metadata )
    elastic_bulk( elastic_conn, bulk_operations['actions'], bulk_operations[, c('_index', '_type', '_id')], scroll_data )

    # retreive another page of search results and convert to data frame
    scroll_response <- POST( elastic_conn$elastic_url, path = '_search/scroll?scroll=10s', body = scroll_id )
    scroll_all_data <- fromJSON( content( scroll_response, 'text' ) )$hits$hits
    scroll_data <- scroll_all_data$`_source`
  }

}

elastic_bulk_reingest_split_index <- function ( elastic_conn, timestamp_field, mapping = '' ) {

  # get a list of days in the index using an aggregation
  agg_days_in_index <- paste0( '{"aggs": {"requests_per_day": {"date_histogram": {"field": "', timestamp_field, '", "interval": "day"} } } }' )
  data_days_count <- fromJSON( content( POST( elastic_conn$elastic_url, path = paste( elastic_conn$elastic_index, '_search?search_type=count', sep = '/'), body = agg_days_in_index ), 'text' ) )$aggregations$requests_per_day$buckets

  # loop over days returned from aggregation
  for ( day in data_days_count$key_as_string ) {
    # date/day
    current_day <- ymd_hms( day )
    next_day <- current_day + days( 1 )

    # index name
    current_index_name <- paste0( elastic_conn$elastic_index, '-', str_replace_all( as.character( current_day ), '-', '.') )

    # create custom mapping if one was supplied (should review template mappings to make this cleaner/unnecessary)
    DELETE( elastic_conn$elastic_url, path = current_index_name )
    if ( mapping != '' ) {
      PUT( elastic_conn$elastic_url, path = current_index_name, body = mapping )
    }

    # initiate a scroll search for a given date
    scroll_filter <- paste0('{"query": {"filtered": {"filter": {"range": {"', timestamp_field, '": {"gte": "', as.character(current_day), '", "lt": "', as.character(next_day), '"} } } } } }')
    scroll_id <- content( POST( elastic_conn$elastic_url, path = paste( elastic_conn$elastic_index, '_search?scroll=1m&search_type=scan&size=2000', sep = '/'), body = scroll_filter) )$`_scroll_id`

    # keep retreiving data from the scroll search and uploading each batch until there is no more
    scroll_response <- POST( elastic_conn$elastic_url, path = '_search/scroll?scroll=5s', body = scroll_id)
    scroll_data <- fromJSON( content( scroll_response, 'text' ) )$hits$hits$`_source`

    while ( !is.null(scroll_data) ) {
      # upload data to new index
      bulk_operations <- data_frame( actions = rep( 'index', nrow( scroll_data ) ), `_index` = current_index_name, `_type` = elastic_conn$elastic_type)
      elastic_bulk( elastic_conn, bulk_operations['actions'], bulk_operations[, c('_index', '_type')], scroll_data)

      # retreive another page of search results and convert to data frame
      scroll_response <- POST( elastic_conn$elastic_url, path = '_search/scroll?scroll=5s', body = scroll_id )
      scroll_data <- fromJSON( content( scroll_response, 'text') )$hits$hits$`_source`
    }

  }

}

elastic_create_new_index <- function( elastic_conn, mapping ) {

  # does index exist
  index_exists <- if ( status_code( HEAD( elastic_conn$elastic_url, path = elastic_conn$elastic_index ) ) == 404 ) FALSE else TRUE

  # if index doesn't exist then create it with the mapping provided
  if ( !index_exists ) {
    response <- PUT( elastic_conn$elastic_url, path = elastic_conn$elastic_index, body = mapping )
    if ( status_code(response) == 200 ) index_exists = FALSE
  }

  # return whether or not a new index was created
  index_exists
}

elastic_query <- function ( elastic_conn, query_def, to_file = FALSE, file_name = paste0('elastic_query--', Sys.time(), '.csv'), to_file_and_df = FALSE ) {

  # create queryDSL json
  query_def_json <- helper_list_to_json( query_def )

  # run query
  helper_search_scroll_retreive_all( elastic_conn, query_def_json, to_file = to_file, file_name = paste0( 'elastic_query--', Sys.time(), '.csv' ), to_file_and_df = to_file_and_df )

}

elastic_aggregation <- function ( elastic_conn, agg_def, agg_metric, query_def= list('query' = list('match_all' = list() ) ) ) {

  # create full aggregation json (including query component)
  agg_json <- helper_create_agg_json( agg_def, agg_metric, query_def )

  # execute aggregation
  response <- POST( elastic_conn$elastic_url, path = paste0( elastic_conn$elastic_index_type, '/_search?search_type=count' ), body = agg_json )
  results <- fromJSON( content( response, "text" ) )$aggregations$agg_time$buckets[, -2]

  # format output (map from nested data frame structure to single data_frame) and return
  as_data_frame( lapply( results, FUN = unlist, recursive = FALSE ) )

}
