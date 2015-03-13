# helper functions that are not meant to be exposed as part of the package ----
# helper_bulk_create_metadata_json    ::  given a set of CRUD actions and associated metadata (e.g. _index, _type, _id), create the JSON header required by the bulk API to perform the action on the associated document
# helper_search_scroll_retreive_all   ::
# helper_list_to_json
# helper_create_query_json

helper_bulk_create_metadata_json <- function ( action_df, metadata_df = NULL ) {
  if ( is.null( metadata_df ) ) {
    json_metadata <- paste0( '{"', action_df[[1]], '": {}}' )
  } else {
    tags_metadata <- colnames( metadata_df )
    json_metadata <- paste0( '{"', action_df[[1]], '": {' )
    for ( tag in tags_metadata ) {
      json_metadata <- paste0( json_metadata, '"', tag, '": ', '"', metadata_df[[tag]], '", ' )
    }
    json_metadata <- paste0( str_sub( json_metadata, 1, str_length( json_metadata ) - 2 ), '}}' )
  }
  json_metadata
}

helper_search_scroll_retreive_all <- function ( elastic_conn, query_def_json, context_open_time = '10s', context_size = '2000', to_file = FALSE, file_name = paste0('elastic_query--', Sys.time(), '.csv'), to_file_and_df = FALSE ) {

  # execute scroll query and collect results by repeatedly retreiving data from the scroll search
  scroll_id <- content( POST( elastic_conn$elastic_url, path = paste0( elastic_conn$elastic_index_type, '/_search?scroll=', context_open_time, '&search_type=scan&size=', context_size), body = query_def_json ) )$`_scroll_id`
  scroll_path <- paste0( '_search/scroll?scroll=', context_open_time )

  # keep retreiving data from the scroll search and uploading each batch until there is no more
  if ( to_file_and_df ) {

    scroll_response <- POST( elastic_conn$elastic_url, path = scroll_path, body = scroll_id )
    query_data <- as_data_frame( fromJSON( content( scroll_response, 'text') )$hits$hits$`_source` )
    write.table(scroll_data, file_name, sep = ',', append = FALSE, col.names = TRUE, row.names = FALSE)
    scroll_response <- POST( elastic_conn$elastic_url, path = scroll_path, body = scroll_id )
    while ( status_code( scroll_response ) == 200 ) {
      scroll_data <- as_data_frame( fromJSON( content( scroll_response, 'text') )$hits$hits$`_source` )
      query_data <- bind_rows( query_data, scroll_data  )
      write.table(scroll_data, file_name, sep = ',', append = TRUE, col.names = FALSE, row.names = FALSE)
      scroll_response <- POST( elastic_conn$elastic_url, path = scroll_path, body = scroll_id )
    }
    return ( query_data )

  } else if ( to_file ) {

    scroll_response <- POST( elastic_conn$elastic_url, path = scroll_path, body = scroll_id )
    scroll_data <- as_data_frame( fromJSON( content( scroll_response, 'text') )$hits$hits$`_source` )
    write.table(scroll_data, file_name, sep = ',', append = FALSE, col.names = TRUE, row.names = FALSE)
    scroll_response <- POST( elastic_conn$elastic_url, path = scroll_path, body = scroll_id )
    while ( status_code( scroll_response ) == 200 ) {
      scroll_data <- as_data_frame( fromJSON( content( scroll_response, 'text') )$hits$hits$`_source` )
      write.table(scroll_data, file_name, sep = ',', append = TRUE, col.names = FALSE, row.names = FALSE)
      scroll_response <- POST( elastic_conn$elastic_url, path = scroll_path, body = scroll_id )
    }
    return ( NULL )

  } else {

    scroll_response <- POST( elastic_conn$elastic_url, path = scroll_path, body = scroll_id )
    query_data <- as_data_frame( fromJSON( content( scroll_response, 'text') )$hits$hits$`_source` )
    scroll_response <- POST( elastic_conn$elastic_url, path = scroll_path, body = scroll_id )
    while ( status_code( scroll_response ) == 200 ) {
      scroll_data <- as_data_frame( fromJSON( content( scroll_response, 'text') )$hits$hits$`_source` )
      query_data <- bind_rows( query_data, scroll_data  )
      scroll_response <- POST( elastic_conn$elastic_url, path = scroll_path, body = scroll_id )
    }
    return ( query_data )
  }

}

helper_list_to_json <- function ( list ) {
  json <- str_replace_all ( toJSON( list, auto_unbox = TRUE ), '([a-zA-z])\\.[1-9]"', '\\1"' )
  json
}

helper_create_query_json <- function ( query_def ) {
  query_def_json <- minify( paste0( '{"query":{"filtered":', helper_list_to_json( query_def ), '}}' ) )
  query_def_json
}

helper_create_agg_json <- function ( agg_type, agg_metric, query_def = list('query' = list('match_all' = list() ) ) ) {

  create_full_agg_json <- function ( agg_type, agg_metric, front_json = '', end_json = '') {
    if ( length( agg_type ) == 0 ) {
      front_json <- paste0( front_json, '"aggs":', toJSON( agg_metric, auto_unbox = TRUE ) )
      return( paste0( front_json, end_json) )
    } else {
      front_json <- paste0( front_json, '"aggs":', str_sub( toJSON( agg_type[1], auto_unbox = TRUE ), 1, -3 ), ',' )
      end_json <- paste0( end_json, '}}')
      agg_type[[1]] <- NULL
      create_full_agg_json( agg_type, agg_metric, front_json, end_json)
    }
  }

  query_json <- str_sub( helper_create_query_json( query_def ), 2, -2)
  agg_json <- create_full_agg_json( agg_type, agg_metric )
  final_agg_json <- minify( paste0( '{', query_json, ',', agg_json, '}' ) )
  final_agg_json
}

