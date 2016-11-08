
helper_ping_elasticsearch <- function(elasticsearch) {
  response <- httr::HEAD(url = elasticsearch$elastic_url, path = elasticsearch$elastic_index_type)
  if (httr::status_code(response) != 200) {
    return(FALSE)
  } else {
    return(TRUE)
  }
}


helper_bulk_create_metadata_json <- function(action_df, metadata_df = NULL) {
  if (is.null(metadata_df)) {
    json_metadata <- paste0('{"', action_df[[1]], '":{}}')
  } else {
    tags_metadata <- colnames(metadata_df)
    json_metadata <- paste0('{"', action_df[[1]], '":{')
    for (tag in tags_metadata) {
      tag_checked <- if (stringr::str_sub(tag, 1, 1) == '_') tag else paste0('_', tag)
      json_metadata <- paste0(json_metadata, '"', tag_checked, '":', '"', metadata_df[[tag]], '",')
    }
    json_metadata <- paste0(stringr::str_sub(json_metadata, 1, stringr::str_length(json_metadata) - 1), '}}')
  }
  json_metadata
}


helper_search_scroll_retreive_all <- function(elastic_conn, query_def_json, context_open_time = '10s', context_size = '2000',
                                               to_file = FALSE, file_name = paste0('elastic_query--', Sys.time(), '.csv'), to_file_and_df = FALSE) {
  # execute scroll query and collect results by repeatedly retreiving data from the scroll search
  scroll_id <- httr::content( httr::POST(elastic_conn$elastic_url,
                                         path = paste0(elastic_conn$elastic_index_type, '/_search?scroll=', context_open_time, '&search_type=scan&size=', context_size),
                                         body = query_def_json ) )$`_scroll_id`

  scroll_path <- paste0( '_search/scroll?scroll=', context_open_time )

  # keep retreiving data from the scroll search and writing to file/memory until there is no more
  if (to_file_and_df) {
    scroll_response <- httr::POST(elastic_conn$elastic_url, path = scroll_path, body = scroll_id)

    query_data <- dplyr::as_data_frame( jsonlite::fromJSON( httr::content(scroll_response, 'text') )$hits$hits$`_source` )
    write.table(query_data, file_name, sep = ',', append = FALSE, col.names = TRUE, row.names = FALSE)

    scroll_response <- httr::POST(elastic_conn$elastic_url, path = scroll_path, body = scroll_id)
    while (httr::status_code(scroll_response) == 200) {
      scroll_data <- dplyr::as_data_frame( jsonlite::fromJSON( httr::content(scroll_response, 'text') )$hits$hits$`_source` )

      query_data <- httr::bind_rows(query_data, scroll_data)
      write.table(scroll_data, file_name, sep = ',', append = TRUE, col.names = FALSE, row.names = FALSE)

      scroll_response <- httr::POST(elastic_conn$elastic_url, path = scroll_path, body = scroll_id)
    }
    return (query_data)

  } else if (to_file) {
    scroll_response <- httr::POST(elastic_conn$elastic_url, path = scroll_path, body = scroll_id)
    scroll_data <- dplyr::as_data_frame( jsonlite::fromJSON( httr::content(scroll_response, 'text') )$hits$hits$`_source` )

    write.table(scroll_data, file_name, sep = ',', append = FALSE, col.names = TRUE, row.names = FALSE)

    scroll_response <- httr::POST( elastic_conn$elastic_url, path = scroll_path, body = scroll_id )
    while (httr::status_code(scroll_response) == 200) {
      scroll_data <- dplyr::as_data_frame( jsonlite::fromJSON( httr::content(scroll_response, 'text') )$hits$hits$`_source` )

      write.table(scroll_data, file_name, sep = ',', append = TRUE, col.names = FALSE, row.names = FALSE)

      scroll_response <- httr::POST(elastic_conn$elastic_url, path = scroll_path, body = scroll_id)
    }
    return ( NULL )

  } else {
    scroll_response <- httr::POST(elastic_conn$elastic_url, path = scroll_path, body = scroll_id)

    query_data <- dplyr::as_data_frame( jsonlite::fromJSON( httr::content(scroll_response, 'text') )$hits$hits$`_source` )

    scroll_response <- httr::POST(elastic_conn$elastic_url, path = scroll_path, body = scroll_id)
    while (httr::status_code(scroll_response ) == 200) {
      scroll_data <- dplyr::as_data_frame( jsonlite::fromJSON( httr::content(scroll_response, 'text') )$hits$hits$`_source` )

      query_data <- dplyr::bind_rows(query_data, scroll_data)

      scroll_response <- httr::POST(elastic_conn$elastic_url, path = scroll_path, body = scroll_id)
    }
    return (query_data)
  }

}


helper_list_to_json <- function(list) {
  json <- stringr::str_replace_all( jsonlite::toJSON(list, auto_unbox = TRUE), '([a-zA-z])\\.[1-9]"', '\\1"')
  json
}


helper_create_query_json <- function(query_def) {
  query_def_json <- jsonlite::minify( paste0('{"query":{"filtered":', helper_list_to_json(query_def), '}}') )
  query_def_json
}


helper_create_agg_json <- function(agg_type, agg_metric, query_def = list('query' = list('match_all' = list())) ) {

  create_full_agg_json <- function(agg_type, agg_metric, front_json = '', end_json = '') {
    if ( length(agg_type) == 0 ) {
      front_json <- paste0(front_json, '"aggs":', jsonlite::toJSON(agg_metric, auto_unbox = TRUE))
      return( paste0(front_json, end_json) )
    } else {
      front_json <- paste0(front_json, '"aggs":', stringr::str_sub( jsonlite::toJSON(agg_type[1], auto_unbox = TRUE ), 1, -3), ',' )
      end_json <- paste0(end_json, '}}')
      agg_type[[1]] <- NULL
      create_full_agg_json(agg_type, agg_metric, front_json, end_json)
    }
  }

  query_json <- stringr::str_sub( helper_create_query_json(query_def), 2, -2)
  agg_json <- create_full_agg_json(agg_type, agg_metric)
  final_agg_json <-jsonlite::minify( paste0('{', query_json, ',', agg_json, '}') )
  final_agg_json
}

