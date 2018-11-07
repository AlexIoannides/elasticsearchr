# Copyright 2016 Alex Ioannides
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


#' Validate healthy Elasticsearch connection.
#'
#' Validates healthy Elasticsearch connections by attempting to call the cluster healthcheck
#' endpoint. In doing to, it defends against incorrect URLs to Elasticsearch clusters. Requires
#' that URLs point directly to a master node - i.e. the endpoint that would return the default
#' Elasticsearch message, "You Know, for Search", e.g. `http://localhost:9200`.
#'
#' @param url The URL to validate.
#' @return Boolean
#'
#' @examples
#' \dontrun{
#' url <- "http://localhost:9200"
#' valid_connection(url)
#' # TRUE
#'
#' url <- "http://localhost:9201"
#' valid_connection(url)
#' #  Error: Failed to connect to localhost port 9201: Connection refused
#' }
valid_connection <- function(url) {
  if (substr(url, nchar(url), nchar(url)) == "/") {
    healthcheck_endpoint <- paste0(url, "_cluster/health")
  } else {
    healthcheck_endpoint <- paste0(url, "/_cluster/health")
  }

  tryCatch(
    {
      response <- httr::GET(healthcheck_endpoint)
      if (response$status_code != 200) {
        msg <- paste0(healthcheck_endpoint, " does not return cluster health:\n",
                      httr::content(response, as = "text"))
        stop(msg, call. = FALSE)
      }

      response_parsed <- httr::content(response, as = "parsed")
      if (response_parsed$status != "red") {
        return(TRUE)
      } else {
        stop("Elasticsearch cluster status is red", call. = FALSE)
      }
    },
    error = function(e) stop(e$message, call. = FALSE)
  )
}


#' Elasticsearch version
#'
#' Returns the major, minor and build version numbers for an Elasticsearch cluster, given a valid
#' URL to an Elasticsearch cluster.
#'
#' @param url A valid URL to an Elasticsearch cluster.
#' @return A list with the \code{major}, \code{minor} and \code{build} numbers.
#'
#' @examples
#' \dontrun{
#' elastic_version("http://localhost:9200")
#' $major
#' [1] 5
#'
#' $minor
#' [1] 0
#'
#' $build
#' [1] 1
#' }
elastic_version <- function(url) {
  valid_connection(url)
  response <- httr::GET(url)
  check_http_code_throw_error(response)
  version_string <- httr::content(response)$version$number
  version <- as.integer(strsplit(version_string, "\\.")[[1]])

  list("major" = version[1], "minor" = version[2], "build" = version[3])
}


#' Valid JSON string predicate function
#'
#' @param json Candidate JSON object as a string.
#' @return Boolean.
#'
#' @examples
#' \dontrun{
#' good_json <- '{"id": 1}'
#' valid_json(good_json)
#' # TRUE
#'
#' bad_json <- '{"id": 1a}'
#' valid_json(bad_json)
#' # FALSE
#' }
valid_json <- function(json) {
  stopifnot(is.character(json))
  jsonlite::validate(json)
}


#' Sanitise column names.
#'
#' Convert data frame column names into an Elasticsearch compatible format.
#'
#' Elasticsearch will not ingest field names with periods ("."), such as "Sepal.Width", as these
#' are reserved for nested objects (in the JSON sense). This function replaces all period with
#' underscores ("_") and the converts everything to lowercase for simplicity.
#'
#' @param colnames A character vector containing data frame column names.
#' @return A character vector with 'clean' column names.
#'
#' @examples
#' \dontrun{
#' df <- iris
#' colnames(df) <- cleaned_field_names(colnames(df))
#' colnames(df)
#' # "sepal_length" "sepal_width"  "petal_length" "petal_width"  "species"
#' }
cleaned_field_names <- function(colnames) {
  tolower(gsub("\\.", "_", colnames))
}


#' Create Bulk API metadata.
#'
#' The fastest way to index, delete or update many documents, is via the Bulk API. This requires
#' that each document have the action combined with the document's metadata (index, type and id)
#' sent to the API. This information is encapulated as a JSON object, that this function is
#' responsible for generating.
#'
#' @param action One of: "index", "create", "update" or "delete".
#' @param index The name of the index where the documents reside (or will reside).
#' @param doc_type The name of the document type where the documents reside (or will reside).
#' @param id [optional] Character vector of document ids.
#' @param n [optional] Integer number of repeated metadata description objects that need to be
#' returned (if \code{id} is not specified).
#'
#' @return A character vector of Bulk API document information objects.
#'
#' @seealso \url{https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html}
#' for more information on the information required by the Elasticsearch Bulk API.
#'
#' @examples
#' \dontrun{
#' create_metadata("index", "iris", "data", n = 2)
#' '{\"index\": {\"_index\": \"iris\", \"_type\": \"data\"}}'
#' '{\"index\": {\"_index\": \"iris\", \"_type\": \"data\"}}'
#' }
create_metadata <- function(action, index, doc_type, id = NULL, n = NULL) {
  stopifnot(action %in% c("index", "create", "update", "delete"))

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


#' Create Bulk API data file.
#'
#' The fastest way to index, delete or update many documents, is via the Bulk API. This function
#' assembles a text file comprising of data and/or actions in the format required by the Bulk API.
#' This is ready to be POSTed to the Bulk API.
#'
#' @param metadata A character vector of Bulk API document information objects, as generated by
#' \code{create_metadata(...)}.
#' @param df [optional] A data.frame with data for indexing or updating.
#' @return The name of the temporary file containing the data for the Elasticsearch Bulk API.
#'
#' @seealso \url{https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html}
#' for more information on the information required by the Elasticsearch Bulk API.
#'
#' @examples
#' \dontrun{
#' bulk_upload_info <- create_metadata("index", "iris", "data", n = nrow(iris))
#' create_bulk_upload_file(bulk_upload_info, iris)
#' # "/var/folders/__/yz_l30s48xj6m_0059b_2twr0000gn/T//RtmpQnvUOt/file98194322b8"
#'
#' bulk_delete_info <- create_metadata("delete", "iris", "data", n = nrow(iris))
#' create_bulk_delete_file(bulk_delete_info)
#' # "/var/folders/__/yz_l30s48xj6m_0059b_2twr0000gn/T//RtmpQnvUOt/file98194322b8"
#' }
#' @name create_bulk_upload_file
NULL

#' @rdname create_bulk_upload_file
create_bulk_upload_file <- function(metadata, df = NULL) {
  if (!is.null(df)) {
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
  } else {
    writeLines(metadata, file(temp_filename2 <- tempfile()))
  }

  closeAllConnections()
  temp_filename2
}

#' @rdname create_bulk_upload_file
create_bulk_delete_file <- function(metadata) {
  writeLines(metadata, file(temp_filename <- tempfile()))
  closeAllConnections()
  temp_filename
}


#' Index data frame with Elasticsearch Bulk API
#'
#' Helper function to orchestrate the assembly of the Bulk API upload file, http request to
#' Elasticsearch and handling any subsequent respose errors. It's primary purpose is to be called
#' repeatedly on 'chunks' of a data frame that is too bid to be indexed with a single call to the
#' Bulk API (and hence the split into smaller more manageable chunks).
#'
#' @param rescource An \code{elastic_rescource} object that contains the information on the
#' Elasticsearch cluster, index and document type, where the indexed data will reside. If this does
#' not already exist, it will be created automatically.
#' @param df data.frame whose rows will be indexed as documents in the Elasticsearch cluster.
#' @return NULL
#'
#' @examples
#' \dontrun{
#' rescource <- elastic("http://localhost:9200", "iris", "data")
#' index_bulk_dataframe(rescource, iris)
#' }
index_bulk_dataframe <- function(rescource, df) {
  has_ids <- "id" %in% colnames(df)
  num_docs <- nrow(df)

  if (has_ids) {
    metadata <- create_metadata("index", rescource$index, rescource$doc_type, df$id)
  } else {
    metadata <- create_metadata("index", rescource$index, rescource$doc_type, n = num_docs)
  }

  bulk_data_file <- create_bulk_upload_file(metadata, df)
  response <- httr::PUT(url = rescource$cluster_url,
                        path = "/_bulk",
                        body = httr::upload_file(bulk_data_file),
                        httr::add_headers("Content-Type" = "application/json"))

  file.remove(bulk_data_file)
  if (httr::status_code(response) == 200 & !httr::content(response)$errors) {
    message("...", appendLF = FALSE)
  } else if (httr::content(response)$errors) {
    messages <- httr::content(response)$items
    warning(jsonlite::prettify(httr::content(response, as = "text")))
  } else {
    check_http_code_throw_error(response)
  }

  NULL
}


#' Execute query with from-size search API.
#'
#' The from-size search API allows a maximum of 10,000 search results (the maximum 'size') to be
#' returned in one call to the API. The 'from' in the name of the API refers to where in the order
#' of all qualifying documents (as ordered by their search score), should results start to be
#' returned from. Anything larger than 10,000 and the results need to be fetched from using the
#' scroll-search API (which is slower as it involves making multiple call-back requests). This API
#' is particularly well suited to returning aggregation results.
#'
#' @param rescource An \code{elastic} rescource object describing on what documents the query is to
#' be execured on.
#' @param api_call_payload A character string containing the JSON payload that described the query
#' to be executed.
#' @return A data.frame of documents returned from the query.
#'
#' @seealso \url{https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-from-size.html}
#' for more information on the information required by the Elasticsearch from-size API.
#'
#' @examples
#' \dontrun{
#' elastic_rescource <- elastic("http://localhost:9200", "iris", "data")
#' query_json <- '{"query": {"match_all": {}}}'
#' results <- from_size_search(elastic_rescource, query_json)
#' head(results)
#' #   sepal_length sepal_width petal_length petal_width species
#' # 1          4.8         3.0          1.4         0.1  setosa
#' # 2          4.3         3.0          1.1         0.1  setosa
#' # 3          5.8         4.0          1.2         0.2  setosa
#' # 4          5.1         3.5          1.4         0.3  setosa
#' # 5          5.2         3.5          1.5         0.2  setosa
#' # 6          5.2         3.4          1.4         0.2  setosa
#' }
from_size_search <- function(rescource, api_call_payload) {

  response <- httr::POST(rescource$search_url, body = api_call_payload,
                         httr::add_headers("Content-Type" = "application/json"))

  check_http_code_throw_error(response)

  parsed_response <- jsonlite::fromJSON(httr::content(response, as = 'text'))
  if ("aggregations" %in% names(parsed_response)) {
    return_data <- extract_aggs_results(response)
    if (length(return_data) == 0) stop("empty response to request")
  } else {
    return_data <- extract_query_results(response)
    if (length(return_data) == 0) stop("empty response to request")
  }

  return_data
}


#' Execute a query with the scroll-search API.
#'
#' The scroll-search API works by returning a 'token' to the user that allows search results to be
#' returned one 'page' at a time. This, large query results (in excess of the 10,000 documents
#' maximum size offered by the from-search API) can be retreived by making multiple calls after the
#' initial query was sent. Although a slower process end-to-end, this API is particularly well
#' suited to returning large query results.
#'
#' @param rescource An \code{elastic} rescource object describing on what documents the query is to
#' be execured on.
#' @param api_call_payload A character string containing the JSON payload that described the query
#' to be executed.
#' @param extract_function A function to be used for extracting the data from the responses sent
#' back from the scroll-search API. Defaults to \code{extract_query_results} that extracts query
#' results, for when the scroll-search API is being used for retreiving query results (as opposed
#' to aggregations or document ids, etc.).
#' @return A data.frame of documents returned from the query.

#' @seealso \url{https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html}
#' for more information on the information required by the Elasticsearch scroll-search API.
#'
#' @examples
#' \dontrun{
#' elastic_rescource <- elastic("http://localhost:9200", "iris", "data")
#' query_json <- '{"query": {"match_all": {}}}'
#' results <- scroll_search(elastic_rescource, query_json)
#' head(results)
#' #   sepal_length sepal_width petal_length petal_width species
#' # 1          4.8         3.0          1.4         0.1  setosa
#' # 2          4.3         3.0          1.1         0.1  setosa
#' # 3          5.8         4.0          1.2         0.2  setosa
#' # 4          5.1         3.5          1.4         0.3  setosa
#' # 5          5.2         3.5          1.5         0.2  setosa
#' # 6          5.2         3.4          1.4         0.2  setosa
#' }
scroll_search <- function(rescource, api_call_payload, extract_function = extract_query_results) {
  scroll_search_url <- paste0(rescource$cluster_url, "/_search/scroll")
  scroll_results <- list()

  initial_scroll_search_url <- paste0(rescource$search_url, "?size=10000&scroll=1m")
  initial_response <- httr::POST(initial_scroll_search_url, body = api_call_payload,
                                 httr::add_headers("Content-Type" = "application/json"))

  check_http_code_throw_error(initial_response)

  scroll_results[[1]] <- extract_function(initial_response)
  next_scroll_id <- httr::content(initial_response)$`_scroll_id`
  has_next <- TRUE
  n <- 2
  while (has_next) {
    message("...", appendLF = FALSE)
    next_api_payload <- paste0('{"scroll": "1m", "scroll_id": "', next_scroll_id, '"}')
    next_response <- httr::POST(scroll_search_url, body = next_api_payload,
                                httr::add_headers("Content-Type" = "application/json"))

    check_http_code_throw_error(next_response)
    if(length(httr::content(next_response)$hits$hits) > 0) {
      scroll_results[[n]] <- extract_function(next_response)
      next_scroll_id <- httr::content(next_response)$`_scroll_id`
      n <- n + 1
    } else {
      has_next <- FALSE
    }
  }

  do.call(rbind, scroll_results)
}


#' Elasticsearch HTTP response data extraction functions.
#'
#' Functions for extracting the different types of data that can be contained in a response to a
#' search API request.
#'
#' @name extract_query_results
#' @param response An HTTP response from a response to a search API request.
#' @return A data.frame of response results.
NULL

#' @rdname extract_query_results
extract_query_results <- function(response) {
  df <- jsonlite::fromJSON(httr::content(response, as = 'text'))$hits$hits$`_source`
  if (length(df) == 0) stop("no query results returned")
  jsonlite::flatten(df)
}

#' @rdname extract_query_results
extract_aggs_results <- function(response) {
  data <- jsonlite::fromJSON(httr::content(response, as = 'text'))
  # are results from a bucket aggregation or metric aggregation?
  if ("buckets" %in% names(data$aggregations[[1]])) {
    df <- data$aggregations[[1]]$buckets
  } else {
    df <- as.data.frame(data$aggregations)
  }
  if (length(df) == 0) stop("no aggs results returned")
  jsonlite::flatten(df)
}

#' @rdname extract_query_results
extract_id_results <- function(response) {
  df <- jsonlite::fromJSON(httr::content(response, as = 'text'))$hits$hits$`_id`
  if (length(df) == 0) stop("no ids returned")
  df
}


#' HTTP response error handling.
#'
#' If an HTTP request returns a status code that is not 200, then this functions throws an
#' exception and prints the prettified response contents to stderr.
#'
#' @param response An HTTP response from a request to a search API request.
#' @return Exception with prettified JSON response printed to stderr.
check_http_code_throw_error <- function(response) {
  response_code <- httr::status_code(response)
  if (!(response_code %in% c(200, 201))) {
    stop(paste("Elasticsearch returned a status code of", httr::status_code(response), "\n"),
         jsonlite::prettify(response))
  }
}
