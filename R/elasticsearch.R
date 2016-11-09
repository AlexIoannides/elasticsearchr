#' Title
#'
#' @param x
#'
#' @return
#' @export
#'
#' @examples
is_elastic <- function(x) inherits(x, "elastic")
is_elastic_rescource <- function(x) inherits(x, "elastic_rescource")
is_elastic_api <- function(x) inherits(x, "elastic_api")
is_elastic_query <- function(x) inherits(x, "elastic_query")
is_elastic_aggs <- function(x) inherits(x, "elastic_aggs")


#' Title
#'
#' @param cluster_url
#' @param index
#' @param doc_type
#'
#' @return
#' @export
#'
#' @examples
elastic <- function(cluster_url, index, doc_type = NULL) {
  stopifnot(is.character(cluster_url), is.character(index), is.character(doc_type) | is.null(doc_type),
            valid_url(cluster_url))

  if (is.null(doc_type)) {
    valid_cluster_url <- paste(cluster_url, index, "_search", sep = "/")
  } else {
    valid_cluster_url <- paste(cluster_url, index, doc_type, "_search", sep = "/")
  }

  structure(list("search_url" = valid_cluster_url, "cluster_url" = cluster_url,
                 "index" = index, "doc_type" = doc_type), class = c("elastic", "elastic_rescource"))
}


#' Title
#'
#' @param rescource
#' @param df
#'
#' @return
#' @export
#'
#' @examples
`%index%` <- function(rescource, df) UseMethod("%index%")

`%index%` <- function(rescource, df) {
  stopifnot(is_elastic_rescource(rescource), is.data.frame(df), !is.null(rescource$doc_type))
  colnames(df) <- cleaned_field_names(colnames(df))
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
                       body = httr::upload_file(bulk_data_file))

  file.remove(bulk_data_file)

  if (httr::status_code(response) == 200 & !httr::content(response)$errors) {
    "... data successfully indexed"
  } else if (httr::content(response)$errors) {
    messages <- httr::content(response)$items
    warning(jsonlite::prettify(httr::content(response, as = "text")))
  } else {
    check_http_code_throw_error(response)
  }
}


#' Title
#'
#' @param rescource
#' @param approve
#'
#' @return
#' @export
#'
#' @examples
`%delete%` <- function(rescource, approve) UseMethod("%delete%")

`%delete%` <- function(rescource, approve) {
  if (!is.character(approve)) {
    ids <- approve
  } else {
    if (approve != "approved") stop("please approve deletion") else ids <- NULL
  }

  if (is.null(ids)) {
    if (is.null(rescource$doc_type)) {
      response <- httr::DELETE(paste(rescource$cluster_url, rescource$index, sep = "/"))
      check_http_code_throw_error(response)
      print(paste0("... ", rescource$index, " has been deleted"))
    } else {
      api_call_payload <- '{"query": {"match_all": {}}}'
      doc_type_ids <- as.vector(scroll_search(rescource, api_call_payload, extract_id_results))
      metadata <- create_metadata("delete", rescource$index, rescource$doc_type, doc_type_ids)
      deletions_file <- create_bulk_delete_file(metadata)
      response <- httr::PUT(url = rescource$cluster_url,
                            path = "/_bulk",
                            body = httr::upload_file(deletions_file))

      file.remove(deletions_file)
      check_http_code_throw_error(response)
      print(paste0("... ", rescource$index, "/", rescource$doc_type, " has been deleted"))
    }
  } else {
    metadata <- create_metadata("delete", rescource$index, rescource$doc_type, ids)
    deletions_file <- create_bulk_delete_file(metadata)
    response <- httr::PUT(url = rescource$cluster_url,
                          path = "/_bulk",
                          body = httr::upload_file(deletions_file))

    file.remove(deletions_file)
    check_http_code_throw_error(response)
    print(paste0("... ", paste0(ids, collapse = ", "), " have been deleted"))
  }
}


#' Title
#'
#' @param json
#' @param size
#'
#' @return
#' @export
#'
#' @examples
query <- function(json, size = 0) {
  stopifnot(jsonlite::validate(json))
  api_call <- paste0('"query":', json)
  structure(list("api_call" = api_call, "size" = size), class = c("elastic", "elastic_api", "elastic_query"))
}


#' Title
#'
#' @param json
#'
#' @return
#' @export
#'
#' @examples
aggs <- function(json) {
  stopifnot(jsonlite::validate(json))
  api_call <- paste0('"aggs":', json)
  structure(list("api_call" = api_call), class = c("elastic", "elastic_api", "elastic_aggs"))
}


#' Title
#'
#' @param search
#'
#' @return
#' @export
#'
#' @examples
print.elastic_api <- function(search) {
  complete_json <- paste0('{', search$api_call, '}')
  jsonlite::prettify(complete_json)
}


#' Title
#'
#' @param query
#' @param aggs
#'
#' @return
#' @export
#'
#' @examples
`+.elastic_api` <- function(query, aggs) {
  stopifnot((is_elastic_query(query) & is_elastic_aggs(aggs)) | (is_elastic_aggs(query) & is_elastic_query(aggs)))

  combined_call <- paste0('"size": 0,', query$api_call, ',', aggs$api_call)
  structure(list(api_call = combined_call), class = c("elastic", "elastic_api", "elastic_aggs"))
}


#' Title
#'
#' @param rescource
#' @param search
#'
#' @return
#' @export
#'
#' @examples
`%search%` <- function(rescource, search) UseMethod("%search%")

`%search%.elastic` <- function(rescource, search) {
  stopifnot(is_elastic_rescource(rescource) & is_elastic_api(search))

  if (is_elastic_query(search)) {
    if (search$size != 0) {
      api_call_payload <- paste0('{"size":', search$size, ', ', search$api_call, '}')
      return(from_size_search(rescource, api_call_payload))

    } else {
      api_call_payload <- paste0('{"size": 10000', ', ', search$api_call, '}')
      return(scroll_search(rescource, api_call_payload))

    }
  } else {
    api_call_payload <- paste0('{', search$api_call, '}')
    return(from_size_search(rescource, api_call_payload))
  }
}
