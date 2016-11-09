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
#' @param x
#'
#' @return
#' @export
#'
#' @examples
is_elastic <- function(x) inherits(x, "elastic")
is_elastic_url <- function(x) inherits(x, "elastic_url")
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
search <- function(cluster_url, index, doc_type = NULL) {
  stopifnot(is.character(cluster_url), is.character(index), is.character(doc_type) | is.null(doc_type),
            valid_url(cluster_url))

  if (is.null(doc_type)) {
    valid_cluster_url <- paste(cluster_url, index, "_search", sep = "/")
  } else {
    valid_cluster_url <- paste(cluster_url, index, doc_type, "_search", sep = "/")
  }

  structure(list("elastic_cluster_url" = valid_cluster_url, "cluster_url" = cluster_url,
                 "index" = index, "doc_type" = doc_type), class = c("elastic", "elastic_url"))
}


`%index%` <- function(search, docs) UseMethod("%index%")

`%index%` <- function(search, df) {
  stopifnot(is_elastic_url(search) & is.data.frame(df))
  colnames(df) <- cleaned_field_names(colnames(df))
  has_ids <- "id" %in% colnames(df)
  num_docs <- nrow(df)

  if (has_ids) {
    metadata <- create_metadata("index", search$index, search$doc_type, df$id)
  } else {
    metadata_json <- create_metadata("index", search$index, search$doc_type)
    metadata <- rep(metadata_json, num_docs)
  }

  bulk_data_file <- create_bulk_upload_file(metadata, df)

  response <- httr::PUT(url = search$cluster_url,
                       path = "/_bulk",
                       body = httr::upload_file(bulk_data_file))

  file.remove(bulk_data_file)

  if (httr::status_code(response) == 200 & !httr::content(response)$errors) {
    "data successfully index"
  } else if (httr::content(response)$errors) {
    messages <- httr::content(response)$items
    warning(jsonlite::prettify(httr::content(response, as = "text")))
  } else {
    stop("invalid request")
  }
}


delete <- function(doc_ids) {

}


#' Title
#'
#' @param json
#'
#' @return
#' @export
#'
#' @examples
query <- function(json) {
  stopifnot(jsonlite::validate(json))
  api_call <- paste0('"query":', json)
  structure(list("api_call" = api_call), class = c("elastic", "elastic_api", "elastic_query"))
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
#' @param x
#'
#' @return
#' @export
#'
#' @examples
print.elastic_api <- function(x) {
  complete_json <- paste0('{', x$api_call, '}')
  jsonlite::prettify(complete_json)
}


#' Title
#'
#' @param x
#' @param y
#'
#' @return
#' @export
#'
#' @examples
`+.elastic_api` <- function(x, y) {
  stopifnot((is_elastic_query(x) & is_elastic_aggs(y)) | (is_elastic_aggs(x) & is_elastic_query(y)))

  combined_call <- paste0('"size": 0,', x$api_call, ',', y$api_call)
  structure(list(api_call = combined_call), class = c("elastic", "elastic_api", "elastic_aggs"))
}


#' Title
#'
#' @param api_url
#' @param api_args
#'
#' @return
#' @export
#'
#' @examples
`%>>%` <- function(x, y) UseMethod("%>>%")

`%>>%.elastic` <- function(api_url, api_args) {
  stopifnot(is_elastic_url(api_url) & is_elastic_api(api_args))
  api_call_payload <- paste0('{', api_args$api_call, '}')

  response <- httr::POST(api_url$elastic_cluster_url, body = api_call_payload)
  if (httr::status_code(response) != 200) {
    stop(paste0("response from server: ", httr::status_code(response)))
  }

  parsed_response <- jsonlite::fromJSON(httr::content(response, as = 'text'))
  if (is_elastic_aggs(api_args)) {
    if (length(parsed_response$aggregations[[1]]$buckets) == 0) stop("empty response to request")
    return_data <- jsonlite::flatten(parsed_response$aggregations[[1]]$buckets)
  } else if (is_elastic_query(api_args)) {
    if (length(parsed_response$hits$hits$`_source`) == 0) stop("empty response to request")
    return_data <- jsonlite::flatten(parsed_response$hits$hits$`_source`)
  }

  return_data
}


# ---- test classes and methods ----
s <- search("http://localhost:9200", "iris", "data")
s %index% iris

q <- query('{"match_all": {}}')
a <- aggs('{"avg_sepal_width_per_cat": { "terms": {"field": "species", "size": 0, "order": [{"avg_sepal_width": "desc"}]}, "aggs": {"avg_sepal_width": {"avg": {"field": "sepal_width"}}} }}')

print(q)
s %>>% q

print(a)
s %>>% a

print(q + a)
s %>>% (q + a)


# ---- testing stuff as inspired from Advanced R and ggplot2 ----
# obj <- function(x) structure(list(x), class = "obj")
# is.obj <- function(x) inherits(x, "obj")
#
# index <- function(x) UseMethod("index")
# index.obj <- function(x) print(x[[1]])
#
# # note that you don't need to define the generic `+` as it's already defined in base R!
# `+.obj` <- function(x, y) {
#   obj(x[[1]] * y[[1]])
# }
#
# # examples
# a <- obj(1)
# b <- obj(2)
#
# index(a + b)
# # [1] 2
