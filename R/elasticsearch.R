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


index <- function(docs_data_frame) {

}


delete <- function(doc_ids) {

}


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

  structure(list("elastic_cluster_url" = valid_cluster_url), class = c("elastic", "elastic_url"))
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
s <- search("http://localhost:9200", "lp", "bids")
q <- query('{"match_all": {}}')
a <- aggs('{"max_bid_per_listing": { "terms": {"field": "listing.id", "size": 10, "order": [{"max_bid": "desc"}]}, "aggs": {"max_bid": {"max": {"field": "bid_details.amount"}}} }}')

print(q)
out_1 <- s %>>% q

print(a)
out_2 <- s %>>% a

print(q + a)
out_3 <- s %>>% (q + a)


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
