# ---- prepare test data frame
iris_data <- iris
colnames(iris_data) <- c("sepal_length", "sepal_width", "petal_length", "petal_width", "species")
iris_data["species"] <- as.character(iris_data$species)
iris_data["sort_key"] <- 1:150


# ---- compute expected aggretaion results ----
iris_test_aggs <- data.frame(
  "key" = c("setosa", "versicolor", "virginica"),
  "doc_count" = c(50, 50, 50),
  "avg_sepal_width.value" = c(3.428, 2.770, 2.974),
  stringsAsFactors = FALSE
  )


# ---- functions for loading and deleting test data from Elasticsearch on http://localhost:9200 ----
load_test_data <- function() {
  # ping Elasticsearch to check that it exists
  tryCatch(
    ping_es_cluster <- httr::GET("http://localhost:9200"),
    error = function(e) stop("can't find Elasticsearch at http://localhost:9200", call. = FALSE)
  )

  # delete any index called 'iris' on localhost
  response <- httr::DELETE("http://localhost:9200/iris")

  # index iris dataset from first principles (i.e. not using the elasticsearchr)
  for (i in 1:150) {
    iris_json_data <- gsub("\\[|\\]", "", jsonlite::toJSON((iris_data[i, ])))
    response <- httr::POST(paste0("http://localhost:9200/iris/data/", i), body = iris_json_data,
                           encode = "json")
    if (httr::status_code(response) != 201) {
      stop("cannot index data into Elasticsearch for running tests", call. = FALSE)
    }
  }

  # wait until all 150 documents have been indexed and are ready for searching until returning
  wait_finish_indexing("http://localhost:9200/iris/data/_search?size=150&q=*", 150)

  TRUE
}


wait_finish_indexing <- function(search_url, results_size) {
  waiting <- TRUE
  start_time <- Sys.time()
  while (waiting) {
    response <- httr::POST(search_url)
    available_data <- nrow(jsonlite::fromJSON(httr::content(response, as = 'text'))$hits$hits)

    if (!is.null(available_data)) {
      if (available_data == results_size) {
        waiting <- FALSE
      } else {
        running_time <- Sys.time() - start_time
        if (running_time > 60) stop("indexing Elasticsearch test data has time-out")
      }
    }

  }

  TRUE
}


wait_finish_delete <- function(search_url) {
  waiting <- TRUE
  start_time <- Sys.time()
  while (waiting) {
    response <- httr::POST(search_url)
    available_data <- nrow(jsonlite::fromJSON(httr::content(response, as = 'text'))$hits$hits)
    if (is.null(available_data)) {
      waiting <- FALSE
    } else {
      running_time <- difftime(Sys.time(), start_time, units = "secs")
      if (running_time > 30) stop("indexing Elasticsearch test data has time-out")
    }

  }

  TRUE
}


delete_test_data <- function() {
  tryCatch(
    response <- httr::DELETE("http://localhost:9200/iris"),
    error = function(e) stop("can't find iris index at http://localhost:9200/iris", call. = FALSE)
  )

  NULL
}
