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


context('api')


# ---- classes, methods and predicates ------------------------------------------------------------


test_that('elastic objects have the correct classes assigned to them', {
  # skip if on CRAN or Travis
  skip_on_travis()
  skip_on_cran()

  # arrange
  es_rescource <- elastic("http://localhost:9200", "iris", "data")

  # act
  elastic_classes <- class(es_rescource)

  # assert
  expect_identical(elastic_classes, c("elastic_rescource", "elastic"))
})


test_that('elastic objects correctly assemble search URLs when doc_types are specified', {
  # skip if on CRAN or Travis
  skip_on_travis()
  skip_on_cran()

  # arrange
  es_rescource <- elastic("http://localhost:9200", "iris", "data")

  # act
  search_url <- es_rescource$search_url

  # assert
  expect_identical(search_url, "http://localhost:9200/iris/data/_search")
})


test_that('elastic objects correctly assemble search URLs when doc_types are not specified', {
  # skip if on CRAN or Travis
  skip_on_travis()
  skip_on_cran()

  # arrange
  es_rescource <- elastic("http://localhost:9200", "iris")

  # act
  search_url <- es_rescource$search_url

  # assert
  expect_identical(search_url, "http://localhost:9200/iris/_search")
})


test_that('query objects have the correct classes assigned to them', {
  # arrange
  everything <- '{"match_all": {}}'

  # act
  es_query <- query(everything)

  # assert
  expect_identical(class(es_query), c("elastic_query", "elastic_api", "elastic"))
})


test_that('query objects will not accept invalid JSON', {
  # arrange
  bad_query_json <- '{"match_all": {}'

  # act & assert
  expect_error(query(bad_query_json))
})


test_that('query objects generate the correct search API call', {
  # arrange
  everything <- '{"match_all": {}}'

  # act
  es_query <- query(everything)

  # assert
  expect_identical(es_query$api_call, '"query":{"match_all": {}}')
})


test_that('sort objects have the correct classes assigned to them', {
  # arrange
  by_sepal_width <- '{"sepal_width": {"order": "asc"}}'

  # act
  es_sort <- sort_on(by_sepal_width)

  # assert
  expect_identical(class(es_sort), c("elastic_sort", "elastic_api", "elastic"))
})


test_that('sort objects will not accept invalid JSON', {
  # arrange
  bad_sort_json <- '{"sepal_width": {"order": "asc"}'

  # act & assert
  expect_error(sort_on(bad_sort_json))
})


test_that('sort objects generate the correct search API call', {
  # arrange
  by_sepal_width <- '{"sepal_width": {"order": "asc"}}'

  # act
  es_sort <- sort_on(by_sepal_width)

  # assert
  expect_identical(es_sort$api_call, '"sort":{"sepal_width": {"order": "asc"}}')
})


test_that('select_fields objects have the correct classes assigned to them', {
  # arrange
  select <- '{"includes": ["field1", "obj1.*"], "excludes": ["field2", "obj2.*"]}'

  # act
  es_source_filter <- select_fields(select)

  # assert
  expect_identical(class(es_source_filter), c("elastic_source_filter", "elastic_api", "elastic"))
})


test_that('select_fields objects will not accept invalid JSON', {
  # arrange
  bad_source_filter_json <- '{"includes": ["field1", "obj1.*"], "excludes": ["field2", "obj2.*"}'

  # act & assert
  expect_error(select_fields(bad_source_filter_json))
})


test_that('select_fields objects generate the correct search API call', {
  # arrange
  select <- '{"includes": ["field1", "obj1.*"], "excludes": ["field2"]}'

  # act
  es_source_filter <- select_fields(select)

  # assert
  expected_call <- '"_source": {"includes": ["field1", "obj1.*"], "excludes": ["field2"]}'
  expect_identical(es_source_filter$api_call, expected_call)
})


test_that('aggs objects have the correct classes assigned to them', {
  # arrange
  avg_sepal_width_per_cat <- '{"avg_sepal_width_per_cat": {
  "terms": {"field": "species"},
  "aggs": {"avg_sepal_width": {"avg": {"field": "sepal_width"}}}}
  }'

  # act
  es_agg <- aggs(avg_sepal_width_per_cat)

  # assert
  expect_identical(class(es_agg), c("elastic_aggs", "elastic_api", "elastic"))
  })


test_that('aggs objects will not accept invalid JSON', {
  # arrange
  bad_aggs_json <- '{"match_all": {}'

  # act & assert
  expect_error(aggs(bad_aggs_json))
})


test_that('aggs objects generate the correct search API call', {
  # arrange
  avg_sepal_width_per_cat <- '{"avg_sepal_width_per_cat": {
  "terms": {"field": "species"},
  "aggs": {"avg_sepal_width": {"avg": {"field": "sepal_width"}}}}
  }'

  # act
  es_agg <- aggs(avg_sepal_width_per_cat)

  # assert
  expected_api_call <- '"aggs":{"avg_sepal_width_per_cat": {
  "terms": {"field": "species"},
  "aggs": {"avg_sepal_width": {"avg": {"field": "sepal_width"}}}}
  }'
  expect_identical(es_agg$api_call, expected_api_call)
})


test_that('list_indices objects have the correct classes assigned to them', {
  # act
  es_info <- list_indices()

  # assert
  expect_identical(class(es_info), c("elastic_info", "elastic_api", "elastic"))
})


test_that('list_fields objects have the correct classes assigned to them', {
  # act
  es_info <- list_fields()

  # assert
  expect_identical(class(es_info), c("elastic_info", "elastic_api", "elastic"))
})


# ---- operators ----------------------------------------------------------------------------------


test_that('%info% list_indices() returns a list of all available indices', {
  # skip if on CRAN or Travis
  skip_on_travis()
  skip_on_cran()

  # arrange
  load_test_data()

  # act
  info_results <- elastic("http://localhost:9200", "iris", "data") %info% list_indices()

  # assert
  expect_equal(info_results, "iris")
  delete_test_data()
})


test_that('%info% list_fields() returns a list of all fields in an index', {
  # skip if on CRAN or Travis
  skip_on_travis()
  skip_on_cran()

  # arrange
  load_test_data()

  # act
  info_results <- elastic("http://localhost:9200", "iris", "data") %info% list_fields()

  # assert
  expected_fields <- c("petal_length", "petal_width", "sepal_length", "sepal_width", "sort_key", "species")
  expect_equal(info_results, expected_fields)
  delete_test_data()
})


test_that('%index% correctly indexes a large (>10mb single chunk) data frame', {
  # skip if on CRAN or Travis
  skip_on_travis()
  skip_on_cran()

  # arrange
  delete_test_data()
  iris_data_bulk <- data.frame(do.call(cbind, lapply(1:40, FUN = function(x) iris_data)))
  iris_data_bulk <- do.call(rbind, lapply(1:50, FUN = function(x) iris_data_bulk))
  iris_data_bulk['sort_key'] <- 1:nrow(iris_data_bulk)
  row.names(iris_data_bulk) <- 1:nrow(iris_data_bulk)
  colnames(iris_data_bulk) <- cleaned_field_names(colnames(iris_data_bulk))

  # act
  elastic("http://localhost:9200", "iris", "data") %index% iris_data_bulk
  wait_finish_indexing("http://localhost:9200/iris/data/_search?size=7500&q=*", nrow(iris_data_bulk))
  query_response <- httr::POST("http://localhost:9200/iris/data/_search?size=7500&q=*")
  query_results <- jsonlite::fromJSON(httr::content(query_response, as = 'text'))$hits$hits$`_source`
  query_results <- query_results[order(query_results$sort_key), ]
  row.names(query_results) <- query_results$sort_key

  # assert
  expect_equal(iris_data_bulk, query_results)
  delete_test_data()
})


test_that('%create% can create an index with a custom mapping', {
  # skip if on CRAN or Travis
  skip_on_travis()
  skip_on_cran()

  # arrange
  delete_test_data()

  # act
  elastic("http://localhost:9200", "iris") %create% mapping_default_simple()
  get_mapping <- httr::GET("http://localhost:9200/iris/_mapping")
  get_mapping_status <- httr::status_code(get_mapping)

  # assert
  expect_equal(get_mapping_status, 200)
  delete_test_data()
})


test_that('%delete% can delete all documents from an index', {
  # skip if on CRAN or Travis
  skip_on_travis()
  skip_on_cran()

  # arrange
  load_test_data()

  # act
  elastic("http://localhost:9200", "iris") %delete% TRUE
  wait_finish_delete("http://localhost:9200/iris/data/_search?size=150&q=*")
  query_response <- httr::POST("http://localhost:9200/iris/data/_search?size=150&q=*")
  query_response_status <- httr::status_code(query_response)

  # assert
  expect_equal(query_response_status, 404)
  delete_test_data()
})


test_that('%delete% can delete all documents from a type', {
  # skip if on CRAN or Travis
  skip_on_travis()
  skip_on_cran()

  # arrange
  load_test_data()

  # act
  elastic("http://localhost:9200", "iris", "data") %delete% TRUE
  wait_finish_delete("http://localhost:9200/iris/data/_search?size=150&q=*")
  query_response <- httr::POST("http://localhost:9200/iris/data/_search?size=150&q=*")
  query_results <- jsonlite::fromJSON(httr::content(query_response, as = 'text'))$hits$hits$`_source`
  get_mapping <- httr::GET("http://localhost:9200/iris/_mapping")
  get_mapping_status <- httr::status_code(get_mapping)

  # assert
  expect_null(query_results)
  expect_equal(get_mapping_status, 200)
  delete_test_data()
})


test_that('%delete% can delete selected documents from a type', {
  # skip if on CRAN or Travis
  skip_on_travis()
  skip_on_cran()

  # arrange
  load_test_data()
  query_response <- httr::POST("http://localhost:9200/iris/data/_search?size=150&q=*")
  doc_ids <- jsonlite::fromJSON(httr::content(query_response, as = 'text'))$hits$hits$`_id`

  # act
  elastic("http://localhost:9200", "iris", "data") %delete% doc_ids
  wait_finish_delete("http://localhost:9200/iris/data/_search?size=150&q=*")
  query_response <- httr::POST("http://localhost:9200/iris/data/_search?size=150&q=*")
  query_results <- jsonlite::fromJSON(httr::content(query_response, as = 'text'))$hits$hits$`_source`
  get_mapping <- httr::GET("http://localhost:9200/iris/_mapping")
  get_mapping_status <- httr::status_code(get_mapping)

  # assert
  expect_null(query_results)
  expect_equal(get_mapping_status, 200)
  delete_test_data()
})


test_that('we can query using the %search% operator and return all documents', {
  # skip if on CRAN or Travis
  skip_on_travis()
  skip_on_cran()

  # arrange
  load_test_data()
  everything <- '{"match_all": {}}'
  es_query <- query(everything)

  # act
  query_results <- elastic("http://localhost:9200", "iris", "data") %search% es_query

  query_results_sorted <- query_results[order(query_results["sort_key"]), ]
  rownames(query_results_sorted) <- query_results_sorted$sort_key

  # assert
  expect_equal(query_results_sorted, iris_data)
  delete_test_data()
})


test_that('we can query using the %search% operator and return documents sorted', {
  # skip if on CRAN or Travis
  skip_on_travis()
  skip_on_cran()

  # arrange
  load_test_data()
  everything <- '{"match_all": {}}'
  by_key <- '{"sort_key": {"order": "asc"}}'
  es_query <- query(everything, size = 10) + sort_on(by_key)

  # act
  query_results <- elastic("http://localhost:9200", "iris", "data") %search% es_query

  query_results_sorted <- query_results[order(query_results["sort_key"]), ]
  rownames(query_results_sorted) <- query_results_sorted$sort_key

  # assert
  expect_equal(query_results_sorted, iris_data[1:10, ])
  delete_test_data()
})


test_that('we can query using the %search% operator and return a subset of fields', {
  # skip if on CRAN or Travis
  skip_on_travis()
  skip_on_cran()

  # arrange
  load_test_data()
  fields <- c("sort_key", "sepal_length", "species")
  everything <- '{"match_all": {}}'
  source_filter_JSON <- '{"includes": ["sepal_length", "species", "sort_key"]}'
  es_query <- query(everything)
  es_source_filter <- select_fields(source_filter_JSON)

  # act
  query_results <-
    elastic("http://localhost:9200", "iris", "data") %search% (es_query + es_source_filter)

  query_results_sorted <- query_results[order(query_results["sort_key"]), fields]
  rownames(query_results_sorted) <- query_results_sorted$sort_key

  # assert
  expect_equal(query_results_sorted, iris_data[, fields])
  delete_test_data()
})


test_that('we can query using the %search% operator and return a sorted subset of fields', {
  # skip if on CRAN or Travis
  skip_on_travis()
  skip_on_cran()

  # arrange
  load_test_data()
  everything <- '{"match_all": {}}'
  by_sepal_length <- '[{"sepal_length": {"order": "asc"}}, {"sort_key": {"order": "asc"}}]'
  source_filter_JSON <- '{"includes": ["sepal_length", "species", "sort_key"]}'

  es_query <- query(everything)
  es_sort <- sort_on(by_sepal_length)
  es_source_filter <- select_fields(source_filter_JSON)
  es_filter_and_sort <- es_source_filter + es_sort

  # act
  query_results <-
    elastic("http://localhost:9200", "iris", "data") %search% (es_query + es_filter_and_sort)

  rownames(query_results) <- 1:150

  # assert
  iris_data_sorted <- iris_data[order(iris_data$sepal_length), colnames(query_results)]
  rownames(iris_data_sorted) <- 1:150
  expect_equal(query_results, iris_data_sorted)
  delete_test_data()
})


test_that('we can use bucket aggregations using the %search% operator', {
  # skip if on CRAN or Travis
  skip_on_travis()
  skip_on_cran()

  # arrange
  load_test_data()
  avg_sepal_width_per_cat <- '{"avg_sepal_width_per_cat": {
  "terms": {"field": "species"},
  "aggs": {"avg_sepal_width": {"avg": {"field": "sepal_width"}}}}
  }'
  es_aggs <- aggs(avg_sepal_width_per_cat)

  # act
  aggs_results <- elastic("http://localhost:9200", "iris", "data") %search% es_aggs

  # assert
  expect_equal(aggs_results, iris_test_aggs_bucket)
  delete_test_data()
})


test_that('we can use base-metric aggregations using the %search% operator', {
  # skip if on CRAN or Travis
  skip_on_travis()
  skip_on_cran()

  # arrange
  load_test_data()
  avg_sepal_width_per_cat <- '{"avg_sepal_width": {"avg": {"field": "sepal_width"}}}'
  es_aggs <- aggs(avg_sepal_width_per_cat)

  # act
  aggs_results <- elastic("http://localhost:9200", "iris", "data") %search% es_aggs

  # assert
  expect_equal(aggs_results, iris_test_aggs_metric)
  delete_test_data()
})


test_that('we can query + sort using the %search% operator', {
  # skip if on CRAN or Travis
  skip_on_travis()
  skip_on_cran()

  # arrange
  load_test_data()

  everything <- '{"match_all": {}}'
  by_sepal_width <- '[{"sepal_width": {"order": "asc"}}, {"sort_key": {"order": "asc"}}]'

  es_query <- query(everything)
  es_sort <- sort_on(by_sepal_width)
  es_query_sorted <- es_query + es_sort

  # act
  query_results <- elastic("http://localhost:9200", "iris", "data") %search% es_query_sorted
  rownames(query_results) <- 1:150

  # assert
  iris_data_sorted <- iris_data[order(iris_data$sepal_width, iris_data$sort_key), ]
  rownames(iris_data_sorted) <- 1:150
  expect_equal(query_results, iris_data_sorted)
  delete_test_data()
})


test_that('we can query + aggregate using the %search% operator', {
  # skip if on CRAN or Travis
  skip_on_travis()
  skip_on_cran()

  # arrange
  load_test_data()

  everything <- '{"match_all": {}}'

  avg_sepal_width_per_cat <- '{"avg_sepal_width_per_cat": {
  "terms": {"field": "species"},
  "aggs": {"avg_sepal_width": {"avg": {"field": "sepal_width"}}}}
  }'

  es_query <- query(everything)
  es_agg <- aggs(avg_sepal_width_per_cat)
  es_agg_on_query <- es_query + es_agg

  # act
  aggs_results <- elastic("http://localhost:9200", "iris", "data") %search% es_agg_on_query

  # assert
  expect_equal(aggs_results, iris_test_aggs_bucket)
  delete_test_data()
  })


test_that('adding a sort object to a query object results in a query object', {
  # arrange
  everything <- '{"match_all": {}}'
  by_sepal_width <- '{"sepal_width": {"order": "asc"}}'

  # act
  es_query <- query(everything)
  es_sort <- sort_on(by_sepal_width)
  es_query_sorted <- es_query + es_sort

  # assert
  expect_identical(class(es_query_sorted), c("elastic_query", "elastic_api", "elastic"))
})


test_that('adding a sort object to a query object generates the correct search API call', {
  # arrange
  everything <- '{"match_all": {}}'
  by_sepal_width <- '{"sepal_width": {"order": "asc"}}'

  # act
  es_query <- query(everything)
  es_sort <- sort_on(by_sepal_width)
  es_query_sorted <- es_query + es_sort

  # assert
  expected_api_call <- '"query":{"match_all": {}},"sort":{"sepal_width": {"order": "asc"}}'

  expect_identical(es_query_sorted$api_call, expected_api_call)
})


test_that('adding an aggs object to a query object results in an aggs object', {
  # arrange
  everything <- '{"match_all": {}}'

  avg_sepal_width_per_cat <- '{"avg_sepal_width_per_cat": {
  "terms": {"field": "species"},
  "aggs": {"avg_sepal_width": {"avg": {"field": "sepal_width"}}}}
  }'

  # act
  es_query <- query(everything)
  es_agg <- aggs(avg_sepal_width_per_cat)
  es_agg_on_query <- es_query + es_agg

  # assert
  expect_identical(class(es_agg_on_query), c("elastic_aggs", "elastic_api", "elastic"))
})


test_that('adding an aggs object to a query object generates the correct search API call', {
  # arrange
  everything <- '{"match_all": {}}'

  avg_sepal_width_per_cat <- '{"avg_sepal_width_per_cat": {
    "terms": {"field": "species"},
    "aggs": {"avg_sepal_width": {"avg": {"field": "sepal_width"}}}}
  }'

  # act
  es_query <- query(everything)
  es_agg <- aggs(avg_sepal_width_per_cat)
  es_agg_on_query <- es_query + es_agg

  # assert
  expected_api_call <- '"query":{"match_all": {}},"aggs":{"avg_sepal_width_per_cat": {
    "terms": {"field": "species"},
    "aggs": {"avg_sepal_width": {"avg": {"field": "sepal_width"}}}}
  }'

  expect_identical(es_agg_on_query$api_call, expected_api_call)
})
