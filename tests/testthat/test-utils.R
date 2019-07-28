# Copyright 2016-2019 Alex Ioannides
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


context('elasticsearchr utils')


test_that('valid_connection identifies valid URLs to Elasticsearch rescources', {
  # skip if on CRAN or Travis
  skip_on_travis()
  skip_on_cran()

  # arrange
  url <- "http://localhost:9200"

  # act
  is_valid_connection <- valid_connection(url)

  # assert
  expect_true(is_valid_connection)
})


test_that('valid_connection identifies invalid URLs to Elasticsearch rescources', {
  # skip if on CRAN or Travis
  skip_on_travis()
  skip_on_cran()

  # arrange
  url <- "localhost:9201"

  # act & assert
  expect_error(valid_connection(url))
})


test_that('elastic_version returns the Elasticsearch version number', {
  # skip if on CRAN or Travis
  skip_on_travis()
  skip_on_cran()

  # arrange
  url <- "http://localhost:9200"

  # act
  version <- elastic_version(url)

  # assert
  expect_type(version$major, "integer")
  expect_type(version$minor, "integer")
  expect_type(version$build, "integer")
})


test_that('cleaned_field_names removes periods from data.frame column names', {
  # arrange
  iris_colnames <- colnames(iris)

  # act
  cleaned_column_names <- cleaned_field_names(iris_colnames)

  # assert
  expect_true(length(grep("\\.", cleaned_column_names)) == 0)
})


test_that('cleaned_field_names converts all column names to lowercase', {
  # arrange
  column_name <- "UPPER_and_lower"

  # act
  cleaned_column_name <- cleaned_field_names(column_name)

  # assert
  expect_equal(cleaned_column_name, "upper_and_lower")
})


test_that('create_metadata creates Bulk API metadata when doc ids are given', {
  # arrange
  doc_ids <- c(1, 2)

  # act
  metadata <- create_metadata("index", "iris", "data", doc_ids)

  # assert
  expected_metadata <- c('{"index": {"_index": "iris", "_type": "data", "_id": "1"}}',
                       '{"index": {"_index": "iris", "_type": "data", "_id": "2"}}')
  expect_equal(metadata, expected_metadata)
})


test_that('create_metadata creates Bulk API metadata when no doc ids are given', {
  # arrange
  n <- 2

  # act
  metadata <- create_metadata("index", "iris", "data", n = 2)

  # assert
  expected_metadata <- c('{"index": {"_index": "iris", "_type": "data"}}',
                       '{"index": {"_index": "iris", "_type": "data"}}')
  expect_equal(metadata, expected_metadata)
})


test_that('create_bulk_upload_file produces the bulk_upload file for indexing data.frame data', {
  # arrange
  df <- iris[1:2,]
  metadata <- create_metadata("index", "iris", "data", n = 2)

  # act
  bulk_upload_file <- create_bulk_upload_file(metadata, df)

  # assert
  bulk_upload_file_contents <- readLines(bulk_upload_file)
  file.remove(bulk_upload_file)

  expected_upload_file <- c(
    '{\"index\": {\"_index\": \"iris\", \"_type\": \"data\"}}',
    '{\"Sepal.Length\":5.1,\"Sepal.Width\":3.5,\"Petal.Length\":1.4,\"Petal.Width\":0.2,\"Species\":\"setosa\"}',
    '{\"index\": {\"_index\": \"iris\", \"_type\": \"data\"}}',
    '{\"Sepal.Length\":4.9,\"Sepal.Width\":3.0,\"Petal.Length\":1.4,\"Petal.Width\":0.2,\"Species\":\"setosa\"}'
  )

  expect_equal(expected_upload_file, bulk_upload_file_contents)
})


test_that('create_bulk_delete_file produces bulk_delete file', {
  # arrange
  ids <- c(1, 2)
  metadata <- create_metadata("delete", "iris", "data", ids)

  # act
  bulk_delete_file <- create_bulk_delete_file(metadata)

  # assert
  bulk_delete_file_contents <- readLines(bulk_delete_file)
  file.remove(bulk_delete_file)

  expected_delete_file <- c('{"delete": {"_index": "iris", "_type": "data", "_id": "1"}}',
                            '{"delete": {"_index": "iris", "_type": "data", "_id": "2"}}')

  expect_equal(expected_delete_file, bulk_delete_file_contents)
})


test_that('index_bulk_dataframe correctly indexes a data frame', {
  # skip if on CRAN or Travis
  skip_on_travis()
  skip_on_cran()

  # arrange
  delete_test_data()

  # act
  index_bulk_dataframe(elastic("http://localhost:9200", "iris", "data"), iris_data)
  wait_finish_indexing("http://localhost:9200/iris/data/_search?size=150&q=*", 150)
  query_response <- httr::POST("http://localhost:9200/iris/data/_search?size=150&q=*")
  query_results <- jsonlite::fromJSON(httr::content(query_response, as = 'text'))$hits$hits$`_source`
  query_results <- query_results[order(query_results$sort_key), ]
  row.names(query_results) <- query_results$sort_key

  # assert
  expect_equal(iris_data, query_results)
  delete_test_data()
})


test_that('index_bulk_dataframe correctly detects and assigns document ids', {
  # skip if on CRAN or Travis
  skip_on_travis()
  skip_on_cran()

  # arrange
  delete_test_data()
  iris_data_ids <- iris_data
  colnames(iris_data_ids) <- c(colnames(iris_data_ids)[1:5], "id")

  # act
  index_bulk_dataframe(elastic("http://localhost:9200", "iris", "data"), iris_data_ids)
  wait_finish_indexing("http://localhost:9200/iris/data/_search?size=150&q=*", 150)
  query_response <- httr::GET("http://localhost:9200/iris/data/150")
  query_results <- data.frame(
    jsonlite::fromJSON(httr::content(query_response, as = 'text'))$`_source`,
    stringsAsFactors = FALSE
  )
  row.names(query_results) <- query_results$id

  # assert
  expect_equal(iris_data_ids[150,], query_results)
  delete_test_data()
})


test_that('from_size_search retrieves query results from Elasticsearch', {
  # skip if on CRAN or Travis
  skip_on_travis()
  skip_on_cran()

  # arrange
  load_test_data()
  query <- '{"size": 150, "query": {"match_all": {}}}'

  # act
  query_results <- from_size_search(list("search_url" = "http://localhost:9200/iris/data/_search"),
                                    query)

  query_results_sorted <- query_results[order(query_results["sort_key"]), ]
  rownames(query_results_sorted) <- query_results_sorted$sort_key

  # assert
  expect_equal(query_results_sorted, iris_data)
  delete_test_data()
})


test_that('from_size_search retrieves aggregation results from Elasticsearch', {
  # skip if on CRAN or Travis
  skip_on_travis()
  skip_on_cran()

  # arrange
  load_test_data()
  aggs <- '{"aggs": {"avg_sepal_width_per_species":{"terms":{"field":"species","size":3},
    "aggs":{"avg_sepal_width":{"avg":{"field":"sepal_width"}}}}}}'

  # act
  aggs_results <- from_size_search(list("search_url" = "http://localhost:9200/iris/data/_search"),
                                    aggs)

  # assert
  expect_equal(aggs_results, iris_test_aggs_bucket)
  delete_test_data()
})


test_that('scroll_search retrieves query results from Elasticsearch', {
  # skip if on CRAN or Travis
  skip_on_travis()
  skip_on_cran()

  # arrange
  load_test_data()
  query <- '{"query": {"match_all": {}}}'

  # act
  query_results <- scroll_search(list("cluster_url" = "http://localhost:9200",
                                      "search_url" = "http://localhost:9200/iris/data/_search"),
                                 query)

  query_results_sorted <- query_results[order(query_results["sort_key"]), ]
  rownames(query_results_sorted) <- query_results_sorted$sort_key

  # assert
  expect_equal(query_results_sorted, iris_data)
  delete_test_data()
})

