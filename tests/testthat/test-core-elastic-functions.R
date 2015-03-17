context('elastic_function')

test_that('elastic_conn produces a valid list object with valid Elasticsearch details', {
  expect_is(elastic_conn('http://localhost:9200', 'wine', 'tag_data', FALSE), 'list')
  expect_equal(length(elastic_conn('http://localhost:9200', 'wine', 'tag_data', FALSE)), 4)

  expect_error(elastic_conn('http://localhost:9200/wine', 'wine', 'tag_data', FALSE))
  expect_error(elastic_conn('http://localhost:9200/', 'wine', 'tag_data', FALSE))
  expect_error(elastic_conn('http://localhost:9200', 'wine/tag_data', 'tag_data', FALSE))
  expect_error(elastic_conn('http://localhost:9200', 'wine', 'wine/tag_data', FALSE))

  expect_error(elastic_conn(9200, 'wine', 'tag_data', FALSE))
  expect_error(elastic_conn('http://localhost:9200/', 9200, 'tag_data', FALSE))
  expect_error(elastic_conn('http://localhost:9200', 'wine', 9200, FALSE))
})

test_that('elastic_create_dated_index_name produces valid index names', {
  expect_is(elastic_create_dated_index_name('logs', Sys.time()), 'character')

  expect_equal(elastic_create_dated_index_name('logs', as.POSIXct("2015-01-31 07:30:00 GMT")), 'logs-2015.01.31')
  expect_equal(elastic_create_dated_index_name('logs', '2015-01-31'), 'logs-2015.01.31')

  expect_error(elastic_create_dated_index_name('logs', '2015-31-01'))
  expect_error(elastic_create_dated_index_name(1, '2015-01-31'))
})

test_that('elastic_bulk_file produces valid files of bulk JSON data that can be sucessfully uoploaded to the Elasticsearch bulk API', {

  # test indexing
  metadata_1 <- data.frame(index = c('my_index', 'my_index', 'another_index'),
                           type = c('my_type_1', 'my_type_2', 'my_type_3'),
                           id = c('A1', 'A2', 'B1'))

  data_1 <- data.frame(name = c('alex', 'mike', 'phil'),
                       age = c(23, 40, 42),
                       height = c(5.8, 5.9, 6.0),
                       verified = c(TRUE, FALSE, NA))

  expected_output_1 <- c('{"index":{"_index":"my_index","_type":"my_type_1","_id":"A1"}}',
                         '{"name":"alex","age":23,"height":5.8,"verified":true}',
                         '{"index":{"_index":"my_index","_type":"my_type_2","_id":"A2"}}',
                         '{"name":"mike","age":40,"height":5.9,"verified":false}',
                         '{"index":{"_index":"another_index","_type":"my_type_3","_id":"B1"}}',
                         '{"name":"phil","age":42,"height":6}')

  bulk_data_1 <- readLines(elastic_bulk_file('index', data_1, metadata_1, file_name = 'bulk_file_1.txt'))
  file.remove('bulk_file_1.txt')
  expect_equal(bulk_data_1, expected_output_1)

  # test updating
  metadata_2 <- data.frame(index = c('my_index', 'my_index', 'another_index'),
                           type = c('my_type_1', 'my_type_2', 'my_type_3'),
                           id = c('A1', 'A2', 'B1'))

  data_2 <- data.frame(name = c('al', NA, NA),
                       age = c(35, NA, 40),
                       height = c(NA, 5.8, NA),
                       verified = c(NA, TRUE, NA))

  expected_output_2 <- c('{"update":{"_index":"my_index","_type":"my_type_1","_id":"A1"}}',
                         '{"name":"al","age":35}',
                         '{"update":{"_index":"my_index","_type":"my_type_2","_id":"A2"}}',
                         '{"height":5.8,"verified":true}',
                         '{"update":{"_index":"another_index","_type":"my_type_3","_id":"B1"}}',
                         '{"age":40}')

  bulk_data_2 <- readLines(elastic_bulk_file('update', data_2, metadata_2, file_name = 'bulk_file_2.txt'))
  file.remove('bulk_file_2.txt')
  expect_equal(bulk_data_2, expected_output_2)

  # test delete
  metadata_3 <- data.frame(index = c('my_index', 'my_index', 'another_index'),
                           type = c('my_type_1', 'my_type_2', 'my_type_3'),
                           id = c('A1', 'A2', 'B1'))

  expected_output_3 <- c('{"delete":{"_index":"my_index","_type":"my_type_1","_id":"A1"}}',
                         '{"delete":{"_index":"my_index","_type":"my_type_2","_id":"A2"}}',
                         '{"delete":{"_index":"another_index","_type":"my_type_3","_id":"B1"}}')

  bulk_data_3 <- readLines(elastic_bulk_file('delete', NULL, metadata_3, file_name = 'bulk_file_3.txt'))
  file.remove('bulk_file_3.txt')
  expect_equal(bulk_data_3, expected_output_3)

  # test incorrect function use
  expect_error(elastic_bulk_file('create', data_2, NULL))
  expect_error(elastic_bulk_file('index', NULL, NULL))
  expect_error(elastic_bulk_file('update', NULL, metadata_2))
  expect_error(elastic_bulk_file('update', data_2, NULL))
  expect_error(elastic_bulk_file('update', data_2, metadata_2[-1,]))
  expect_error(elastic_bulk_file('delete', data_2, NULL))
})
