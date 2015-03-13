context('elastic_function')

test_that('elastic_conn produces a valid list object', {
  connection_valid <- elastic_conn('http://localhost:9200', 'wine', 'tag_data')
  expect_is(connection_valid, 'list')
  expect_equal(length(connection_valid), 4)
})
