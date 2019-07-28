# elasticsearchr 0.3.1

* Changed back-end to use dplyr::bind_rows as opposed to do.call(rbind) to handle NAs consistently.


# elasticsearchr 0.3.0

* Added support for source filtering with the `select_fields` function.
* Added the `%info%` operator to be used for retreiving cluster and index information.
* Added the `list_indices` function for retrieving a list of all available indices.
* Added the `list_fields` function for retrieving a list of all available indices.


# elasticsearchr 0.2.3

* Refactored `valid_url` with the enhanced `valid_connection` function, based on HTTP requests to the cluster health API.


# elasticsearchr 0.2.2

* Added support for HTTPS endpoints.


# elasticsearchr 0.2.1

* Modified all HTTP requests to have `Content-Type` headers for Elasticsearch 6.x compatibility;
* Fixed an issue where `extract_aggs_results` could not handle results from base metric aggregations;
* Fixed an issue where `valid_url` will not return `TRUE` when Elasticsearch port numbers are not 4 digits long;


# elasticsearchr 0.2.0

* BREAKING CHANGE: `sort` has been renamed to `sort_on` to avoid clashing with the sort function in base R;
* Fixed an issue with `valid_url` that was causing an error on r-oldrel-windows-ix86+x86_64;
* Fixed data.frame index bug - Elasticsearch Bulk API was failing when data.frame was large (>15mb); and,
* Fixed an issue with `search_scroll` printing dots to stdout as opposed to returning them as message output.
* Enhanced `search_scroll` such that it retrieves a maximum of 10,000 documents per-scroll to speed-up query retrieval;


# elasticsearchr 0.1.0

* Initial release.
