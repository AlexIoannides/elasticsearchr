# elasticsearchr 0.2.0

* BREAKING CHANGE: `sort` has been renamed to `sort_on` to avoid clashing with the sort function in base R;
* Fixed an issue with `valid_url` that was causing an error on r-oldrel-windows-ix86+x86_64;
* Fixed data.frame index bug - Elasticsearch Bulk API was failing when data.frame was large (>15mb); and,
* Fixed an issue with `search_scroll` printing dots to stdout as opposed to returning them as message output.
* Enhanced `search_scroll` such that it retrieves a maximum of 100,000 documents per-scroll to speed-up query retrieval;


# elasticsearchr 0.1.0

* Initial release.



