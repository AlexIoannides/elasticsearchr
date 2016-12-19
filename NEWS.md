# elasticsearchr 0.1.0.900

* BREAKING CHANGE: `sort` has been renamed to `sort_on` to avoid clashing with the sort function in base R;
* Fixed an issue with `valid_url` that was causing an error on r-oldrel-windows-ix86+x86_64;
* Fixed data.frame index bug - Elasticsearch Bulk API was failing when data.frame was large (>15mb); and,
* Fixed an issue with `search_scroll` printing dots to stdout as opposed to returning them as message output.

# elasticsearchr 0.1.0

* Initial release.



