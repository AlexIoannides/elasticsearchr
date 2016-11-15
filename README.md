[![Build Status](https://travis-ci.org/AlexIoannides/elasticsearchr.svg?branch=master)](https://travis-ci.org/AlexIoannides/elasticsearchr)
# elasticsearchr

- v0.1.0 - a work-in-progress (e.g. no unit-test), but definitely useable.
- Elasticsearch v2.3.5

A lightweight Elasticsearch client for R. Implements a simple DSL for indexing, deleting, querying and aggregating data using Elasticsearch.

```r
# ---- test classes and methods ----
es <- elastic("http://localhost:9200", "iris", "data")
es %create% mapping_default_simple()

es %index% iris

q <- query('{
  "match_all": {}
}')

a <- aggs('{
  "avg_sepal_width_per_cat": {
    "terms": {
      "field": "species",
      "size": 0,
      "order": [
        {
          "avg_sepal_width": "desc"
        }
        ]
    },
    "aggs": {
      "avg_sepal_width": {
        "avg": {
          "field": "sepal_width"
        }
      }
    }
  }
}')

print(q)
es %search% q

print(a)
es %search% a

print(q + a)
es %search% (q + a)

elastic("http://localhost:9200", "iris", "data") %delete% "approved"
elastic("http://localhost:9200", "iris") %delete% "approved"
```
