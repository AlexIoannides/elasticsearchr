<!-- [![codecov](https://codecov.io/github/alexioannides/elasticsearchr/branch/master/graphs/badge.svg)](https://codecov.io/github/alexioannides/elasticsearchr) -->
[![Build Status](https://travis-ci.org/AlexIoannides/elasticsearchr.svg?branch=master)](https://travis-ci.org/AlexIoannides/elasticsearchr) [![AppVeyor Build Status](https://ci.appveyor.com/api/projects/status/github/AlexIoannides/elasticsearchr?branch=master&svg=true)](https://ci.appveyor.com/project/AlexIoannides/elasticsearchr) [![cran version](http://www.r-pkg.org/badges/version/elasticsearchr)](https://cran.r-project.org/package=elasticsearchr) [![rstudio mirror downloads](http://cranlogs.r-pkg.org/badges/grand-total/elasticsearchr)](https://github.com/metacran/cranlogs.app)

- built and tested using Elasticsearch v2.x, v5.x, v6.x.
- query and aggregation syntax used in the examples below assume that Elasticsearch v2.x is being used.

![][esr_img]

# elasticsearchr: a Lightweight Elasticsearch Client for R
[Elasticsearch][es] is a distributed [NoSQL][nosql] document store search-engine and [column-oriented database][es_column], whose **fast** (near real-time) reads and powerful aggregation engine make it an excellent choice as an 'analytics database' for R&D, production-use or both. Installation is simple, it ships with sensible default settings that allow it to work effectively out-of-the-box, and all interaction is made via a set of intuitive and extremely [well documented][es_docs] [RESTful][restful] APIs. I've been using it for two years now and I am evangelical.

The `elasticsearchr` package implements a simple Domain-Specific Language (DSL) for indexing, deleting, querying, sorting and aggregating data in Elasticsearch, from within R. The main purpose of this package is to remove the labour involved with assembling HTTP requests to Elasticsearch's REST APIs and processing the responses. Instead, users of this package need only send and receive data frames to Elasticsearch resources. Users needing richer functionality are encouraged to investigate the excellent `elastic` package from the good people at [rOpenSci][ropensci].

This package is available on [CRAN][cran] or from [this GitHub repository][githubrepo]. To install the latest development version from GitHub, make sure that you have the `devtools` package installed (this comes bundled with RStudio), and then execute the following on the R command line:

```r
devtools::install_github("alexioannides/elasticsearchr")
```

## Installing Elasticsearch
Elasticsearch can be downloaded [here][es_download], where the instructions for installing and starting it can also be found. OS X users (such as myself) can also make use of [Homebrew][homebrew] to install it with the command,

```bash
$ brew install elasticsearch
```

And then start it by executing `$ elasticsearch` from within any Terminal window. Successful installation and start-up can be checked by navigating any web browser to `http://localhost:9200`, where the following message should greet you (give or take the cluster name that changes with every restart),

```js
{
  "name" : "Kraven the Hunter",
  "cluster_name" : "elasticsearch",
  "version" : {
    "number" : "2.3.5",
    "build_hash" : "90f439ff60a3c0f497f91663701e64ccd01edbb4",
    "build_timestamp" : "2016-07-27T10:36:52Z",
    "build_snapshot" : false,
    "lucene_version" : "5.5.0"
  },
  "tagline" : "You Know, for Search"
}
```

## Elasticsearch 101
If you followed the installation steps above, you have just installed a single Elasticsearch 'node'. When **not** testing on your laptop, Elasticsearch usually comes in clusters of nodes (usually there are at least 3). The easiest easy way to get access to a managed Elasticsearch cluster is by using the [Elastic Cloud][es_cloud] managed service provided by [Elastic][elastic] (note that Amazon Web Services offer something similar too). For the rest of this brief tutorial I will assuming you're running a single node on your laptop (a great way of working with data that is too big for memory).

In Elasticsearch a 'row' of data is stored as a 'document'. A document is a [JSON][json] object - for example, the first row of R's `iris` dataset,

```r
#   sepal_length sepal_width petal_length petal_width species
# 1          5.1         3.5          1.4         0.2  setosa
```

would be represented as follows using JSON,

```js
{
  "sepal_length": 5.1,
  "sepal_width": 3.5,
  "petal_length": 1.4,
  "petal_width": 0.2,
  "species": "setosa"
}
```

Documents are classified into 'types' and stored in an 'index'. In a crude analogy with traditional SQL databases that is often used, we would associate an index with a database instance and the document types as tables within that database. In practice this example is not accurate - it is better to think of all documents as residing in a single - possibly sparse - table (defined by the index), where the document types represent non-unique sub-sets of columns in the table. This is especially so as fields that occur in multiple document types (within the same index), must have the same data-type - for example, if `"name"` exists in document type `customer` as well as in document type `address`, then `"name"` will need to be a `string` in both.

Each document is considered a 'resource' that has a Uniform Resource Locator (URL) associated with it. Elasticsearch URLs all have the following format: `http://your_cluster:9200/your_index/your_doc_type/your_doc_id`. For example, the above `iris` document could be living at `http://localhost:9200/iris/data/1` - you could even point a web browser to this location and investigate the document's contents.

Although Elasticsearch - like most NoSQL databases - is often referred to as being 'schema free', as we have already see this is not entirely correct. What is true, however, is that the schema - or 'mapping' as it's called in Elasticsearch - does not _need_ to be declared up-front (although you certainly can do this). Elasticsearch is more than capable of guessing the types of fields based on new data indexed for the first time.

For more information on any of these basic concepts take a look [here][basic_concepts]

## `elasticsearchr`: a Quick Start
`elasticsearchr` is a **lightweight** client - by this I mean that it only aims to do 'just enough' work to make using Elasticsearch with R easy and intuitive. You will still need to read the [Elasticsearch documentation][es_docs] to understand how to compose queries and aggregations. What follows is a quick summary of what is possible.

### Elasticsearch Data Resources
Elasticsearch resources, as defined by the URLs described above, are defined as `elastic` objects in `elasticsearchr`. For example,

```r
es <- elastic("http://localhost:9200", "iris", "data")
```

Refers to documents of type 'data' in the 'iris' index located on an Elasticsearch node on my laptop. Note that:
- it is possible to leave the document type empty if you need to refer to all documents in an index; and,
- `elastic` objects can be defined even if the underling resources have yet to be brought into existence.

### Indexing New Data
To index (insert) data from a data frame, use the `%index%` operator as follows:

```r
elastic("http://localhost:9200", "iris", "data") %index% iris
```

In this example, the `iris` dataset is indexed into the 'iris' index and given a document type called 'data'. Note that I have not provided any document ids here. **To explicitly specify document ids there must be a column in the data frame that is labelled `id`**, from which the document ids will be taken.

### Deleting Data
Documents can be deleted in three different ways using the `%delete%` operator. Firstly, an entire index (including the mapping information) can be erased by referencing just the index in the resource - e.g.,

```r
elastic("http://localhost:9200", "iris") %delete% TRUE
```

Alternatively, documents can be deleted on a type-by-type basis leaving the index and it's mappings untouched, by referencing both the index and the document type as the resource - e.g.,

```r
elastic("http://localhost:9200", "iris", "data") %delete% TRUE
```

Finally, specific documents can be deleted by referencing their ids directly - e.g.,

```r
elastic("http://localhost:9200", "iris", "data") %delete% c("1", "2", "3", "4", "5")
```

### Queries
Any type of query that Elasticsearch makes available can be defined in a `query` object using the native Elasticsearch JSON syntax - e.g. to match every document we could use the `match_all` query,

```r
for_everything <- query('{
  "match_all": {}
}')
```

To execute this query we use the `%search%` operator on the appropriate resource - e.g.,

```r
elastic("http://localhost:9200", "iris", "data") %search% for_everything

#     sepal_length sepal_width petal_length petal_width    species
# 1            4.9         3.0          1.4         0.2     setosa
# 2            4.9         3.1          1.5         0.1     setosa
# 3            5.8         4.0          1.2         0.2     setosa
# 4            5.4         3.9          1.3         0.4     setosa
# 5            5.1         3.5          1.4         0.3     setosa
# 6            5.4         3.4          1.7         0.2     setosa
# ...
```

### Sorting Query Results
Query results can be sorted on multiple fields by defining a `sort` object using the same Elasticsearch JSON syntax - e.g. to sort by `sepal_width` in ascending order the required `sort` object would be defined as,

```r
by_sepal_width <- sort_on('{"sepal_width": {"order": "asc"}}')
```

This is then added to a `query` object whose results we want sorted and executed using the `%search%` operator as before - e.g.,

```r
elastic("http://localhost:9200", "iris", "data") %search% (for_everything + by_sepal_width)

#   sepal_length sepal_width petal_length petal_width    species
# 1          5.0         2.0          3.5         1.0 versicolor
# 2          6.0         2.2          5.0         1.5  virginica
# 3          6.0         2.2          4.0         1.0 versicolor
# 4          6.2         2.2          4.5         1.5 versicolor
# 5          4.5         2.3          1.3         0.3     setosa
# 6          6.3         2.3          4.4         1.3 versicolor
# ...
```

### Aggregations
Similarly, any type of aggregation that Elasticsearch makes available can be defined in an `aggs` object - e.g. to compute the average `sepal_width` per-species of flower we would specify the following aggregation,

```r
avg_sepal_width <- aggs('{
  "avg_sepal_width_per_species": {
    "terms": {
      "field": "species",
      "size": 3
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
```

_(Elasticsearch 5.x and 6.x users please note that when using the out-of-the-box mappings the above aggregation requires that `"field": "species"` be changed to `"field": "species.keyword"` - see [here][es_five_mappings] for more information as to why)_

This aggregation is also executed via the `%search%` operator on the appropriate resource - e.g.,

```r
elastic("http://localhost:9200", "iris", "data") %search% avg_sepal_width

#          key doc_count avg_sepal_width.value
# 1     setosa        50                 3.428
# 2 versicolor        50                 2.770
# 3  virginica        50                 2.974
```

Queries and aggregations can be combined such that the aggregations are computed on the results of the query. For example, to execute the combination of the above query and aggregation, we would execute,

```r
elastic("http://localhost:9200", "iris", "data") %search% (for_everything + avg_sepal_width)

#          key doc_count avg_sepal_width.value
# 1     setosa        50                 3.428
# 2 versicolor        50                 2.770
# 3  virginica        50                 2.974
```

where the combination yields,

```r
print(for_everything + avg_sepal_width)

# {
#     "size": 0,
#     "query": {
#         "match_all": {
#
#         }
#     },
#     "aggs": {
#         "avg_sepal_width_per_species": {
#             "terms": {
#                 "field": "species",
#                 "size": 0
#             },
#             "aggs": {
#                 "avg_sepal_width": {
#                     "avg": {
#                         "field": "sepal_width"
#                     }
#                 }
#             }
#         }
#     }
# }
```

For comprehensive coverage of all query and aggregations types please refer to the rather excellent [official documentation][es_docs] (newcomers to Elasticsearch are advised to start with the 'Query String' query).

### Mappings
Finally, I have included the ability to create an empty index with a custom mapping, using the `%create%` operator - e.g.,

```r
elastic("http://localhost:9200", "iris") %create% mapping_default_simple()
```

Where in this instance `mapping_default_simple()` is a default mapping that I have shipped with `elasticsearchr`. It switches-off the text analyser for all fields of type 'string' (i.e. switches off free text search), allows all text search to work with case-insensitive lower-case terms, and maps any field with the name 'timestamp' to type 'date', so long as it has the appropriate string or long format.

## Forthcoming Attractions
I do not have a grand vision for `elasticsearchr` - I want to keep it a lightweight client that requires knowledge of Elasticsearch - but I would like to add the ability to compose major query and aggregation types, without having to type-out lots of JSON, and to be able to retrieve simple information like the names of all indices in a cluster, and all the document types within an index, etc. Future development will likely be focused in these areas, but I am open to your suggestions (open an issue [here][githubissues]).

## Acknowledgements
A big thank you to Hadley Wickham and Jeroen Ooms, the authors of the `httr` and `jsonlite` packages that `elasticsearchr` leans upon _heavily_.


[esr_img]: https://alexioannides.files.wordpress.com/2016/12/elasticsearchr2.png "Elasticsearchr"

[elastic]: https://www.elastic.co "Elastic corp."

[es]: https://www.elastic.co/products/elasticsearch "Elasticsearch"

[es_column]: https://www.elastic.co/blog/elasticsearch-as-a-column-store "Elasticsearch as a Column Store"

[cran]: https://cran.r-project.org/package=elasticsearchr "elasticsearchr on CRAN"

[githubrepo]: https://github.com/AlexIoannides/elasticsearchr "Alex's GitHub repository"

[githubissues]: https://github.com/AlexIoannides/elasticsearchr/issues "elasticsearchr issues"

[es_download]: https://www.elastic.co/downloads/elasticsearch "Download"

[nosql]: https://en.wikipedia.org/wiki/NoSQL "What is NoSQL?"

[es_docs]: https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html "Elasticsearch documentation"

[restful]: https://en.wikipedia.org/wiki/Representational_state_transfer "RESTful?"

[ropensci]: https://github.com/ropensci/elastic "rOpenSci"

[homebrew]: http://brew.sh/ "Homebrew for OS X"

[es_cloud]: https://www.elastic.co/cloud/as-a-service "Elastic Cloud"

[json]: https://en.wikipedia.org/wiki/JSON "JSON"

[basic_concepts]: https://www.elastic.co/guide/en/elasticsearch/reference/current/_basic_concepts.html "Basic Concepts"

[es_five_mappings]: https://www.elastic.co/guide/en/elasticsearch/reference/5.0/breaking_50_mapping_changes.html "Text fields in Elasticsearch 5.x"
