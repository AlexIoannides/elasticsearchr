# ---- test classes and methods ----
e <- elastic("http://localhost:9200", "iris", "data")
e %index% iris

q <- query('{"match_all": {}}')
a <- aggs('{"avg_sepal_width_per_cat": { "terms": {"field": "species", "size": 0, "order": [{"avg_sepal_width": "desc"}]}, "aggs": {"avg_sepal_width": {"avg": {"field": "sepal_width"}}} }}')

print(q)
e %search% q

print(a)
e %search% a

print(q + a)
e %search% (q + a)


# ---- testing stuff as inspired from Advanced R and ggplot2 ----
# obj <- function(x) structure(list(x), class = "obj")
# is.obj <- function(x) inherits(x, "obj")
#
# index <- function(x) UseMethod("index")
# index.obj <- function(x) print(x[[1]])
#
# # note that you don't need to define the generic `+` as it's already defined in base R!
# `+.obj` <- function(x, y) {
#   obj(x[[1]] * y[[1]])
# }
#
# # examples
# a <- obj(1)
# b <- obj(2)
#
# index(a + b)
# # [1] 2
