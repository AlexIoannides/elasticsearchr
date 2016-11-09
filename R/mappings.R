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


#' Title
#'
#' @return
#' @export
#'
#' @examples
mapping_default_alex <- function() {
  jsonlite::prettify(
  '{
    "settings": {
      "index": {
        "analysis": {
          "analyzer": {
            "analyzer_lowercase": {
              "tokenizer": "keyword",
              "filter": "lowercase"
            }
          }
        }
      }
    },
    "mappings": {
      "_default_": {
        "dynamic_templates": [
          {
            "strings": {
              "match_mapping_type": "string",
              "mapping": {
                "type": "string",
                "analyzer": "analyzer_lowercase"
              }
            }
          },
          {
            "time": {
              "match": "timestamp",
              "mapping": {
                "type": "date",
                "format": "yyyy-MM-dd HH:mm:ss.SSS||yyyy-MM-dd||epoch_millis"
              }
            }
          }]
      }
    }
  }')
}
