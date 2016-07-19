# nosqlimport-elasticsearch

A module for [nosqlimport](https://www.npmjs.com/package/nosqlimport) that allows data to be published to Elasticsearch.

## Installation

```sh
npm install -g nosqlimport nosqlimport-elasticsearch
```

## Import data to Elasticsearch

```sh
cat test.tsv | nosqlimport -n elasticsearch -u http://localhost:9200/mydatabase --db mycollection
```

See [nosqlimport](https://www.npmjs.com/package/nosqlimport) for further options.
