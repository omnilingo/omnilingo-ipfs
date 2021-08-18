# omnilingo-ipfs

[![Matrix #omnilingo:matrix.org](https://img.shields.io/matrix/omnilingo:matrix.org?color=blue&label=matrix%20chat&server_fqdn=matrix.org&style=flat-square)](https://matrix.to/#/#omnilingo:matrix.org?via=matrix.org)
[![GitHub licence](https://img.shields.io/badge/licence-AGPL--3.0-orange)](https://github.com/omnilingo/omnilingo-ipfs/blob/master/COPYING)


## Steps 

Import data into your local IPFS node and generate an index:

```bash
$ importer.py dataset_dir index_path
```

Index the data, extracting a balanced subset of clips by a complexity metric:

```bash
$ index.py locale index_path
```

Publish data to the global index in OmniLingo on IPFS:

```bash
$ publisher.py locale cid
```
