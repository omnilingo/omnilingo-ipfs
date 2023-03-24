# omnilingo-ipfs

[![Matrix #omnilingo:matrix.org](https://img.shields.io/matrix/omnilingo:matrix.org?color=blue&label=matrix%20chat&server_fqdn=matrix.org&style=flat-square)](https://matrix.to/#/#omnilingo:matrix.org?via=matrix.org)
[![GitHub licence](https://img.shields.io/badge/licence-AGPL--3.0-orange)](https://github.com/omnilingo/omnilingo-ipfs/blob/master/COPYING)


## Steps 

There are three main steps in adding your data to OmniLingo. The first
step is importing the data into IPFS, the second is indexing the data and
the final step is publishing the data.

### Import

Import data into your local IPFS node and generate an index:

```bash
$ importer.py dataset_dir index_path
```

e.g. 

```bash
$ importer.py ./cv-corpus-7.0-2021-07-21/tr/ tr.json
```

where the `dataset_dir` is in [Common Voice format](doc/FORMAT.md).

### Index

Index the data, extracting a balanced subset of clips by a complexity metric:

```bash
$ indexer.py locale index_path
```

e.g. 

```bash
$ indexer.py tr tr.json
```

This will return a CID that looks like `QmXpgcavH2shpBbfnFoymPxEw2zpr4MdAgi1aaoZT4Yeho`

### Publish

Publish data to the global index in OmniLingo on IPFS:

```bash
$ publisher.py locale cid
```

e.g. 

```bash
$ publisher.py tr QmXpgcavH2shpBbfnFoymPxEw2zpr4MdAgi1aaoZT4Yeho
```

Publish to a name using the local node ID:

```bash
ipfs name publish cid 
```

e.g. 

```bash
ipfs name publish QmXpgcavH2shpBbfnFoymPxEw2zpr4MdAgi1aaoZT4Yeho
```

# Publishing models

To publish model files (e.g. for the pronunciation assistance) you need a directory, containing two files:

* `models/LOCALE.tflite`: The binary for the ASR model
* `models/LOCALE.json`: Metadata for the model

The metadata file, e.g. `pt.json` for Portuguese, should look like:

```json
{"format": "coqui", "type": "asr", "licence":"AGPL-3.0", "src":"https://itml.cl.indiana.edu/models/"}
```

