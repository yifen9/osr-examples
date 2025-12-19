[![Compat Helper](https://github.com/yifen9/osr-examples/actions/workflows/compat-helper.yaml/badge.svg)](https://github.com/yifen9/osr-examples/actions/workflows/compat-helper.yaml) [![Image](https://github.com/yifen9/osr-examples/actions/workflows/image.yaml/badge.svg)](https://github.com/yifen9/osr-examples/actions/workflows/image.yaml)

# OSR Examples

Analysis pipelines to generate figures/tables for the papers from ORCID-derived academic mobility products.

## What this repository does

- Consumes prebuilt Parquet products (country-to-country mobility edges, affiliation edges, etc.).
- Computes:
  - country SpringRank (static)
  - windowed SpringRank by year (sliding window)
  - ...
- Produces:
  - `out/<paper>/<tag>/...` (step outputs)
  - `papers/<paper>/...` (paper bundle)

## Data

Download the ORCID-derived products from Zenodo:

- ORCID-derived Academic Mobility Networks (DOI: 10.5281/zenodo.17983291)

Expected layout:

```bash
products/orcid/<date>/<tag>/
  edges/
  _meta.json
```

## Quickstart

Take NetSci 2026 for example.

```bash
just init
just pl-ns26
```

Outputs will appear under:

`out/netsci2026/<tag>/`

## Provenance

Each pipeline step emits `_meta.json` describing parameters and upstream pointers.
The paper bundle collects them under `papers/<paper>/meta/`.

## Citation

See `CITATION.cff`.
