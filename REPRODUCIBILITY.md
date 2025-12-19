# Reproducibility

This repository (`osr-examples`) contains the analysis pipeline used to generate figures and tables for the papers from prebuilt ORCID-derived mobility products.

## Scope

- Inputs: ORCID-derived products (prebuilt Parquet edge tables).
- Computations: country-level SpringRank and windowed SpringRank; system-level dispersion metrics; chord diagrams for extrema years.
- Outputs: CSV tables and vector figures under `out/<paper>/<tag>/`, and a paper bundle under `papers/<paper>/`.

## Repository boundary

- `orcid-springrank` builds the ORCID-derived products (Parquet edges + provenance) from raw sources and provides the SpringRank computation entrypoints.
- `osr-examples` consumes those products only; it does not ingest ORCID XML directly.

## Requirements

- [Julia](https://julialang.org) and [just](https://just.systems/man/en).
- Or use `devcontainer` (see [full docs](https://code.visualstudio.com/docs/devcontainers/containers)).

## Setup

```bash
just init
```

## Data

### ORCID-derived products

This pipeline expects a product directory structure:

```bash
products/orcid/<date>/<tag>/
  edges/
    aff.parquet
    org_country.parquet
    ...
  _meta.json
```

The products can be downloaded from Zenodo:

- ORCID-derived Academic Mobility Networks (DOI: 10.5281/zenodo.17983291)

## Running the pipeline

Take NetSci 2026 for example.

Run the full NetSci 2026 pipeline:

```bash
just pl-ns26
```

Or run steps individually:

```bash
just pl-ns26-00
just pl-ns26-01
...
```

Each step writes outputs into:

```bash
out/<paper>/<tag>/<step_dir>/
```

and includes `_meta.json` with parameters and upstream pointers.

## Provenance

- Each step emits `_meta.json` describing parameters and upstream pointers.
- The paper bundle copies step metadata into `papers/<paper>/meta/`.

## Verification

- The pipeline is deterministic given identical inputs and dependency versions (Julia Manifest.toml).
- To verify, compare generated `papers/<paper>/out/<tag>/` with the archived artifacts referenced in the Zenodo record.

## License

- Code: MIT.
- Data: Creative Commons Attribution 4.0 International.
