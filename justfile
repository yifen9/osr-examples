add PROJECT NAME:
    julia --project={{PROJECT}} -e 'using Pkg; Pkg.add("{{NAME}}")'

rm PROJECT NAME:
    julia --project={{PROJECT}} -e 'using Pkg; Pkg.rm("{{NAME}}")'

up:
    julia --project=. -e 'using Pkg; Pkg.update()'
    julia --project=dev -e 'using Pkg; Pkg.update()'

init:
    julia --project=. -e 'using Pkg; Pkg.instantiate()'
    julia --project=dev -e 'using Pkg; Pkg.instantiate()'
    julia --project=dev -e 'using Pkg; Pkg.develop(path=".")'

resolve:
    julia --project=. -e 'using Pkg; Pkg.resolve()'
    julia --project=dev -e 'using Pkg; Pkg.resolve()'

pc:
    just init
    julia --project=. -e 'using Pkg; Pkg.precompile()'
    julia --project=dev -e 'using Pkg; Pkg.precompile()'

fmt:
    julia --project=dev -e 'using Pkg; Pkg.instantiate(); using JuliaFormatter; format("src"); format("scripts")'

PRODUCT_ORCID_DATE := "2025_10"
PRODUCT_ORCID_TAG := "20251217164227_a2c50a6a55e6b02a"

ns26-pack-out TAG=PRODUCT_ORCID_TAG:
    bash scripts/tools/ns26_pack_out.sh {{TAG}}

ns26-publish TAG=PRODUCT_ORCID_TAG DST="papers/netsci2026":
    bash scripts/tools/ns26_publish_assets.sh {{TAG}} {{DST}}

pl-ns26:
    just pl-ns26-00 && \
    just pl-ns26-01 && \
    just pl-ns26-02 && \
    just pl-ns26-03 && \
    just pl-ns26-04 && \
    just pl-ns26-05 && \
    just pl-ns26-06 && \
    just pl-ns26-07

pl-ns26-00 PRODUCT_DATE=PRODUCT_ORCID_DATE PRODUCT_TAG=PRODUCT_ORCID_TAG OUT_ROOT="out/netsci2026" TAG=PRODUCT_TAG:
    julia --project=. scripts/pipeline/netsci2026/00_meta.jl \
        --input-root products/orcid/{{PRODUCT_DATE}}/{{PRODUCT_TAG}} \
        --output-root {{OUT_ROOT}} \
        --tag {{TAG}}

pl-ns26-01 PRODUCT_DATE=PRODUCT_ORCID_DATE PRODUCT_TAG=PRODUCT_ORCID_TAG OUT_ROOT="out/netsci2026" TAG=PRODUCT_TAG:
    julia --project=. scripts/pipeline/netsci2026/01_country_springrank.jl \
        --input-root products/orcid/{{PRODUCT_DATE}}/{{PRODUCT_TAG}} \
        --output-root {{OUT_ROOT}} \
        --tag {{TAG}} \
        --config papers/netsci2026/config/01_country_springrank.yaml

pl-ns26-02 PRODUCT_DATE=PRODUCT_ORCID_DATE PRODUCT_TAG=PRODUCT_ORCID_TAG OUT_ROOT="out/netsci2026" TAG=PRODUCT_TAG:
    julia --project=. scripts/pipeline/netsci2026/02_country_springrank_window.jl \
        --input-root products/orcid/{{PRODUCT_DATE}}/{{PRODUCT_TAG}} \
        --output-root {{OUT_ROOT}} \
        --tag {{TAG}} \
        --config papers/netsci2026/config/02_country_springrank_window.yaml

pl-ns26-03 OUT_ROOT="out/netsci2026" TAG=PRODUCT_ORCID_TAG:
    julia --project=. scripts/pipeline/netsci2026/03_filter_above_mean.jl \
        --output-root {{OUT_ROOT}} \
        --tag {{TAG}}

pl-ns26-04 PRODUCT_DATE=PRODUCT_ORCID_DATE PRODUCT_TAG=PRODUCT_ORCID_TAG OUT_ROOT="out/netsci2026" TAG=PRODUCT_TAG:
    julia --project=. scripts/pipeline/netsci2026/04_country_springrank_trends.jl \
        --output-root {{OUT_ROOT}} \
        --tag {{TAG}}

pl-ns26-05 OUT_ROOT="out/netsci2026" TAG=PRODUCT_ORCID_TAG:
    julia --project=. scripts/pipeline/netsci2026/05_country_springrank_dispersion.jl \
        --output-root {{OUT_ROOT}} \
        --tag {{TAG}} \
        --config papers/netsci2026/config/05_country_springrank_dispersion.yaml

pl-ns26-06 PRODUCT_DATE=PRODUCT_ORCID_DATE PRODUCT_TAG=PRODUCT_ORCID_TAG OUT_ROOT="out/netsci2026" TAG=PRODUCT_TAG:
    julia --project=. scripts/pipeline/netsci2026/06_chord_extrema_years.jl \
        --output-root {{OUT_ROOT}} \
        --tag {{TAG}} \
        --config papers/netsci2026/config/06_country_chord_extrema.yaml

pl-ns26-07 PRODUCT_DATE=PRODUCT_ORCID_DATE PRODUCT_TAG=PRODUCT_ORCID_TAG OUT_ROOT="out/netsci2026" PAPERS_ROOT="papers/netsci2026" TAG=PRODUCT_TAG:
    julia --project=. scripts/pipeline/netsci2026/07_stage_papers.jl \
        --output-root {{OUT_ROOT}} \
        --papers-root {{PAPERS_ROOT}} \
        --tag {{TAG}}
