#!/usr/bin/env bash
set -euo pipefail

TAG="${1:-}"
DST_PAPER="${2:-papers/netsci2026}"

if [[ -z "${TAG}" ]]; then
  echo "usage: ns26_publish_assets.sh <tag> [dst_paper_dir]" >&2
  exit 2
fi

SRC="out/netsci2026/${TAG}"

mkdir -p "${DST_PAPER}/figure" "${DST_PAPER}/table" "${DST_PAPER}/meta"
mkdir -p "${DST_PAPER}/meta/01" "${DST_PAPER}/meta/02" "${DST_PAPER}/meta/03" "${DST_PAPER}/meta/04" "${DST_PAPER}/meta/05" "${DST_PAPER}/meta/06"

cp -f "${SRC}/04_country_springrank_trends/springrank_trends.svg" \
  "${DST_PAPER}/figure/fig1_country_springrank_trends.svg"

cp -f "${SRC}/05_country_springrank_dispersion/dispersion_weighted_kept_years.svg" \
  "${DST_PAPER}/figure/fig2_country_springrank_dispersion_weighted_std.svg"

cp -f "${SRC}/06_chord_extrema_years/chord_max.svg" \
  "${DST_PAPER}/figure/fig3a_chord_dispersion_max.svg"
cp -f "${SRC}/06_chord_extrema_years/chord_min.svg" \
  "${DST_PAPER}/figure/fig3b_chord_dispersion_min.svg"

cp -f "${SRC}/06_chord_extrema_years/chord_max.png" \
  "${DST_PAPER}/figure/fig3a_chord_dispersion_max.png"
cp -f "${SRC}/06_chord_extrema_years/chord_min.png" \
  "${DST_PAPER}/figure/fig3b_chord_dispersion_min.png"

cp -f "${SRC}/04_country_springrank_trends/keep_countries_04.csv" \
  "${DST_PAPER}/table/table_s1_keep_countries_for_trends.csv"

cp -f "${SRC}/03_filter_above_mean/keep_countries_01.csv" \
  "${DST_PAPER}/table/table_s2_keep_countries_static.csv"
cp -f "${SRC}/03_filter_above_mean/keep_countries_02.csv" \
  "${DST_PAPER}/table/table_s3_keep_countries_window.csv"
cp -f "${SRC}/03_filter_above_mean/keep_years_02.csv" \
  "${DST_PAPER}/table/table_s4_keep_years_window.csv"

cp -f "${SRC}/05_country_springrank_dispersion/dispersion_weighted_all_years.csv" \
  "${DST_PAPER}/table/table_s5_dispersion_weighted_all_years.csv"
cp -f "${SRC}/05_country_springrank_dispersion/dispersion_extrema_kept_years.csv" \
  "${DST_PAPER}/table/table_s6_dispersion_extrema.csv"

cp -f "${SRC}/01_country_springrank/springrank.csv" \
  "${DST_PAPER}/table/table_s7_country_springrank_static.csv"
cp -f "${SRC}/02_country_springrank_window/springrank_windowed.csv" \
  "${DST_PAPER}/table/table_s8_country_springrank_windowed_all.csv"

cp -f "${SRC}/_meta.json" "${DST_PAPER}/meta/run_meta.json"
cp -f "${SRC}/01_country_springrank/_meta.json" "${DST_PAPER}/meta/01/_meta.json"
cp -f "${SRC}/02_country_springrank_window/_meta.json" "${DST_PAPER}/meta/02/_meta.json"
cp -f "${SRC}/03_filter_above_mean/_meta.json" "${DST_PAPER}/meta/03/_meta.json"
cp -f "${SRC}/04_country_springrank_trends/_meta.json" "${DST_PAPER}/meta/04/_meta.json"
cp -f "${SRC}/05_country_springrank_dispersion/_meta.json" "${DST_PAPER}/meta/05/_meta.json"
cp -f "${SRC}/06_chord_extrema_years/_meta.json" "${DST_PAPER}/meta/06/_meta.json"

echo "OK: figures/tables/meta copied from ${SRC} into ${DST_PAPER}/"
