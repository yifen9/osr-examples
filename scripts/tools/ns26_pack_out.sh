#!/usr/bin/env bash
set -euo pipefail

TAG="${1:-}"
if [[ -z "${TAG}" ]]; then
  echo "usage: ns26_pack_out.sh <tag>" >&2
  exit 2
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SRC="${REPO_ROOT}/out/netsci2026/${TAG}"
DST_PARENT="${REPO_ROOT}/papers/netsci2026/out"
DST="${DST_PARENT}/${TAG}"
META_DIR="${REPO_ROOT}/papers/netsci2026/meta"

if [[ ! -d "${SRC}" ]]; then
  echo "missing source dir: ${SRC}" >&2
  exit 3
fi

mkdir -p "${DST_PARENT}"
mkdir -p "${META_DIR}"

rm -rf "${DST}"
cp -a "${SRC}" "${DST_PARENT}/"

TS_UTC="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
GIT_COMMIT="$(git -C "${REPO_ROOT}" rev-parse HEAD 2>/dev/null || echo "unknown")"
GIT_DIRTY="$(git -C "${REPO_ROOT}" status --porcelain 2>/dev/null | wc -l | tr -d ' ')"

FILE_COUNT="$(find "${DST}" -type f | wc -l | tr -d ' ')"
TOTAL_BYTES="$(du -sb "${DST}" 2>/dev/null | awk '{print $1}' || echo "0")"

PROV_TXT="${META_DIR}/provenance_out_${TAG}.txt"
{
  echo "timestamp_utc: ${TS_UTC}"
  echo "tag: ${TAG}"
  echo "src: ${SRC}"
  echo "dst: ${DST}"
  echo "git_commit: ${GIT_COMMIT}"
  echo "git_dirty_files: ${GIT_DIRTY}"
  echo "file_count: ${FILE_COUNT}"
  echo "total_bytes: ${TOTAL_BYTES}"
  echo ""
  echo "meta_files:"
  find "${DST}" -name "_meta.json" -type f | sort | sed "s#^${REPO_ROOT}/##" | sed 's/^/  - /'
} > "${PROV_TXT}"

echo "${DST}"
echo "${PROV_TXT}"
