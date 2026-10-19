#!/usr/bin/env bash
# Build the Lambda layers into layers/build/<name>/python (no Docker needed).
#
# Wheels are resolved for the Lambda runtime (Linux x86_64, CPython 3.12), not for this
# machine, so the result can be built on macOS/Linux/WSL alike. Run before `cdk deploy`.
set -euo pipefail

cd "$(dirname "$0")/.."
LIMIT_MB=250   # Lambda's limit for function + layers, unzipped

for requirements in layers/*.txt; do
  name="$(basename "$requirements" .txt)"
  target="layers/build/${name}/python"
  rm -rf "layers/build/${name}"
  mkdir -p "${target}"
  echo "==> building layer '${name}' from ${requirements}"
  uv pip install --quiet \
    --python-platform x86_64-manylinux2014 \
    --python-version 3.12 \
    --only-binary :all: \
    --target "${target}" \
    -r "${requirements}"
  # trim what Lambda never needs
  find "${target}" -type d \( -name __pycache__ -o -name tests -o -name test \) -prune -exec rm -rf {} +
  find "${target}" -name '*.pyc' -delete
  size_mb=$(du -sm "layers/build/${name}" | cut -f1)
  echo "    ${name}: ${size_mb} MB unzipped (limit ${LIMIT_MB} MB shared with the function)"
  if [ "${size_mb}" -ge "${LIMIT_MB}" ]; then
    echo "layer '${name}' is too large for Lambda" >&2
    exit 1
  fi
done
