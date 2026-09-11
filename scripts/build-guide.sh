#!/bin/sh
# Markdown stays the tested source; regenerate the Typst and PDF together.
set -eu
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
mkdir -p target/guide
pandoc docs/user-guide.md --to typst --lua-filter docs/guide-filter.lua \
    --output target/guide/body.typ
cat docs/guide-style.typ target/guide/body.typ > docs/user-guide.typ
typst compile docs/user-guide.typ docs/user-guide.pdf
