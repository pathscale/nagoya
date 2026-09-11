#!/bin/sh
# Typst is the maintained source. Rustdoc tests extract its Rust examples.
set -eu
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
typst compile docs/user-guide.typ docs/user-guide.pdf
typst compile docs/why-nagoya.typ docs/why-nagoya.pdf
