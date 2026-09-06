#!/usr/bin/env bash
# Rebuild the committed UI bundle. Requires node v22+ with npx; esbuild and
# typescript are fetched into the npx cache outside this repo — no
# node_modules, no package.json, ever. The bundle is a committed artifact
# (same posture as generated proto stubs): `go build` never runs this.
# Output: dist/app.js + dist/app.css (esbuild splits the imported CSS) and
# dist/index.html (copied so the embedded FS root serves it at /).
set -euo pipefail
cd "$(dirname "$0")"
npx -y -p typescript@5 tsc -p tsconfig.json
npx -y -p esbuild@0.25 esbuild src/app.ts --bundle --minify --target=es2022 --outfile=dist/app.js
cp index.html dist/index.html
echo "wrote dist/app.js dist/app.css dist/index.html"
