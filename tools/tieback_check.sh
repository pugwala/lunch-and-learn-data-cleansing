#!/bin/bash
# tieback_check.sh [--staged]: fails if anything in the repo links this mock course back to a real employer.
#
# The data is fictional on purpose. This guard keeps out the real agency's name and abbreviation, real unit names,
# a real product and a real office name, plus wording that implied a real organization. Each word is written with
# one letter in brackets (t[d]cj): the pattern still matches the word, but this file never contains it, so a plain
# text search of the repo stays clean.
#
#   tools/tieback_check.sh            check the files git tracks, plus the text inside any PDF
#   tools/tieback_check.sh --staged   check what is staged for the next commit (the pre-commit hook uses this)
#
# Exit 0 = clean, 1 = a match (each one is printed), 2 = could not run.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 2

PATTERN='t[d]cj|h[u]ntsville|e[l]lis unit|e[s]telle|f[e]rguson unit|g[o]ree|w[y]nne|v[i]vantio|d[m]o[_-]|\bd[m]o\b|our r[e]al source systems|agency d[a]tabricks workspace'
SCOPE=(); [ "${1:-}" = --staged ] && SCOPE=(--cached)
found=0

# Text files: git grep reads tracked (or staged) content, so ignored build output never counts.
if git grep -n -i -I -E "${SCOPE[@]}" "$PATTERN" -- ':!*.pdf'; then found=1; fi

# PDFs: their text is compressed, so extract it first (needs pypdf; skipped with a note if it isn't installed).
pdfs=$(git ls-files "${SCOPE[@]}" -- '*.pdf')
if [ -n "$pdfs" ]; then
  if python3 -c "import pypdf" 2>/dev/null; then
    while IFS= read -r f; do
      text=$(if [ "${1:-}" = --staged ]; then git show ":$f"; else cat "$f"; fi |
             python3 -c "import io, sys, pypdf; print('\n'.join(p.extract_text() or '' for p in pypdf.PdfReader(io.BytesIO(sys.stdin.buffer.read())).pages))")
      if grep -n -i -E "$PATTERN" <<< "$text" | sed "s|^|$f (PDF text): |"; then found=1; fi
    done <<< "$pdfs"
  else
    echo "note: pypdf not installed, PDF text not checked (pip install pypdf)" >&2
  fi
fi

if [ $found = 1 ]; then
  echo "tieback_check: the lines above link this repo to a real organization. Replace them with fictional names." >&2
  exit 1
fi
echo "tieback_check: clean"
