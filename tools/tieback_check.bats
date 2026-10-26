#!/usr/bin/env bats
# Tests for tools/tieback_check.sh, each in a throwaway git repo. Banned words are assembled from fragments
# ("t""dcj") so this file never contains them.   bats tools/tieback_check.bats
# GUARD=<path> tests a different copy of the script (used to prove each test fails when the guard is broken).

setup() {
  R=$BATS_TEST_TMPDIR/repo
  mkdir -p "$R/tools" && cd "$R" || return 1
  cp "${GUARD:-$BATS_TEST_DIRNAME/tieback_check.sh}" tools/tieback_check.sh
  git init -q . && git config user.email t@example.invalid && git config user.name test
  echo "offender_number,sid_number" > data.csv
  git add -A && git commit -q -m clean
  BANNED="t""dcj"
}

# run_guard ARGS: run the guard under test from inside the throwaway repo.
run_guard() { run bash tools/tieback_check.sh "$@"; }

# make_pdf FILE TEXT: write a one-page PDF whose text is TEXT (uncompressed, so pypdf can read it back).
make_pdf() {
  python3 - "$1" "$2" <<'PY'
import sys
out, text = sys.argv[1], sys.argv[2]
stream = f"BT /F1 12 Tf 72 720 Td ({text}) Tj ET".encode()
objs = [b"<< /Type /Catalog /Pages 2 0 R >>", b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>",
        b"<< /Length %d >>\nstream\n" % len(stream) + stream + b"\nendstream",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"]
pdf, offsets = b"%PDF-1.4\n", []
for i, o in enumerate(objs, 1):
    offsets.append(len(pdf)); pdf += b"%d 0 obj\n" % i + o + b"\nendobj\n"
xref = len(pdf)
pdf += b"xref\n0 %d\n0000000000 65535 f \n" % (len(objs) + 1) + b"".join(b"%010d 00000 n \n" % x for x in offsets)
pdf += b"trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n" % (len(objs) + 1, xref)
open(out, "wb").write(pdf)
PY
}

@test "a clean repo passes" {
  run_guard
  [ "$status" -eq 0 ]
  [[ $output == *"tieback_check: clean"* ]]
}

@test "a committed file with a banned word fails and is named" {
  echo "the ${BANNED}_number column" > notes.md && git add notes.md && git commit -q -m bad
  run_guard
  [ "$status" -eq 1 ]
  [[ $output == *"notes.md"* ]]
}

@test "--staged checks what will be committed, not the working copy" {
  echo "real unit: H""untsville" > staged.txt && git add staged.txt
  echo "real unit: Coast City" > staged.txt           # fixed on disk but not re-staged: the commit would still leak
  run_guard --staged
  [ "$status" -eq 1 ]
  [[ $output == *"staged.txt"* ]]
}

@test "git-ignored files are not checked" {
  echo "build/" > .gitignore && git add .gitignore && git commit -q -m ignore
  mkdir build && echo "${BANNED}" > build/compiled.sql
  run_guard
  [ "$status" -eq 0 ]
}

@test "a PDF whose text has a banned word fails" {
  python3 -c "import pypdf" 2>/dev/null || skip "pypdf not installed"
  make_pdf handout.pdf "Every ${BANNED^^} number" && git add handout.pdf && git commit -q -m pdf
  run_guard
  [ "$status" -eq 1 ]
  [[ $output == *"handout.pdf (PDF text)"* ]]
}
