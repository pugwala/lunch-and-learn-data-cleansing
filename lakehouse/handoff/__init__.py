"""The handoff between the two courses: course 1's cleansed output, published as pinned files for course 2.

Course 2 starts where course 1 ends (the boundary document, 2026-11-02), so it never reads course 1's live tables or
imports its code. The two meet here, through files:

    publish.py   course 1's side: run its own code, check the 19 Facilitator Guide numbers (parity.py), and only
                 then publish one Parquet file per table plus a manifest of fingerprints
    parity.py    course 1's side: the 19 numbers, shared with the lake's parity gate (tests/test_parity.py)
    receive.py   course 2's side: the version it builds on (course1.lock.json), the check that the files on disk
                 are exactly that version, and a DuckDB connection that reads them

publish.py and parity.py move to course 1 when course 2 gets its own repo (Priority 8); receive.py and the lock stay.
"""
