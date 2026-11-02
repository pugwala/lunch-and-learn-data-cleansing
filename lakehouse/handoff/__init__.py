"""The handoff between the two courses: course 1's cleansed output, published as pinned files for course 2.

Course 2 starts where course 1 ends (the boundary document, 2026-11-02), so it never runs course 1's code or reads
its live tables. The two meet here, through files:

    publish.py   course 1's side: run its own code, check the 19 Facilitator Guide numbers (parity.py), and only
                 then publish one Parquet file per table plus a manifest of fingerprints
    parity.py    course 1's side: the 19 numbers, shared with the lake's parity gate (tests/test_parity.py)
    receive.py   course 2's side: what it accepts (contract.yml: tables, keys, column types), adopting a version
                 that keeps it (course1.lock.json), the check that the files on disk are exactly that version, and a
                 DuckDB connection that reads them; lake.run() lands them in the lake

publish.py and parity.py move to course 1 when course 2 gets its own repo (Priority 8); receive.py, the contract and
the lock stay.
"""
