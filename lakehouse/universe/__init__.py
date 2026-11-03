"""The expanded world around the course's 100,000 offender records (dataset expansion, approved 2026-11-01).

The core 100,000 records are course 1's cleansed output, which course 2 receives as a pinned handoff
(lakehouse/handoff); nothing here runs course 1's code or reads its live tables. This package adds the world around
them, table by table: people and powers, places, fights, damages, charges, court
outcomes, missions and free-text incident reports (reports.py, with its mention key), about 16.7M rows at scale M.
Chunk 2 is the foundation every table is built on:

    rng       one random stream per (seed, table, chunk), so any chunk rebuilds alone and builds run in parallel
    writer    writes a table as numbered Parquet chunks, vectorized, optionally across worker processes
    scale     the S/M/L knob: row counts from the approved data model, always keeping today's people
    manifest  every file's row count and fingerprint, plus the seed, scale and handoff version; verify() checks them
    core      the core read from the pinned handoff, and course 2's own word lists (course_words.json)

Chunk 10 adds bench: a scale built and run end to end (dbt, queries) in a scratch DuckLake of its own, timed and
measured, never touching the live lake.
"""
