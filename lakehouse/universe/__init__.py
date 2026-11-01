"""The expanded world around the course's 100,000 offender records (dataset expansion, approved 2026-11-01).

The course generator (generator/gen_hero_offenders.py) still makes the core 100,000 records, untouched. This
package adds the world around them, table by table: people and powers, places, fights, damages, charges, court
outcomes and missions, about 16.6M rows at scale M. Chunk 2 is the foundation every table is built on:

    rng       one random stream per (seed, table, chunk), so any chunk rebuilds alone and builds run in parallel
    writer    writes a table as numbered Parquet chunks, vectorized, optionally across worker processes
    scale     the S/M/L knob: row counts from the approved data model, always keeping today's people
    manifest  every file's row count and fingerprint, plus the seed, scale and core fingerprint; verify() checks them
    core      the core's pinned fingerprint, and a check that the course generator still makes it
"""
