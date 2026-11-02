"""The core: course 1's cleansed 100,000 records, which the expanded world is built around and must never change.

Course 2 starts where course 1 ends, so nothing here runs course 1's generator or reads its live tables. Course 1
publishes its cleansed output as pinned files (lakehouse/handoff: the version in course1.lock.json); extract_core()
checks every file against the lock, then reads today's people, powers, STG memberships, records and units from them.

The word lists the world draws names, cities, powers and groups from (course_words.json) are course 2's own copy,
taken from course 1's generator on 2026-11-02 (with its "today", 2026-09-24). Course 2 owns them now: they may grow
apart from the course's on purpose, and a build never looks at the course's lists.
"""
import datetime
import json
from pathlib import Path

from ..handoff import receive

CORE_ROWS, CORE_SEED = 100_000, 2026
WORDS = Path(__file__).with_name("course_words.json")
sha256 = receive.sha256

_WORDS = {}


def course_constants() -> dict:
    """Course 2's word lists (course_words.json): names, cities, powers, STG groups, alias parts, and the course's
    "today" as a date. Read once per process."""
    if not _WORDS:
        data = json.loads(WORDS.read_text())
        data["TODAY"] = datetime.date.fromisoformat(data["TODAY"])
        _WORDS.update(data)
    return _WORDS


def handoff_info() -> dict:
    """What a build records about its core: the pinned handoff version and the raw file course 1 made it from."""
    lock = receive.read_lock()
    return {"handoff_version": lock["version"], "source_sha256": lock["source"]["sha256"]}


EXTRACT = {
    # One row per SID: the person's latest record, plus their first and latest sentence dates over all their records
    # (the same min and max course 2's gold.dim_person takes).
    "people": """
        WITH latest AS (SELECT * FROM silver.hero_offenders WHERE sid_number IS NOT NULL
                        QUALIFY row_number() OVER (PARTITION BY sid_number
                                                   ORDER BY sentence_date DESC, offender_number DESC) = 1),
             sentences AS (SELECT sid_number, min(sentence_date) AS first_sentence_date,
                                  max(sentence_date) AS latest_sentence_date
                           FROM silver.hero_offenders WHERE sid_number IS NOT NULL GROUP BY sid_number)
        SELECT l.sid_number, l.last_name_std AS last_name, l.first_name_std AS first_name, l.alias_std AS alias,
               l.date_of_birth AS birth_date, l.gender, l.unit_name, d.first_sentence_date, d.latest_sentence_date
        FROM latest l JOIN sentences d USING (sid_number) ORDER BY l.sid_number""",
    # Every power each person holds today.
    "powers": """
        SELECT DISTINCT sid_number, trim(upper(unnest(string_split(superpower, ';')))) AS power
        FROM silver.hero_offenders WHERE sid_number IS NOT NULL ORDER BY ALL""",
    # Every STG affiliation, with the status on the person's latest record in that group.
    "memberships": """
        SELECT sid_number, stg_group, stg_status AS status FROM silver.hero_offenders
        WHERE sid_number IS NOT NULL AND stg_group IS NOT NULL AND stg_group <> 'None'
        QUALIFY row_number() OVER (PARTITION BY sid_number, stg_group ORDER BY sentence_date DESC, offender_number DESC) = 1
        ORDER BY sid_number, stg_group""",
    # Today's offender records that a fight can lead to: those with a SID and a sentence date. Incident i is the
    # fight behind the i-th of them.
    "records": """
        SELECT offender_number, sid_number, sentence_date, offense_code_std AS offense_code FROM silver.hero_offenders
        WHERE sid_number IS NOT NULL AND sentence_date IS NOT NULL ORDER BY offender_number""",
    # The custody units.
    "units": """
        SELECT DISTINCT unit_code, unit_name FROM silver.hero_offenders WHERE unit_code IS NOT NULL ORDER BY unit_code""",
}


def extract_core(out_dir: Path, handoff_dir: Path = None) -> dict:
    """Write today's people, powers, STG memberships, linkable offender records and custody units to
    <out_dir>/_core/ as Parquet, read from course 1's handoff (the pinned one unless handoff_dir is given) after
    checking it against the lock; check their counts against the pinned ones, and return {name: path}."""
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    from . import scale
    con = receive.connect(receive.require(handoff_dir), receive.read_lock())
    folder = Path(out_dir) / "_core"
    folder.mkdir(parents=True, exist_ok=True)
    paths = {}
    for name, sql in EXTRACT.items():
        table = con.execute(sql).arrow().read_all()
        if name == "powers":
            table = table.filter(pc.not_equal(table["power"], ""))
        paths[name] = folder / f"{name}.parquet"
        pq.write_table(table, paths[name], compression="zstd")
    expected = {"people": scale.CORE_PEOPLE, "powers": scale.CORE_PERSON_POWERS, "memberships": scale.CORE_TEAM_MEMBERS,
                "records": scale.CORE_RECORDS}
    for name, want in expected.items():
        got = pq.read_metadata(paths[name]).num_rows
        if got != want:
            raise RuntimeError(f"The handoff's core changed: {name} has {got:,} rows; the world is built for {want:,}")
    return paths
