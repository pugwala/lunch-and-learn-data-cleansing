"""The core: the course's 100,000 records, which the expanded world is built around and must never change.

CORE_SHA256 is the fingerprint of the file the course generator makes with its defaults (100,000 rows, seed 2026),
the same file the lake loads (/lake/landing/hero_offender_data_v6_s2026_r100000.csv). course_core_sha256() runs
the course generator itself, unchanged, so a test can prove the core still comes out byte-for-byte the same.
"""
import hashlib
import importlib.util
from pathlib import Path

CORE_SHA256 = "649dc8398937e22f2fddf3203bb14023d91a3d0789839b41c072465b1cc1fde8"
CORE_ROWS, CORE_SEED = 100_000, 2026
GENERATOR = Path(__file__).resolve().parents[2] / "generator" / "gen_hero_offenders.py"


def sha256(path: Path) -> str:
    """A file's SHA-256, read in 1 MB blocks."""
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def course_core_sha256(work_dir: Path) -> str:
    """Run the course generator (100,000 rows, seed 2026) into work_dir and return the data file's SHA-256."""
    spec = importlib.util.spec_from_file_location("gen_hero_offenders_for_core_check", GENERATOR)
    gen = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gen)
    work_dir = Path(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)
    out = work_dir / "core.csv"
    gen.generate(rows=CORE_ROWS, out=str(out), seed=CORE_SEED, answer_key=str(work_dir / "core_answer_key.csv"))
    return sha256(out)


_COURSE = {}


def course_constants() -> dict:
    """The course generator's word lists (names, cities, powers, STG groups, alias parts), read from its module
    without running it. Nothing here changes the course."""
    if not _COURSE:
        spec = importlib.util.spec_from_file_location("gen_hero_offenders_constants", GENERATOR)
        gen = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(gen)
        for name in ("CITIES", "POWERS", "TIER_ORDER", "STG_GROUPS", "LAST_NAMES", "MALE_FIRST", "FEMALE_FIRST",
                     "ADJ", "NOUN", "PREFIX", "SUFFIX", "TODAY"):
            _COURSE[name] = getattr(gen, name)
    return _COURSE


EXTRACT = {
    # One row per SID: the lake's latest record for that person, plus their first and latest sentence dates.
    "people": """
        WITH latest AS (SELECT * FROM lake.silver.hero_offenders WHERE sid_number IS NOT NULL
                        QUALIFY row_number() OVER (PARTITION BY sid_number
                                                   ORDER BY sentence_date DESC, offender_number DESC) = 1)
        SELECT l.sid_number, l.last_name_std AS last_name, l.first_name_std AS first_name, l.alias_std AS alias,
               l.date_of_birth AS birth_date, l.gender, l.unit_name, d.first_sentence_date, d.latest_sentence_date
        FROM latest l JOIN lake.gold.dim_person d USING (sid_number) ORDER BY l.sid_number""",
    # Every power each person holds today.
    "powers": """
        SELECT DISTINCT sid_number, trim(upper(unnest(string_split(superpower, ';')))) AS power
        FROM lake.silver.hero_offenders WHERE sid_number IS NOT NULL ORDER BY ALL""",
    # Every STG affiliation, with the status on the person's latest record in that group.
    "memberships": """
        SELECT sid_number, stg_group, stg_status AS status FROM lake.silver.hero_offenders
        WHERE sid_number IS NOT NULL AND stg_group IS NOT NULL AND stg_group <> 'None'
        QUALIFY row_number() OVER (PARTITION BY sid_number, stg_group ORDER BY sentence_date DESC, offender_number DESC) = 1
        ORDER BY sid_number, stg_group""",
    # Today's offender records that a fight can lead to: those with a SID and a sentence date. Incident i is the
    # fight behind the i-th of them.
    "records": """
        SELECT offender_number, sid_number, sentence_date FROM lake.silver.hero_offenders
        WHERE sid_number IS NOT NULL AND sentence_date IS NOT NULL ORDER BY offender_number""",
    # The custody units.
    "units": """
        SELECT DISTINCT unit_code, unit_name FROM lake.silver.hero_offenders WHERE unit_code IS NOT NULL ORDER BY unit_code""",
}


def extract_core(con, out_dir: Path) -> dict:
    """Write today's people, powers, STG memberships, linkable offender records and custody units from the lake to <out_dir>/_core/ as Parquet,
    check their counts against the pinned ones, and return {name: path}."""
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    from . import scale
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
            raise RuntimeError(f"The lake's core changed: {name} has {got:,} rows; the world is built for {want:,}")
    return paths
