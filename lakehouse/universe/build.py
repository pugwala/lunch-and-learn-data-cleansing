"""Build the expanded world: python -m lakehouse.universe.build --scale M [--seed 2026] [--out DIR] [--workers N]

Extracts today's people from the lake (read-only), builds every table in dependency order with world.py's factories,
and writes the manifest. Fights come after people, and power uses after participants, because each reads
the tables before it. The default folder is /lake/landing/universe/<scale>. A build replaces the folder's tables;
manifest.verify() then checks every file.
"""
import argparse
import os
import shutil
import sys
import time
from functools import partial
from pathlib import Path

from . import core, fights, manifest, scale, world, writer

ORDER = ["location", "power", "team", "person", "person_power", "team_member",
         "incident", "participant", "power_use", "damage"]
FACTORIES = {"location": world.location, "power": world.power, "team": world.team, "person": world.person,
             "person_power": world.person_power, "team_member": world.team_member,
             "incident": fights.incident, "participant": fights.participant, "power_use": fights.power_use,
             "damage": fights.damage}
DEFAULT_ROOT = Path("/lake/landing/universe")


def context(out_dir: Path, scale_name: str, seed: int) -> dict:
    """What every factory is told: the scale, the seed, and where the core extract and every table live."""
    out_dir = Path(out_dir)
    return {"scale": scale_name, "seed": seed, **{n: str(out_dir / "_core" / f"{n}.parquet") for n in core.EXTRACT},
            **{f"{t}_dir": str(out_dir / t) for t in ORDER}}


def factory(name: str, ctx: dict):
    """A table's factory with the build's context bound, ready for writer.build_table."""
    return partial(FACTORIES[name], ctx=ctx)


def build(out_dir: Path, scale_name: str = "M", seed: int = core.CORE_SEED, workers: int = 1, tables=None,
          con=None, log=print, chunk_rows: int = writer.CHUNK_ROWS) -> dict:
    """Build the tables (all, or the named ones in dependency order) into out_dir and write its manifest."""
    out_dir = Path(out_dir)
    if con is None:
        sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
        from lakehouse import lake
        con = lake.lake_connect(read_only=True)
    core.extract_core(con, out_dir)
    ctx = context(out_dir, scale_name, seed)
    built = {}
    for name in [t for t in ORDER if tables is None or t in tables]:
        shutil.rmtree(out_dir / name, ignore_errors=True)
        started = time.time()
        built[name] = writer.build_table(name, scale.rows(name, scale_name), factory(name, ctx), out_dir,
                                         seed=seed, workers=workers, chunk_rows=chunk_rows)
        log(f"  {name:13} {built[name]['rows']:>11,} rows  {time.time() - started:6.1f}s")
    manifest.write(out_dir, seed=seed, scale_name=scale_name, tables=built)
    return built


def main(argv=None):
    """Command line: build one scale of the world."""
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--scale", default="M", choices=list(scale.FACTORS))
    ap.add_argument("--seed", type=int, default=core.CORE_SEED)
    ap.add_argument("--out", type=Path)
    ap.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 2) - 2))
    args = ap.parse_args(argv)
    out = args.out or DEFAULT_ROOT / args.scale
    print(f"Building the world at scale {args.scale} (seed {args.seed}) into {out}")
    started = time.time()
    build(out, args.scale, args.seed, args.workers)
    problems = manifest.verify(out)
    print(f"Done in {time.time() - started:.0f}s; manifest " + ("verified" if not problems else f"PROBLEMS: {problems}"))
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
