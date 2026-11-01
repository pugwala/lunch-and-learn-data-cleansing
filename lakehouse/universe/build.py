"""Build the expanded world: python -m lakehouse.universe.build --scale M [--seed 2026] [--out DIR] [--workers N]

Extracts today's people from the lake (read-only), builds every table in dependency order with world.py's factories,
and writes the manifest. The default folder is /lake/landing/universe/<scale>. A build replaces the folder's tables;
manifest.verify() then checks every file.
"""
import argparse
import os
import shutil
import sys
import time
from functools import partial
from pathlib import Path

from . import core, manifest, scale, world, writer

ORDER = ["location", "power", "team", "person", "person_power", "team_member"]
FACTORIES = {"location": world.location, "power": world.power, "team": world.team, "person": world.person,
             "person_power": world.person_power, "team_member": world.team_member}
DEFAULT_ROOT = Path("/lake/landing/universe")


def build(out_dir: Path, scale_name: str = "M", seed: int = core.CORE_SEED, workers: int = 1, tables=None,
          con=None, log=print) -> dict:
    """Build the tables (all, or the named ones in dependency order) into out_dir and write its manifest."""
    out_dir = Path(out_dir)
    if con is None:
        sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
        from lakehouse import lake
        con = lake.lake_connect(read_only=True)
    paths = core.extract_core(con, out_dir)
    ctx = {"scale": scale_name, **{k: str(v) for k, v in paths.items()},
           **{f"{t}_dir": str(out_dir / t) for t in ORDER}}
    built = {}
    for name in [t for t in ORDER if tables is None or t in tables]:
        shutil.rmtree(out_dir / name, ignore_errors=True)
        started = time.time()
        built[name] = writer.build_table(name, scale.rows(name, scale_name), partial(FACTORIES[name], ctx=ctx),
                                         out_dir, seed=seed, workers=workers)
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
