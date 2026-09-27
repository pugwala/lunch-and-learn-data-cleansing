"""One-time setup: point the README, config, and every notebook at your copy of the repo.

Usage:  python tools/set_repo.py OWNER/REPO
        python tools/set_repo.py https://github.com/OWNER/REPO
"""
import sys
from pathlib import Path

PLACEHOLDER = "YOUR-ORG/lunch-and-learn-data-cleansing"
ROOT = Path(__file__).resolve().parent.parent


def main() -> None:
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    new = sys.argv[1].strip().rstrip("/").removesuffix(".git")
    if "github.com/" in new:
        new = new.split("github.com/", 1)[1]
    if new.count("/") != 1:
        sys.exit("Expected OWNER/REPO, for example tdcj-itd/lunch-and-learn-data-cleansing")
    files = [ROOT / "README.md", ROOT / "lnl" / "config.py", *sorted((ROOT / "notebooks").glob("*.ipynb"))]
    changed = 0
    for path in files:
        text = path.read_text(encoding="utf-8")
        if PLACEHOLDER in text:
            path.write_text(text.replace(PLACEHOLDER, new), encoding="utf-8")
            changed += 1
            print(f"updated {path.relative_to(ROOT)}")
    print(f"Done: {changed} files now point at {new}." if changed else "Nothing to change (already set?).")


if __name__ == "__main__":
    main()
