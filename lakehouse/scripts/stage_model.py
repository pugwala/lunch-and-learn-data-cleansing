"""Stage session 7's embedding model on the lake disk at a pinned revision, so the notebooks run offline.

    python lakehouse/scripts/stage_model.py           # download it if missing (run with HF_HUB_OFFLINE=0)
    python lakehouse/scripts/stage_model.py --check   # exit 0 if staged at the pinned revision, 1 if not

The model is the one lnl/config.py names (EMBEDDING_MODEL = "all-MiniLM-L6-v2", which sentence-transformers
resolves to sentence-transformers/all-MiniLM-L6-v2). It goes into Hugging Face's cache under HF_HOME
(/lake/models/huggingface in compose), so it survives container rebuilds. bootstrap.sh runs this.
"""
import os
import sys
from pathlib import Path

REPO = "sentence-transformers/all-MiniLM-L6-v2"
REVISION = "1110a243fdf4706b3f48f1d95db1a4f5529b4d41"   # 2026-06-01, Apache-2.0
# What SentenceTransformer loads: 88 MB. The whole repo (ONNX and OpenVINO copies too) is about 1 GB.
FILES = ["modules.json", "config_sentence_transformers.json", "sentence_bert_config.json", "config.json",
         "model.safetensors", "tokenizer.json", "tokenizer_config.json", "special_tokens_map.json", "vocab.txt",
         "1_Pooling/config.json", "README.md"]


def model_dir() -> Path:
    """This model's folder in Hugging Face's hub cache under HF_HOME."""
    home = Path(os.environ.get("HF_HOME", Path.home() / ".cache" / "huggingface"))
    return home / "hub" / f"models--{REPO.replace('/', '--')}"


def staged() -> bool:
    """True when every file of the pinned revision is present and the plain name ('main') resolves to it."""
    ref = model_dir() / "refs" / "main"
    snapshot = model_dir() / "snapshots" / REVISION
    return ref.exists() and ref.read_text().strip() == REVISION and all((snapshot / f).exists() for f in FILES)


def stage() -> None:
    """Download the pinned revision's files and point 'main' at it: offline lookups of the plain name use 'main'."""
    from huggingface_hub import snapshot_download
    snapshot_download(REPO, revision=REVISION, allow_patterns=FILES)
    ref = model_dir() / "refs" / "main"
    ref.parent.mkdir(parents=True, exist_ok=True)
    ref.write_text(REVISION)


def main(argv) -> int:
    """Report (--check) or stage the model; print one line saying which."""
    if staged():
        print(f"{REPO} staged at {REVISION[:7]}")
        return 0
    if "--check" in argv:
        print(f"{REPO} not staged at {REVISION[:7]}")
        return 1
    stage()
    print(f"staged {REPO} at {REVISION[:7]}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
