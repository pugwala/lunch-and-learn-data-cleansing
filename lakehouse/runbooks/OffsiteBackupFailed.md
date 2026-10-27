# OffsiteBackupFailed

**Severity:** critical · **Fires when:** the latest off-machine backup failed · **Comes from:** offsite-backup (lakehouse/backup/offsite.sh), through node-exporter

## What it means

A step of the nightly backup failed: no marker on the target (the share didn't mount), `restic init`, `backup`,
`forget` (retention) or `check` (the repository's integrity).

## What to check

`docker compose --profile core logs --tail 100 offsite-backup` names the step (`offsite-backup: FAILED: ...`).

## How to fix it

- No marker: the TrueNAS share isn't mounted, or the folder was never prepared. Mount it (`sudo mount -a`), or run
  `bash scripts/bootstrap.sh`, which prepares a local folder when there is no share.
- `restic check` failed: the repository is damaged. Don't prune or delete anything; keep the logs and ask before
  going further (`restic repair` exists, but repairs drop data).
- A wrong password: RESTIC_PASSWORD in `.env` must be the one the repository was made with. Keep a copy of it off
  this VM, or a backup can't be read after losing the VM.
- Then `docker compose --profile core restart offsite-backup` to try again.

## When it clears

When the next backup succeeds.
