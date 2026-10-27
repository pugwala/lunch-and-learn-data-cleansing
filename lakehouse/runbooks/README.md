# Runbooks

One page per alert in `lakehouse/monitoring/prometheus/rules/lakehouse.yml`: what it means, what to check, how to fix
it, when it clears. Each alert's notification on the phone ends with a link to its page.

Commands run on the VM from `~/lunch-and-learn-data-cleansing/lakehouse`. Start the stack only by naming services or
with `scripts/bootstrap.sh`, never a bare `docker compose up` (it also runs the loader). The web pages are on the
tailnet at `https://<LAKE_TS_HOST>/...`.

| Group | Alerts |
| --- | --- |
| Platform | [TargetDown](TargetDown.md) · [SiteDown](SiteDown.md) · [CertificateExpiresSoon](CertificateExpiresSoon.md) · [DiskFilling](DiskFilling.md) · [DiskAlmostFull](DiskAlmostFull.md) · [MemoryLow](MemoryLow.md) · [PostgresDown](PostgresDown.md) |
| Pipeline | [JobFailing](JobFailing.md) · [DailyLakeStale](DailyLakeStale.md) · [NotebookSmokeStale](NotebookSmokeStale.md) · [LakeHousekeepingStale](LakeHousekeepingStale.md) |
| Data | [BusinessRuleViolated](BusinessRuleViolated.md) · [SilverDroppedRows](SilverDroppedRows.md) · [FlaggedShareHigh](FlaggedShareHigh.md) · [DataQualityBelowFloor](DataQualityBelowFloor.md) · [MoreRulesBelowFloor](MoreRulesBelowFloor.md) |
| Backups | [CatalogBackupStale](CatalogBackupStale.md) · [CatalogBackupFailed](CatalogBackupFailed.md) · [OffsiteBackupStale](OffsiteBackupStale.md) · [OffsiteBackupFailed](OffsiteBackupFailed.md) · [RestoreDrillStale](RestoreDrillStale.md) · [RestoreDrillFailed](RestoreDrillFailed.md) |
| Data products | [DataProductStale](DataProductStale.md) · [DataProductCoverageLow](DataProductCoverageLow.md) · [BusinessInputLate](BusinessInputLate.md) |
| Alerting itself | [AlertDeliveryFailing](AlertDeliveryFailing.md) · [Watchdog](Watchdog.md) |

Adding an alert: add its rule with a `runbook_url` annotation, and a page here with the same four sections;
`lakehouse/acceptance/test_runbooks.py` fails until both exist.
