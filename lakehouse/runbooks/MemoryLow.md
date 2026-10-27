# MemoryLow

**Severity:** warning · **Fires when:** under 10% of the VM's memory has been available for 10 minutes · **Comes from:** node-exporter

## What it means

The VM is short of memory. Services may slow down, and the kernel may kill one (often the biggest: OpenSearch or
OpenMetadata).

## What to check

1. Who's using it: `docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}' | sort -k2 -h`.
2. Did something get killed? `sudo dmesg | grep -i -E 'killed process|out of memory' | tail`.

## How to fix it

- Restart the service that has grown: `docker compose --profile core restart <service>`.
- If it's simply too little for everything, give the VM more memory in Proxmox.

## When it clears

When available memory is back over 10%.
