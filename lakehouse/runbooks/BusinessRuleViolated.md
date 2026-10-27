# BusinessRuleViolated

**Severity:** warning · **Fires when:** something in the catalog breaks a SHACL business rule · **Comes from:** the `business_rules_hold` check on `fuseki/glossary`, in the daily run

## What it means

A business rule in lakehouse/ontology/shapes.ttl doesn't hold for something in the published graphs (the `shape`
label says which rule): a glossary term without a real definition, a business rule with no enforcer, a data product
that names no inputs, an input with no owner or deadline.

## What to check

1. Which things, and why:
   `docker compose --profile core exec -T jupyter python lakehouse/ontology/check_rules.py`.
2. Dagster, the latest `daily_lake`, the `business_rules_hold` check, lists the same.

## How to fix it

- Fix the source file, not Fuseki (it's rebuilt every run): terms and the rulebook in lakehouse/glossary/glossary.yml,
  products in `lakehouse/products/`, inputs in `lakehouse/products/inputs/`.
- Then `bash scripts/bootstrap.sh` (or wait for 05:00) to republish and recheck.

## When it clears

After the next daily run (or bootstrap) finds every rule holding.
