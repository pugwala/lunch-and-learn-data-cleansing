"""Governance and metadata tags, attached automatically when a source lands (Priority 4b, 2026-11-02).

    sources.yml   every ingested source: the tables it lands, its system, governance (CJIS) and refresh
    rules.yml     the tag vocabulary, and the column rules that find personal data by name or by its values
    tags.py       the tagger: column tags, table tags (source, lineage, layer, PII rollup), written to the lake
                  as ops.table_tags and ops.column_tags; om_sync.py publishes them to OpenMetadata

Hand-set Sensitivity tags (dbt meta.sensitivity) and the Cube masks they drive are untouched: these tags help people
find, sort and govern data, and a steward can confirm or correct them in OpenMetadata.
"""
